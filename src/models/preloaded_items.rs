use chrono::{DateTime, Utc};
use enclose::enclose;
use futures::FutureExt;
use http::Request;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

use crate::runtime::msg::{Action, ActionPlayer, Internal, Msg};
use crate::runtime::{Effect, EffectFuture, Effects, Env, EnvFutureExt, UpdateWithCtx};
use crate::constants::PRELOADED_ITEMS_STORAGE_KEY;

use crate::models::ctx::Ctx;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum PreloadStatus {
    Pending,
    InProgress { progress: f64 },
    Ready,
    Failed { reason: String },
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PreloadEntry {
    pub info_hash: String,
    pub file_idx: u64,
    pub imdb_id: String,
    pub title: String,
    pub status: PreloadStatus,
    pub added_at: DateTime<Utc>,
    /// Current download speed in bytes/sec (0.0 when not downloading).
    #[serde(default)]
    pub speed_bps: f64,
}

/// Holds all active and recently-completed preload entries.
#[derive(Clone, PartialEq, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PreloadedItems {
    /// Keyed by lower-cased info hash.
    pub items: HashMap<String, PreloadEntry>,
}

impl PreloadedItems {
    /// Apply restart-recovery: any entries that were Pending/InProgress when
    /// the app was closed are transitions to Failed, because the polling loop
    /// is no longer running and we cannot resume mid-download.
    ///
    /// Ready entries are kept as-is — the file is still on disk.
    pub fn apply_restart_recovery(&mut self) {
        for entry in self.items.values_mut() {
            match &entry.status {
                PreloadStatus::Pending | PreloadStatus::InProgress { .. } => {
                    entry.status = PreloadStatus::Failed {
                        reason: "App was restarted during download. Re-trigger preload to resume."
                            .to_string(),
                    };
                }
                _ => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Response type from the stream-server GET endpoint
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PreloadProgressResponse {
    // progress: 0.0 – 1.0
    progress: f64,
    state: ServerPreloadState,
    /// Download speed in bytes/sec; present only when state is Downloading.
    #[serde(default)]
    speed_bps: f64,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "status", rename_all = "camelCase")]
enum ServerPreloadState {
    Pending,
    Downloading,
    Ready,
    Failed { reason: String },
}

// ---------------------------------------------------------------------------
// UpdateWithCtx
// ---------------------------------------------------------------------------

impl<E: Env + 'static> UpdateWithCtx<E> for PreloadedItems {
    fn update(&mut self, msg: &Msg, ctx: &Ctx) -> Effects {
        match msg {
            // ---- start preload ---------------------------------------------------
            Msg::Action(Action::Player(ActionPlayer::Preload {
                info_hash,
                file_idx,
                imdb_id,
                title,
            })) => {
                let info_hash = info_hash.to_lowercase();

                // Idempotent: if already tracking, skip.
                if let Some(entry) = self.items.get(&info_hash) {
                    match &entry.status {
                        PreloadStatus::Pending | PreloadStatus::InProgress { .. } => {
                            return Effects::none().unchanged();
                        }
                        _ => {}
                    }
                }

                self.items.insert(
                    info_hash.clone(),
                    PreloadEntry {
                        info_hash: info_hash.clone(),
                        file_idx: *file_idx,
                        imdb_id: imdb_id.clone(),
                        title: title.clone(),
                        status: PreloadStatus::Pending,
                        added_at: E::now(),
                        speed_bps: 0.0,
                    },
                );

                let base = ctx.profile.settings.streaming_server_url.clone();
                Effects::one(start_preload_effect::<E>(base, info_hash, *file_idx))
                    .join(Effects::one(save_to_storage_effect::<E>(self)))
            }

            // ---- poll status -----------------------------------------------------
            Msg::Action(Action::Player(ActionPlayer::PollPreload { info_hash })) => {
                let info_hash = info_hash.to_lowercase();

                // Only poll if we're actually tracking it.
                match self.items.get(&info_hash).map(|e| &e.status) {
                    Some(PreloadStatus::Pending | PreloadStatus::InProgress { .. }) => {
                        let base = ctx.profile.settings.streaming_server_url.clone();
                        Effects::one(poll_preload_effect::<E>(base, info_hash))
                            .unchanged()
                    }
                    _ => Effects::none().unchanged(),
                }
            }

            // ---- progress update from server -------------------------------------
            Msg::Internal(Internal::PreloadProgress { info_hash, progress, speed_bps }) => {
                if let Some(entry) = self.items.get_mut(info_hash) {
                    let new_status = if *progress >= 1.0 {
                        PreloadStatus::Ready
                    } else {
                        PreloadStatus::InProgress { progress: *progress }
                    };
                    let speed_changed = (entry.speed_bps - speed_bps).abs() > 1.0;
                    let status_changed = entry.status != new_status;
                    if status_changed || speed_changed {
                        entry.status = new_status;
                        entry.speed_bps = *speed_bps;
                        Effects::one(save_to_storage_effect::<E>(self))
                    } else {
                        Effects::none().unchanged()
                    }
                } else {
                    Effects::none().unchanged()
                }
            }

            // ---- failure update from server --------------------------------------
            Msg::Internal(Internal::PreloadFailed { info_hash, reason }) => {
                if let Some(entry) = self.items.get_mut(info_hash) {
                    let new_status = PreloadStatus::Failed { reason: reason.clone() };
                    if entry.status != new_status {
                        entry.status = new_status;
                        Effects::one(save_to_storage_effect::<E>(self))
                    } else {
                        Effects::none().unchanged()
                    }
                } else {
                    Effects::none().unchanged()
                }
            }

            // ---- cancel preload --------------------------------------------------
            Msg::Action(Action::Player(ActionPlayer::CancelPreload { info_hash })) => {
                let info_hash = info_hash.to_lowercase();
                if self.items.remove(&info_hash).is_some() {
                    let base = ctx.profile.settings.streaming_server_url.clone();
                    Effects::one(cancel_preload_effect::<E>(base, info_hash))
                        .join(Effects::one(save_to_storage_effect::<E>(self)))
                        .unchanged()
                } else {
                    Effects::none().unchanged()
                }
            }

            // ---- hard-delete preload (abort + delete files from disk) ------------
            Msg::Action(Action::Player(ActionPlayer::DeletePreload { info_hash })) => {
                let info_hash = info_hash.to_lowercase();
                if self.items.remove(&info_hash).is_some() {
                    let base = ctx.profile.settings.streaming_server_url.clone();
                    Effects::one(delete_preload_effect::<E>(base, info_hash))
                        .join(Effects::one(save_to_storage_effect::<E>(self)))
                        .unchanged()
                } else {
                    Effects::none().unchanged()
                }
            }

            _ => Effects::none().unchanged(),
        }
    }
}

// ---------------------------------------------------------------------------
// Effect helpers
// ---------------------------------------------------------------------------

/// POST /{infoHash}/{fileIdx}/preload — start the download.
fn start_preload_effect<E: Env + 'static>(
    base_url: Url,
    info_hash: String,
    file_idx: u64,
) -> Effect {
    let endpoint = base_url
        .join(&format!("{}/{}/preload", info_hash, file_idx))
        .expect("preload start URL builder failed");

    let request = Request::post(endpoint.as_str())
        .body(())
        .expect("preload start request builder failed");

    EffectFuture::Concurrent(
        E::fetch::<(), serde_json::Value>(request)
            .map(enclose!((info_hash) move |result| match result {
                // POST succeeded: the server has queued the download.  The model
                // already shows Pending, so no further update is needed here.
                Ok(_) => Msg::Internal(Internal::Noop),
                Err(err) => Msg::Internal(Internal::PreloadFailed {
                    info_hash,
                    reason: err.message(),
                }),
            }))
            .boxed_env(),
    )
    .into()
}

/// GET /{infoHash}/{fileIdx}/preload — poll the current progress.
fn poll_preload_effect<E: Env + 'static>(base_url: Url, info_hash: String) -> Effect {
    // The file_idx is not used for the GET endpoint (any value works); we use 0.
    let endpoint = base_url
        .join(&format!("{}/0/preload", info_hash))
        .expect("preload poll URL builder failed");

    let request = Request::get(endpoint.as_str())
        .body(())
        .expect("preload poll request builder failed");

    EffectFuture::Concurrent(
        E::fetch::<(), PreloadProgressResponse>(request)
            .map(enclose!((info_hash) move |result| match result {
                Ok(resp) => match resp.state {
                    ServerPreloadState::Failed { reason } => {
                        Msg::Internal(Internal::PreloadFailed { info_hash, reason })
                    }
                    _ => Msg::Internal(Internal::PreloadProgress {
                        info_hash,
                        progress: resp.progress,
                        speed_bps: resp.speed_bps,
                    }),
                },
                Err(err) => Msg::Internal(Internal::PreloadFailed {
                    info_hash,
                    reason: err.message(),
                }),
            }))
            .boxed_env(),
    )
    .into()
}

/// DELETE /{infoHash}/{fileIdx}/preload — cancel (stream-server keeps files on disk).
fn cancel_preload_effect<E: Env + 'static>(base_url: Url, info_hash: String) -> Effect {
    let endpoint = base_url
        .join(&format!("{}/0/preload", info_hash))
        .expect("preload cancel URL builder failed");

    let request = Request::delete(endpoint.as_str())
        .body(())
        .expect("preload cancel request builder failed");

    EffectFuture::Concurrent(
        E::fetch::<(), serde_json::Value>(request)
            .map(move |_| Msg::Internal(Internal::Noop))
            .boxed_env(),
    )
    .into()
}

/// DELETE /{infoHash}/0/preload?delete=true — abort + delete files from disk.
fn delete_preload_effect<E: Env + 'static>(base_url: Url, info_hash: String) -> Effect {
    let endpoint = base_url
        .join(&format!("{}/0/preload?delete=true", info_hash))
        .expect("preload delete URL builder failed");

    let request = Request::delete(endpoint.as_str())
        .body(())
        .expect("preload delete request builder failed");

    EffectFuture::Concurrent(
        E::fetch::<(), serde_json::Value>(request)
            .map(move |_| Msg::Internal(Internal::Noop))
            .boxed_env(),
    )
    .into()
}

/// Persist the full `PreloadedItems` map to the key-value storage.
///
/// Errors are silently swallowed (storage is best-effort; the in-memory
/// state is always authoritative during the current session).
fn save_to_storage_effect<E: Env + 'static>(items: &PreloadedItems) -> Effect {
    EffectFuture::Sequential(
        E::set_storage(PRELOADED_ITEMS_STORAGE_KEY, Some(items))
            .map(|_| Msg::Internal(Internal::Noop))
            .boxed_env(),
    )
    .into()
}
