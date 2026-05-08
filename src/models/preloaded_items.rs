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

/// Maximum number of torrents the model will actively download at once.
/// Queue additional requests behind those that are already in flight.
pub const MAX_CONCURRENT_PRELOADS: usize = 1;

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum PreloadStatus {
    /// Waiting in queue — POST not sent yet; a previous download is in progress.
    Queued,
    /// POST sent to the server; server has acknowledged and is starting the download.
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
    /// Number of entries that have been started (POST sent) and are not yet done.
    fn active_count(&self) -> usize {
        self.items
            .values()
            .filter(|e| {
                matches!(
                    e.status,
                    PreloadStatus::Pending | PreloadStatus::InProgress { .. }
                )
            })
            .count()
    }

    /// Find the oldest Queued entry (by `added_at`), start it, and return
    /// the resulting effect (or `Effects::none()` if the queue is empty).
    fn advance_queue<E: Env + 'static>(
        &mut self,
        base_url: &Url,
    ) -> Effects {
        // Find the oldest entry with Queued status.
        let next = self
            .items
            .values_mut()
            .filter(|e| e.status == PreloadStatus::Queued)
            .min_by_key(|e| e.added_at);

        if let Some(entry) = next {
            entry.status = PreloadStatus::Pending;
            let info_hash = entry.info_hash.clone();
            let file_idx  = entry.file_idx;
            Effects::one(start_preload_effect::<E>(base_url.clone(), info_hash, file_idx))
        } else {
            Effects::none().unchanged()
        }
    }

    /// Apply restart-recovery: any entries that were Pending/InProgress or Queued
    /// when the app was closed are cleared (server session is gone).
    ///
    /// Ready entries are kept as-is — the file is still on disk.
    pub fn apply_restart_recovery(&mut self) {
        self.items.retain(|_, entry| {
            !matches!(
                entry.status,
                PreloadStatus::Queued
                    | PreloadStatus::Pending
                    | PreloadStatus::InProgress { .. }
            )
        });
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

                // Idempotent: if already actively tracking, skip.
                if let Some(entry) = self.items.get(&info_hash) {
                    match &entry.status {
                        PreloadStatus::Queued
                        | PreloadStatus::Pending
                        | PreloadStatus::InProgress { .. } => {
                            return Effects::none().unchanged();
                        }
                        _ => {}
                    }
                }

                // Decide whether to start immediately or queue behind active downloads.
                let active = self.active_count();
                let initial_status = if active < MAX_CONCURRENT_PRELOADS {
                    PreloadStatus::Pending
                } else {
                    PreloadStatus::Queued
                };
                let start_now = initial_status == PreloadStatus::Pending;

                self.items.insert(
                    info_hash.clone(),
                    PreloadEntry {
                        info_hash: info_hash.clone(),
                        file_idx: *file_idx,
                        imdb_id: imdb_id.clone(),
                        title: title.clone(),
                        status: initial_status,
                        added_at: E::now(),
                        speed_bps: 0.0,
                    },
                );

                let base = ctx.profile.settings.streaming_server_url.clone();
                let start_effects = if start_now {
                    Effects::one(start_preload_effect::<E>(base, info_hash, *file_idx))
                } else {
                    Effects::none()
                };
                start_effects.join(Effects::one(save_to_storage_effect::<E>(self)))
            }

            // ---- poll status -----------------------------------------------------
            Msg::Action(Action::Player(ActionPlayer::PollPreload { info_hash })) => {
                let info_hash = info_hash.to_lowercase();

                // Only poll if we're actively downloading (not queued).
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
                        entry.status = new_status.clone();
                        entry.speed_bps = *speed_bps;

                        // If this entry just became Ready, advance the queue.
                        let queue_effects = if new_status == PreloadStatus::Ready {
                            let base = ctx.profile.settings.streaming_server_url.clone();
                            self.advance_queue::<E>(&base)
                        } else {
                            Effects::none().unchanged()
                        };

                        queue_effects
                            .join(Effects::one(save_to_storage_effect::<E>(self)))
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

                        // Failed entry frees a slot — advance the queue.
                        let base = ctx.profile.settings.streaming_server_url.clone();
                        let queue_effects = self.advance_queue::<E>(&base);

                        queue_effects
                            .join(Effects::one(save_to_storage_effect::<E>(self)))
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
                if let Some(removed) = self.items.remove(&info_hash) {
                    let base = ctx.profile.settings.streaming_server_url.clone();
                    // Only send DELETE to server if the download had actually started.
                    let cancel_effects = match removed.status {
                        PreloadStatus::Queued => Effects::none(),
                        _ => Effects::one(cancel_preload_effect::<E>(base.clone(), info_hash)),
                    };
                    // A slot may now be free — advance the queue.
                    let queue_effects = self.advance_queue::<E>(&base);
                    cancel_effects
                        .join(queue_effects)
                        .join(Effects::one(save_to_storage_effect::<E>(self)))
                        .unchanged()
                } else {
                    Effects::none().unchanged()
                }
            }

            // ---- hard-delete preload (abort + delete files from disk) ------------
            Msg::Action(Action::Player(ActionPlayer::DeletePreload { info_hash })) => {
                let info_hash = info_hash.to_lowercase();
                if let Some(removed) = self.items.remove(&info_hash) {
                    let base = ctx.profile.settings.streaming_server_url.clone();
                    let delete_effects = match removed.status {
                        PreloadStatus::Queued => Effects::none(),
                        _ => Effects::one(delete_preload_effect::<E>(base.clone(), info_hash)),
                    };
                    let queue_effects = self.advance_queue::<E>(&base);
                    delete_effects
                        .join(queue_effects)
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
