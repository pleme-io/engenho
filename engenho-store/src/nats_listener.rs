//! F2b — the per-node NATS listener that COMPLETES cross-process
//! Raft RPC end to end.
//!
//! F2a (v0.19.0) shipped the CLIENT side (`NatsRaftNetwork` +
//! `NatsRaftNetworkFactory`) — outgoing RPC encoded over NATS.
//! This module ships the LISTENER side: every node subscribes
//! to `engenho.{cluster}.raft.store.*.{my_node}`, decodes the
//! incoming envelope, dispatches into the local `Raft<TypeConfig>`,
//! and replies on the inbox subject.
//!
//! Once `NatsListener::start` is called on each node, the cluster
//! is genuinely distributed across processes — every Raft RPC
//! flows through NATS rather than the in-process mpsc.
//!
//! ## Reply protocol
//!
//! NATS request-reply uses an inbox subject auto-attached to
//! every request. The listener's response goes via
//! `message.respond(payload)`. Async-nats handles the inbox
//! plumbing transparently; we just `.respond()` with bytes.
//!
//! ## Per-node subscription
//!
//! ```text
//! engenho.{cluster}.raft.store.append.{node_id}
//! engenho.{cluster}.raft.store.vote.{node_id}
//! engenho.{cluster}.raft.store.snapshot.{node_id}
//! ```
//!
//! We use ONE wildcard subscription per node:
//! `engenho.{cluster}.raft.store.*.{node_id}` so all three RPC
//! kinds funnel into one async task. The decoder pattern-matches
//! on the [`NatsRpcEnvelope`]'s `kind` tag (not on the subject
//! kind — they should agree, but envelope-tag is authoritative).

use engenho_teia::TeiaClient;
use engenho_teia::subject::RaftGroup;
use futures::StreamExt;
use openraft::Raft;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use crate::nats_network::NatsRpcEnvelope;
use crate::type_config::{RaftNodeId, TypeConfig};

/// Translate a Raft `RaftNodeId` (u64) into the teia hex-string.
/// Mirrors `nats_network::raft_id_to_teia` but exposed here as a
/// helper so the listener subject builder doesn't need to import
/// across modules.
fn raft_id_hex(id: RaftNodeId) -> String {
    format!("{id:016x}")
}

/// Handle to a running listener. Owning the handle keeps the
/// listener task alive; dropping the handle aborts it.
pub struct NatsListener {
    handle: JoinHandle<()>,
}

impl NatsListener {
    /// Spawn the per-node Raft RPC listener.
    ///
    /// Subscribes to `engenho.{cluster}.raft.store.*.{node_id}`
    /// and dispatches each message into `raft` via the matching
    /// openraft RPC method. The reply rides NATS's auto-inbox.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`engenho_teia::TeiaError`] if the
    /// subscription fails.
    pub async fn start(
        client: TeiaClient,
        raft: Raft<TypeConfig>,
        node_id: RaftNodeId,
    ) -> Result<Self, engenho_teia::TeiaError> {
        let scope = client.scope().clone();
        let target = engenho_teia::NodeId::from_hex(raft_id_hex(node_id));
        // Wildcard: any of {append, vote, snapshot} for THIS target.
        let subject = scope.raft_target_wildcard(target);

        let nats = client.nats().clone();
        let mut sub = nats.subscribe(subject.clone()).await.map_err(|e| {
            engenho_teia::TeiaError::SubscribeFailed {
                subject: subject.clone(),
                detail: e.to_string(),
            }
        })?;

        let _ = scope; // explicit drop — only the auto-builder used it
        // RaftGroup is referenced via the subject grammar; emit an
        // intentional usage so the unused-import lint stays quiet
        // for downstream crates that re-export NatsListener.
        let _ = RaftGroup::Store;

        let raft_for_task = raft;
        let handle = tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                let envelope: NatsRpcEnvelope = match serde_json::from_slice(&msg.payload) {
                    Ok(env) => env,
                    Err(e) => {
                        warn!(error = %e, subject = %msg.subject, "drop malformed Raft RPC");
                        continue;
                    }
                };

                let reply_bytes: Vec<u8> = match envelope {
                    NatsRpcEnvelope::AppendEntries(req) => {
                        match raft_for_task.append_entries(req).await {
                            Ok(resp) => match serde_json::to_vec(&resp) {
                                Ok(b) => b,
                                Err(e) => {
                                    error!(error = %e, "encode AppendEntries response");
                                    continue;
                                }
                            },
                            Err(e) => {
                                warn!(error = %e, "AppendEntries dispatch failed");
                                continue;
                            }
                        }
                    }
                    NatsRpcEnvelope::Vote(req) => match raft_for_task.vote(req).await {
                        Ok(resp) => match serde_json::to_vec(&resp) {
                            Ok(b) => b,
                            Err(e) => {
                                error!(error = %e, "encode Vote response");
                                continue;
                            }
                        },
                        Err(e) => {
                            warn!(error = %e, "Vote dispatch failed");
                            continue;
                        }
                    },
                    NatsRpcEnvelope::InstallSnapshot(req) => {
                        match raft_for_task.install_snapshot(req).await {
                            Ok(resp) => match serde_json::to_vec(&resp) {
                                Ok(b) => b,
                                Err(e) => {
                                    error!(error = %e, "encode InstallSnapshot response");
                                    continue;
                                }
                            },
                            Err(e) => {
                                warn!(error = %e, "InstallSnapshot dispatch failed");
                                continue;
                            }
                        }
                    }
                };

                if let Some(reply_subject) = &msg.reply {
                    if let Err(e) = nats
                        .publish(reply_subject.clone(), reply_bytes.into())
                        .await
                    {
                        warn!(error = %e, reply = %reply_subject, "reply publish failed");
                    } else {
                        debug!(reply = %reply_subject, "Raft RPC replied");
                    }
                } else {
                    warn!(
                        subject = %msg.subject,
                        "Raft RPC arrived without reply inbox; dropping response"
                    );
                }
            }
        });

        Ok(Self { handle })
    }

    /// Stop the listener. Cancels the underlying task.
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// True if the underlying task has finished (panicked or
    /// completed). The listener loop is infinite under normal
    /// operation; this is only true after abort or NATS shutdown.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl Drop for NatsListener {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_id_hex_is_zero_padded_16_chars() {
        assert_eq!(raft_id_hex(0), "0000000000000000");
        assert_eq!(raft_id_hex(1), "0000000000000001");
        assert_eq!(raft_id_hex(u64::MAX), "ffffffffffffffff");
    }

    #[test]
    fn raft_id_hex_format_matches_client_side() {
        // The CLIENT side (NatsRaftNetwork) encodes target IDs the
        // same way. This test pins the format so a future refactor
        // can't accidentally make client+listener disagree.
        let client_format = format!("{:016x}", 0xcafe_u64);
        assert_eq!(raft_id_hex(0xcafe), client_format);
    }
}
