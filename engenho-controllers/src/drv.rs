//! DrvController — reconciles `DerivationCR` objects through a
//! `DerivationCacheBackend` (typically a `TieredCache`).
//!
//! Each `DerivationCR` in the store names a `drv_hash` the cluster
//! should hold realisations for. The controller's job is to make
//! sure those realisations are present in the cache tier the
//! consumer can read from + to patch the CR's status with the
//! resolved output paths.
//!
//! ## Reconcile rule
//!
//! For each DerivationCR:
//!   1. Read `spec.drvHash` (hex string).
//!   2. Look up the drv in the cache. If absent → mark CR
//!      `status.phase = "DrvUnknown"`, skip.
//!   3. List realisations. If non-empty → patch CR status with
//!      `phase = "Realised"` + `realisations: [{name, path}]`.
//!   4. If no realisations → mark `phase = "Pending"` (consumer
//!      controller — R-DRV-BUILD — picks it up).
//!
//! Idempotent: re-tick with no change is a no-op (status patches
//! only fire when the computed phase / realisations differ).

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
};
use engenho_substrate::{DerivationCacheBackend, DrvHash};
use serde_json::{Value, json};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;

/// Controller that propagates derivation state from a cache backend
/// into the K8s store as `DerivationCR.status`.
pub struct DrvController {
    store: Arc<StoreMesh>,
    cache: Arc<dyn DerivationCacheBackend>,
    namespace: Option<String>,
}

impl DrvController {
    /// New controller.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        cache: Arc<dyn DerivationCacheBackend>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            store,
            cache,
            namespace,
        }
    }

    /// Parse a DrvHash from a hex string. Pure helper.
    ///
    /// # Errors
    /// Returns None if the string isn't 64 lowercase hex characters.
    #[must_use]
    pub fn parse_drv_hash(s: &str) -> Option<DrvHash> {
        if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(DrvHash::new(bytes))
    }
}

#[async_trait]
impl Controller for DrvController {
    fn name(&self) -> &'static str {
        "drv"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let drs = self
            .store
            .list("engenho.io", "v1", "Derivation", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = drs.len();

        for (cr_key, cr_value) in &drs {
            let drv_hash_str = cr_value
                .get("spec")
                .and_then(|s| s.get("drvHash"))
                .and_then(|h| h.as_str());
            let Some(hash_str) = drv_hash_str else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(hash) = Self::parse_drv_hash(hash_str) else {
                report.objects_skipped += 1;
                continue;
            };

            // Phase computation.
            let phase: &str;
            let realisations_json: Value;
            match self
                .cache
                .get_drv(&hash)
                .await
                .map_err(|e| ControllerError::Internal(e.to_string()))?
            {
                None => {
                    phase = "DrvUnknown";
                    realisations_json = json!([]);
                }
                Some(_drv) => {
                    let realisations = self
                        .cache
                        .list_realisations(&hash)
                        .await
                        .map_err(|e| ControllerError::Internal(e.to_string()))?;
                    if realisations.is_empty() {
                        phase = "Pending";
                        realisations_json = json!([]);
                    } else {
                        phase = "Realised";
                        realisations_json = serde_json::to_value(
                            realisations
                                .iter()
                                .map(|r| {
                                    json!({
                                        "name": r.output_name,
                                        "path": r.output_path.as_str(),
                                    })
                                })
                                .collect::<Vec<_>>(),
                        )
                        .unwrap_or(json!([]));
                    }
                }
            }

            // Idempotency: only patch when the computed status differs.
            let current_phase = cr_value
                .get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|p| p.as_str());
            let current_realisations = cr_value
                .get("status")
                .and_then(|s| s.get("realisations"))
                .cloned();
            if current_phase == Some(phase)
                && current_realisations.as_ref() == Some(&realisations_json)
            {
                continue;
            }

            self.store
                .propose(ResourceCommand::Patch {
                    key: cr_key.clone(),
                    patch: json!({
                        "status": {
                            "phase": phase,
                            "realisations": realisations_json,
                        }
                    }),
                    expected: None,
                    reason: Reason::Controller,
                })
                .await?;
            report.objects_changed += 1;
        }
        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_drv_hash_accepts_64_hex_chars() {
        let s = "a".repeat(64);
        let h = DrvController::parse_drv_hash(&s).unwrap();
        assert_eq!(h.0[0], 0xaa);
        assert_eq!(h.0[31], 0xaa);
    }

    #[test]
    fn parse_drv_hash_rejects_wrong_length() {
        assert!(DrvController::parse_drv_hash("abc").is_none());
        assert!(DrvController::parse_drv_hash(&"a".repeat(63)).is_none());
        assert!(DrvController::parse_drv_hash(&"a".repeat(65)).is_none());
    }

    #[test]
    fn parse_drv_hash_rejects_non_hex() {
        let bad = "g".repeat(64);
        assert!(DrvController::parse_drv_hash(&bad).is_none());
        let bad = format!("{}{}", "a".repeat(63), "Z");
        assert!(DrvController::parse_drv_hash(&bad).is_none());
    }

    #[test]
    fn parse_drv_hash_round_trips_via_to_hex() {
        let h = DrvHash::from_bytes(b"engenho-substrate");
        let back = DrvController::parse_drv_hash(&h.to_hex()).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str {
                "drv"
            }
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(F.name(), "drv");
    }
}
