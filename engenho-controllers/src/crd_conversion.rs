//! CRD CONVERSION — reading a stored custom resource at a version other
//! than the one it was written in.
//!
//! ★ THE BUG THIS CLOSES IS SILENT AND CORRUPTING. engenho serves every
//! `served: true` version of a CRD, and every one of them reads and writes
//! the SAME stored object at its own GVK — `storage: true` was recorded and
//! not load-bearing. So a CR written as `v1beta1` is handed to a `v1`
//! client verbatim, with `apiVersion` still claiming `v1beta1` and fields
//! laid out the way the older schema expected. The client does not error:
//! it reads the fields it recognises, silently misses the ones that moved,
//! and writes back a shape the other version cannot parse.
//!
//! ★ THERE IS EXACTLY ONE STORAGE VERSION, AND IT IS AN INVARIANT, NOT A
//! CONVENTION. A CRD declaring two is not "ambiguous" — it is invalid, and
//! accepting it means the same object is written to two schemas and neither
//! reader can trust what it gets. [`storage_version`] refuses rather than
//! picking, because picking is how the corruption starts.
//!
//! ★ `None` IS A LEGAL STRATEGY AND IT IS NOT "NO CONVERSION". Upstream's
//! `strategy: None` means "the versions are structurally identical, just
//! relabel `apiVersion`". That is a real, correct conversion for the common
//! case of a version bump with no schema change — and it is why a relabel
//! must actually happen rather than the object being passed through
//! untouched with a stale `apiVersion` a client will believe.
//!
//! ★ A WEBHOOK STRATEGY WITHOUT A REACHABLE WEBHOOK FAILS THE READ. It does
//! NOT fall back to a relabel. Falling back would hand the client an object
//! in the wrong schema while reporting success, which is precisely the
//! silent corruption above — only now with a configuration that claims to
//! prevent it.

use serde_json::Value;
use thiserror::Error;

/// How a CRD says its versions convert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Strategy {
    /// Versions are structurally identical; only `apiVersion` changes.
    None,
    /// An external webhook performs the conversion.
    Webhook { url: String },
}

/// Why a conversion could not be performed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConversionError {
    /// The CRD declares no storage version, or more than one.
    #[error("CRD must declare exactly one storage version, found {found}")]
    StorageVersionAmbiguous { found: usize },
    /// The requested version is not served by this CRD.
    #[error("version {version:?} is not served by this CRD")]
    VersionNotServed { version: String },
    /// A webhook strategy is declared but the webhook could not be used.
    ///
    /// Deliberately NOT a fallback to a relabel — see the module note.
    #[error("conversion webhook is required for this CRD but is unavailable: {reason}")]
    WebhookUnavailable { reason: String },
    /// The stored object does not carry an `apiVersion`.
    #[error("stored object has no apiVersion to convert from")]
    MissingApiVersion,
}

/// The single storage version a CRD declares.
///
/// Refuses on zero or many rather than picking one: a CRD with two storage
/// versions writes the same object to two schemas, and neither reader can
/// trust what it gets. Picking is how that corruption starts.
pub fn storage_version(crd: &Value) -> Result<String, ConversionError> {
    let versions = crd
        .get("spec")
        .and_then(|s| s.get("versions"))
        .and_then(Value::as_array)
        .map_or_else(Vec::new, |vs| {
            vs.iter()
                .filter(|v| v.get("storage").and_then(Value::as_bool) == Some(true))
                .filter_map(|v| v.get("name").and_then(Value::as_str))
                .collect()
        });
    match versions.as_slice() {
        [one] => Ok((*one).to_string()),
        other => Err(ConversionError::StorageVersionAmbiguous { found: other.len() }),
    }
}

/// Read a CRD's conversion strategy.
///
/// Absent `spec.conversion` means `None` — upstream's default, and the
/// right one: a CRD with a single version needs no conversion machinery.
#[must_use]
pub fn strategy_of(crd: &Value) -> Strategy {
    let conv = crd.get("spec").and_then(|s| s.get("conversion"));
    let named = conv
        .and_then(|c| c.get("strategy"))
        .and_then(Value::as_str)
        .unwrap_or("None");
    if named != "Webhook" {
        return Strategy::None;
    }
    let url = conv
        .and_then(|c| c.get("webhook"))
        .and_then(|w| w.get("clientConfig"))
        .and_then(|cc| cc.get("url"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Strategy::Webhook { url }
}

/// Is `version` served by this CRD?
#[must_use]
pub fn is_served(crd: &Value, version: &str) -> bool {
    crd.get("spec")
        .and_then(|s| s.get("versions"))
        .and_then(Value::as_array)
        .is_some_and(|vs| {
            vs.iter().any(|v| {
                v.get("name").and_then(Value::as_str) == Some(version)
                    && v.get("served").and_then(Value::as_bool) != Some(false)
            })
        })
}

/// Convert a stored object to `target_version` under the `None` strategy.
///
/// The relabel is the WHOLE conversion here, and it must actually happen:
/// passing the object through untouched leaves an `apiVersion` the client
/// will believe, and a client that asked for `v1` and is told `v1beta1`
/// either errors or — worse — proceeds against the wrong schema.
pub fn convert_none(
    crd: &Value,
    object: &Value,
    target_version: &str,
) -> Result<Value, ConversionError> {
    if !is_served(crd, target_version) {
        return Err(ConversionError::VersionNotServed {
            version: target_version.to_string(),
        });
    }
    let current = object
        .get("apiVersion")
        .and_then(Value::as_str)
        .ok_or(ConversionError::MissingApiVersion)?;
    // Keep the group, replace only the version — `example.com/v1beta1`
    // becomes `example.com/v1`. Rebuilding the whole string from the CRD's
    // group would silently "fix" an object stored under a different group,
    // which is a mismatch worth surfacing rather than papering over.
    let group = current.rsplit_once('/').map_or("", |(g, _)| g);
    let mut out = object.clone();
    out["apiVersion"] = Value::String(if group.is_empty() {
        target_version.to_string()
    } else {
        format!("{group}/{target_version}")
    });
    Ok(out)
}

/// Plan the conversion for a read at `target_version`.
///
/// Returns the converted object for the `None` strategy, or the error
/// naming why a webhook is needed. A caller that CAN reach the webhook
/// calls it; one that cannot must surface this rather than fall back.
pub fn convert_for_read(
    crd: &Value,
    object: &Value,
    target_version: &str,
) -> Result<Value, ConversionError> {
    // The storage version must be unambiguous before anything is read, or
    // the object's provenance is already unknowable.
    let _ = storage_version(crd)?;
    match strategy_of(crd) {
        Strategy::None => convert_none(crd, object, target_version),
        Strategy::Webhook { url } => Err(ConversionError::WebhookUnavailable {
            reason: if url.is_empty() {
                "strategy is Webhook but clientConfig declares no url".to_string()
            } else {
                format!("webhook at {url} has not been called")
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn crd(versions: Value, conversion: Option<Value>) -> Value {
        let mut c = json!({ "spec": { "group": "example.com", "versions": versions } });
        if let Some(conv) = conversion {
            c["spec"]["conversion"] = conv;
        }
        c
    }

    fn two_versions() -> Value {
        json!([
            { "name": "v1beta1", "served": true, "storage": false },
            { "name": "v1", "served": true, "storage": true }
        ])
    }

    fn stored() -> Value {
        json!({
            "apiVersion": "example.com/v1beta1",
            "kind": "Widget",
            "metadata": { "name": "w" },
            "spec": { "size": 3 }
        })
    }

    #[test]
    fn a_read_at_another_version_relabels_the_object() {
        // Anti-vacuity, and the bug: before this a v1 client received an
        // object still claiming v1beta1 and proceeded against the wrong
        // schema without an error.
        let c = crd(two_versions(), None);
        let out = convert_for_read(&c, &stored(), "v1").expect("converts");
        assert_eq!(out["apiVersion"], "example.com/v1");
        // Everything else is untouched — a None-strategy conversion is a
        // relabel, not a rewrite.
        assert_eq!(out["spec"]["size"], 3);
        assert_eq!(out["metadata"]["name"], "w");
    }

    #[test]
    fn exactly_one_storage_version_is_an_invariant_not_a_convention() {
        // A CRD with two writes the same object to two schemas and neither
        // reader can trust what it gets. Refusing beats picking.
        assert_eq!(storage_version(&crd(two_versions(), None)), Ok("v1".into()));

        let none = json!([{ "name": "v1", "served": true, "storage": false }]);
        assert_eq!(
            storage_version(&crd(none, None)),
            Err(ConversionError::StorageVersionAmbiguous { found: 0 })
        );

        let two = json!([
            { "name": "v1", "served": true, "storage": true },
            { "name": "v2", "served": true, "storage": true }
        ]);
        assert_eq!(
            storage_version(&crd(two, None)),
            Err(ConversionError::StorageVersionAmbiguous { found: 2 })
        );
    }

    #[test]
    fn a_webhook_strategy_fails_the_read_rather_than_falling_back() {
        // Falling back to a relabel would hand the client an object in the
        // WRONG schema while reporting success — the silent corruption this
        // module exists to stop, with a configuration that claims to
        // prevent it.
        let c = crd(
            two_versions(),
            Some(json!({
                "strategy": "Webhook",
                "webhook": { "clientConfig": { "url": "https://conv.example/convert" } }
            })),
        );
        match convert_for_read(&c, &stored(), "v1") {
            Err(ConversionError::WebhookUnavailable { reason }) => {
                assert!(reason.contains("conv.example"), "got: {reason}");
            }
            other => panic!("expected WebhookUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn a_webhook_strategy_with_no_url_says_which_half_is_missing() {
        // "the webhook is unavailable" and "you never configured one" are
        // different operator actions.
        let c = crd(
            two_versions(),
            Some(json!({ "strategy": "Webhook", "webhook": { "clientConfig": {} } })),
        );
        match convert_for_read(&c, &stored(), "v1") {
            Err(ConversionError::WebhookUnavailable { reason }) => {
                assert!(reason.contains("no url"), "got: {reason}");
            }
            other => panic!("expected WebhookUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn an_absent_conversion_block_means_the_none_strategy() {
        // Upstream's default, and the right one: a single-version CRD needs
        // no conversion machinery.
        assert_eq!(strategy_of(&crd(two_versions(), None)), Strategy::None);
        assert_eq!(
            strategy_of(&crd(two_versions(), Some(json!({ "strategy": "None" })))),
            Strategy::None
        );
    }

    #[test]
    fn an_unserved_version_is_refused() {
        // Serving a version the CRD withdrew would resurrect a schema the
        // author deliberately retired.
        let versions = json!([
            { "name": "v1", "served": true, "storage": true },
            { "name": "v1alpha1", "served": false, "storage": false }
        ]);
        let c = crd(versions, None);
        assert!(!is_served(&c, "v1alpha1"));
        assert_eq!(
            convert_for_read(&c, &stored(), "v1alpha1"),
            Err(ConversionError::VersionNotServed {
                version: "v1alpha1".into()
            })
        );
    }

    #[test]
    fn the_group_is_preserved_not_rebuilt_from_the_crd() {
        // Rebuilding it would silently "fix" an object stored under a
        // different group — a mismatch worth surfacing, not papering over.
        let c = crd(two_versions(), None);
        let mut foreign = stored();
        foreign["apiVersion"] = json!("other.io/v1beta1");
        let out = convert_for_read(&c, &foreign, "v1").expect("converts");
        assert_eq!(out["apiVersion"], "other.io/v1");
    }

    #[test]
    fn an_object_with_no_api_version_is_refused_not_guessed() {
        let c = crd(two_versions(), None);
        let mut bare = stored();
        bare.as_object_mut().unwrap().remove("apiVersion");
        assert_eq!(
            convert_for_read(&c, &bare, "v1"),
            Err(ConversionError::MissingApiVersion)
        );
    }

    #[test]
    fn an_ambiguous_storage_version_fails_the_read_before_anything_is_converted() {
        // The object's provenance is already unknowable at that point;
        // converting anyway would produce a confident wrong answer.
        let two = json!([
            { "name": "v1", "served": true, "storage": true },
            { "name": "v2", "served": true, "storage": true }
        ]);
        assert_eq!(
            convert_for_read(&crd(two, None), &stored(), "v1"),
            Err(ConversionError::StorageVersionAmbiguous { found: 2 })
        );
    }
}
