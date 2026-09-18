//! A LIST must encode as `<Kind>List`, with its items intact.
//!
//! ## The defect this pins
//!
//! The apiserver rendered list responses with the ITEM's GVK, so a
//! `SecretList` body was deserialized against the `Secret` descriptor. The
//! codec is deliberately lenient (`deny_unknown_fields(false)`, so a newer
//! apiserver field never breaks an older client), which meant every field of
//! the list — `items` included — was silently DROPPED.
//!
//! Nothing errored. Measured on ryn 2026-09-17, the same LIST two ways: JSON
//! 1196 bytes, protobuf 30. `helm list` returned empty and `helm upgrade`
//! said "has no deployed releases" about a release Helm had itself just
//! written correctly, because its Go client negotiates protobuf.

use engenho_kube_proto::{Gvk, decode_protobuf, encode_response};
use serde_json::json;

fn secret_list() -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "SecretList",
        "metadata": { "resourceVersion": "619075" },
        "items": [
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": "sh.helm.release.v1.probe.v1",
                    "namespace": "default",
                    // ★ An RFC3339 string, as every real object carries. The
                    // codec must normalize it to the proto Time message for
                    // EVERY item, not just the list's own metadata.
                    "creationTimestamp": "2026-09-18T00:56:18Z"
                },
                "type": "helm.sh/release.v1",
                "data": { "release": "SDRzSUFBQUFBQUFDLw==" }
            }
        ]
    })
}

/// ★ The item must survive the round trip. Asserting only that the body is
/// non-empty would pass against the defect: the broken encoding produced a
/// valid 30-byte message, just an empty one.
#[test]
fn a_list_round_trips_with_its_items() {
    let gvk = Gvk {
        api_version: "v1".to_string(),
        kind: "SecretList".to_string(),
    };
    let bytes = encode_response(&gvk, &secret_list()).expect("a SecretList must encode");
    let back = decode_protobuf(&bytes).expect("and decode");

    let items = back
        .get("items")
        .and_then(|i| i.as_array())
        .expect("items must survive encoding");
    assert_eq!(items.len(), 1, "the list lost its items: {back}");
    assert_eq!(
        items[0].pointer("/metadata/name").and_then(|v| v.as_str()),
        Some("sh.helm.release.v1.probe.v1"),
        "the item must keep its identity: {back}"
    );
    assert_eq!(
        items[0].get("type").and_then(|v| v.as_str()),
        Some("helm.sh/release.v1"),
        "Helm branches on Secret.type; losing it makes a release invisible"
    );
}

/// ★ The negative control, and the actual defect: encoding a list against the
/// ITEM's descriptor must not quietly succeed with an empty result. If this
/// ever passes with items intact, the leniency has changed and the bug above
/// cannot recur; if it fails loudly, that is also fine. What must NOT happen
/// is what happened — a clean encode that silently dropped everything.
#[test]
fn encoding_a_list_against_the_item_descriptor_does_not_preserve_items() {
    let item_gvk = Gvk {
        api_version: "v1".to_string(),
        kind: "Secret".to_string(),
    };
    let lost = match encode_response(&item_gvk, &secret_list()) {
        Err(_) => true, // refused outright — acceptable
        Ok(bytes) => match decode_protobuf(&bytes) {
            Err(_) => true,
            Ok(back) => back
                .get("items")
                .and_then(|i| i.as_array())
                .is_none_or(|a| a.is_empty()),
        },
    };
    assert!(
        lost,
        "this is the shape of the original defect: the ITEM descriptor \
         cannot carry a list, so anything it produces must not look like a \
         populated list"
    );
}

/// ★ Every ITEM's `creationTimestamp` must be normalized, not just the list's.
///
/// metav1.Time is a protobuf MESSAGE that JSON-marshals as an RFC3339 STRING.
/// Normalizing only the outer metadata left each item's timestamp a string and
/// prost-reflect rejected it:
/// `invalid type: string "2026-09-18T00:56:18Z", expected a map`.
///
/// This was unreachable until lists were encoded as `<Kind>List`: before that
/// the item descriptor dropped `items` wholesale, so no item's metadata was
/// ever examined. A silent failure was hiding this one behind it.
#[test]
fn item_timestamps_are_normalized_not_just_the_lists_own() {
    let gvk = Gvk {
        api_version: "v1".to_string(),
        kind: "SecretList".to_string(),
    };
    let bytes = encode_response(&gvk, &secret_list())
        .expect("an item carrying an RFC3339 timestamp must encode");
    let back = decode_protobuf(&bytes).expect("and decode");
    let ts = back
        .pointer("/items/0/metadata/creationTimestamp")
        .expect("the item keeps its timestamp");
    assert!(
        !ts.is_null(),
        "the timestamp must survive the round trip: {back}"
    );
}
