//! Range pagination — the typed [`ListPage`] view + the
//! integrity-tagged [`ContinueToken`].
//!
//! ## Consistency model: cursor-based (M0.1), NOT MVCC snapshot isolation
//!
//! M0.1 pagination is CURSOR-based: each page ranges the LIVE catalog
//! `BTreeMap` from `Bound::Excluded(last_key)`. For a QUIESCENT key set
//! the page series is gap-free + dup-free, and a key sorting AT OR BEFORE
//! the cursor cannot resurface (mechanical cursor exclusion). It does NOT
//! provide true etcd-style consistent-list semantics: a key inserted
//! AFTER the cursor between page calls WILL surface on a later page
//! (real etcd excludes it by revision). The [`ContinueToken`]'s
//! `snapshot_rev` is only the envelope `resourceVersion` LABEL captured
//! by the first page — it is NOT a read-isolation mechanism.
//!
//! DESTINATION (deferred): revision-indexed historical reads — page each
//! request AS OF the token's `snapshot_rev`, which requires retaining
//! historical MVCC views (per-key revision history / time-travel index),
//! not the single live materialized map M0.1 keeps. Do not claim
//! snapshot consistency for the page series until that lands.
//!
//! ## Relation to CAS
//!
//! CAS (optimistic concurrency) and pagination both key off the
//! revision: the [`crate::revision::VersionMeta::mod_revision`] is the
//! value a CAS precondition compares against, and the same revision is
//! the LABEL the continue token carries. CAS IS deterministic +
//! replicated; pagination's revision is currently a label only (see
//! above).
//!
//! ## The continue token is opaque + tamper-evident
//!
//! The wire form is a 1-byte version tag, an 8-byte BLAKE3 prefix over
//! the JSON body, and the hex-encoded JSON. ANY decode failure (wrong
//! tag, digest mismatch, truncation, bad hex, bad JSON) is a typed
//! [`ContinueInvalid`] — the apiserver maps it to HTTP 410 / Expired.
//! Clients treat the token as opaque (the K8s contract).

use serde::{Deserialize, Serialize};

use crate::resource::{ResourceKey, ResourceValue};
use crate::revision::Revision;

/// One page of resources, borrowed from the catalog. Produced by
/// [`crate::state::ResourceCatalog::list_page`].
///
///   * `items` — the page's `(key, value)` pairs in total key order.
///   * `next` — the last key returned IFF more matching items remain
///     after it (the cursor for the following page); else `None`.
///   * `remaining` — count of still-unreturned matching items after
///     this page.
#[derive(Debug)]
pub struct ListPage<'a> {
    pub items: Vec<(&'a ResourceKey, &'a ResourceValue)>,
    pub next: Option<ResourceKey>,
    pub remaining: u64,
}

/// The opaque continue cursor a paged LIST round-trips to its client.
///
///   * `snapshot_rev` — the revision LABEL captured by the FIRST page of
///     the series, threaded unchanged through every continue. It is the
///     `resourceVersion` reported on the LIST envelope; it is NOT a
///     read-isolation mechanism (M0.1 reads the live catalog each page —
///     see the module-level doc). The cursor that actually advances the
///     series is `last_key`.
///   * `last_key` — the last EMITTED key of the previous page; the next
///     page resumes STRICTLY after it (`Bound::Excluded`). This is the
///     load-bearing pagination cursor in M0.1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinueToken {
    pub snapshot_rev: Revision,
    pub last_key: ResourceKey,
}

/// The 1-byte version tag prefixing every encoded continue token. Bumps
/// when the on-wire shape changes; a stale tag decodes to
/// [`ContinueInvalid`].
const CONTINUE_TOKEN_VERSION: u8 = 1;

/// The BLAKE3 digest prefix length (bytes) — tamper / corruption
/// detection over the JSON body.
const DIGEST_PREFIX_LEN: usize = 8;

/// The typed "continue token is invalid / expired / corrupt" error —
/// the 410 Gone / Expired equivalent. Maps to `ApiError::Gone` in the
/// apiserver. Carries no caller-supplied bytes (a corrupt token's bytes
/// are not echoed back).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("continue token is invalid or expired ({reason})")]
pub struct ContinueInvalid {
    /// Stable machine-readable reason — `bad_hex` / `too_short` /
    /// `bad_version` / `digest_mismatch` / `bad_json`.
    pub reason: &'static str,
}

impl ContinueInvalid {
    #[must_use]
    fn of(reason: &'static str) -> Self {
        Self { reason }
    }

    /// Stable identifier — see [`engenho_substrate::ErrorKind`].
    #[must_use]
    pub fn kind(&self) -> &'static str {
        "continue_invalid"
    }
}

impl engenho_substrate::ErrorKind for ContinueInvalid {
    fn kind(&self) -> &'static str {
        Self::kind(self)
    }
}

impl ContinueToken {
    #[must_use]
    pub fn new(snapshot_rev: Revision, last_key: ResourceKey) -> Self {
        Self {
            snapshot_rev,
            last_key,
        }
    }

    /// Encode to an opaque lowercase-hex string:
    /// `hex( [version:1] ++ [blake3(json)[..8]] ++ json )`.
    ///
    /// Typed emission: the JSON body comes from `serde_json` (a typed
    /// `Serialize` value), and the hex is a deterministic 4-bit nibble
    /// map — no `format!()` of the wire bytes, no new dep.
    #[must_use]
    pub fn encode(&self) -> String {
        // Infallible for this concrete struct.
        let json = serde_json::to_vec(self).unwrap_or_default();
        let digest = blake3::hash(&json);
        let mut raw = Vec::with_capacity(1 + DIGEST_PREFIX_LEN + json.len());
        raw.push(CONTINUE_TOKEN_VERSION);
        raw.extend_from_slice(&digest.as_bytes()[..DIGEST_PREFIX_LEN]);
        raw.extend_from_slice(&json);
        to_hex(&raw)
    }

    /// Decode + verify an opaque continue token. ANY failure (bad hex,
    /// truncation, wrong version tag, digest mismatch, bad JSON) is a
    /// typed [`ContinueInvalid`] — never a silent wrong answer.
    ///
    /// # Errors
    ///
    /// [`ContinueInvalid`] on any of the failure modes above.
    pub fn decode(s: &str) -> Result<Self, ContinueInvalid> {
        let raw = from_hex(s).ok_or_else(|| ContinueInvalid::of("bad_hex"))?;
        if raw.len() < 1 + DIGEST_PREFIX_LEN {
            return Err(ContinueInvalid::of("too_short"));
        }
        if raw[0] != CONTINUE_TOKEN_VERSION {
            return Err(ContinueInvalid::of("bad_version"));
        }
        let digest_prefix = &raw[1..1 + DIGEST_PREFIX_LEN];
        let json = &raw[1 + DIGEST_PREFIX_LEN..];
        let expect = blake3::hash(json);
        if expect.as_bytes()[..DIGEST_PREFIX_LEN] != *digest_prefix {
            return Err(ContinueInvalid::of("digest_mismatch"));
        }
        serde_json::from_slice(json).map_err(|_| ContinueInvalid::of("bad_json"))
    }
}

/// Deterministic lowercase-hex encode (hand-rolled 4-bit nibble map, no
/// new dep) — every emitted token is a function of its bytes.
fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Lowercase-hex decode. Returns `None` on odd length or a non-hex
/// nibble (surfaced as `ContinueInvalid::bad_hex` by the caller).
fn from_hex(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let nibble = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    for pair in bytes.chunks_exact(2) {
        let hi = nibble(pair[0])?;
        let lo = nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", "default", name)
    }

    #[test]
    fn continue_token_round_trips() {
        let t = ContinueToken::new(Revision(42), key("podinfo"));
        let s = t.encode();
        let back = ContinueToken::decode(&s).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn tampered_token_is_rejected() {
        let t = ContinueToken::new(Revision(7), key("p3"));
        let s = t.encode();

        // Flip one hex nibble somewhere in the JSON body region (past the
        // version + digest prefix → 2 * (1 + 8) = 18 hex chars).
        let mut chars: Vec<char> = s.chars().collect();
        let pos = 20; // inside the body
        chars[pos] = if chars[pos] == 'a' { 'b' } else { 'a' };
        let flipped: String = chars.into_iter().collect();
        assert!(ContinueToken::decode(&flipped).is_err());

        // Truncated.
        assert!(ContinueToken::decode(&s[..s.len() - 4]).is_err());

        // Garbage (not hex).
        assert!(ContinueToken::decode("zzzz").is_err());
        assert!(ContinueToken::decode("garbage").is_err());
    }

    #[test]
    fn wrong_version_tag_is_rejected() {
        let t = ContinueToken::new(Revision(1), key("p"));
        let s = t.encode();
        // First byte = version tag (first two hex chars). Bump it.
        let mut raw = from_hex(&s).unwrap();
        raw[0] = raw[0].wrapping_add(1);
        // Re-hex without recomputing the digest → version mismatch fires
        // before the digest check.
        let bad = to_hex(&raw);
        let err = ContinueToken::decode(&bad).unwrap_err();
        assert_eq!(err.reason, "bad_version");
    }

    #[test]
    fn hex_round_trips() {
        let bytes = [0x00u8, 0x01, 0xff, 0xab, 0x10];
        let hex = to_hex(&bytes);
        assert_eq!(hex, "0001ffab10");
        assert_eq!(from_hex(&hex).unwrap(), bytes);
        assert!(from_hex("abc").is_none(), "odd length rejected");
        assert!(from_hex("zz").is_none(), "non-hex rejected");
    }

    #[test]
    fn continue_invalid_kind_is_stable() {
        let e = ContinueInvalid::of("bad_hex");
        assert_eq!(e.kind(), "continue_invalid");
    }
}
