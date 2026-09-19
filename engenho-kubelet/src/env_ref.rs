//! `$(VAR)` expansion — the one implementation of upstream's
//! `third_party/forked/golang/expansion` rules in this crate.
//!
//! Three callers share it: a container's env values and its argv (in
//! [`crate::kubelet`]), and an exec probe's argv (in
//! [`crate::probe::ProbeSpec::from_container`]). They differ only in the map
//! of variables they pass; the syntax is the same, so it lives here once.

use std::collections::BTreeMap;

/// Expand `$(VAR)` references in an env value or an argv element, using
/// the variables already defined for this container.
///
/// **Kubernetes does this and every workload assumes it.** A manifest
/// writes `value: "source-controller.$(RUNTIME_NAMESPACE).svc.cluster.local."`
/// and expects a hostname; without expansion the container receives the
/// literal text and fails at whatever it hands the string to — far from
/// the kubelet, with nothing naming it. Measured on rio 2026-09-15: Flux's
/// kustomize-controller logged
/// `Get "http://source-controller.$(RUNTIME_NAMESPACE).svc.cluster.local./…"`
/// and could not fetch a single artifact, so nothing was ever applied.
///
/// Upstream's three rules, all load-bearing:
///   * only variables defined EARLIER in the list are visible — the caller
///     passes the map accumulated so far, so order is the semantics;
///   * `$$` escapes to a single literal `$`;
///   * an UNRESOLVABLE `$(VAR)` is left exactly as written, never blanked.
///     Blanking would turn a typo into a silently-empty hostname, which is
///     strictly harder to debug than the unexpanded text.
#[must_use]
pub(crate) fn expand_env_refs(raw: &str, defined: &BTreeMap<String, String>) -> String {
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'$' {
            // Push the whole UTF-8 character, not the byte: indexing by
            // byte and emitting bytes would split a multi-byte codepoint.
            let ch = raw[i..].chars().next().unwrap_or('$');
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        match bytes.get(i + 1) {
            Some(b'$') => {
                out.push('$');
                i += 2;
            }
            Some(b'(') => {
                if let Some(close) = raw[i + 2..].find(')') {
                    let name = &raw[i + 2..i + 2 + close];
                    if let Some(v) = defined.get(name) {
                        out.push_str(v);
                    } else {
                        // Unresolvable: emit verbatim, including the
                        // delimiters, so the operator sees what was asked
                        // for rather than an empty string.
                        out.push_str(&raw[i..i + 3 + close]);
                    }
                    i += 3 + close;
                } else {
                    // No closing paren — not a reference at all.
                    out.push('$');
                    i += 1;
                }
            }
            _ => {
                out.push('$');
                i += 1;
            }
        }
    }
    out
}
