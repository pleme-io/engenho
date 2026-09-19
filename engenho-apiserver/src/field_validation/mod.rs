//! `?fieldValidation=` — what a write does with a field its kind does not
//! have, or a field given twice (plan T4.6).
//!
//! Upstream decodes every write body into the kind's Go struct. A key the
//! struct has no field for is DROPPED — in every mode — and a key given
//! twice keeps its last value. `fieldValidation` only decides what the
//! client hears about it (`k8s.io/apiserver` `handlers/rest.go`,
//! `handlers/create.go`, `update.go`, `patch.go`):
//!
//! | directive          | unknown / duplicate field                         |
//! |--------------------|---------------------------------------------------|
//! | `Ignore`           | dropped / last wins, nothing said                 |
//! | `Warn` (absent)    | the same, plus one `Warning: 299` header each     |
//! | `Strict`           | the write is refused, naming every one            |
//! | anything else      | 422 on the options struct, before the body is read |
//!
//! kubectl 1.27 and later sends `Strict` by default (`--validate=true`), so
//! this is the check every `kubectl apply` and `kubectl create` asks for.
//!
//! ★ WHERE EACH VERB IS CHECKED, and why there:
//!
//! * POST, PUT, server-side apply — the body IS an object. The request's
//!   bytes are scanned once against the kind's schema at the border
//!   ([`check_object`]), before the write pipeline reads the store, because
//!   upstream's decode happens before anything else: a strict refusal
//!   outranks a 404 or a 409.
//! * merge, strategic and JSON PATCH — the patch is not the object. Its bytes
//!   are scanned for duplicates at the border; unknown fields are looked for
//!   in the PATCHED object inside the write pipeline, where it exists
//!   ([`PatchFields::judge_patched`]). Only fields the patch introduced are
//!   reported and dropped: upstream never stores an unknown field, so every
//!   unknown field in its patched object came from the patch. engenho may
//!   hold objects written before this check existed, and a label change must
//!   not silently delete what they carry.
//!
//! ★ TIER.
//! * A write through the router storing an unknown field of a schema-backed
//!   kind: **caught**, not unrepresentable. The router drops them before it
//!   builds the `ObjectBody` of a POST or PUT, and the pipeline drops what a
//!   PATCH introduced; but `ObjectBody` carries no proof of it, so a caller
//!   of `ResourceHandler::create`/`replace` that skips the router, a
//!   mutating webhook's rewrite, and a controller writing straight to the
//!   store are not checked. `tests/w2_typed_border.rs` pins every verb.
//! * The directive itself is **parse-time-rejected**: a value outside the
//!   three is upstream's 422 before the body is read, never a silent
//!   default.
//! * A kind with no vendored schema (the opaque catalog rows, every custom
//!   resource) gets the duplicate check only. When the client ASKED for
//!   `Strict` or `Warn`, the response says so in a warning rather than
//!   letting silence read as "no unknown fields" ([`NotChecked`]).
//! * That the schema agrees with upstream's Go structs is measured per row
//!   against upstream's own roundtrip fixtures (`tests/w2_typed_border.rs`).
//!
//! ★ DEVIATIONS, recorded rather than hidden:
//! * server-side apply: upstream refuses an unknown field in an apply
//!   configuration whatever the directive (a 500 from structured-merge-diff,
//!   `.spec.x: field not declared in schema`) and reports duplicates in YAML
//!   wording with line numbers. engenho applies the directive to both and
//!   reports both in the JSON decoder's wording.
//! * the strategic-merge refusal quotes the patched object as JSON where
//!   upstream quotes the patch as a Go map.
//! * `/status`, `/scale` and `/token` writes parse and validate the
//!   directive but check no field; an explicit `Strict` or `Warn` is
//!   answered with a [`NotChecked`] warning.
//! * engenho's strategic merge (`engenho_store::strategic_merge`) does not
//!   consume `$setElementOrder/<list>` or `$deleteFromPrimitiveList/<list>`;
//!   it copies them into the object, so every client-side `kubectl apply`
//!   of a pod template stored one. They are dropped here, unreported (the
//!   client's patch was well formed); the ordering and deletion they ask
//!   for are still not performed.

pub mod scan;
mod schema;

use std::fmt;

use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::header::WARNING;
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use crate::error::ApiError;
use crate::object_body::Undecodable;

pub use scan::{Finding, FindingKind, Findings, MAX_FINDINGS, Origin, StrictPath};
pub(crate) use schema::row;

use scan::Target;

// ── the directive ──────────────────────────────────────────────────────────

/// How a write treats unknown and duplicate fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldValidation {
    /// Drop them silently.
    Ignore,
    /// Drop them and say so, one warning each. Upstream's default.
    Warn,
    /// Refuse the write.
    Strict,
}

/// The options struct upstream decodes a write's query into; it names the
/// 422 for a bad `fieldValidation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOptions {
    /// POST.
    Create,
    /// PUT.
    Update,
    /// PATCH, including server-side apply.
    Patch,
}

impl WriteOptions {
    fn kind(self) -> &'static str {
        match self {
            Self::Create => "CreateOptions",
            Self::Update => "UpdateOptions",
            Self::Patch => "PatchOptions",
        }
    }
}

/// A parsed `?fieldValidation=`: the mode, and whether the client named it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Directive {
    mode: FieldValidation,
    asked: bool,
}

impl Default for Directive {
    /// What an absent parameter means: `Warn`, not asked for.
    fn default() -> Self {
        Self {
            mode: FieldValidation::Warn,
            asked: false,
        }
    }
}

impl Directive {
    /// Parse the raw query value. Absent and empty are the default; the
    /// three values are case-sensitive, as upstream's set is.
    ///
    /// # Errors
    ///
    /// [`UnsupportedDirective`] for any other value — upstream's 422.
    pub fn parse(raw: Option<&str>, options: WriteOptions) -> Result<Self, UnsupportedDirective> {
        let (mode, asked) = match raw {
            None | Some("") => return Ok(Self::default()),
            Some("Ignore") => (FieldValidation::Ignore, true),
            Some("Warn") => (FieldValidation::Warn, true),
            Some("Strict") => (FieldValidation::Strict, true),
            Some(other) => {
                return Err(UnsupportedDirective {
                    options,
                    value: other.to_owned(),
                });
            }
        };
        Ok(Self { mode, asked })
    }

    /// A directive the client named explicitly (tests, internal callers).
    #[must_use]
    pub fn asked(mode: FieldValidation) -> Self {
        Self { mode, asked: true }
    }

    /// The mode in force.
    #[must_use]
    pub fn mode(self) -> FieldValidation {
        self.mode
    }

    /// Whether the client asked to hear about unknown fields: an explicit
    /// `Strict` or `Warn`.
    fn wants_to_know(self) -> bool {
        self.asked && self.mode != FieldValidation::Ignore
    }

    /// Apply the directive to what was found. `Strict` refuses through
    /// `refusal`; `Warn` records a warning per finding; `Ignore` says
    /// nothing.
    fn judge(
        self,
        found: &Findings,
        warnings: &mut Warnings,
        refusal: impl FnOnce(StrictDecodingError<'_>) -> ApiError,
    ) -> Result<(), ApiError> {
        if found.is_empty() {
            return Ok(());
        }
        match self.mode {
            FieldValidation::Strict => Err(refusal(StrictDecodingError(found))),
            FieldValidation::Warn => {
                for finding in found.as_slice() {
                    warnings.add(finding);
                }
                Ok(())
            }
            FieldValidation::Ignore => Ok(()),
        }
    }
}

/// A `fieldValidation` value upstream does not accept. Renders upstream's
/// `NewInvalid` over `ValidateFieldValidation`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "{kind}.meta.k8s.io \"\" is invalid: fieldValidation: Unsupported value: {value}: \
     supported values: \"\", \"Ignore\", \"Strict\", \"Warn\"",
    kind = options.kind(),
    value = GoQuoted(value)
)]
pub struct UnsupportedDirective {
    options: WriteOptions,
    value: String,
}

impl From<UnsupportedDirective> for ApiError {
    fn from(e: UnsupportedDirective) -> Self {
        Self::Invalid(e.to_string())
    }
}

// ── what a refusal says ────────────────────────────────────────────────────

/// Upstream's `runtime.NewStrictDecodingError`: every finding, in order.
#[derive(Clone, Copy, Debug)]
pub struct StrictDecodingError<'a>(&'a Findings);

impl fmt::Display for StrictDecodingError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("strict decoding error: ")?;
        for (i, finding) in self.0.as_slice().iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{finding}")?;
        }
        Ok(())
    }
}

/// Upstream's refusal of a strict PATCH: `NewInvalid` over
/// `field.Invalid(field.NewPath("patch"), <value>, <strict error>)`, with
/// an empty group-kind and name, hence the sentence's odd start.
struct PatchInvalid<'a> {
    value: &'a str,
    error: StrictDecodingError<'a>,
}

impl fmt::Display for PatchInvalid<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            " \"\" is invalid: patch: Invalid value: {}: {}",
            GoQuoted(self.value),
            self.error
        )
    }
}

/// A value rendered as Go's `strconv.Quote` renders a string: in double
/// quotes, with `"`, `\` and control characters escaped.
pub(crate) struct GoQuoted<T>(pub(crate) T);

impl<T: fmt::Display> fmt::Display for GoQuoted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Escaping<'a, 'b>(&'a mut fmt::Formatter<'b>);
        impl fmt::Write for Escaping<'_, '_> {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                for c in s.chars() {
                    match c {
                        '"' => self.0.write_str("\\\"")?,
                        '\\' => self.0.write_str("\\\\")?,
                        '\n' => self.0.write_str("\\n")?,
                        '\r' => self.0.write_str("\\r")?,
                        '\t' => self.0.write_str("\\t")?,
                        c if c.is_control() && u32::from(c) < 0x100 => {
                            write!(self.0, "\\x{:02x}", u32::from(c))?;
                        }
                        c if c.is_control() => write!(self.0, "\\u{:04x}", u32::from(c))?,
                        c => fmt::Write::write_char(self.0, c)?,
                    }
                }
                Ok(())
            }
        }
        f.write_str("\"")?;
        fmt::Write::write_fmt(&mut Escaping(f), format_args!("{}", self.0))?;
        f.write_str("\"")
    }
}

// ── warnings ───────────────────────────────────────────────────────────────

/// The warnings a response carries, as upstream's per-request recorder
/// keeps them (`k8s.io/apiserver/pkg/endpoints/filters/warning.go`): each
/// text once, in the order first added.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Warnings {
    texts: Vec<String>,
}

/// Upstream's recorder budget: 4096 runes in total; past it every warning
/// is cut to 256 runes, and once the cut ones fill the budget the rest are
/// dropped.
const TOTAL_RUNES: usize = 4 * 1024;
const ITEM_RUNES: usize = 256;

impl Warnings {
    /// Record one warning. A text already recorded is not recorded again.
    pub fn add(&mut self, text: &dyn fmt::Display) {
        let text = text.to_string();
        if !text.is_empty() && !self.texts.contains(&text) {
            self.texts.push(text);
        }
    }

    /// Every warning, in order.
    #[must_use]
    pub fn texts(&self) -> &[String] {
        &self.texts
    }

    /// The texts the headers carry after upstream's truncation.
    fn budgeted(&self) -> Vec<&str> {
        fn cut(text: &str) -> &str {
            text.char_indices()
                .nth(ITEM_RUNES)
                .and_then(|(at, _)| text.get(..at))
                .unwrap_or(text)
        }
        let mut written = 0;
        let mut truncating = false;
        let mut out: Vec<&str> = Vec::new();
        for (i, text) in self.texts.iter().enumerate() {
            if truncating && written >= TOTAL_RUNES {
                break;
            }
            let item = if truncating { cut(text) } else { text.as_str() };
            let runes = item.chars().count();
            if truncating || written + runes <= TOTAL_RUNES {
                written += runes;
                out.push(item);
                continue;
            }
            truncating = true;
            out = self.texts.iter().take(i + 1).map(|t| cut(t)).collect();
            written = out.iter().map(|t| t.chars().count()).sum();
        }
        out
    }

    /// Add one `Warning: 299 - "<text>"` header per warning to `headers`.
    pub fn write_to(&self, headers: &mut HeaderMap) {
        for text in self.budgeted() {
            let header = WarningHeader(text).to_string();
            if let Ok(value) = HeaderValue::from_bytes(header.as_bytes()) {
                headers.append(WARNING, value);
            }
        }
    }

    /// The response for `result`, carrying these warnings whether the write
    /// succeeded or not — upstream's recorder writes them on either.
    #[must_use]
    pub fn attach(&self, result: Result<Response, ApiError>) -> Response {
        let mut response = match result {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
        self.write_to(response.headers_mut());
        response
    }
}

/// `299 - "<text>"`: upstream's `NewWarningHeader` — agent `-`, the text as
/// an RFC 7230 quoted-string. A control character becomes a space, as Go's
/// header writer turns a newline into one.
struct WarningHeader<'a>(&'a str);

impl fmt::Display for WarningHeader<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("299 - \"")?;
        for c in self.0.chars() {
            match c {
                '"' => f.write_str("\\\"")?,
                '\\' => f.write_str("\\\\")?,
                c if c.is_control() => f.write_str(" ")?,
                c => fmt::Write::write_char(f, c)?,
            }
        }
        f.write_str("\"")
    }
}

/// What a write was not checked for, said when the client asked.
#[derive(Clone, Copy, Debug)]
pub enum NotChecked<'a> {
    /// A kind with no vendored schema: duplicates were checked, unknown
    /// fields were not.
    UnknownFields {
        /// The kind.
        kind: &'a str,
        /// Its `apiVersion`.
        api_version: &'a str,
    },
    /// A subresource write: nothing was checked.
    Subresource {
        /// `status`, `scale`, `token`.
        name: &'a str,
    },
}

impl fmt::Display for NotChecked<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownFields { kind, api_version } => write!(
                f,
                "fieldValidation: unknown fields are not detected for {kind} in \
                 {api_version}; only duplicate fields were checked"
            ),
            Self::Subresource { name } => write!(
                f,
                "fieldValidation: the {name} subresource is not checked for unknown or \
                 duplicate fields"
            ),
        }
    }
}

/// The directive of a subresource write: parsed (a bad value is still
/// upstream's 422) and, when asked for, answered with a [`NotChecked`]
/// warning.
///
/// # Errors
///
/// [`UnsupportedDirective`] for a value upstream refuses.
pub fn subresource_directive(
    raw: Option<&str>,
    options: WriteOptions,
    name: &str,
    warnings: &mut Warnings,
) -> Result<(), UnsupportedDirective> {
    let directive = Directive::parse(raw, options)?;
    if directive.wants_to_know() {
        warnings.add(&NotChecked::Subresource { name });
    }
    Ok(())
}

// ── an object body: POST, PUT, apply ───────────────────────────────────────

/// The kind a body is checked as.
#[derive(Clone, Copy, Debug)]
pub struct KindRef<'a> {
    /// API group.
    pub group: &'a str,
    /// API version.
    pub version: &'a str,
    /// Kind.
    pub kind: &'a str,
    /// `apiVersion`.
    pub api_version: &'a str,
}

/// Check a body that IS an object, apply the directive, and drop every
/// unknown field from `value` — the object `raw` decoded to.
///
/// `raw` is `None` for a body that did not arrive as JSON (protobuf cannot
/// carry an unknown JSON key or a repeated one).
///
/// # Errors
///
/// Under `Strict`, upstream's 400 for an undecodable body, naming every
/// finding.
pub fn check_object(
    directive: Directive,
    kind: KindRef<'_>,
    raw: Option<&[u8]>,
    value: &mut Value,
    warnings: &mut Warnings,
) -> Result<(), ApiError> {
    let schema = row(kind.group, kind.version, kind.kind);
    let found = match raw {
        Some(raw) => {
            let target = schema.map_or(Target::Untyped, Target::Kind);
            scan::scan(raw, target).map_err(|e| {
                ApiError::BadRequest(
                    Undecodable {
                        version: kind.version,
                        kind: kind.kind,
                        error: &e,
                    }
                    .to_string(),
                )
            })?
        }
        None => Findings::default(),
    };
    directive.judge(&found, warnings, |error| {
        ApiError::BadRequest(
            Undecodable {
                version: kind.version,
                kind: kind.kind,
                error: &error,
            }
            .to_string(),
        )
    })?;
    match schema {
        Some(schema) => {
            for path in scan::unknown_paths(schema, value) {
                path.remove_from(value);
            }
        }
        None if directive.wants_to_know() => warnings.add(&NotChecked::UnknownFields {
            kind: kind.kind,
            api_version: kind.api_version,
        }),
        None => {}
    }
    Ok(())
}

// ── a patch: merge, strategic, JSON ────────────────────────────────────────

/// A PATCH's field validation, carried into the write pipeline: the
/// directive, what the border found in the patch's own bytes, and the
/// warnings the response will carry.
#[derive(Clone, Debug, Default)]
pub struct PatchFields {
    directive: Directive,
    border: Findings,
    warnings: Warnings,
}

/// Which bytes a patch arrived as, for the border scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PatchBytes {
    /// A merge or strategic-merge patch: an untyped object.
    Merge,
    /// A JSON patch: an operation list.
    Json,
    /// Not JSON (protobuf): nothing to scan.
    Opaque,
}

impl PatchFields {
    /// Scan a patch's bytes for what upstream's strict patch decode reports
    /// before the patch is applied: duplicate keys, and in a JSON patch an
    /// operation's unknown keys.
    ///
    /// # Errors
    ///
    /// A 400 when `raw` is not JSON; every caller has parsed it already.
    pub fn scan(directive: Directive, raw: &[u8], bytes: PatchBytes) -> Result<Self, ApiError> {
        let target = match bytes {
            PatchBytes::Merge => Target::Untyped,
            PatchBytes::Json => Target::JsonPatch,
            PatchBytes::Opaque => {
                return Ok(Self {
                    directive,
                    ..Self::default()
                });
            }
        };
        let border = scan::scan(raw, target)
            .map_err(|e| ApiError::BadRequest(PatchUndecodable(&e).to_string()))?;
        Ok(Self {
            directive,
            border,
            warnings: Warnings::default(),
        })
    }

    /// No directive from a client: upstream's default, nothing found yet.
    #[must_use]
    pub fn unasked() -> Self {
        Self::default()
    }

    /// The warnings the response carries.
    #[must_use]
    pub fn warnings(&self) -> &Warnings {
        &self.warnings
    }

    /// Judge the object a patch would store. Unknown fields `patched` has
    /// and `prior` did not are the patch's: they join the border's
    /// findings, the directive is applied, and they are dropped from
    /// `patched`. Replaces the warnings of any earlier attempt.
    ///
    /// `strategic` says the patch was a strategic merge. Its `$`-directives
    /// (`$setElementOrder/<list>`, `$deleteFromPrimitiveList/<list>`,
    /// `$retainKeys`, `$patch`) are instructions to the merge, never fields:
    /// upstream's merge consumes them. Where engenho's merge left one in the
    /// object it is dropped — wherever it sits, whoever put it there — and
    /// never reported, because the client sent a well-formed patch.
    ///
    /// # Errors
    ///
    /// Under `Strict`, upstream's 422 for a patch that does not decode
    /// strictly.
    pub(crate) fn judge_patched(
        &mut self,
        kind: KindRef<'_>,
        prior: &Value,
        patched: &mut Value,
        strategic: bool,
    ) -> Result<(), ApiError> {
        let mut warnings = Warnings::default();
        let schema = row(kind.group, kind.version, kind.kind);
        let mut introduced = Vec::new();
        if let Some(schema) = schema {
            let before = scan::unknown_paths(schema, prior);
            for path in scan::unknown_paths(schema, patched) {
                if strategic && path.names_merge_directive() {
                    path.remove_from(patched);
                } else if !before.contains(&path) {
                    introduced.push(path);
                }
            }
        }
        let mut found = self.border.clone();
        found.extend(scan::unknown_findings(&introduced));
        self.directive.judge(&found, &mut warnings, |error| {
            // The patched object as JSON: what upstream quotes for a merge
            // or JSON patch. `Value`'s serializer cannot fail.
            let value = serde_json::to_string(patched).unwrap_or_default();
            ApiError::Invalid(
                PatchInvalid {
                    value: &value,
                    error,
                }
                .to_string(),
            )
        })?;
        for path in &introduced {
            path.remove_from(patched);
        }
        if schema.is_none() && self.directive.wants_to_know() {
            warnings.add(&NotChecked::UnknownFields {
                kind: kind.kind,
                api_version: kind.api_version,
            });
        }
        self.warnings = warnings;
        Ok(())
    }
}

/// Upstream's 400 for a patch body that is not JSON.
struct PatchUndecodable<'a>(&'a serde_json::Error);

impl fmt::Display for PatchUndecodable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error decoding patch: {}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configmap() -> KindRef<'static> {
        KindRef {
            group: "",
            version: "v1",
            kind: "ConfigMap",
            api_version: "v1",
        }
    }

    #[test]
    fn the_directive_parses_upstreams_four_answers() {
        let parse = |raw| Directive::parse(raw, WriteOptions::Create);
        assert_eq!(parse(None), Ok(Directive::default()));
        assert_eq!(parse(Some("")), Ok(Directive::default()));
        assert_eq!(parse(None).map(Directive::mode), Ok(FieldValidation::Warn));
        assert_eq!(
            parse(Some("Strict")),
            Ok(Directive::asked(FieldValidation::Strict))
        );
        assert_eq!(
            parse(Some("Warn")),
            Ok(Directive::asked(FieldValidation::Warn))
        );
        assert_eq!(
            parse(Some("Ignore")),
            Ok(Directive::asked(FieldValidation::Ignore))
        );
        let bad = Directive::parse(Some("strict"), WriteOptions::Patch).expect_err("case matters");
        assert_eq!(
            bad.to_string(),
            "PatchOptions.meta.k8s.io \"\" is invalid: fieldValidation: Unsupported value: \
             \"strict\": supported values: \"\", \"Ignore\", \"Strict\", \"Warn\""
        );
        assert!(matches!(ApiError::from(bad), ApiError::Invalid(_)));
    }

    #[test]
    fn go_quoting_escapes_what_strconv_quote_escapes() {
        assert_eq!(
            GoQuoted("a\"b\\c\nd\u{1}").to_string(),
            r#""a\"b\\c\nd\x01""#
        );
    }

    #[test]
    fn a_warning_header_is_a_quoted_string() {
        let mut w = Warnings::default();
        w.add(&"unknown field \"spec.x\"");
        w.add(&"unknown field \"spec.x\"");
        let mut headers = HeaderMap::new();
        w.write_to(&mut headers);
        let all: Vec<_> = headers.get_all(WARNING).iter().collect();
        assert_eq!(all, [r#"299 - "unknown field \"spec.x\"""#]);
    }

    #[test]
    fn warnings_past_the_budget_are_cut_then_dropped() {
        let mut w = Warnings::default();
        for i in 0..40 {
            w.add(&format_args!("{i:03}{}", "x".repeat(297)));
        }
        let kept = w.budgeted();
        assert!(kept.iter().all(|t| t.chars().count() == ITEM_RUNES));
        assert_eq!(kept.len(), TOTAL_RUNES.div_ceil(ITEM_RUNES));
        assert!(kept[0].starts_with("000"), "the earliest warnings are kept");
    }

    #[test]
    fn strict_refuses_with_upstreams_decode_sentence() {
        let mut value = serde_json::json!({"metadata": {"name": "c"}, "dta": {}});
        let raw = br#"{"metadata":{"name":"c","name":"d"},"dta":{}}"#;
        let err = check_object(
            Directive::asked(FieldValidation::Strict),
            configmap(),
            Some(raw),
            &mut value,
            &mut Warnings::default(),
        )
        .expect_err("strict refuses");
        assert!(matches!(&err, ApiError::BadRequest(_)), "{err:?}");
        assert_eq!(
            err.to_string(),
            "invalid request: ConfigMap in version \"v1\" cannot be handled as a ConfigMap: \
             strict decoding error: duplicate field \"metadata.name\", unknown field \"dta\""
        );
    }

    #[test]
    fn warn_and_ignore_drop_the_unknown_field_and_differ_only_in_what_they_say() {
        for (mode, said) in [
            (
                FieldValidation::Warn,
                vec![r#"unknown field "dta""#.to_owned()],
            ),
            (FieldValidation::Ignore, vec![]),
        ] {
            let mut value = serde_json::json!({"metadata": {"name": "c"}, "dta": {"k": "v"}});
            let mut warnings = Warnings::default();
            check_object(
                Directive::asked(mode),
                configmap(),
                Some(br#"{"metadata":{"name":"c"},"dta":{"k":"v"}}"#),
                &mut value,
                &mut warnings,
            )
            .expect("not strict");
            assert_eq!(
                value,
                serde_json::json!({"metadata": {"name": "c"}}),
                "{mode:?}"
            );
            assert_eq!(warnings.texts(), said.as_slice(), "{mode:?}");
        }
    }

    #[test]
    fn a_kind_without_a_schema_says_what_was_not_checked_when_asked() {
        let widget = KindRef {
            group: "example.com",
            version: "v1",
            kind: "Widget",
            api_version: "example.com/v1",
        };
        let raw = br#"{"spec":{"size":1,"size":2,"anything":1}}"#;
        let mut value = serde_json::json!({"spec": {"size": 2, "anything": 1}});
        let mut warnings = Warnings::default();
        check_object(
            Directive::asked(FieldValidation::Warn),
            widget,
            Some(raw),
            &mut value,
            &mut warnings,
        )
        .expect("warn");
        assert_eq!(
            warnings.texts(),
            [
                r#"duplicate field "spec.size""#,
                "fieldValidation: unknown fields are not detected for Widget in \
                 example.com/v1; only duplicate fields were checked"
            ]
        );
        assert_eq!(
            value["spec"]["anything"], 1,
            "nothing is dropped without a schema"
        );

        let mut quiet = Warnings::default();
        check_object(
            Directive::default(),
            widget,
            Some(br#"{"spec":{}}"#),
            &mut value,
            &mut quiet,
        )
        .expect("default");
        assert!(quiet.texts().is_empty(), "an unasked default stays quiet");
    }

    #[test]
    fn a_strategic_merge_directive_left_in_the_object_is_dropped_unreported() {
        let pod = KindRef {
            group: "",
            version: "v1",
            kind: "Pod",
            api_version: "v1",
        };
        let prior = serde_json::json!({"spec": {"$setElementOrder/volumes": []}});
        let mut patched = serde_json::json!({"spec": {
            "$setElementOrder/containers": [{"name": "c"}],
            "$setElementOrder/volumes": [],
            "containers": [{"name": "c"}]
        }});
        let mut fields = PatchFields::scan(
            Directive::asked(FieldValidation::Strict),
            b"{}",
            PatchBytes::Merge,
        )
        .expect("json");
        fields
            .judge_patched(pod, &prior, &mut patched, true)
            .expect("a directive is not an unknown field");
        assert!(fields.warnings().texts().is_empty());
        assert_eq!(
            patched,
            serde_json::json!({"spec": {"containers": [{"name": "c"}]}}),
            "every directive goes, the one an earlier patch left too"
        );

        let mut merge = serde_json::json!({"spec": {"$setElementOrder/containers": []}});
        let err = fields
            .judge_patched(pod, &serde_json::json!({}), &mut merge, false)
            .expect_err("in a merge patch a $-key is a key");
        assert!(
            err.to_string()
                .contains(r#"unknown field "spec.$setElementOrder/containers""#)
        );
    }

    #[test]
    fn a_patch_answers_only_for_the_fields_it_introduced() {
        let pod = KindRef {
            group: "",
            version: "v1",
            kind: "Pod",
            api_version: "v1",
        };
        let prior = serde_json::json!({"metadata": {"name": "p"}, "legacy": 1});
        let mut patched = serde_json::json!({"metadata": {"name": "p"}, "legacy": 1, "fresh": 2});
        let mut fields = PatchFields::scan(
            Directive::asked(FieldValidation::Warn),
            br#"{"fresh":2}"#,
            PatchBytes::Merge,
        )
        .expect("json");
        fields
            .judge_patched(pod, &prior, &mut patched, false)
            .expect("warn");
        assert_eq!(fields.warnings().texts(), [r#"unknown field "fresh""#]);
        assert_eq!(
            patched,
            serde_json::json!({"metadata": {"name": "p"}, "legacy": 1}),
            "the patch's field is dropped, the stored one is not the patch's to delete"
        );

        let mut strict = PatchFields::scan(
            Directive::asked(FieldValidation::Strict),
            br#"{"fresh":2,"fresh":3}"#,
            PatchBytes::Merge,
        )
        .expect("json");
        let mut patched = serde_json::json!({"fresh": 3});
        let err = strict
            .judge_patched(pod, &serde_json::json!({}), &mut patched, false)
            .expect_err("strict");
        assert!(matches!(&err, ApiError::Invalid(_)), "{err:?}");
        assert_eq!(
            err.to_string(),
            r#" "" is invalid: patch: Invalid value: "{\"fresh\":3}": strict decoding error: duplicate field "fresh", unknown field "fresh""#
        );
    }
}
