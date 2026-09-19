//! T3.2b — the catalog is sealed. Nothing outside the crate can take a copy
//! of it, and the read surface that replaced `current_catalog()` clones only
//! what it hands back.
//!
//! | behaviour | pinned by | tier |
//! |---|---|---|
//! | the catalog type cannot be named outside the crate | `compile_fail,E0603` doctest in `lib.rs` | type: `pub(crate)` under `#![deny(private_interfaces)]` |
//! | `current_catalog()` is gone | `compile_fail,E0599` doctest in `lib.rs` | type |
//! | no public method of a catalog-holding type returns an owned collection without a declared bound | `no_public_store_signature_returns_an_undeclared_collection` | test gate over the source |
//! | the scanner behind that gate sees what it must, and only that | `the_signature_scanner_*` | test |
//! | scalar reads and visitors allocate nothing catalog-sized | `the_read_surface_clones_nothing_it_does_not_return_*` | test |
//! | `for_each_resource` visits every stored object once, in key order, with its meta | `for_each_resource_visits_every_object_once_with_its_meta` | test |
//! | `for_each_change_since` is the watch replay's window, and refuses below the watermark without visiting | `for_each_change_since_*` | test |
//! | `last_applied_index` tracks applies and `wait_for_applied` reads it | `last_applied_index_tracks_applies` | test |
//!
//! What is only CAUGHT, not impossible: an owned collection derived from the
//! catalog (a `Vec` of every resource, a copy of the ring) returned from a
//! public method. The type system cannot tell a scoped `Vec` from a whole
//! one, so the gate below requires every such signature to carry a declared
//! [`Bound`] — and `Bound` has no arm for "the whole catalog". A reviewer
//! can still mislabel one; that part is review.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{
    CompactedTooOld, InProcessRouter, ResourceKey, Revision, StoreMesh, WatchOpts, WatchSignal,
    default_config,
};
use serde_json::json;
use support::{
    bytes_allocated_by, durable_mesh, memory_mesh, put, release_secret,
    seed_bulk_then_one_configmap,
};

// =================================================================
// The public-signature gate
// =================================================================

/// Why a public signature may hand out an owned collection.
///
/// ★ There is deliberately no arm for "the whole catalog" or "the replay
/// ring": such a signature has no representable justification here, so the
/// gate below fails until it is removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bound {
    /// One GVK, optionally one namespace: the LIST the caller asked for.
    Scope,
    /// At most `limit` items of one scope.
    Page,
    /// Derived from something other than the catalog.
    NotTheCatalog,
}

/// Every public method of a catalog-holding type that returns an owned
/// collection, and why that is bounded. A new one fails the gate until it is
/// classified here; an entry whose method is gone fails it too, so the list
/// cannot rot.
const DECLARED: &[(&str, &str, Bound)] = &[
    ("StoreMesh", "list", Bound::Scope),
    ("StoreMesh", "list_at_revision", Bound::Scope),
    ("StoreMesh", "list_page_at_revision", Bound::Page),
    ("InMemoryStore", "list_at_revision", Bound::Scope),
    ("InMemoryStore", "list_page_at_revision", Bound::Page),
    ("FjallStore", "list_at_revision", Bound::Scope),
    ("FjallStore", "list_page_at_revision", Bound::Page),
    // Hit counts per `ImageInconsistency` kind: bounded by that enum.
    ("StoreMesh", "image_tripwire", Bound::NotTheCatalog),
    ("FjallStore", "image_tripwire", Bound::NotTheCatalog),
];

/// The crate's own sources, as `(path, text)`, excluding the test-only
/// `catalog_tests` suites (declared `#[cfg(test)]` in `lib.rs`).
fn crate_sources() -> Vec<(String, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .expect("read src")
            .map(|e| e.expect("dir entry").path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "catalog_tests") {
                    continue;
                }
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).expect("read source");
                out.push((path.display().to_string(), text));
            }
        }
    }
    let mut out = Vec::new();
    walk(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut out,
    );
    out
}

#[test]
fn no_public_store_signature_returns_an_undeclared_collection() {
    let sources = crate_sources();
    let texts: Vec<&str> = sources.iter().map(|(_, t)| t.as_str()).collect();
    let found = signature_scan::collection_returns(&texts);

    // The scan must reach the store at all: every catalog-holding handle is
    // seen as a holder, and its known list method is found. Without this a
    // scanner that saw nothing would pass the checks below vacuously.
    for holder in ["StoreMesh", "InMemoryStore", "FjallStore"] {
        assert!(
            found.holders.contains(holder),
            "the scan did not recognise {holder} as holding the catalog; holders: {:?}",
            found.holders
        );
    }
    assert!(
        found.methods.len() >= DECLARED.len(),
        "the scan found {} collection-returning methods, fewer than the {} declared: {:?}",
        found.methods.len(),
        DECLARED.len(),
        found.methods
    );

    let declared: BTreeMap<(&str, &str), Bound> =
        DECLARED.iter().map(|(t, m, b)| ((*t, *m), *b)).collect();
    let undeclared: Vec<&(String, String)> = found
        .methods
        .iter()
        .filter(|(t, m)| !declared.contains_key(&(t.as_str(), m.as_str())))
        .collect();
    assert!(
        undeclared.is_empty(),
        "a public method of a catalog-holding type returns an owned collection with no \
         declared bound: {undeclared:?}. If it returns the whole catalog or its replay ring, \
         that is the clone T3.2b removed; otherwise add it to DECLARED with its Bound"
    );

    let stale: Vec<(&(&str, &str), &Bound)> = declared
        .iter()
        .filter(|((t, m), _)| !found.methods.iter().any(|(ft, fm)| ft == t && fm == m))
        .collect();
    assert!(
        stale.is_empty(),
        "DECLARED names methods the scan no longer finds returning a collection: {stale:?}"
    );
}

/// The scanner's positive and negative controls, on a source written to
/// trip every way a text scan goes wrong: braces inside comments, strings
/// and char literals; `pub(crate)`; a `#[cfg(test)]` impl; a trait impl; a
/// `where` clause; a struct that carries a collection; a holder reached only
/// through another holder.
#[test]
fn the_signature_scanner_flags_exactly_the_collection_returns_of_holders() {
    const SRC: &str = r##"
        pub(crate) struct ResourceCatalog { resources: u8 }
        struct Inner { catalog: ResourceCatalog }
        pub struct Leaky { inner: std::sync::Arc<Inner> }
        pub struct Wrapper { leaky: Leaky }
        pub struct Unrelated { n: u8 }
        pub struct Bag { pub items: Vec<u8> }
        pub struct Nested { pub bag: Bag }

        impl Leaky {
            /// A brace in a doc comment: { and a fake `pub fn f() -> Vec<u8> {`
            pub fn name(&self) -> &'static str { "}{ pub fn g() -> Vec<u8> {" }
            pub fn everything(&self) -> Vec<(ResourceKey, ResourceValue)> {
                let _open = '{';
                let _quote = '\'';
                let _dquote = '"';
                let _raw = r#"}"#;
                /* } nested /* { */ */
                todo!()
            }
            pub(crate) fn internal(&self) -> Vec<u8> { Vec::new() }
            #[cfg(test)]
            pub fn fixture(&self, _: [u8; 4]) -> Vec<u8> { Vec::new() }
            pub async fn history(&self) -> std::collections::VecDeque<Change>
            where
                Self: Sized,
            {
                todo!()
            }
            pub fn one<'a>(&'a self, k: &'a str) -> Option<&'a str> { Some(k) }
            pub fn bag(&self) -> Result<Nested, ()> { todo!() }
            pub fn generic<F: Fn(u8) -> Vec<u8>>(&self, f: F) -> u8 { 0 }
        }
        impl Wrapper {
            pub fn map(&self) -> BTreeMap<String, u8> { todo!() }
        }
        impl Unrelated {
            pub fn names(&self) -> Vec<String> { Vec::new() }
        }
        impl Clone for Leaky {
            fn clone(&self) -> Self { todo!() }
        }
        #[cfg(test)]
        mod tests {
            impl super::Leaky {
                pub fn test_only(&self) -> Vec<u8> { vec![] }
            }
        }
    "##;
    let found = signature_scan::collection_returns(&[SRC]);
    let got: Vec<(&str, &str)> = found
        .methods
        .iter()
        .map(|(t, m)| (t.as_str(), m.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("Leaky", "bag"),
            ("Leaky", "everything"),
            ("Leaky", "history"),
            ("Wrapper", "map"),
        ],
        "flagged: a Vec, a VecDeque behind a where clause, a struct carrying a Vec two \
         levels down, and a holder reached through another holder. Not flagged: a \
         borrowed Option, a Fn bound in the generics, pub(crate), a #[cfg(test)] impl, a \
         non-holder, and braces inside comments, strings and char literals"
    );
    assert!(found.holders.contains("Leaky") && found.holders.contains("Wrapper"));
    assert!(!found.holders.contains("Unrelated"));
}

/// A text scan over Rust source, just deep enough for the gate above.
///
/// Comments, string and char literals are blanked first, so braces and
/// keywords inside them are invisible; `#[cfg(test)]` items are blanked
/// next. A "holder" is any type whose definition names the catalog or
/// another holder; a "carrier" is any `pub` type whose definition names an
/// owned collection or another carrier. The result is every `pub` method in
/// an inherent impl of a holder whose return type names a collection or a
/// carrier.
mod signature_scan {
    use std::collections::BTreeSet;

    const COLLECTIONS: &[&str] = &[
        "Vec",
        "VecDeque",
        "BTreeMap",
        "HashMap",
        "BTreeSet",
        "HashSet",
        "BinaryHeap",
        "LinkedList",
    ];

    /// What [`collection_returns`] found.
    pub struct Found {
        /// Every type that holds the catalog, directly or through another.
        pub holders: BTreeSet<String>,
        /// `(holder, method)` for every public method returning a collection.
        pub methods: BTreeSet<(String, String)>,
    }

    pub fn collection_returns(sources: &[&str]) -> Found {
        let cleaned: Vec<Vec<Tok>> = sources
            .iter()
            .map(|s| tokens(&without_test_items(&code_only(s))))
            .collect();
        let defs: Vec<TypeDef> = cleaned.iter().flat_map(|t| type_defs(t)).collect();

        let holders = fixpoint(&defs, |d| d.names("ResourceCatalog"), false);
        let carriers = fixpoint(&defs, |d| COLLECTIONS.iter().any(|c| d.names(c)), true);

        let mut methods = BTreeSet::new();
        for toks in &cleaned {
            for (self_ty, name, ret) in pub_methods(toks) {
                if !holders.contains(&self_ty) {
                    continue;
                }
                let hit = ret
                    .iter()
                    .any(|t| COLLECTIONS.contains(&t.as_str()) || carriers.contains(t.as_str()));
                if hit {
                    methods.insert((self_ty, name));
                }
            }
        }
        Found { holders, methods }
    }

    /// The least set of type names closed under "its body names a member",
    /// seeded by `seed`. `pub_only` restricts membership to `pub` types.
    fn fixpoint(
        defs: &[TypeDef],
        seed: impl Fn(&TypeDef) -> bool,
        pub_only: bool,
    ) -> BTreeSet<String> {
        let eligible = |d: &&TypeDef| !pub_only || d.is_pub;
        let mut set: BTreeSet<String> = defs
            .iter()
            .filter(eligible)
            .filter(|d| seed(d))
            .map(|d| d.name.clone())
            .collect();
        loop {
            let before = set.len();
            for d in defs.iter().filter(eligible) {
                if set.iter().any(|m| d.names(m)) {
                    set.insert(d.name.clone());
                }
            }
            if set.len() == before {
                return set;
            }
        }
    }

    /// A struct, enum or type alias: its name, whether it is plain `pub`,
    /// and the identifiers in its body.
    struct TypeDef {
        name: String,
        is_pub: bool,
        body: Vec<String>,
    }

    impl TypeDef {
        fn names(&self, ident: &str) -> bool {
            self.body.iter().any(|t| t == ident)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Tok(String);

    impl Tok {
        fn is(&self, s: &str) -> bool {
            self.0 == s
        }
    }

    /// Identifiers become one token each; every other non-space char is a
    /// token of its own.
    fn tokens(code: &str) -> Vec<Tok> {
        let mut out = Vec::new();
        let mut ident = String::new();
        for c in code.chars() {
            if c.is_alphanumeric() || c == '_' {
                ident.push(c);
                continue;
            }
            if !ident.is_empty() {
                out.push(Tok(std::mem::take(&mut ident)));
            }
            if !c.is_whitespace() {
                out.push(Tok(c.to_string()));
            }
        }
        if !ident.is_empty() {
            out.push(Tok(ident));
        }
        out
    }

    /// Index of the token that closes the bracket opened at `open`.
    fn close_of(toks: &[Tok], open: usize) -> usize {
        let (o, c) = match toks[open].0.as_str() {
            "{" => ("{", "}"),
            "(" => ("(", ")"),
            "[" => ("[", "]"),
            _ => ("<", ">"),
        };
        let mut depth = 0usize;
        for (i, t) in toks.iter().enumerate().skip(open) {
            // `->` is not a closing angle.
            if o == "<" && t.is(">") && i > 0 && toks[i - 1].is("-") {
                continue;
            }
            if t.is(o) {
                depth += 1;
            } else if t.is(c) {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
        }
        toks.len() - 1
    }

    fn type_defs(toks: &[Tok]) -> Vec<TypeDef> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < toks.len() {
            let kw = &toks[i];
            // A definition: the keyword, not `r#type`, then an identifier.
            let is_def = (kw.is("struct") || kw.is("enum") || kw.is("type"))
                && !(i > 0 && toks[i - 1].is("#"))
                && toks
                    .get(i + 1)
                    .is_some_and(|n| n.0.chars().next().is_some_and(char::is_alphabetic));
            if !is_def {
                i += 1;
                continue;
            }
            let is_pub = i > 0 && toks[i - 1].is("pub");
            let name = toks[i + 1].0.clone();
            // The body runs to the matching `}` of a braced body, else to `;`.
            let mut j = i + 2;
            while j < toks.len() && !toks[j].is("{") && !toks[j].is(";") {
                if toks[j].is("(") {
                    j = close_of(toks, j);
                }
                j += 1;
            }
            let end = if j < toks.len() && toks[j].is("{") {
                close_of(toks, j)
            } else {
                j
            };
            let body = toks[i + 2..end.min(toks.len())]
                .iter()
                .map(|t| t.0.clone())
                .collect();
            out.push(TypeDef { name, is_pub, body });
            i = end.max(i + 1);
        }
        out
    }

    /// `(self type, method, return-type tokens)` for every plain-`pub`
    /// method of an inherent impl.
    fn pub_methods(toks: &[Tok]) -> Vec<(String, String, Vec<String>)> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < toks.len() {
            if !toks[i].is("impl") {
                i += 1;
                continue;
            }
            // Header: from `impl` to its `{`.
            let mut j = i + 1;
            if j < toks.len() && toks[j].is("<") {
                j = close_of(toks, j) + 1;
            }
            let header_start = j;
            while j < toks.len() && !toks[j].is("{") && !toks[j].is(";") {
                j += 1;
            }
            if j >= toks.len() || toks[j].is(";") {
                i = j + 1;
                continue;
            }
            let body_open = j;
            let body_close = close_of(toks, body_open);
            let header = &toks[header_start..body_open];
            let is_trait_impl = header.iter().any(|t| t.is("for"));
            if let (false, Some(self_ty)) = (is_trait_impl, self_type(header)) {
                for (name, ret) in methods_in(&toks[body_open + 1..body_close]) {
                    out.push((self_ty.clone(), name, ret));
                }
            }
            i = body_close + 1;
        }
        out
    }

    /// The last identifier of the self-type path, outside any generics:
    /// `crate::store::InMemoryStore` → `InMemoryStore`, `Foo<T>` → `Foo`.
    fn self_type(header: &[Tok]) -> Option<String> {
        let mut depth = 0usize;
        let mut last = None;
        for (i, t) in header.iter().enumerate() {
            if t.is("where") && depth == 0 {
                break;
            }
            if t.is("<") {
                depth += 1;
            } else if t.is(">") && !(i > 0 && header[i - 1].is("-")) {
                depth = depth.saturating_sub(1);
            } else if depth == 0 && t.0.chars().next().is_some_and(char::is_alphabetic) {
                last = Some(t.0.clone());
            }
        }
        last
    }

    /// The plain-`pub` fns directly inside one impl body (depth 0 there).
    fn methods_in(body: &[Tok]) -> Vec<(String, Vec<String>)> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < body.len() {
            let t = &body[i];
            if t.is("{") {
                i = close_of(body, i) + 1;
                continue;
            }
            if !t.is("pub") || body.get(i + 1).is_some_and(|n| n.is("(")) {
                i += 1;
                continue;
            }
            // `pub` [const] [async] [unsafe] fn NAME
            let mut j = i + 1;
            while j < body.len()
                && (body[j].is("const") || body[j].is("async") || body[j].is("unsafe"))
            {
                j += 1;
            }
            if !body.get(j).is_some_and(|f| f.is("fn")) || j + 1 >= body.len() {
                i += 1;
                continue;
            }
            let name = body[j + 1].0.clone();
            let mut k = j + 2;
            if k < body.len() && body[k].is("<") {
                k = close_of(body, k) + 1;
            }
            if k < body.len() && body[k].is("(") {
                k = close_of(body, k) + 1;
            }
            let mut ret = Vec::new();
            if k + 1 < body.len() && body[k].is("-") && body[k + 1].is(">") {
                k += 2;
                let mut depth = 0usize;
                while k < body.len() {
                    let r = &body[k];
                    if depth == 0 && (r.is("{") || r.is(";") || r.is("where")) {
                        break;
                    }
                    if r.is("<") || r.is("(") || r.is("[") {
                        depth += 1;
                    } else if (r.is(">") && !body[k - 1].is("-")) || r.is(")") || r.is("]") {
                        depth = depth.saturating_sub(1);
                    }
                    ret.push(r.0.clone());
                    k += 1;
                }
            }
            out.push((name, ret));
            i = k;
        }
        out
    }

    /// `code` with every `#[cfg(test)]` item blanked: to its `;`, or
    /// through the matching `}` of its body.
    fn without_test_items(code: &str) -> String {
        const ATTR: &str = "#[cfg(test)]";
        let mut chars: Vec<char> = code.chars().collect();
        let mut from = 0;
        loop {
            let text: String = chars.iter().collect();
            let Some(byte_at) = text[char_to_byte(&text, from)..].find(ATTR) else {
                break;
            };
            let start = text[..char_to_byte(&text, from) + byte_at].chars().count();
            let mut i = start + ATTR.chars().count();
            let mut depth = 0usize;
            // `(` and `[` nesting, so the `;` in `[u8; 4]` does not end the item.
            let mut nest = 0usize;
            let mut end = chars.len();
            while i < chars.len() {
                match chars[i] {
                    '(' | '[' => nest += 1,
                    ')' | ']' => nest = nest.saturating_sub(1),
                    '{' => depth += 1,
                    '}' => {
                        depth = depth.saturating_sub(1);
                        if depth == 0 {
                            end = i + 1;
                            break;
                        }
                    }
                    ';' if depth == 0 && nest == 0 => {
                        end = i + 1;
                        break;
                    }
                    _ => {}
                }
                i += 1;
            }
            for c in &mut chars[start..end] {
                if *c != '\n' {
                    *c = ' ';
                }
            }
            from = end;
        }
        chars.into_iter().collect()
    }

    fn char_to_byte(s: &str, chars: usize) -> usize {
        s.char_indices().nth(chars).map_or(s.len(), |(b, _)| b)
    }

    /// `src` with every comment, string literal and char literal blanked to
    /// spaces (newlines kept), so nothing inside them reads as code.
    pub fn code_only(src: &str) -> String {
        let c: Vec<char> = src.chars().collect();
        let mut out: Vec<char> = Vec::with_capacity(c.len());
        let blank = |out: &mut Vec<char>, ch: char| out.push(if ch == '\n' { '\n' } else { ' ' });
        let ident_before = |i: usize| i > 0 && (c[i - 1].is_alphanumeric() || c[i - 1] == '_');
        let mut i = 0;
        while i < c.len() {
            let at = |k: usize| c.get(k).copied();
            // `// …` to end of line.
            if c[i] == '/' && at(i + 1) == Some('/') {
                while i < c.len() && c[i] != '\n' {
                    blank(&mut out, c[i]);
                    i += 1;
                }
                continue;
            }
            // `/* … */`, nested.
            if c[i] == '/' && at(i + 1) == Some('*') {
                let mut depth = 0usize;
                while i < c.len() {
                    if c[i] == '/' && at(i + 1) == Some('*') {
                        depth += 1;
                        out.extend([' ', ' ']);
                        i += 2;
                    } else if c[i] == '*' && at(i + 1) == Some('/') {
                        depth -= 1;
                        out.extend([' ', ' ']);
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        blank(&mut out, c[i]);
                        i += 1;
                    }
                }
                continue;
            }
            // Raw strings: r"…", r#"…"#, br"…" (not the raw identifier r#type).
            let raw_at = if c[i] == 'r' && !ident_before(i) {
                Some(i + 1)
            } else if c[i] == 'b' && at(i + 1) == Some('r') && !ident_before(i) {
                Some(i + 2)
            } else {
                None
            };
            if let Some(mut j) = raw_at {
                let mut hashes = 0;
                while at(j) == Some('#') {
                    hashes += 1;
                    j += 1;
                }
                if at(j) == Some('"') {
                    let mut k = j + 1;
                    'close: while k < c.len() {
                        if c[k] == '"' && (1..=hashes).all(|h| at(k + h) == Some('#')) {
                            k += 1 + hashes;
                            break 'close;
                        }
                        k += 1;
                    }
                    for &ch in &c[i..k.min(c.len())] {
                        blank(&mut out, ch);
                    }
                    i = k;
                    continue;
                }
            }
            // "…" with escapes (b"…" reaches here at its quote).
            if c[i] == '"' {
                blank(&mut out, c[i]);
                i += 1;
                while i < c.len() && c[i] != '"' {
                    if c[i] == '\\' {
                        blank(&mut out, c[i]);
                        i += 1;
                    }
                    if i < c.len() {
                        blank(&mut out, c[i]);
                        i += 1;
                    }
                }
                if i < c.len() {
                    blank(&mut out, c[i]);
                    i += 1;
                }
                continue;
            }
            // 'x' and '\n' are char literals; 'a (a lifetime) is code.
            if c[i] == '\'' {
                // An escape is at least two chars (`\'`, `\n`, `\u{…}`), so
                // the closing quote is searched from the third char on.
                let end = if at(i + 1) == Some('\\') {
                    (i + 3..c.len()).find(|&k| c[k] == '\'')
                } else if at(i + 2) == Some('\'') {
                    Some(i + 2)
                } else {
                    None
                };
                if let Some(end) = end {
                    for &ch in &c[i..=end] {
                        blank(&mut out, ch);
                    }
                    i = end + 1;
                    continue;
                }
            }
            out.push(c[i]);
            i += 1;
        }
        out.into_iter().collect()
    }
}

// =================================================================
// The read surface: behaviour
// =================================================================

fn pod(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

/// A replay from revision zero is the one read whose cost IS the ring, by
/// design; it is the positive control that the counter counts and the seed
/// is big enough to tell a clone from a scalar.
async fn ring_bytes(mesh: &StoreMesh) -> usize {
    let (replay, bytes) =
        bytes_allocated_by(mesh.watch_from(WatchOpts::from_revision(Revision::ZERO))).await;
    drop(replay.expect("nothing is compacted yet"));
    assert!(
        bytes > 2 * 1024 * 1024,
        "positive control: replaying the ring allocated only {bytes} bytes — the counting \
         allocator or the seed is broken, so every bound below would prove nothing"
    );
    bytes
}

/// Every read that replaced `current_catalog()` costs what it returns. Each
/// of them used to be a deep clone of every resource plus the replay ring,
/// which on rio held megabytes of Helm release Secrets.
async fn assert_the_read_surface_clones_nothing_it_does_not_return(mesh: &StoreMesh) {
    seed_bulk_then_one_configmap(mesh).await;
    let ring = ring_bytes(mesh).await;
    const SMALL: usize = 64 * 1024;

    let (applied, bytes) = bytes_allocated_by(mesh.last_applied_index()).await;
    assert!(applied > 200, "every seed write applied: {applied}");
    assert!(
        bytes < SMALL,
        "last_applied_index allocated {bytes} bytes against {ring} for the ring"
    );

    let (_, bytes) = bytes_allocated_by(mesh.compacted_revision()).await;
    assert!(bytes < SMALL, "compacted_revision allocated {bytes} bytes");

    let (_, bytes) = bytes_allocated_by(mesh.current_revision()).await;
    assert!(bytes < SMALL, "current_revision allocated {bytes} bytes");

    let (done, bytes) =
        bytes_allocated_by(mesh.wait_for_applied(applied, Duration::from_secs(1))).await;
    assert!(done);
    assert!(bytes < SMALL, "wait_for_applied allocated {bytes} bytes");

    let configmap = ResourceKey::namespaced("", "v1", "ConfigMap", "default", "one");
    let (got, bytes) = bytes_allocated_by(mesh.get_with_meta(&configmap)).await;
    assert!(got.is_some());
    assert!(
        bytes < SMALL,
        "get_with_meta of one small object allocated {bytes} bytes"
    );

    let mut visited = 0usize;
    let (_, bytes) = bytes_allocated_by(mesh.for_each_resource(|_, _, _| visited += 1)).await;
    assert_eq!(visited, 2, "the Secret and the ConfigMap");
    assert!(
        bytes < SMALL,
        "visiting every resource allocated {bytes} bytes: the visitor path is cloning"
    );

    let mut changes = 0usize;
    let (window, bytes) =
        bytes_allocated_by(mesh.for_each_change_since(Revision::ZERO, |_| changes += 1)).await;
    assert_eq!(window, Ok(()));
    assert_eq!(changes, 201, "200 Secret rewrites and one ConfigMap");
    assert!(
        bytes < SMALL,
        "visiting the whole replay window allocated {bytes} bytes against {ring} to replay \
         it: the change visitor is cloning the ring it only has to read"
    );
}

// Multi-thread flavor: the test body is polled on this thread while raft's
// tasks run on the workers, so the thread-local counter sees only the read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_read_surface_clones_nothing_it_does_not_return_memory() {
    let mesh = memory_mesh("t3-2b-memory").await;
    assert_the_read_surface_clones_nothing_it_does_not_return(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_read_surface_clones_nothing_it_does_not_return_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path(), "t3-2b-durable").await;
    assert_the_read_surface_clones_nothing_it_does_not_return(&mesh).await;
}

#[tokio::test]
async fn for_each_resource_visits_every_object_once_with_its_meta() {
    let mesh = memory_mesh("t3-2b-visit").await;
    let keys = [
        pod("b"),
        pod("a"),
        ResourceKey::cluster_scoped("", "v1", "Node", "n1"),
        ResourceKey::namespaced("apps", "v1", "Deployment", "default", "d"),
    ];
    for (i, k) in keys.iter().enumerate() {
        put(&mesh, k.clone(), json!({ "i": i })).await;
    }
    // Rewrite one so its meta is not the create meta.
    put(&mesh, pod("a"), json!({ "i": 99 })).await;

    let mut seen = Vec::new();
    mesh.for_each_resource(|key, value, meta| seen.push((key.clone(), value.clone(), meta)))
        .await;

    let mut expected_keys: Vec<ResourceKey> = keys.to_vec();
    expected_keys.sort();
    assert_eq!(
        seen.iter().map(|(k, _, _)| k.clone()).collect::<Vec<_>>(),
        expected_keys,
        "every stored object exactly once, in key order"
    );
    for (key, value, meta) in &seen {
        let (v, m) = mesh.get_with_meta(key).await.expect("stored");
        assert_eq!(
            (value, meta),
            (&v, &m),
            "{key:?}: the visitor saw what get_with_meta reads"
        );
    }
    let (_, a_meta) = mesh.get_with_meta(&pod("a")).await.expect("stored");
    assert_eq!(a_meta.version, 2, "the rewrite is visible in the meta");
    assert!(mesh.get_with_meta(&pod("absent")).await.is_none());
}

/// The change visitor reads exactly the window a watch replay from the same
/// revision delivers — the same rule, not a second copy of it.
#[tokio::test]
async fn for_each_change_since_is_the_watch_replays_window() {
    let mesh = memory_mesh("t3-2b-window").await;
    for i in 0..6u32 {
        put(&mesh, pod(&format!("p{}", i % 3)), json!({ "i": i })).await;
    }
    let now = mesh.current_revision().await;
    for from in 0..=now.get() {
        let mut visited = Vec::new();
        mesh.for_each_change_since(Revision(from), |c| visited.push(c.revision.get()))
            .await
            .expect("nothing compacted");

        let mut stream = mesh
            .watch_from(WatchOpts::from_revision(Revision(from)))
            .await
            .expect("nothing compacted");
        let mut replayed = Vec::new();
        while let Some(Ok(WatchSignal::Event(ev))) = stream.try_next() {
            replayed.push(ev.resource_version);
        }
        assert_eq!(visited, replayed, "the window after revision {from}");
        assert_eq!(
            visited,
            (from + 1..=now.get()).collect::<Vec<_>>(),
            "every change after {from}, in order"
        );
    }
}

/// A resume point below the watermark is refused with the watermark, and
/// the visitor never runs — a caller cannot mistake part of the window for
/// all of it. A durable store that reopens starts with its watermark at the
/// revision it loaded (T3.3), which is how the refusal is reached here.
#[tokio::test]
async fn for_each_change_since_refuses_below_the_watermark_without_visiting() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let mesh = durable_mesh(dir.path(), "t3-2b-gone").await;
        for name in ["a", "b", "c"] {
            put(&mesh, pod(name), json!({})).await;
        }
        Arc::try_unwrap(mesh)
            .ok()
            .expect("sole owner")
            .terminate()
            .await
            .expect("terminate");
    }
    let (mesh, fresh) = StoreMesh::start_or_resume(
        1,
        "in-process://1".into(),
        InProcessRouter::new(),
        default_config("t3-2b-gone").expect("config"),
        dir.path(),
    )
    .await
    .expect("resume");
    assert!(!fresh);
    assert!(mesh.wait_for_leadership(support::LEADERSHIP).await);
    assert_eq!(mesh.current_revision().await, Revision(3));
    assert_eq!(mesh.compacted_revision().await, Revision(3));

    let mut visits = 0usize;
    let refused = mesh
        .for_each_change_since(Revision(1), |_| visits += 1)
        .await;
    assert_eq!(
        refused,
        Err(CompactedTooOld {
            requested: Revision(1),
            compacted: Revision(3),
        })
    );
    assert_eq!(visits, 0, "a refused window visits nothing");

    let at_watermark = mesh
        .for_each_change_since(Revision(3), |_| visits += 1)
        .await;
    assert_eq!(at_watermark, Ok(()), "the watermark itself is servable");
    assert_eq!(visits, 0);
    mesh.terminate().await.expect("terminate");
}

#[tokio::test]
async fn last_applied_index_tracks_applies() {
    let mesh = memory_mesh("t3-2b-applied").await;
    let before = mesh.last_applied_index().await;
    let result = mesh
        .propose(ResourceCommand::Put {
            key: release_secret(),
            value: json!({ "data": {} }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .expect("propose");
    let after = mesh.last_applied_index().await;
    assert!(after > before, "{before} → {after}");
    assert_eq!(after, result.applied_index, "the index the apply reported");
    assert!(
        mesh.wait_for_applied(after, Duration::from_millis(100))
            .await
    );
    assert!(
        !mesh
            .wait_for_applied(after + 1_000, Duration::from_millis(100))
            .await,
        "an index never applied is waited for, not reported"
    );
}
