//! The read census: the kinds a controller's source reads from the store by
//! literal kind (T1.7). Test-only.
//!
//! A controller's [`engenho_controllers::DeclaresReads`] declaration is what
//! its driver's wake filter is derived from, so a kind the controller reads
//! and does not declare is a kind whose changes it sleeps through. This
//! census is the check on the declaration: it lexes the controller's own
//! source and collects the kind of every store read written with a literal
//! kind —
//!
//! * `store.list(group, version, "Kind", ns)` and the other `list*` reads;
//! * `store.get(&ResourceKey::namespaced(group, version, "Kind", ..))`,
//!   inline or through a `let key = ResourceKey::…(..)` binding above it;
//! * `store.current_catalog()`, which reads every kind.
//!
//! It is a gate, not a proof. A read whose kind is computed at run time
//! (from a table, an owner reference, a helper that builds the key
//! elsewhere) is counted as computed and not named; a store handle not
//! called `store` is not seen. Items under `#[cfg(test)]` are skipped, so a
//! test's reads never count against a controller.

use std::collections::BTreeSet;

/// A slice of a source file that is one controller's.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Section {
    /// Where the text came from, for failure messages.
    pub(crate) path: &'static str,
    text: &'static str,
    from: Option<&'static str>,
    until: Option<&'static str>,
}

impl Section {
    /// A whole file.
    pub(crate) const fn file(path: &'static str, text: &'static str) -> Self {
        Self {
            path,
            text,
            from: None,
            until: None,
        }
    }

    /// The part of a file from the first `from` up to the first `until`
    /// after it (or the end).
    pub(crate) const fn between(
        path: &'static str,
        text: &'static str,
        from: &'static str,
        until: Option<&'static str>,
    ) -> Self {
        Self {
            path,
            text,
            from: Some(from),
            until,
        }
    }

    /// The text of the section. Panics (this is test code) when a marker is
    /// missing, so a renamed type fails the census loudly instead of
    /// shrinking what it scans.
    fn text(&self) -> &'static str {
        let start = self.from.map_or(0, |marker| {
            self.text
                .find(marker)
                .unwrap_or_else(|| panic!("{}: section marker {marker:?} not found", self.path))
        });
        let rest = &self.text[start..];
        let end = self.until.map_or(rest.len(), |marker| {
            rest.find(marker)
                .unwrap_or_else(|| panic!("{}: section end {marker:?} not found", self.path))
        });
        &rest[..end]
    }
}

/// What a census of some source found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Census {
    /// Kinds read by literal.
    pub(crate) kinds: BTreeSet<String>,
    /// Store reads whose kind is not a literal here.
    pub(crate) computed: usize,
    /// Whether the source reads the whole catalog.
    pub(crate) every: bool,
}

impl Census {
    /// The census of every section, merged.
    pub(crate) fn of(sections: &[Section]) -> Self {
        let mut census = Self::default();
        for section in sections {
            census.scan(&lex(section.text()));
        }
        census
    }

    fn scan(&mut self, tokens: &[Tok]) {
        let tokens = strip_test_items(tokens);
        for i in 0..tokens.len() {
            let Some(method) = store_call(&tokens, i) else {
                continue;
            };
            let args = call_args(&tokens, i + 2);
            match method {
                "list" | "list_at_revision" | "list_page_at_revision" => {
                    match args.get(2).and_then(|arg| single_str(arg)) {
                        Some(kind) => {
                            self.kinds.insert(kind.to_owned());
                        }
                        None => self.computed += 1,
                    }
                }
                "get" => match args.first().and_then(|key| keyed_kind(&tokens, i, key)) {
                    Some(kind) => {
                        self.kinds.insert(kind);
                    }
                    None => self.computed += 1,
                },
                "current_catalog" => self.every = true,
                _ => {}
            }
        }
    }
}

/// A token of Rust source, as much of it as the census needs. Shared with
/// the impl census (T5.11), which reads the same sources for a different
/// question.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Tok {
    Ident(String),
    Str(String),
    Punct(char),
    /// A number, a lifetime or a char literal: nothing the census reads.
    Other,
}

/// Lex `src` into tokens, dropping comments and whitespace. String, raw
/// string and char literals are read whole, so text inside them is never
/// mistaken for code.
pub(crate) fn lex(src: &str) -> Vec<Tok> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c.is_whitespace() {
            i += 1;
        } else if c == '/' && next == Some('/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && next == Some('*') {
            i = skip_block_comment(&chars, i);
        } else if c == '"' {
            let (text, end) = read_str(&chars, i + 1);
            out.push(Tok::Str(text));
            i = end;
        } else if let Some((tok, end)) = (c == 'r' || c == 'b')
            .then(|| raw_or_byte_string(&chars, i))
            .flatten()
        {
            out.push(tok);
            i = end;
        } else if c == '\'' {
            i = skip_char_or_lifetime(&chars, i);
            out.push(Tok::Other);
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            out.push(Tok::Ident(chars[start..i].iter().collect()));
        } else if c.is_ascii_digit() {
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            out.push(Tok::Other);
        } else {
            out.push(Tok::Punct(c));
            i += 1;
        }
    }
    out
}

fn skip_block_comment(chars: &[char], start: usize) -> usize {
    let mut depth = 0usize;
    let mut i = start;
    while i < chars.len() {
        if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
            depth += 1;
            i += 2;
        } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    i
}

/// A `"…"` body starting after the opening quote: its text and the index
/// after the closing quote.
fn read_str(chars: &[char], mut i: usize) -> (String, usize) {
    let mut text = String::new();
    while i < chars.len() {
        match chars[i] {
            '\\' => {
                if let Some(escaped) = chars.get(i + 1) {
                    text.push(*escaped);
                }
                i += 2;
            }
            '"' => return (text, i + 1),
            c => {
                text.push(c);
                i += 1;
            }
        }
    }
    (text, i)
}

/// `r"…"`, `r#"…"#`, `b"…"`, `br"…"` at `i`, if one starts there.
fn raw_or_byte_string(chars: &[char], i: usize) -> Option<(Tok, usize)> {
    let mut j = i;
    if chars.get(j) == Some(&'b') {
        j += 1;
    }
    let raw = chars.get(j) == Some(&'r');
    if raw {
        j += 1;
    }
    // A preceding identifier character means this `r`/`b` is inside a word.
    if i > 0 && (chars[i - 1].is_alphanumeric() || chars[i - 1] == '_') {
        return None;
    }
    if !raw {
        return (j > i && chars.get(j) == Some(&'"')).then(|| {
            let (text, end) = read_str(chars, j + 1);
            (Tok::Str(text), end)
        });
    }
    let hashes = chars[j..].iter().take_while(|c| **c == '#').count();
    j += hashes;
    if chars.get(j) != Some(&'"') {
        return None;
    }
    let body = j + 1;
    let mut k = body;
    while k < chars.len() {
        if chars[k] == '"' && chars[k + 1..].iter().take(hashes).all(|c| *c == '#') {
            let text: String = chars[body..k].iter().collect();
            return Some((Tok::Str(text), k + 1 + hashes));
        }
        k += 1;
    }
    Some((Tok::Other, chars.len()))
}

/// Skip a char literal (`'x'`, `'\n'`, `'\''`, `'"'`) or a lifetime (`'a`)
/// at `i`.
fn skip_char_or_lifetime(chars: &[char], i: usize) -> usize {
    match (chars.get(i + 1), chars.get(i + 2)) {
        (Some('\\'), _) => {
            // Past the backslash and the character it escapes, which may
            // itself be a quote; then to the closing quote.
            let mut j = i + 3;
            while j < chars.len() && chars[j] != '\'' {
                j += 1;
            }
            j + 1
        }
        (Some(_), Some('\'')) => i + 3,
        _ => {
            let mut j = i + 1;
            while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            j
        }
    }
}

/// `tokens` without any item annotated `#[cfg(test)]`.
pub(crate) fn strip_test_items(tokens: &[Tok]) -> Vec<Tok> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        if is_cfg_test(tokens, i) {
            i = end_of_item(tokens, i + 7);
        } else {
            out.push(tokens[i].clone());
            i += 1;
        }
    }
    out
}

fn is_cfg_test(tokens: &[Tok], i: usize) -> bool {
    let want = [
        Tok::Punct('#'),
        Tok::Punct('['),
        Tok::Ident("cfg".into()),
        Tok::Punct('('),
        Tok::Ident("test".into()),
        Tok::Punct(')'),
        Tok::Punct(']'),
    ];
    tokens.get(i..i + want.len()) == Some(&want[..])
}

/// The index after the item starting at `i`: through its `;` or its
/// brace-matched body, whichever comes first at depth zero.
fn end_of_item(tokens: &[Tok], mut i: usize) -> usize {
    let mut depth = 0usize;
    while i < tokens.len() {
        match tokens[i] {
            Tok::Punct('(' | '[') => depth += 1,
            Tok::Punct(')' | ']') => depth = depth.saturating_sub(1),
            Tok::Punct(';') if depth == 0 => return i + 1,
            Tok::Punct('{') if depth == 0 => return matching_close(tokens, i) + 1,
            _ => {}
        }
        i += 1;
    }
    i
}

/// The index of the bracket closing the one at `open`.
fn matching_close(tokens: &[Tok], open: usize) -> usize {
    let mut depth = 0usize;
    for (i, tok) in tokens.iter().enumerate().skip(open) {
        match tok {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
            _ => {}
        }
    }
    tokens.len()
}

/// When `tokens[i]` is the `.` of `store.<method>(` or `store().<method>(`,
/// the method name.
fn store_call(tokens: &[Tok], i: usize) -> Option<&str> {
    if tokens.get(i) != Some(&Tok::Punct('.')) || i == 0 {
        return None;
    }
    let Some(Tok::Ident(method)) = tokens.get(i + 1) else {
        return None;
    };
    if tokens.get(i + 2) != Some(&Tok::Punct('(')) {
        return None;
    }
    let receiver_is_store = match &tokens[i - 1] {
        Tok::Ident(name) => name == "store",
        Tok::Punct(')') => {
            i >= 3
                && tokens[i - 2] == Tok::Punct('(')
                && tokens[i - 3] == Tok::Ident("store".into())
        }
        Tok::Str(_) | Tok::Punct(_) | Tok::Other => false,
    };
    receiver_is_store.then_some(method.as_str())
}

/// The top-level, comma-separated arguments of the call whose `(` is at
/// `open`.
fn call_args(tokens: &[Tok], open: usize) -> Vec<Vec<Tok>> {
    let close = matching_close(tokens, open);
    let mut args = vec![Vec::new()];
    let mut depth = 0usize;
    for tok in &tokens[open + 1..close.min(tokens.len())] {
        match tok {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => depth = depth.saturating_sub(1),
            Tok::Punct(',') if depth == 0 => {
                args.push(Vec::new());
                continue;
            }
            _ => {}
        }
        if let Some(last) = args.last_mut() {
            last.push(tok.clone());
        }
    }
    args.retain(|a| !a.is_empty());
    args
}

/// The literal, when an argument is exactly one string literal.
fn single_str(arg: &[Tok]) -> Option<&str> {
    match arg {
        [Tok::Str(s)] => Some(s),
        _ => None,
    }
}

/// The literal kind of the key a `store.get(key)` at `call` reads: a
/// `ResourceKey::namespaced(..)`/`cluster_scoped(..)` written inline, or
/// the nearest `let <name> = ResourceKey::…(..)` above the call.
fn keyed_kind(tokens: &[Tok], call: usize, key: &[Tok]) -> Option<String> {
    let key = key.strip_prefix(&[Tok::Punct('&')][..]).unwrap_or(key);
    if let Some(kind) = key_constructor_kind(key) {
        return Some(kind);
    }
    let [Tok::Ident(name)] = key else {
        return None;
    };
    let name = Tok::Ident(name.clone());
    for i in (0..call).rev() {
        // A binding in another function is not this one's.
        if tokens[i] == Tok::Ident("fn".into()) {
            return None;
        }
        let binds_name = tokens[i] == Tok::Ident("let".into())
            && (tokens.get(i + 1) == Some(&name)
                || (tokens.get(i + 1) == Some(&Tok::Ident("mut".into()))
                    && tokens.get(i + 2) == Some(&name)));
        if binds_name {
            let eq = (i..call).find(|j| tokens[*j] == Tok::Punct('='))?;
            let semi = (eq..call).find(|j| tokens[*j] == Tok::Punct(';'))?;
            return key_constructor_kind(&tokens[eq + 1..semi]);
        }
    }
    None
}

/// The literal kind of a `…ResourceKey::namespaced(g, v, "Kind", ..)` or
/// `cluster_scoped` call that makes up `expr`.
fn key_constructor_kind(expr: &[Tok]) -> Option<String> {
    let at = expr.windows(4).position(|w| {
        w[0] == Tok::Ident("ResourceKey".into())
            && w[1] == Tok::Punct(':')
            && w[2] == Tok::Punct(':')
            && matches!(&w[3], Tok::Ident(c) if c == "namespaced" || c == "cluster_scoped")
    })?;
    let open = at + 4;
    if expr.get(open) != Some(&Tok::Punct('(')) {
        return None;
    }
    let args = call_args(expr, open);
    args.get(2)
        .and_then(|arg| single_str(arg))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn census(src: &'static str) -> Census {
        Census::of(&[Section::file("fixture.rs", src)])
    }

    fn kinds(src: &'static str) -> Vec<String> {
        census(src).kinds.into_iter().collect()
    }

    /// The positive control: every shape the census claims to see, seen.
    /// A census that went blind would pass every controller vacuously.
    #[test]
    fn it_sees_every_read_shape_it_claims() {
        let found = kinds(
            r#"
            async fn tick(&self) {
                let a = self.store.list("", "v1", "Pod", None).await;
                let b = self
                    .store
                    .list(
                        "apps",
                        "v1",
                        "ReplicaSet",
                        self.namespace.as_deref(),
                    )
                    .await;
                let c = this.store().list_at_revision("", "v1", "Node", None).await;
                let key = ResourceKey::namespaced("", "v1", "Secret", ns, &name);
                let d = self.store.get(&key).await;
                let e = store.get(&ResourceKey::cluster_scoped("", "v1", "PersistentVolume", n)).await;
                let mut slice_key = engenho_store::resource::ResourceKey::namespaced(
                    "discovery.k8s.io", "v1", "EndpointSlice", ns, svc,
                );
                let f = self.store.get(&slice_key).await;
            }
            "#,
        );
        assert_eq!(
            found,
            [
                "EndpointSlice",
                "Node",
                "PersistentVolume",
                "Pod",
                "ReplicaSet",
                "Secret"
            ]
        );
    }

    /// A key rebound between two reads: each read takes the binding
    /// nearest above it.
    #[test]
    fn a_read_takes_the_nearest_binding_above_it() {
        let found = kinds(
            r#"
            fn f() {
                let key = ResourceKey::namespaced("", "v1", "ConfigMap", ns, &n);
                store.get(&key);
                let key = ResourceKey::namespaced("", "v1", "Secret", ns, &n);
                store.get(&key);
            }
            "#,
        );
        assert_eq!(found, ["ConfigMap", "Secret"]);
    }

    /// Text that only looks like a read: comments, strings, other receivers,
    /// a JSON field lookup, a key built for a write, and test code.
    #[test]
    fn it_does_not_see_what_is_not_a_read() {
        let c = census(
            r##"
            // self.store.list("", "v1", "InAComment", None)
            /* store.list("", "v1", "InABlock", None) */
            const DOC: &str = "self.store.list(\"\", \"v1\", \"InAString\", None)";
            const RAW: &str = r#"store.list("", "v1", "InARawString", None)"#;
            const QUOTE: char = '"';
            fn f<'a>(x: &'a Value) {
                let spec = pod.get("spec");
                let m = self.local.lock().await.get(key);
                let z = zone.list();
                let put_key = ResourceKey::namespaced("", "v1", "WrittenOnly", ns, n);
                self.store.propose(Put { key: put_key });
            }
            #[cfg(test)]
            mod tests {
                fn t() { store.list("", "v1", "InATest", None); }
            }
            #[cfg(test)]
            fn helper() { store.list("", "v1", "InATestFn", None); }
            fn after() { store.list("", "v1", "AfterTheTests", None); }
            "##,
        );
        assert_eq!(c.kinds.into_iter().collect::<Vec<_>>(), ["AfterTheTests"]);
        assert_eq!(c.computed, 0);
        assert!(!c.every);
    }

    /// A binding of the same name in another function says nothing about
    /// this one's key.
    #[test]
    fn a_binding_in_another_function_is_not_this_ones() {
        let c = census(
            r#"
            fn one() { let key = ResourceKey::namespaced("", "v1", "Elsewhere", ns, n); }
            fn two(key: &ResourceKey) { self.store.get(key); }
            "#,
        );
        assert!(c.kinds.is_empty());
        assert_eq!(c.computed, 1);
    }

    /// Escaped quotes in char literals do not open a string that swallows
    /// the read after them.
    #[test]
    fn an_escaped_quote_char_does_not_hide_the_next_read() {
        let found = kinds(
            r#"
            fn f() {
                let q = '\'';
                let d = '"';
                store.list("", "v1", "Visible", None);
            }
            "#,
        );
        assert_eq!(found, ["Visible"]);
    }

    #[test]
    fn a_computed_kind_is_counted_not_named() {
        let c = census(
            r"
            fn f() {
                for d in namespaced_kinds() {
                    store.list(d.group, d.version, d.kind, Some(ns));
                }
                for key in &candidates { self.store.get(key); }
                let lease = lease_key(node);
                self.store.get(&lease);
            }
            ",
        );
        assert!(c.kinds.is_empty());
        assert_eq!(c.computed, 3);
    }

    #[test]
    fn reading_the_whole_catalog_reads_every_kind() {
        assert!(census("fn f() { let c = store.current_catalog().await; }").every);
    }

    #[test]
    fn a_section_is_the_text_between_its_markers() {
        const SRC: &str = r#"
            struct A; fn a() { store.list("", "v1", "Alpha", None); }
            struct B; fn b() { store.list("", "v1", "Beta", None); }
        "#;
        let a = Census::of(&[Section::between(
            "fixture.rs",
            SRC,
            "struct A",
            Some("struct B"),
        )]);
        let b = Census::of(&[Section::between("fixture.rs", SRC, "struct B", None)]);
        assert_eq!(a.kinds.into_iter().collect::<Vec<_>>(), ["Alpha"]);
        assert_eq!(b.kinds.into_iter().collect::<Vec<_>>(), ["Beta"]);
    }

    #[test]
    #[should_panic(expected = "section marker")]
    fn a_missing_marker_fails_loudly() {
        let _ = Census::of(&[Section::between(
            "fixture.rs",
            "fn f() {}",
            "struct Gone",
            None,
        )]);
    }
}
