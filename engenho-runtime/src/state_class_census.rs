use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use engenho_types::generated_v1_34::RESOURCE_CATALOG;
use engenho_types::state_class::StoredKind;

use crate::impl_census::{Source, workspace_sources};
use crate::read_census::{Tok, call_args, lex, store_call, strip_test_items};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Keyed {
    pub(crate) group: Option<String>,
    pub(crate) kind: String,
    pub(crate) path: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KindCensus {
    pub(crate) keyed: BTreeSet<Keyed>,
    pub(crate) computed: usize,
    pub(crate) sites: usize,
}

const LIST_READS: &[&str] = &["list", "list_at_revision", "list_page_at_revision"];

impl KindCensus {
    pub(crate) fn of(sources: &[Source]) -> Self {
        let test_only = test_only_files(sources);
        let shipped: Vec<&Source> = sources
            .iter()
            .filter(|s| !test_only.contains(&s.path))
            .collect();
        let global = string_consts(shipped.iter().map(|s| s.text.as_str()));
        let mut census = Self::default();
        for source in shipped {
            census.scan(source, &global);
        }
        census
    }

    fn scan(&mut self, source: &Source, global: &BTreeMap<String, Option<String>>) {
        let tokens = strip_test_items(&lex(&source.text));
        let local = string_consts(std::iter::once(source.text.as_str()));
        for i in 0..tokens.len() {
            let args = if let Some(open) = key_constructor_at(&tokens, i) {
                call_args(&tokens, open)
            } else if store_call(&tokens, i).is_some_and(|m| LIST_READS.contains(&m)) {
                call_args(&tokens, i + 2)
            } else {
                continue;
            };
            self.sites += 1;
            let resolve = |arg: Option<&Vec<Tok>>| arg.and_then(|a| resolve(a, &local, global));
            match resolve(args.get(2)) {
                Some(kind) => {
                    self.keyed.insert(Keyed {
                        group: resolve(args.first()),
                        kind,
                        path: source.path.clone(),
                    });
                }
                None => self.computed += 1,
            }
        }
    }

    pub(crate) fn kinds(&self) -> BTreeSet<(Option<&str>, &str)> {
        self.keyed
            .iter()
            .map(|k| (k.group.as_deref(), k.kind.as_str()))
            .collect()
    }
}

fn key_constructor_at(tokens: &[Tok], i: usize) -> Option<usize> {
    let hit = tokens.get(i..i + 5).is_some_and(|w| {
        w[0] == Tok::Ident("ResourceKey".into())
            && w[1] == Tok::Punct(':')
            && w[2] == Tok::Punct(':')
            && matches!(&w[3], Tok::Ident(c) if c == "namespaced" || c == "cluster_scoped")
            && w[4] == Tok::Punct('(')
    });
    hit.then_some(i + 4)
}

fn resolve(
    arg: &[Tok],
    local: &BTreeMap<String, Option<String>>,
    global: &BTreeMap<String, Option<String>>,
) -> Option<String> {
    if let [Tok::Str(s)] = arg {
        return Some(s.clone());
    }
    let is_path = arg
        .iter()
        .all(|t| matches!(t, Tok::Ident(_) | Tok::Punct(':')));
    let Some(Tok::Ident(name)) = arg.last().filter(|_| is_path) else {
        return None;
    };
    local
        .get(name)
        .or_else(|| global.get(name))
        .cloned()
        .flatten()
}

fn string_consts<'a>(texts: impl Iterator<Item = &'a str>) -> BTreeMap<String, Option<String>> {
    let mut consts: BTreeMap<String, Option<String>> = BTreeMap::new();
    for text in texts {
        let tokens = strip_test_items(&lex(text));
        for i in 0..tokens.len() {
            if tokens[i] != Tok::Ident("const".into()) {
                continue;
            }
            let (Some(Tok::Ident(name)), Some(Tok::Punct(':'))) =
                (tokens.get(i + 1), tokens.get(i + 2))
            else {
                continue;
            };
            let Some(eq) = (i + 3..tokens.len())
                .take_while(|j| tokens[*j] != Tok::Punct(';'))
                .find(|j| tokens[*j] == Tok::Punct('='))
            else {
                continue;
            };
            let names_str = tokens[i + 3..eq].contains(&Tok::Ident("str".into()));
            if let (true, Some(Tok::Str(value)), Some(Tok::Punct(';'))) =
                (names_str, tokens.get(eq + 1), tokens.get(eq + 2))
            {
                consts
                    .entry(name.clone())
                    .and_modify(|seen| {
                        if seen.as_deref() != Some(value.as_str()) {
                            *seen = None;
                        }
                    })
                    .or_insert_with(|| Some(value.clone()));
            }
        }
    }
    consts
}

fn test_only_files(sources: &[Source]) -> BTreeSet<String> {
    let mut dirs: BTreeSet<String> = BTreeSet::new();
    let mut files: BTreeSet<String> = BTreeSet::new();
    for source in sources {
        for name in cfg_test_mods(&lex(&source.text)) {
            let path = Path::new(&source.path);
            let parent = path.parent().unwrap_or_else(|| Path::new(""));
            let owns_dir = matches!(
                path.file_name().and_then(|n| n.to_str()),
                Some("lib.rs" | "main.rs" | "mod.rs")
            );
            let base = if owns_dir {
                parent.to_path_buf()
            } else {
                parent.join(path.file_stem().unwrap_or_default())
            };
            files.insert(base.join(format!("{name}.rs")).display().to_string());
            dirs.insert(base.join(&name).display().to_string());
        }
    }
    sources
        .iter()
        .map(|s| s.path.clone())
        .filter(|p| {
            files.contains(p) || dirs.iter().any(|d| Path::new(p).starts_with(Path::new(d)))
        })
        .collect()
}

fn cfg_test_mods(tokens: &[Tok]) -> Vec<String> {
    let cfg_test = [
        Tok::Punct('#'),
        Tok::Punct('['),
        Tok::Ident("cfg".into()),
        Tok::Punct('('),
        Tok::Ident("test".into()),
        Tok::Punct(')'),
        Tok::Punct(']'),
    ];
    let mut names = Vec::new();
    for i in 0..tokens.len() {
        if tokens.get(i..i + cfg_test.len()) != Some(&cfg_test[..]) {
            continue;
        }
        let mut j = i + cfg_test.len();
        while tokens.get(j) == Some(&Tok::Punct('#')) {
            let Some(close) = (j..tokens.len()).find(|k| tokens[*k] == Tok::Punct(']')) else {
                break;
            };
            j = close + 1;
        }
        if tokens.get(j) == Some(&Tok::Ident("pub".into())) {
            j += 1;
            if tokens.get(j) == Some(&Tok::Punct('(')) {
                j = (j..tokens.len())
                    .find(|k| tokens[*k] == Tok::Punct(')'))
                    .map_or(j, |k| k + 1);
            }
        }
        if let (Some(Tok::Ident(kw)), Some(Tok::Ident(name)), Some(Tok::Punct(';'))) =
            (tokens.get(j), tokens.get(j + 1), tokens.get(j + 2))
            && kw == "mod"
        {
            names.push(name.clone());
        }
    }
    names
}

pub(crate) fn unclassified<'a>(census: &'a KindCensus, registry: &[StoredKind]) -> Vec<&'a Keyed> {
    census
        .keyed
        .iter()
        .filter(|k| {
            !registry
                .iter()
                .any(|row| row.kind == k.kind && k.group.as_deref().is_none_or(|g| g == row.group))
        })
        .collect()
}

pub(crate) fn undeclared<'a>(
    census: &KindCensus,
    registry: &'a [StoredKind],
) -> Vec<&'a StoredKind> {
    registry
        .iter()
        .filter(|row| {
            let in_catalog = RESOURCE_CATALOG
                .iter()
                .any(|d| d.group == row.group && d.kind == row.kind);
            let keyed = census
                .keyed
                .iter()
                .any(|k| k.kind == row.kind && k.group.as_deref() == Some(row.group));
            !in_catalog && !keyed
        })
        .collect()
}

pub(crate) fn workspace_census() -> KindCensus {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the runtime crate sits inside the workspace");
    KindCensus::of(&workspace_sources(root))
}

#[cfg(test)]
mod tests {
    use engenho_types::state_class::{STORED_KINDS, StateClass, StoredKind};

    use super::{KindCensus, Source, unclassified, undeclared, workspace_census};

    fn source(path: &str, text: &str) -> Source {
        Source {
            path: path.to_owned(),
            krate: "fixture".to_owned(),
            text: text.to_owned(),
        }
    }

    fn fixture() -> Vec<Source> {
        vec![
            source(
                "fx/src/lib.rs",
                r#"
                pub const GROUP: &str = "example.com";
                const WIDGET_KIND: &'static str = "Widget";
                #[cfg(test)]
                mod fakes;
                #[cfg(test)]
                #[allow(dead_code)]
                pub(crate) mod helpers;
                fn put(store: &Store) {
                    let a = ResourceKey::namespaced("", "v1", "Pod", "ns", "a");
                    let b = ResourceKey::cluster_scoped(GROUP, "v1", WIDGET_KIND, "b");
                    let c = engenho_store::resource::ResourceKey::namespaced(
                        crate::GROUP, "v1", self::WIDGET_KIND, "ns", "c",
                    );
                    store.list("apps", "v1", "Deployment", None);
                    store.list_at_revision(g, v, kind, None, 3);
                    let d = ResourceKey::namespaced(g, v, &kind, ns, n);
                    let e = ResourceKey::cluster_scoped(Node::GVK.group, "v1", Node::GVK.kind, n);
                    // ResourceKey::namespaced("", "v1", "Commented", "ns", "x");
                    let s = "ResourceKey::namespaced(\"\", \"v1\", \"Quoted\", \"ns\", \"x\")";
                }
                #[cfg(test)]
                mod tests {
                    fn t() { ResourceKey::namespaced("", "v1", "InlineTest", "ns", "x"); }
                }
                "#,
            ),
            source(
                "fx/src/fakes.rs",
                r#"fn f() { ResourceKey::namespaced("", "v1", "FromTestFile", "ns", "x"); }"#,
            ),
            source(
                "fx/src/helpers.rs",
                r#"fn f() { ResourceKey::namespaced("", "v1", "FromTestHelpers", "ns", "x"); }"#,
            ),
            source(
                "fx/src/deep/mod.rs",
                r#"
                #[cfg(test)]
                mod suite;
                fn f() { ResourceKey::namespaced("", "v1", "Service", "ns", "x"); }
                "#,
            ),
            source(
                "fx/src/deep/suite/mod.rs",
                r#"fn f() { ResourceKey::namespaced("", "v1", "FromTestDir", "ns", "x"); }"#,
            ),
            source(
                "fx/src/deep/suite/more.rs",
                r#"fn f() { ResourceKey::namespaced("", "v1", "FromTestSubDir", "ns", "x"); }"#,
            ),
        ]
    }

    #[test]
    fn the_scan_sees_every_shape_it_claims_and_nothing_under_test() {
        let census = KindCensus::of(&fixture());
        let kinds: Vec<(Option<&str>, &str)> = census.kinds().into_iter().collect();
        assert_eq!(
            kinds,
            [
                (Some(""), "Pod"),
                (Some(""), "Service"),
                (Some("apps"), "Deployment"),
                (Some("example.com"), "Widget"),
            ]
        );
        assert_eq!(census.sites, 8, "{census:?}");
        assert_eq!(census.computed, 3, "{census:?}");
    }

    #[test]
    fn an_unregistered_kind_is_reported_and_a_registered_one_is_not() {
        let census = KindCensus::of(&fixture());
        let missing: Vec<(Option<&str>, &str)> = unclassified(&census, STORED_KINDS)
            .into_iter()
            .map(|k| (k.group.as_deref(), k.kind.as_str()))
            .collect();
        assert_eq!(missing, [(Some("example.com"), "Widget")]);

        let with_widget = [StoredKind {
            group: "example.com",
            kind: "Widget",
            class: StateClass::Declared,
        }];
        let registry: Vec<StoredKind> = STORED_KINDS.iter().copied().chain(with_widget).collect();
        assert!(unclassified(&census, &registry).is_empty());

        let declared = |group, kind| StoredKind {
            group,
            kind,
            class: StateClass::Declared,
        };
        let stale = declared("example.com", "Gadget");
        let registry = [
            declared("", "Pod"),
            declared("example.com", "Widget"),
            stale,
        ];
        assert_eq!(undeclared(&census, &registry), [&stale]);
    }

    #[test]
    fn the_workspace_scan_finds_what_it_must() {
        let census = workspace_census();
        assert!(
            census.sites >= 100,
            "only {} keyed store sites",
            census.sites
        );
        let kinds = census.kinds();
        let distinct: std::collections::BTreeSet<&str> = kinds.iter().map(|(_, k)| *k).collect();
        assert!(
            distinct.len() >= 30,
            "only {} distinct kinds: {distinct:?}",
            distinct.len()
        );
        for control in [
            (Some("engenho.io"), "Derivation"),
            (Some("engenho.io"), "MaterializationReceipt"),
            (Some("rbac.authorization.k8s.io"), "ClusterRole"),
            (Some(""), "Pod"),
        ] {
            assert!(kinds.contains(&control), "the scan missed {control:?}");
        }
    }

    #[test]
    fn every_stored_kind_has_a_state_class() {
        let census = workspace_census();
        let missing = unclassified(&census, STORED_KINDS);
        assert!(
            missing.is_empty(),
            "the store keys these kinds and engenho_types::state_class::STORED_KINDS gives them \
             no StateClass (docs/FLEET-DESIGN.md §1): {missing:#?}"
        );
    }

    #[test]
    fn every_state_class_row_names_a_kind_something_stores() {
        let census = workspace_census();
        let stale = undeclared(&census, STORED_KINDS);
        assert!(
            stale.is_empty(),
            "these STORED_KINDS rows are neither in RESOURCE_CATALOG nor keyed by any shipped \
             source: {stale:#?}"
        );
    }
}
