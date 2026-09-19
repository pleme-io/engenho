//! Which pods a scheduler places.

/// The namespaces a [`crate::Scheduler`] places pods from.
///
/// `scheduler.namespace: ""` is the config's spelling of [`Self::All`].
/// There is no arm for "the namespace named empty". The store lists no pod
/// under that name, so a scheduler scoped to it would place nothing while
/// every tick reported success. [`ScopedNamespace`] cannot hold an empty
/// name, so that scope cannot be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceScope {
    /// Pods in every namespace.
    All,
    /// Pods in exactly this namespace.
    Only(ScopedNamespace),
}

/// The namespace a scheduler is scoped to. Never empty: the only way to
/// build one is [`NamespaceScope::from_name`], which reads an empty name as
/// [`NamespaceScope::All`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedNamespace(String);

impl ScopedNamespace {
    /// The namespace name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl NamespaceScope {
    /// Read a namespace the way `scheduler.namespace` spells it: empty is
    /// every namespace, anything else is that one namespace.
    #[must_use]
    pub fn from_name(name: &str) -> Self {
        if name.is_empty() {
            Self::All
        } else {
            Self::Only(ScopedNamespace(name.to_owned()))
        }
    }

    /// The store's namespace filter for a Pod list. `None` lists every
    /// namespace.
    #[must_use]
    pub fn list_filter(&self) -> Option<&str> {
        match self {
            Self::All => None,
            Self::Only(ns) => Some(ns.as_str()),
        }
    }
}

/// The spelling [`crate::Scheduler::new`] takes. `Some("")` reads as
/// [`NamespaceScope::All`], the same as the config's empty string.
impl From<Option<String>> for NamespaceScope {
    fn from(namespace: Option<String>) -> Self {
        namespace
            .as_deref()
            .map_or(Self::All, NamespaceScope::from_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_name_is_every_namespace() {
        assert_eq!(NamespaceScope::from_name(""), NamespaceScope::All);
        assert_eq!(NamespaceScope::All.list_filter(), None);
    }

    #[test]
    fn a_name_is_that_namespace_alone() {
        let scope = NamespaceScope::from_name("team-a");
        assert_eq!(scope.list_filter(), Some("team-a"));
    }

    #[test]
    fn an_optional_empty_name_never_scopes_to_the_empty_namespace() {
        assert_eq!(NamespaceScope::from(None), NamespaceScope::All);
        assert_eq!(
            NamespaceScope::from(Some(String::new())),
            NamespaceScope::All
        );
        assert_eq!(
            NamespaceScope::from(Some("team-a".to_owned())).list_filter(),
            Some("team-a")
        );
    }
}
