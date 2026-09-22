//! Resolving `User=` / `Group=` / `SupplementaryGroups=` into the numbers the
//! kernel takes, and refusing what cannot be resolved honestly.
//!
//! ## `DynamicUser=yes` is REFUSED without an override
//!
//! A dynamic user is allocated by systemd for the lifetime of the unit, with
//! `/var/lib/private/<name>` behind a bind mount and a uid that changes. None
//! of that exists here, and the failure of guessing is not visible: the
//! service would run as root, write root-owned state, and only misbehave
//! later. So a unit with `DynamicUser=yes` is refused unless the caller names
//! a real account with `--user`, which is exactly what the Nix side does (it
//! declares a static `users.users.<name>` for the workload).

use std::fmt;

use crate::accounts::{AccountError, Database};
use crate::specifier::AccountFacts;
use crate::unit::IdentityRequest;

/// What `--user` / `--group` said.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overrides {
    /// `--user`.
    pub user: Option<String>,
    /// `--group`.
    pub group: Option<String>,
}

/// Who the service runs as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identity {
    /// No `User=` and no override: the service keeps the daemon's own
    /// credentials (root, under the engenho daemon's unit).
    Invoker,
    /// A resolved account: privileges are dropped to it.
    Account(Account),
}

/// A resolved account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    /// The user name.
    pub name: String,
    /// Its uid.
    pub uid: u32,
    /// The primary gid the service runs with.
    pub gid: u32,
    /// The primary group's name.
    pub group: String,
    /// Every gid `setgroups` is called with: the primary one and each
    /// supplementary group, in ascending order.
    pub groups: Vec<u32>,
    /// `$HOME`, and what `%h` expands to.
    pub home: String,
    /// `$SHELL`.
    pub shell: String,
}

impl Account {
    /// The facts the `%u`, `%U`, `%g`, `%G`, `%h`, `%s` specifiers expand to.
    #[must_use]
    pub fn facts(&self) -> AccountFacts {
        AccountFacts {
            user: self.name.clone(),
            uid: self.uid,
            group: self.group.clone(),
            gid: self.gid,
            home: self.home.clone(),
            shell: self.shell.clone(),
        }
    }
}

impl Identity {
    /// The specifier facts for this identity — root's when there is no
    /// account, which is what systemd expands them to for a unit with no
    /// `User=`.
    #[must_use]
    pub fn facts(&self) -> AccountFacts {
        match self {
            Self::Invoker => AccountFacts::root(),
            Self::Account(account) => account.facts(),
        }
    }

    /// The account, if privileges are to be dropped.
    #[must_use]
    pub const fn account(&self) -> Option<&Account> {
        match self {
            Self::Invoker => None,
            Self::Account(account) => Some(account),
        }
    }
}

/// Why an identity could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    /// An account database lookup failed.
    Accounts(AccountError),
    /// `DynamicUser=yes` with no `--user` override.
    DynamicUser {
        /// The `User=` the unit names, which is the DYNAMIC name, not an
        /// account that exists.
        declared: Option<String>,
    },
    /// `Group=` without `User=`: this runner changes both together or
    /// neither.
    GroupWithoutUser {
        /// The group asked for.
        group: String,
    },
    /// A user with no primary group in `/etc/group`.
    NoPrimaryGroup {
        /// The user.
        user: String,
        /// Its gid.
        gid: u32,
    },
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accounts(e) => write!(f, "{e}"),
            Self::DynamicUser { declared } => {
                f.write_str("DynamicUser=yes: this runner allocates no users")?;
                if let Some(declared) = declared {
                    write!(f, " (the unit's User={declared} names the DYNAMIC user)")?;
                }
                f.write_str(
                    ". Declare a real account on the node and pass --user <name> [--group <name>]",
                )
            }
            Self::GroupWithoutUser { group } => write!(
                f,
                "Group={group} without User=: this runner changes user and group together, so \
                 pass --user <name> as well"
            ),
            Self::NoPrimaryGroup { user, gid } => {
                write!(f, "{user}'s primary group {gid} is in no /etc/group entry")
            }
        }
    }
}

impl std::error::Error for IdentityError {}

impl From<AccountError> for IdentityError {
    fn from(e: AccountError) -> Self {
        Self::Accounts(e)
    }
}

/// Resolve what the unit asked for, with `overrides` winning.
///
/// # Errors
///
/// An [`IdentityError`] for a dynamic user with no override, an unknown user
/// or group, or a `Group=` with no user.
pub fn resolve(
    request: &IdentityRequest,
    overrides: &Overrides,
    db: &Database,
) -> Result<Identity, IdentityError> {
    let named = overrides.user.clone().or_else(|| {
        if request.dynamic {
            None
        } else {
            request.user.clone()
        }
    });
    let Some(named) = named else {
        if request.dynamic {
            return Err(IdentityError::DynamicUser {
                declared: request.user.clone(),
            });
        }
        if let Some(group) = overrides.group.clone().or_else(|| request.group.clone()) {
            return Err(IdentityError::GroupWithoutUser { group });
        }
        return Ok(Identity::Invoker);
    };

    let user = db.user(&named)?.clone();
    // `Group=` from the unit is ignored when a dynamic user was overridden:
    // it names the dynamic group, which does not exist either.
    let declared_group = overrides.group.clone().or_else(|| {
        if request.dynamic {
            None
        } else {
            request.group.clone()
        }
    });
    let group = if let Some(declared) = declared_group {
        db.group(&declared)?
    } else {
        db.group_by_gid(user.gid)
            .ok_or_else(|| IdentityError::NoPrimaryGroup {
                user: user.name.clone(),
                gid: user.gid,
            })?
    };
    let (gid, group_name) = (group.gid, group.name.clone());

    // What `initgroups(3)` would produce: the primary gid, every group that
    // lists the user as a member, and every SupplementaryGroups= entry.
    let mut groups = vec![gid];
    groups.extend(db.memberships(&user.name));
    for name in &request.supplementary {
        groups.push(db.group(name)?.gid);
    }
    groups.sort_unstable();
    groups.dedup();

    Ok(Identity::Account(Account {
        name: user.name,
        uid: user.uid,
        gid,
        group: group_name,
        groups,
        home: user.home,
        shell: user.shell,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    const PASSWD: &str = "root:x:0:0::/root:/bin/sh\n\
         hass:x:286:286::/var/lib/hass:/bin/nologin\n\
         zwave-js:x:310:310::/var/empty:/bin/nologin\n";
    const GROUP: &str = "root:x:0:\n\
         hass:x:286:\n\
         zwave-js:x:310:\n\
         dialout:x:27:zwave-js\n\
         render:x:303:hass\n\
         video:x:26:\n";

    fn db() -> Database {
        Database::parse(
            PASSWD,
            GROUP,
            Path::new("/etc/passwd"),
            Path::new("/etc/group"),
        )
        .unwrap()
    }

    fn request(
        user: Option<&str>,
        group: Option<&str>,
        supplementary: &[&str],
        dynamic: bool,
    ) -> IdentityRequest {
        IdentityRequest {
            user: user.map(ToString::to_string),
            group: group.map(ToString::to_string),
            supplementary: supplementary.iter().map(ToString::to_string).collect(),
            dynamic,
        }
    }

    #[test]
    fn a_static_user_resolves_with_its_groups() {
        let identity = resolve(
            &request(Some("hass"), Some("hass"), &["video"], false),
            &Overrides::default(),
            &db(),
        )
        .unwrap();
        let account = identity.account().unwrap();
        assert_eq!((account.uid, account.gid), (286, 286));
        assert_eq!(
            account.groups,
            [26, 286, 303],
            "the primary gid, SupplementaryGroups=video, and /etc/group membership of render"
        );
        assert_eq!(account.home, "/var/lib/hass");
        assert_eq!(identity.facts().user, "hass");
    }

    #[test]
    fn dynamic_user_is_refused_without_an_override_and_resolved_with_one() {
        let dynamic = request(Some("zwave-js"), None, &["dialout"], true);
        assert_eq!(
            resolve(&dynamic, &Overrides::default(), &db()),
            Err(IdentityError::DynamicUser {
                declared: Some("zwave-js".into())
            })
        );
        let overridden = Overrides {
            user: Some("zwave-js".into()),
            group: None,
        };
        let account = resolve(&dynamic, &overridden, &db()).unwrap();
        let account = account.account().unwrap();
        assert_eq!((account.uid, account.gid), (310, 310));
        assert_eq!(account.groups, [27, 310]);
    }

    #[test]
    fn no_user_is_the_invoker_and_a_lone_group_is_refused() {
        assert_eq!(
            resolve(
                &request(None, None, &[], false),
                &Overrides::default(),
                &db()
            )
            .unwrap(),
            Identity::Invoker
        );
        assert_eq!(
            resolve(
                &request(None, Some("video"), &[], false),
                &Overrides::default(),
                &db()
            ),
            Err(IdentityError::GroupWithoutUser {
                group: "video".into()
            })
        );
    }

    #[test]
    fn an_unknown_user_or_group_is_named() {
        assert!(matches!(
            resolve(
                &request(Some("nope"), None, &[], false),
                &Overrides::default(),
                &db()
            ),
            Err(IdentityError::Accounts(AccountError::UnknownUser { .. }))
        ));
        assert!(matches!(
            resolve(
                &request(Some("hass"), None, &["nope"], false),
                &Overrides::default(),
                &db()
            ),
            Err(IdentityError::Accounts(AccountError::UnknownGroup { .. }))
        ));
    }

    #[test]
    fn an_override_wins_over_the_units_user() {
        let identity = resolve(
            &request(Some("hass"), Some("hass"), &[], false),
            &Overrides {
                user: Some("zwave-js".into()),
                group: Some("video".into()),
            },
            &db(),
        )
        .unwrap();
        let account = identity.account().unwrap();
        assert_eq!(account.name, "zwave-js");
        assert_eq!((account.gid, account.group.as_str()), (26, "video"));
    }
}
