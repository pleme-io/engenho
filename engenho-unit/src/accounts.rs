//! `/etc/passwd` and `/etc/group`, parsed in Rust.
//!
//! No NSS, no `getpwnam`, no libc: a lookup is a read of the two files under
//! the [`crate::layout::Layout`]'s root, which is also what makes it testable
//! against a fixture. NixOS writes both files declaratively, so the fleet's
//! accounts are entirely there — a node using LDAP or `systemd-sysusers`'
//! dynamic allocation is out of scope and shows up as an unknown user, which
//! is a typed error rather than a silent fallback to root.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};

/// One `/etc/passwd` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    /// Field 1.
    pub name: String,
    /// Field 3.
    pub uid: u32,
    /// Field 4.
    pub gid: u32,
    /// Field 6.
    pub home: String,
    /// Field 7.
    pub shell: String,
}

/// One `/etc/group` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// Field 1.
    pub name: String,
    /// Field 3.
    pub gid: u32,
    /// Field 4, split on commas.
    pub members: Vec<String>,
}

/// The two account databases.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Database {
    users: Vec<User>,
    groups: Vec<Group>,
}

/// Why the account databases could not be read or a lookup failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountError {
    /// A database could not be read.
    Unreadable {
        /// The file.
        path: PathBuf,
        /// What the OS said.
        detail: String,
    },
    /// A line with too few fields.
    Malformed {
        /// The file.
        path: PathBuf,
        /// The 1-based line.
        line: usize,
    },
    /// No such user.
    UnknownUser {
        /// The name or id looked up.
        user: String,
    },
    /// No such group.
    UnknownGroup {
        /// The name or id looked up.
        group: String,
    },
}

impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable { path, detail } => {
                write!(f, "{} cannot be read: {detail}", path.display())
            }
            Self::Malformed { path, line } => {
                write!(f, "{}: line {line} has too few fields", path.display())
            }
            Self::UnknownUser { user } => write!(
                f,
                "no user {user:?} in /etc/passwd — declare it on the node (users.users) or pass --user"
            ),
            Self::UnknownGroup { group } => write!(
                f,
                "no group {group:?} in /etc/group — declare it on the node (users.groups) or pass --group"
            ),
        }
    }
}

impl std::error::Error for AccountError {}

impl Database {
    /// Read `/etc/passwd` and `/etc/group` from `passwd` and `group`.
    ///
    /// # Errors
    ///
    /// [`AccountError::Unreadable`] or [`AccountError::Malformed`].
    pub fn read(passwd: &Path, group: &Path) -> Result<Self, AccountError> {
        let passwd_text = read(passwd)?;
        let group_text = read(group)?;
        Self::parse(&passwd_text, &group_text, passwd, group)
    }

    /// Parse both databases from their text.
    ///
    /// # Errors
    ///
    /// [`AccountError::Malformed`] naming the file and line.
    pub fn parse(
        passwd_text: &str,
        group_text: &str,
        passwd_path: &Path,
        group_path: &Path,
    ) -> Result<Self, AccountError> {
        let mut users = Vec::new();
        for (number, fields) in entries(passwd_text) {
            // name:x:uid:gid:gecos:home:shell
            let bad = || AccountError::Malformed {
                path: passwd_path.to_path_buf(),
                line: number,
            };
            if fields.len() < 7 {
                return Err(bad());
            }
            users.push(User {
                name: fields[0].to_string(),
                uid: fields[2].parse().map_err(|_| bad())?,
                gid: fields[3].parse().map_err(|_| bad())?,
                home: fields[5].to_string(),
                shell: fields[6].to_string(),
            });
        }
        let mut groups = Vec::new();
        for (number, fields) in entries(group_text) {
            // name:x:gid:member,member
            let bad = || AccountError::Malformed {
                path: group_path.to_path_buf(),
                line: number,
            };
            if fields.len() < 4 {
                return Err(bad());
            }
            groups.push(Group {
                name: fields[0].to_string(),
                gid: fields[2].parse().map_err(|_| bad())?,
                members: fields[3]
                    .split(',')
                    .filter(|m| !m.is_empty())
                    .map(ToString::to_string)
                    .collect(),
            });
        }
        Ok(Self { users, groups })
    }

    /// The user named `who`, which may also be a numeric uid.
    ///
    /// # Errors
    ///
    /// [`AccountError::UnknownUser`].
    pub fn user(&self, who: &str) -> Result<&User, AccountError> {
        let by_id = who.parse::<u32>().ok();
        self.users
            .iter()
            .find(|u| u.name == who || Some(u.uid) == by_id)
            .ok_or_else(|| AccountError::UnknownUser {
                user: who.to_string(),
            })
    }

    /// The group named `which`, which may also be a numeric gid.
    ///
    /// # Errors
    ///
    /// [`AccountError::UnknownGroup`].
    pub fn group(&self, which: &str) -> Result<&Group, AccountError> {
        let by_id = which.parse::<u32>().ok();
        self.groups
            .iter()
            .find(|g| g.name == which || Some(g.gid) == by_id)
            .ok_or_else(|| AccountError::UnknownGroup {
                group: which.to_string(),
            })
    }

    /// The group with `gid`, if the database names one.
    #[must_use]
    pub fn group_by_gid(&self, gid: u32) -> Option<&Group> {
        self.groups.iter().find(|g| g.gid == gid)
    }

    /// Every group `user` belongs to by membership — what `getgrouplist(3)`
    /// (and so systemd's `initgroups`) would return, minus the primary gid.
    ///
    /// The gids come back in `/etc/group` order, deduplicated.
    #[must_use]
    pub fn memberships(&self, user: &str) -> Vec<u32> {
        let mut seen = BTreeSet::new();
        for group in &self.groups {
            if group.members.iter().any(|m| m == user) {
                seen.insert(group.gid);
            }
        }
        seen.into_iter().collect()
    }
}

fn read(path: &Path) -> Result<String, AccountError> {
    std::fs::read_to_string(path).map_err(|e| AccountError::Unreadable {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })
}

/// Non-empty, non-comment lines with their 1-based number, split on `:`.
fn entries(text: &str) -> impl Iterator<Item = (usize, Vec<&str>)> {
    text.lines()
        .enumerate()
        .map(|(i, line)| (i + 1, line))
        .filter(|(_, line)| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
        .map(|(number, line)| (number, line.split(':').collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PASSWD: &str = "root:x:0:0:System administrator:/root:/run/current-system/sw/bin/bash\n\
         # a comment\n\
         hass:x:286:286::/var/lib/hass:/run/current-system/sw/bin/nologin\n\
         mosquitto:x:301:301::/var/lib/mosquitto:/run/current-system/sw/bin/nologin\n";
    const GROUP: &str = "root:x:0:\n\
         dialout:x:27:zwave-js,hass\n\
         hass:x:286:\n\
         render:x:303:frigate,hass\n\
         mosquitto:x:301:\n";

    fn db() -> Database {
        Database::parse(
            PASSWD,
            GROUP,
            Path::new("/etc/passwd"),
            Path::new("/etc/group"),
        )
        .unwrap()
    }

    #[test]
    fn a_user_is_found_by_name_or_by_uid() {
        let db = db();
        let hass = db.user("hass").unwrap();
        assert_eq!((hass.uid, hass.gid), (286, 286));
        assert_eq!(hass.home, "/var/lib/hass");
        assert_eq!(db.user("286").unwrap().name, "hass");
        assert_eq!(
            db.user("nope"),
            Err(AccountError::UnknownUser {
                user: "nope".into()
            })
        );
    }

    #[test]
    fn a_group_is_found_by_name_or_by_gid_and_carries_its_members() {
        let db = db();
        assert_eq!(db.group("dialout").unwrap().gid, 27);
        assert_eq!(db.group("27").unwrap().name, "dialout");
        assert_eq!(db.group("render").unwrap().members, ["frigate", "hass"]);
        assert_eq!(db.group_by_gid(301).unwrap().name, "mosquitto");
        assert!(db.group("nope").is_err());
    }

    #[test]
    fn memberships_are_what_initgroups_would_return() {
        let db = db();
        assert_eq!(db.memberships("hass"), [27, 303]);
        assert!(db.memberships("mosquitto").is_empty());
    }

    #[test]
    fn comments_and_blank_lines_are_skipped_and_a_short_line_is_an_error() {
        assert!(db().user("root").is_ok());
        let err = Database::parse(
            "root:x:0\n",
            GROUP,
            Path::new("/etc/passwd"),
            Path::new("/etc/group"),
        );
        assert_eq!(
            err,
            Err(AccountError::Malformed {
                path: "/etc/passwd".into(),
                line: 1
            })
        );
    }
}
