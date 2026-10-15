//! `Node.status.nodeInfo`, measured on this host.

use engenho_types::generated_v1_34::types::NodeSystemInfo;

use crate::backend::ContainerRuntime;

const OS_RELEASE: &[&str] = &["/etc/os-release", "/usr/lib/os-release"];
const MACOS_VERSION: &str = "/System/Library/CoreServices/SystemVersion.plist";
const MACHINE_ID: &[&str] = &["/etc/machine-id", "/var/lib/dbus/machine-id"];
const PRODUCT_UUID: &str = "/sys/class/dmi/id/product_uuid";
const BOOT_ID: &str = "/proc/sys/kernel/random/boot_id";

/// This host's `nodeInfo` for a kubelet driving `runtime`. A field this host
/// cannot measure is left empty, as upstream leaves it.
#[must_use]
pub fn measured(runtime: &dyn ContainerRuntime) -> NodeSystemInfo {
    NodeSystemInfo {
        architecture: kube_arch().to_owned(),
        boot_id: read_trimmed(&[BOOT_ID]),
        container_runtime_version: runtime
            .version()
            .map(|v| format!("{}://{v}", runtime.name()))
            .unwrap_or_default(),
        kernel_version: rustix::system::uname()
            .release()
            .to_string_lossy()
            .into_owned(),
        kube_proxy_version: String::new(),
        kubelet_version: kubelet_version(),
        machine_id: read_trimmed(MACHINE_ID),
        operating_system: kube_os().to_owned(),
        os_image: os_image(),
        swap: None,
        system_uuid: read_trimmed(&[PRODUCT_UUID]),
    }
}

/// The version this kubelet reports: the Kubernetes API face it serves, with
/// engenho's own release as build metadata.
#[must_use]
pub fn kubelet_version() -> String {
    format!(
        "{}+engenho.{}",
        engenho_types::KUBE_VERSION,
        env!("CARGO_PKG_VERSION")
    )
}

/// Kubernetes' `kubernetes.io/arch` label speaks Go's `GOARCH`, not Rust's
/// `std::env::consts::ARCH`.
///
/// The two disagree on exactly the values that matter here: Rust says
/// `aarch64` and `x86_64` where Kubernetes says `arm64` and `amd64`. This
/// is the difference between a label that works and a label that looks
/// right and matches nothing — the reference pangea Postgres carries
/// `nodeSelector: {kubernetes.io/arch: arm64}`, and against an `aarch64`
/// label the scheduler's `NodeSelector` filter (T5.7, an exact match)
/// rejects the node with `NodeSelectorMismatch`. Registration re-asserts the
/// label on every boot, so the pod stays Pending for good, with a
/// correct-looking label visible in `kubectl get node -o yaml`.
///
/// Unknown architectures pass through verbatim rather than guessing: a
/// wrong-but-plausible label is worse than an unfamiliar one, because it
/// matches a selector that meant something else.
#[must_use]
pub fn kube_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        "arm" => "arm",
        "powerpc64" => "ppc64le",
        "s390x" => "s390x",
        other => other,
    }
}

/// The `kubernetes.io/os` label, in Go's `GOOS` vocabulary.
///
/// Rust says `macos`; Kubernetes says `darwin`. Same failure mode as
/// [`kube_arch`].
#[must_use]
pub fn kube_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

fn read_trimmed(paths: &[&str]) -> String {
    paths
        .iter()
        .filter_map(|p| std::fs::read_to_string(p).ok())
        .map(|s| s.trim().to_owned())
        .find(|s| !s.is_empty())
        .unwrap_or_default()
}

fn os_image() -> String {
    OS_RELEASE
        .iter()
        .filter_map(|p| std::fs::read_to_string(p).ok())
        .find_map(|text| pretty_name(&text))
        .or_else(|| {
            std::fs::read_to_string(MACOS_VERSION)
                .ok()
                .and_then(|text| macos_product(&text))
        })
        .unwrap_or_default()
}

fn pretty_name(os_release: &str) -> Option<String> {
    os_release.lines().find_map(|line| {
        let value = line.trim().strip_prefix("PRETTY_NAME=")?;
        let value = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
            .unwrap_or(value);
        (!value.is_empty()).then(|| value.to_owned())
    })
}

fn plist_string<'a>(plist: &'a str, key: &str) -> Option<&'a str> {
    let after_key = plist.split(&format!("<key>{key}</key>")).nth(1)?;
    let start = after_key.find("<string>")? + "<string>".len();
    let end = after_key[start..].find("</string>")?;
    Some(after_key[start..start + end].trim())
}

fn macos_product(plist: &str) -> Option<String> {
    let name = plist_string(plist, "ProductName")?;
    let version = plist_string(plist, "ProductVersion")?;
    Some(format!("{name} {version}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_image_is_the_os_release_pretty_name() {
        let nixos =
            "NAME=NixOS\nID=nixos\nPRETTY_NAME=\"NixOS 25.11 (Xantusia)\"\nVERSION_ID=\"25.11\"\n";
        assert_eq!(
            pretty_name(nixos).as_deref(),
            Some("NixOS 25.11 (Xantusia)")
        );
        assert_eq!(
            pretty_name("PRETTY_NAME=Alpine\n").as_deref(),
            Some("Alpine")
        );
        assert_eq!(pretty_name("NAME=x\n"), None);
        assert_eq!(pretty_name("PRETTY_NAME=\"\"\n"), None);
    }

    #[test]
    fn macos_reports_its_product_name_and_version() {
        let plist = "<dict>\n\t<key>BuildID</key>\n\t<string>X</string>\n\t<key>ProductName</key>\n\t<string>macOS</string>\n\t<key>ProductVersion</key>\n\t<string>26.7.1</string>\n</dict>";
        assert_eq!(macos_product(plist).as_deref(), Some("macOS 26.7.1"));
        assert_eq!(macos_product("<dict></dict>"), None);
    }

    #[test]
    fn the_kubelet_version_is_the_api_face_with_engenho_as_build_metadata() {
        let v = kubelet_version();
        assert!(v.starts_with(engenho_types::KUBE_VERSION), "{v}");
        assert!(v.contains("+engenho."), "{v}");
    }

    #[test]
    fn this_host_reports_what_it_can_measure() {
        let info = measured(&crate::backend::FakeBackend::new());
        assert!(!info.architecture.is_empty());
        assert!(!info.operating_system.is_empty());
        assert!(!info.kernel_version.is_empty());
        assert!(!info.kubelet_version.is_empty());
        assert!(info.kube_proxy_version.is_empty());
        assert!(
            info.container_runtime_version.is_empty(),
            "a runtime that states no version is not given one"
        );
        if cfg!(target_os = "linux") {
            assert!(!info.boot_id.is_empty());
        }
    }
}
