{ pkgs, module, package }:
let
  inherit (pkgs) lib;

  pod = node: name: container: {
    apiVersion = "v1";
    kind = "Pod";
    metadata = { inherit name; namespace = "default"; };
    spec = {
      nodeName = node;
      restartPolicy = "Always";
      containers = [ ({ inherit name; } // container) ];
    };
  };

  pods = node: lib.concatMapStringsSep "\n---\n" builtins.toJSON [
    (pod node "hog" {
      image = "nix:${pkgs.stress-ng}";
      command = [ "stress-ng" "--vm" "1" "--vm-bytes" "256M" "--vm-keep" "--timeout" "0" ];
      resources.limits.memory = "64Mi";
    })
    (pod node "spin" {
      image = "nix:${pkgs.stress-ng}";
      command = [ "stress-ng" "--cpu" "1" "--timeout" "0" ];
      resources.limits.cpu = "250m";
    })
    (pod node "idle" {
      image = "nix:${pkgs.coreutils}";
      command = [ "sleep" "infinity" ];
    })
  ];

  node = name: extra: {
    imports = [ module extra ];
    networking.hostName = name;
    virtualisation = { memorySize = 2048; cores = 2; };
    environment.systemPackages = [ pkgs.attr pkgs.procps ];
    environment.etc."engenho/manifests.d/pods.yaml".text = pods name;
    services.engenho = {
      enable = true;
      inherit package;
      daemon.enable = true;
    };
  };
in
pkgs.testers.runNixOSTest {
  name = "engenho-native-cgroups";

  nodes = {
    delegated = node "delegated" {
      services.engenho.config.runtime = { kubeletBackend = "native"; nodeName = "delegated"; };
    };
    undelegated = node "undelegated" {
      services.engenho.settings = lib.mkForce {
        runtime = { kubelet_backend = "native"; native_cgroups = "delegated"; node_name = "undelegated"; };
      };
    };
  };

  testScript = ''
    start_all()

    delegated.wait_for_unit("engenho-daemon.service")
    unit = "/sys/fs/cgroup" + delegated.succeed("systemctl show -p ControlGroup --value engenho-daemon.service").strip()
    workloads = f"{unit}/workloads"
    delegated.succeed(f"test \"$(getfattr --only-values -n user.delegate {unit})\" = 1")
    delegated.wait_until_succeeds(f"test -s {unit}/supervisor/cgroup.procs", timeout=120)

    delegated.wait_until_succeeds("pgrep -x sleep", timeout=300)
    idle = delegated.succeed("pgrep -x sleep").strip()
    delegated.succeed(f"grep -qx {idle} {unit}/supervisor/cgroup.procs")

    spin = f"{workloads}/default_spin_spin"
    delegated.wait_until_succeeds(f"grep -qx '25000 100000' {spin}/cpu.max", timeout=300)
    delegated.wait_until_succeeds(
        f"awk '$1 == \"throttled_usec\" && $2 > 0 {{ hit = 1 }} END {{ exit !hit }}' {spin}/cpu.stat",
        timeout=120,
    )

    delegated.wait_until_succeeds(
        f"awk '$1 == \"oom_kill\" && $2 >= 1 {{ hit = 1 }} END {{ exit !hit }}' {workloads}/memory.events",
        timeout=300,
    )
    leaf = unit.removeprefix("/sys/fs/cgroup") + "/workloads/default_hog_hog"
    delegated.succeed(f"journalctl -k --no-pager | grep -F 'oom_memcg={leaf},'")
    delegated.succeed("systemctl is-active engenho-daemon.service")
    delegated.succeed("test \"$(systemctl show -p NRestarts --value engenho-daemon.service)\" = 0")
    delegated.succeed(f"kill -0 {idle}")
    delegated.succeed(f"grep -qx '25000 100000' {spin}/cpu.max")

    undelegated.wait_for_unit("engenho-daemon.service")
    plain = "/sys/fs/cgroup" + undelegated.succeed("systemctl show -p ControlGroup --value engenho-daemon.service").strip()
    undelegated.fail(f"test \"$(getfattr --only-values -n user.delegate {plain})\" = 1")
    undelegated.wait_until_succeeds("pgrep -x sleep", timeout=300)
    undelegated.wait_until_succeeds(
        "journalctl -u engenho-daemon.service --no-pager | grep -F 'cannot enforce'",
        timeout=300,
    )
    undelegated.succeed(f"test ! -e {plain}/workloads")
    undelegated.fail("pgrep -x stress-ng")
    undelegated.succeed("systemctl is-active engenho-daemon.service")
  '';
}
