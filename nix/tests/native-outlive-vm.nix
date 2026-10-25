{ pkgs, module, package }:
let
  inherit (pkgs) lib;

  pods = builtins.toJSON {
    apiVersion = "v1";
    kind = "Pod";
    metadata = { name = "keeper"; namespace = "default"; };
    spec = {
      nodeName = "outlive";
      restartPolicy = "Always";
      containers = [
        {
          name = "keeper";
          image = "nix:${pkgs.coreutils}";
          command = [ "sleep" "infinity" ];
          resources.limits.memory = "64Mi";
        }
      ];
    };
  };
in
pkgs.testers.runNixOSTest {
  name = "engenho-native-outlive";

  nodes.outlive = {
    imports = [ module ];
    networking.hostName = "outlive";
    virtualisation = { memorySize = 2048; cores = 2; };
    environment.systemPackages = [ pkgs.procps ];
    environment.etc."engenho/manifests.d/pods.yaml".text = pods;
    services.engenho = {
      enable = true;
      inherit package;
      daemon.enable = true;
      config.runtime = {
        kubeletBackend = "native";
        nativeWorkloads = "outlive";
        nodeName = "outlive";
      };
    };
  };

  testScript = ''
    def keeper():
        return outlive.succeed("pgrep -x sleep").strip()

    def born(pid):
        return outlive.succeed(f"cut -d' ' -f22 /proc/{pid}/stat").strip()

    def main_pid():
        return outlive.succeed("systemctl show -p MainPID --value engenho-daemon.service").strip()

    def claims():
        return int(outlive.succeed("journalctl -u engenho-daemon.service --no-pager | grep -cF 'claimed by its pod' || true").strip())

    outlive.wait_for_unit("engenho-daemon.service")
    outlive.succeed("test \"$(systemctl show -p KillMode --value engenho-daemon.service)\" = process")
    unit = "/sys/fs/cgroup" + outlive.succeed("systemctl show -p ControlGroup --value engenho-daemon.service").strip()
    outlive.wait_until_succeeds("pgrep -x sleep", timeout=300)
    pid = keeper()
    start = born(pid)
    outlive.succeed(f"grep -qx {pid} {unit}/workloads/default_keeper_keeper/cgroup.procs")

    first = main_pid()
    outlive.succeed("systemctl restart engenho-daemon.service")
    outlive.wait_for_unit("engenho-daemon.service")
    outlive.wait_until_succeeds("journalctl -u engenho-daemon.service --no-pager | grep -F 'claimed by its pod'", timeout=300)
    assert main_pid() != first, "the daemon restarted"
    assert keeper() == pid, "the workload kept its pid across a restart"
    assert born(pid) == start, "the same process, not a reused pid"
    outlive.succeed(f"grep -qx {main_pid()} {unit}/supervisor/cgroup.procs")

    before = claims()
    second = main_pid()
    outlive.succeed(f"kill -9 {second}")
    outlive.wait_until_succeeds(f"test \"$(systemctl show -p MainPID --value engenho-daemon.service)\" != {second} && test \"$(systemctl show -p MainPID --value engenho-daemon.service)\" != 0", timeout=120)
    outlive.wait_until_succeeds(f"test $(journalctl -u engenho-daemon.service --no-pager | grep -cF 'claimed by its pod') -gt {before}", timeout=300)
    assert keeper() == pid, "the workload kept its pid across a crash"
    assert born(pid) == start

    outlive.succeed("systemctl stop engenho-daemon.service")
    outlive.succeed(f"kill -0 {pid}")
    outlive.succeed("systemctl start engenho-daemon.service")
    outlive.wait_for_unit("engenho-daemon.service")
    outlive.wait_until_succeeds(f"test $(journalctl -u engenho-daemon.service --no-pager | grep -cF 'claimed by its pod') -gt {before + 1}", timeout=300)
    assert keeper() == pid, "the workload kept its pid across a stop and a start"
  '';
}
