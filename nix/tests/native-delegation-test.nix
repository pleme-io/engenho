{ nixpkgs, system, module, package }:
let
  inherit (nixpkgs) lib;
  pkgs = nixpkgs.legacyPackages.${system};

  evalNode = extra:
    (lib.nixosSystem {
      inherit system;
      modules = [
        module
        {
          boot.isContainer = true;
          system.stateVersion = "25.11";
          services.engenho = {
            enable = true;
            inherit package;
            daemon.enable = true;
          };
        }
        extra
      ];
    }).config;

  engenhoFailures = config:
    map (a: a.message)
      (builtins.filter (a: !a.assertion && lib.hasInfix "services.engenho" a.message) config.assertions);

  delegateOf = config: config.systemd.services.engenho-daemon.serviceConfig.Delegate or null;

  native = evalNode { services.engenho.config.runtime.kubeletBackend = "native"; };
  nativeOff = evalNode {
    services.engenho.config.runtime = { kubeletBackend = "native"; nativeCgroups = "off"; };
  };
  podman = evalNode { };
  forcedOff = evalNode {
    services.engenho.config.runtime.kubeletBackend = "native";
    systemd.services.engenho-daemon.serviceConfig.Delegate = lib.mkForce "no";
  };

  rows = [
    { name = "a-native-node-delegates-its-cgroup";
      ok = delegateOf native == "yes" && engenhoFailures native == [ ];
      got = builtins.toJSON { delegate = delegateOf native; failures = engenhoFailures native; }; }
    { name = "native-with-cgroups-off-does-not-delegate";
      ok = delegateOf nativeOff == null && engenhoFailures nativeOff == [ ];
      got = builtins.toJSON { delegate = delegateOf nativeOff; failures = engenhoFailures nativeOff; }; }
    { name = "a-podman-node-does-not-delegate";
      ok = delegateOf podman == null && engenhoFailures podman == [ ];
      got = builtins.toJSON { delegate = delegateOf podman; failures = engenhoFailures podman; }; }
    { name = "delegation-forced-off-on-a-native-node-fails-an-assertion";
      ok = lib.any (lib.hasInfix "Delegate=yes") (engenhoFailures forcedOff);
      got = builtins.toJSON (engenhoFailures forcedOff); }
  ];

  failed = builtins.filter (r: !r.ok) rows;
in
if failed == [ ]
then pkgs.runCommand "engenho-native-delegation" { } ''
  echo "engenho native delegation: ${toString (builtins.length rows)} rows hold" > $out
''
else throw ''
  engenho native delegation FAILED:
  ${lib.concatMapStringsSep "\n" (r: "  ✗ ${r.name}\n      got: ${r.got}") failed}
''
