# Eval-time proof that the typed surface behaves, without building engenho.
#
# Runs the option machinery over `typed-config.nix` alone (the trio's own
# `settings` option is stubbed) so the test stays IFD-free and pins the three
# properties that make the module worth having:
#
#   1. an unset leaf is ABSENT from the rendered YAML, not `null`
#   2. a set leaf lands at its snake_case wire key
#   3. a bad enum value FAILS EVAL rather than reaching the node
{ pkgs ? import <nixpkgs> { } }:
let
  inherit (pkgs) lib;

  # Evaluate typed-config.nix with a stub for the option the trio owns.
  evalWith = userConfig:
    (lib.evalModules {
      modules = [
        ../typed-config.nix
        # Stand-in for `mkModuleTrio`'s `settings` (types.attrs).
        ({ lib, ... }: {
          options.services.engenho.settings = lib.mkOption {
            type = lib.types.attrs;
            default = { };
          };
        })
        userConfig
      ];
    }).config.services.engenho.settings;

  # Same, but with a home-manager-shaped context: `config.home.homeDirectory`
  # exists. This is what `dataDir`'s default keys off to tell a per-user agent
  # from a system daemon.
  evalAsHomeManager = userConfig:
    (lib.evalModules {
      modules = [
        ../typed-config.nix
        ({ lib, ... }: {
          options.services.engenho.settings = lib.mkOption {
            type = lib.types.attrs;
            default = { };
          };
          options.home.homeDirectory = lib.mkOption { type = lib.types.str; };
          config.home.homeDirectory = "/Users/probe";
        })
        userConfig
      ];
    }).config.services.engenho.settings;

  # ── 1. Absence, not null ────────────────────────────────────────────────
  # THE property this module exists for. engenho's tiered fold supplies its
  # own prescribed defaults — including the DERIVED per-node cluster name. A
  # key written as `null` is read as an explicit operator opinion and
  # SUPPRESSES that fold, so an "empty" config would silently un-derive every
  # node's identity. Absence is the only correct encoding of "unset".
  empty = evalWith { };

  # ── 2. Set leaves reach their wire keys ─────────────────────────────────
  populated = evalWith {
    services.engenho.config = {
      runtime = {
        listenAddr = "0.0.0.0:6443";
        kubeletBackend = "fake";
        tls.enabled = false;
      };
      scheduler.strategy = "bin_pack";
      controllers.enable = { gc = false; };
      networking.datapathMode = "compute_only";
      teia.servers = [ "nats://10.0.0.1:4222" ];
    };
  };

  checks = [
    { name = "unset-config-renders-empty";
      ok = empty == { };
      got = builtins.toJSON empty; }

    { name = "no-null-leaks-into-yaml";
      ok = !(lib.hasInfix "null" (builtins.toJSON populated));
      got = builtins.toJSON populated; }

    { name = "camelCase-option-becomes-snake_case-key";
      ok = populated.runtime.listen_addr == "0.0.0.0:6443";
      got = builtins.toJSON (populated.runtime or { }); }

    { name = "nested-tls-is-projected";
      ok = populated.runtime.tls.enabled == false;
      got = builtins.toJSON (populated.runtime.tls or { }); }

    { name = "enum-value-passes-through";
      ok = populated.scheduler.strategy == "bin_pack"
        && populated.networking.datapath_mode == "compute_only";
      got = builtins.toJSON { s = populated.scheduler; n = populated.networking; }; }

    { name = "unset-sections-absent-when-others-set";
      # `consistency` and `revoada` were never touched above — they must not
      # appear at all, or they would suppress engenho's own defaults.
      ok = !(populated ? consistency) && !(populated ? revoada);
      got = builtins.toJSON (lib.attrNames populated); }

    { name = "partial-controller-toggle-emits-only-what-was-set";
      ok = populated.controllers.enable == { gc = false; };
      got = builtins.toJSON (populated.controllers or { }); }

    # ── 3. A bad enum must FAIL EVAL ────────────────────────────────────
    # The whole point: `kubelet_backend = "docker"` in a freeform attrset
    # evaluates fine, renders fine, and dies on the node at boot. Typed, it
    # cannot get that far.
    { name = "bad-enum-is-rejected-at-eval";
      ok = !(builtins.tryEval
        (evalWith { services.engenho.config.runtime.kubeletBackend = "docker"; })
      ).success;
      got = "expected eval failure for kubeletBackend=docker"; }

    { name = "bad-scheduler-strategy-is-rejected";
      ok = !(builtins.tryEval
        (evalWith { services.engenho.config.scheduler.strategy = "random"; })
      ).success;
      got = "expected eval failure for scheduler.strategy=random"; }

    # ── 4. data_dir is user-appropriate ON THE HM ARM ONLY ──────────────
    # The defect this pins: `/var/lib/engenho` is engenho's own prescribed
    # default and is RIGHT for a system daemon, but an unprivileged user
    # launchd agent cannot write it. Nothing catches that at eval or at
    # activation -- the daemon boots, logs a correct-looking config, and
    # dies on `fjall open ... PermissionDenied`, restarting every 10s.
    { name = "system-arm-leaves-data_dir-to-engenho";
      # No `config.home` => system arm => stay out of the way, so engenho's
      # own /var/lib default still applies.
      ok = !((evalWith { }) ? runtime);
      got = builtins.toJSON (evalWith { }); }

    { name = "home-manager-arm-derives-data_dir-under-HOME";
      # `or null` deliberately: without it a regression that drops the key
      # throws `attribute 'runtime' missing` from inside builtins.filter,
      # which fails the build but never names the row. A gate should say
      # WHICH invariant broke.
      ok = ((evalAsHomeManager { }).runtime or { }).data_dir or null
        == "/Users/probe/.local/share/engenho";
      got = builtins.toJSON ((evalAsHomeManager { }).runtime or { }); }

    { name = "explicit-data_dir-still-wins-on-the-hm-arm";
      ok = ((evalAsHomeManager {
             services.engenho.config.runtime.dataDir = "/tmp/elsewhere";
           }).runtime or { }).data_dir or null == "/tmp/elsewhere";
      got = builtins.toJSON ((evalAsHomeManager {
             services.engenho.config.runtime.dataDir = "/tmp/elsewhere";
           }).runtime or { }); }
  ];

  failed = builtins.filter (c: !c.ok) checks;
in
if failed == [ ]
then pkgs.runCommand "engenho-typed-config-test" { } ''
  echo "engenho typed-config: ${toString (builtins.length checks)} checks passed" > $out
''
else throw ''
  engenho typed-config test FAILED:
  ${lib.concatMapStringsSep "\n" (c: "  ✗ ${c.name}\n      got: ${c.got}") failed}
''
