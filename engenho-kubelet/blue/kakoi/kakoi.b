@moduledoc """
kakoi (囲い) decides the enclosure of one native container: which identity it runs as and what it keeps.

Secure is what a pod gets by saying nothing: a non-root account, no new privileges, no capabilities. Root is a grant the pod's declaration names with a reason. A demand the node cannot honour is refused for that one pod, or reported where the node cannot enforce anything yet. Rust hands the facts as JSON text and applies the JSON verdict; no word reaches the host.

    default_uid #=> 65532
"""

@doc """
field: A record's value at a key, or the default when it is absent or the record is not one.

    field({"a" => 1}, "a", 0) #=> 1
    field({}, "a", 0) #=> 0
    field(nil, "a", 0) #=> 0
"""
def field(m, key, default)
  if not map?(m)
    default
  elsif get(m, key).nil?
    default
  else
    get(m, key)
  end
end

@doc """
default_uid: The account a container runs as when its pod names none.

    default_uid #=> 65532
"""
def default_uid()
  65532
end

@doc """
namespace_uid: The account of a namespace's pods that name none: stable, non-root, and its own, so one namespace cannot signal another or read its files.

    namespace_uid("rubrica") == namespace_uid("rubrica") #=> true
    namespace_uid("rubrica") != namespace_uid("kensa") #=> true
    namespace_uid("rubrica") >= 100000 #=> true
"""
def namespace_uid(namespace)
  digits = chars(blake3_hex(namespace))
  value = reduce(
    (acc, c) -> acc * 16 + text_index("0123456789abcdef", c, 0),
    0,
    take(4, digits)
  )
  100_000 + value
end

@doc """
hidden_paths: What an enclosed container never sees, beyond what its account already cannot read.

    member?("/run/secrets", hidden_paths) #=> true
"""
def hidden_paths()
  %w[
    /root
    /home
    /Users
    /run/secrets
    /run/secrets.d
    /etc/ssh
    /etc/engenho
    /var/lib
    /var/log
    /srv
  ]
end

@doc """
under?: Whether a path is the base or below it.

    under?("/var/lib/x", "/var/lib") #=> true
    under?("/var/lib", "/var/lib") #=> true
    under?("/var/library", "/var/lib") #=> false
"""
def under?(path, base)
  path == base or starts_with?(path, "#{base}/")
end

@doc """
hide_list: The hidden paths that do not hold or sit inside one of the container's own volumes.

    hide_list(["/var/lib/app"]) == remove_item(hidden_paths, "/var/lib") #=> true
"""
def hide_list(volumes)
  filter(
    fn(hidden)
      not any?((v) -> under?(v, hidden) or under?(hidden, v), volumes)
    end,
    hidden_paths
  )
end

@doc """
scratch_list: The directories an enclosed container gets fresh and private, unless its own volume lives in one.

    scratch_list([]) #=> ["/tmp", "/var/tmp"]
    scratch_list(["/var/tmp/job"]) #=> ["/tmp"]
"""
def scratch_list(volumes)
  filter(
    (dir) -> not any?((v) -> under?(v, dir) or under?(dir, v), volumes),
    %w[/tmp /var/tmp]
  )
end

def remove_item(xs, x)
  filter((y) -> y != x, xs)
end

def any?(f, xs)
  length(filter(f, xs)) != 0
end

@doc """
grant_annotation: The pod annotation that grants root, its value the reason written where a reader sees it.

    grant_annotation #=> "kakoi.pleme.io/root"
"""
def grant_annotation()
  "kakoi.pleme.io/root"
end

@doc """
granted?: Whether the facts carry a grant with a stated reason; an empty reason grants nothing.

    granted?({"grant" => "drops to its own account"}) #=> true
    granted?({"grant" => ""}) #=> false
    granted?({}) #=> false
"""
def granted?(facts)
  field(facts, "grant", "") != ""
end

@doc """
demands: Each thing the container declares that only a grant allows, as text naming it.

    demands({"run_as_user" => 0}) #=> ["runAsUser 0"]
    demands({"run_as_user" => 1000}) #=> []
    demands({"privileged" => true}) #=> ["privileged"]
    demands({"allow_escalation" => true}) #=> ["allowPrivilegeEscalation"]
    demands({"cap_add" => ["NET_ADMIN"]}) #=> ["capabilities.add NET_ADMIN"]
"""
def demands(declared)
  caps = field(declared, "cap_add", [])
  append(
    if field(declared, "run_as_user", -1) == 0
      ["runAsUser 0"]
    else
      []
    end,
    if field(declared, "privileged", false)
      ["privileged"]
    else
      []
    end,
    if field(declared, "allow_escalation", false)
      ["allowPrivilegeEscalation"]
    else
      []
    end,
    if length(caps) == 0
      []
    else
      ["capabilities.add #{join(caps, ",")}"]
    end
  )
end

@doc """
contradiction: The reason a declaration cannot be true, or nil.

    contradiction({"run_as_non_root" => true, "run_as_user" => 0}) #=> "runAsNonRoot and runAsUser 0 contradict"
    contradiction({"run_as_non_root" => true}) #=> nil
"""
def contradiction(declared)
  if field(declared, "run_as_non_root", false) and
      field(declared, "run_as_user", -1) == 0
    "runAsNonRoot and runAsUser 0 contradict"
  else
    outside_range(declared)
  end
end

@doc """
outside_range: The reason an id cannot exist on a host, or nil.

    outside_range({"run_as_user" => -1}) #=> "runAsUser -1 is outside the host's range"
    outside_range({"run_as_group" => 4294967296}) #=> "runAsGroup 4294967296 is outside the host's range"
    outside_range({"run_as_user" => 0}) #=> nil
"""
def outside_range(declared)
  uid = declared["run_as_user"]
  gid = declared["run_as_group"]
  if uid != nil and (uid < 0 or uid > 4_294_967_295)
    "runAsUser #{to_s(uid)} is outside the host's range"
  elsif gid != nil and (gid < 0 or gid > 4_294_967_295)
    "runAsGroup #{to_s(gid)} is outside the host's range"
  end
end

@doc """
refusal: Why this container may not start under an enforcing node, or nil.

    refusal({"declared" => {"run_as_user" => 0}}) #=> "this pod asks for runAsUser 0 and names no grant: add the annotation kakoi.pleme.io/root with its reason"
    refusal({"declared" => {"run_as_user" => 0}, "grant" => "needed"}) #=> nil
"""
def refusal(facts)
  declared = field(facts, "declared", {})
  asked = demands(declared)
  impossible = contradiction(declared)
  if impossible != nil
    impossible
  elsif length(asked) != 0 and not granted?(facts)
    "this pod asks for #{join(asked, ", ")} and names no grant: add the annotation #{grant_annotation} with its reason"
  end
end

@doc """
identity: The uid and gid a container runs as: what it declares, else the default, else with a grant the daemon's own.

    identity({"declared" => {}}) #=> [65532, 65532]
    identity({"declared" => {"run_as_user" => 1000}}) #=> [1000, 1000]
    identity({"declared" => {"run_as_user" => 1000, "run_as_group" => 2000}}) #=> [1000, 2000]
    identity({"declared" => {}, "grant" => "needed"}) #=> [nil, nil]
"""
def identity(facts)
  declared = field(facts, "declared", {})
  named = declared["run_as_user"]
  uid = if named != nil
    named
  elsif granted?(facts)
    nil
  elsif field(facts, "namespace", "") != ""
    namespace_uid(facts["namespace"])
  else
    default_uid
  end
  group = declared["run_as_group"]
  [
    uid,
    if group != nil
      group
    else
      uid
    end
  ]
end

@doc """
declared_only: The identity a node applies when it enforces nothing beyond what the pod states.

    declared_only({"declared" => {"run_as_user" => 1000}}) #=> [1000, nil]
"""
def declared_only(facts)
  declared = field(facts, "declared", {})
  [declared["run_as_user"], declared["run_as_group"]]
end

def verdict_text(refused, ids, confine, warnings, volumes?)
  own = volumes or []
  json_stringify(
    {
      "refused" => refused,
      "uid" => ids[0],
      "gid" => ids[1],
      "no_new_privs" => confine,
      "clear_caps" => confine,
      "enclosed" => confine,
      "hide" => if confine
        hide_list(own)
      else
        []
      end,
      "scratch" => if confine
        scratch_list(own)
      else
        []
      end,
      "warnings" => warnings
    }
  )
end

@doc """
enclose: The verdict for one container: refused or its identity, what it keeps, and what the node could not enforce.

    json_read(enclose(json_stringify({"mode" => "off", "can_enforce" => true, "declared" => {"run_as_user" => 0}}))).refused #=> nil
"""
def enclose(facts_text)
  facts = json_read(facts_text)
  mode = field(facts, "mode", "enforce")
  why = refusal(facts)
  outside = outside_range(field(facts, "declared", {}))
  if outside != nil
    verdict_text(outside, [nil, nil], false, [])
  elsif mode == "off"
    verdict_text(nil, declared_only(facts), false, [])
  elsif not field(facts, "can_enforce", false)
    verdict_text(
      nil,
      [nil, nil],
      false,
      ["this node's daemon cannot change identity, so the container runs as the daemon's own account"]
    )
  elsif mode == "audit"
    verdict_text(
      nil,
      declared_only(facts),
      false,
      if why.nil?
        []
      else
        ["would refuse under enforce: #{why}"]
      end
    )
  elsif why != nil
    verdict_text(why, [nil, nil], false, [])
  else
    granted = granted?(facts)
    verdict_text(
      nil,
      identity(facts),
      not granted,
      [],
      field(facts, "volumes", [])
    )
  end
end

def answer(facts)
  json_read(enclose(json_stringify(facts)))
end

test "a pod that says nothing runs as the default account with nothing kept"
  v = answer({"mode" => "enforce", "can_enforce" => true, "declared" => {}})
  assert v.refused.nil?
  assert v.uid == 65532
  assert v.gid == 65532
  assert v.enclosed == true
  assert v.no_new_privs == true
  assert v.clear_caps == true
end

test "every demand is refused without a grant, and each names the annotation"
  each_demand = [
    {"run_as_user" => 0},
    {"privileged" => true},
    {"allow_escalation" => true},
    {"cap_add" => ["SYS_ADMIN"]}
  ]
  every_refused = map(
    fn(d)
      answer(
        {"mode" => "enforce", "can_enforce" => true, "declared" => d}
      ).refused
    end,
    each_demand
  )
  assert every?(
    (r) -> r != nil and contains?(r, grant_annotation),
    every_refused
  )
end

test "a grant with a reason starts the same demands; an empty reason does not"
  granted = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "grant" => "unit-run drops to the unit's account",
      "declared" => {"run_as_user" => 0, "privileged" => true}
    }
  )
  assert granted.refused.nil?
  assert granted.uid == 0
  assert granted.no_new_privs == false
  empty = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "grant" => "",
      "declared" => {"run_as_user" => 0}
    }
  )
  assert empty.refused != nil
end

test "a grant with no identity named keeps the daemon's account"
  v = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "grant" => "why",
      "declared" => {}
    }
  )
  assert v.uid.nil?
  assert v.no_new_privs == false
end

test "a contradiction is refused even with a grant"
  v = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "grant" => "why",
      "declared" => {"run_as_non_root" => true, "run_as_user" => 0}
    }
  )
  assert v.refused == "runAsNonRoot and runAsUser 0 contradict"
end

test "an enclosed container hides the host's private paths, except where its own volume lives"
  v = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "namespace" => "rubrica",
      "volumes" => ["/srv/pool/data/rubrica"],
      "declared" => {}
    }
  )
  assert v.enclosed == true
  assert member?("/run/secrets", v.hide)
  assert member?("/srv", v.hide) == false
  assert v.uid == namespace_uid("rubrica")
end

test "a grant is not enclosed, and neither audit nor off"
  granted = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "grant" => "x",
      "declared" => {}
    }
  )
  assert granted.enclosed == false
  audit = answer({"mode" => "audit", "can_enforce" => true, "declared" => {}})
  assert audit.enclosed == false
end

test "a non-root identity needs no grant and keeps nothing"
  v = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "declared" => {"run_as_user" => 1000}
    }
  )
  assert v.refused.nil?
  assert v.uid == 1000
  assert v.gid == 1000
  assert v.clear_caps == true
end

test "audit applies only what is declared, and says what enforce would refuse"
  v = answer(
    {
      "mode" => "audit",
      "can_enforce" => true,
      "declared" => {"run_as_user" => 0}
    }
  )
  assert v.refused.nil?
  assert v.uid == 0
  assert v.no_new_privs == false
  assert contains?(first(v.warnings), "would refuse under enforce")
  quiet = answer({"mode" => "audit", "can_enforce" => true, "declared" => {}})
  assert quiet.uid.nil?
  assert length(quiet.warnings) == 0
end

test "an id outside the host's range is refused in every mode"
  every_mode = map(
    fn(m)
      answer(
        {
          "mode" => m,
          "can_enforce" => true,
          "declared" => {"run_as_user" => -1}
        }
      ).refused
    end,
    %w[enforce audit off]
  )
  assert every?((r) -> r != nil, every_mode)
end

test "off applies only what is declared and says nothing"
  v = answer(
    {"mode" => "off", "can_enforce" => true, "declared" => {"run_as_user" => 7}}
  )
  assert v.uid == 7
  assert v.no_new_privs == false
  assert length(v.warnings) == 0
end

test "a node that cannot enforce never claims it did"
  v = answer({"mode" => "enforce", "can_enforce" => false, "declared" => {}})
  assert v.refused.nil?
  assert v.uid.nil?
  assert v.no_new_privs == false
  assert contains?(first(v.warnings), "cannot change identity")
end
