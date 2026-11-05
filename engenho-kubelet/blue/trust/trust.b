@moduledoc """
trust decides whether a node runs the Nix closure a pod names.

A path stands on one of four grounds: a signature that verifies under a key this node trusts, a content address that proves itself, or a build on this node. A path with none of them is refused for that one pod under enforce, named under audit, and passed in silence under off. Rust reads the store, checks each signature and hands the facts as JSON text; no word reaches the host.

    trusted_keys({"keys" => ["a:1"], "store_keys" => ["b:2"]}) #=> ["a:1"]
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
trusted_keys: The keys a signature counts under. A node's declared list narrows it; a node that declares none trusts what its own Nix store trusts.

    trusted_keys({"keys" => [], "store_keys" => ["b:2"]}) #=> ["b:2"]
    trusted_keys({"keys" => ["a:1"], "store_keys" => ["b:2"]}) #=> ["a:1"]
    trusted_keys({}) #=> []
"""
def trusted_keys(facts)
  declared = field(facts, "keys", [])
  if length(declared) != 0
    declared
  else
    field(facts, "store_keys", [])
  end
end

@doc """
local_builds_trusted?: Whether a path built on this node stands with no signature. It does unless the node says otherwise, since a local build has no signature to offer.

    local_builds_trusted?({}) #=> true
    local_builds_trusted?({"local_builds" => "refuse"}) #=> false
"""
def local_builds_trusted?(facts)
  field(facts, "local_builds", "trust") == "trust"
end

@doc """
ground: What one path stands on: signed, content, local, or nil for nothing.

    ground({"verified_by" => ["k:1"]}, ["k:1"], true) #=> "signed"
    ground({"verified_by" => ["k:1"]}, ["other:2"], true) #=> nil
    ground({"content_addressed" => true}, [], true) #=> "content"
    ground({"ultimate" => true}, [], true) #=> "local"
    ground({"ultimate" => true}, [], false) #=> nil
    ground({}, [], true) #=> nil
"""
def ground(path, trusted, trusts_local)
  counted = filter(
    (key) -> member?(key, trusted),
    field(path, "verified_by", [])
  )
  if length(counted) != 0
    "signed"
  elsif field(path, "content_addressed", false)
    "content"
  elsif trusts_local and field(path, "ultimate", false)
    "local"
  end
end

@doc """
fault: Why a path with no ground stands on nothing, by what it claims.

    fault({"path" => "/p", "claimed" => []}) #=> "/p has no signature and was not built on this node"
    fault({"path" => "/p", "claimed" => ["k"]}) #=> "/p is signed by k, which this node does not trust or which does not verify"
"""
def fault(path)
  claimed = field(path, "claimed", [])
  if length(claimed) == 0
    "#{field(path, "path", "a path")} has no signature and was not built on this node"
  else
    "#{field(path, "path", "a path")} is signed by #{join(claimed, ", ")}, which this node does not trust or which does not verify"
  end
end

@doc """
standing: Each path of the closure with what it stands on, as [path, ground].

    standing({"paths" => [{"path" => "/p", "ultimate" => true}]}) #=> [[{"path" => "/p", "ultimate" => true}, "local"]]
"""
def standing(facts)
  trusted = trusted_keys(facts)
  trusts_local = local_builds_trusted?(facts)
  map(
    (path) -> [path, ground(path, trusted, trusts_local)],
    field(facts, "paths", [])
  )
end

@doc """
refusal: Why a closure may not run, or nil.

    refusal({"closure" => "/c", "blind" => "no store"}) #=> "closure /c cannot be checked: no store"
    refusal({"closure" => "/c", "paths" => []}) #=> "closure /c cannot be checked: the store names no path"
    refusal({"closure" => "/c", "paths" => [{"path" => "/c", "ultimate" => true}]}) #=> nil
"""
def refusal(facts)
  closure = field(facts, "closure", "?")
  blind = field(facts, "blind", nil)
  if blind != nil
    "closure #{closure} cannot be checked: #{blind}"
  elsif length(field(facts, "paths", [])) == 0
    "closure #{closure} cannot be checked: the store names no path"
  else
    bad = filter((row) -> row[1].nil?, standing(facts))
    if length(bad) != 0
      others = length(bad) - 1
      suffix = if others == 0
        ""
      else
        " (and #{others} more)"
      end
      "closure #{closure}: #{fault(bad[0][0])}#{suffix}"
    end
  end
end

@doc """
local_warning: What a reader should know about a closure that runs on local builds, or nil.

    local_warning({"closure" => "/c", "paths" => [{"path" => "/c"}]}) #=> nil
    local_warning({"closure" => "/c", "paths" => [{"path" => "/c", "ultimate" => true}]}) #=> "closure /c holds 1 of 1 paths built on this node, with no signature; this node trusts its own store"
"""
def local_warning(facts)
  rows = standing(facts)
  local = filter((row) -> row[1] == "local", rows)
  if length(local) != 0
    "closure #{field(facts, "closure", "?")} holds #{length(local)} of #{length(rows)} paths built on this node, with no signature; this node trusts its own store"
  end
end

@doc """
verdict_text: The JSON answer Rust applies.

    json_read(verdict_text("no", [])).refused #=> "no"
"""
def verdict_text(refused, warnings)
  json_stringify({"refused" => refused, "warnings" => warnings})
end

@doc """
decide: The verdict for one closure: refused or not, and what a reader should see.

    json_read(decide(json_stringify({"mode" => "off", "closure" => "/c", "paths" => []}))).refused #=> nil
    json_read(decide(json_stringify({"mode" => "enforce", "closure" => "/c", "paths" => []}))).refused #=> "closure /c cannot be checked: the store names no path"
"""
def decide(facts_text)
  facts = json_read(facts_text)
  mode = field(facts, "mode", "enforce")
  why = refusal(facts)
  local = local_warning(facts)
  noted = if local.nil?
    []
  else
    [local]
  end
  if mode == "off"
    verdict_text(nil, [])
  elsif why.nil?
    verdict_text(nil, noted)
  elsif mode == "audit"
    verdict_text(nil, append(["would refuse under enforce: #{why}"], noted))
  else
    verdict_text(why, noted)
  end
end

test "a signature that verifies under a store key stands"
  facts = {
    "store_keys" => ["k:1"],
    "paths" => [{"path" => "/p", "verified_by" => ["k:1"]}],
    "closure" => "/p"
  }
  assert refusal(facts).nil?
end

test "a declared list replaces the store's keys"
  facts = {
    "keys" => ["mine:9"],
    "store_keys" => ["k:1"],
    "paths" => [{"path" => "/p", "verified_by" => ["k:1"], "claimed" => ["k"]}],
    "closure" => "/p"
  }
  assert refusal(facts) ==
    "closure /p: /p is signed by k, which this node does not trust or which does not verify"
end

test "an unsigned path that was not built here is refused under enforce"
  facts = {
    "mode" => "enforce",
    "closure" => "/c",
    "paths" => [{"path" => "/c"}]
  }
  verdict = json_read(decide(json_stringify(facts)))
  assert verdict.refused ==
    "closure /c: /c has no signature and was not built on this node"
end

test "audit names what enforce would refuse and lets it run"
  facts = {"mode" => "audit", "closure" => "/c", "paths" => [{"path" => "/c"}]}
  verdict = json_read(decide(json_stringify(facts)))
  assert verdict.refused.nil?
  assert verdict.warnings ==
    ["would refuse under enforce: closure /c: /c has no signature and was not built on this node"]
end

test "off says nothing"
  facts = {"mode" => "off", "closure" => "/c", "paths" => [{"path" => "/c"}]}
  verdict = json_read(decide(json_stringify(facts)))
  assert verdict.refused.nil?
  assert verdict.warnings == []
end

test "a local build runs and is said"
  facts = {
    "mode" => "enforce",
    "closure" => "/c",
    "paths" => [{"path" => "/c", "ultimate" => true}]
  }
  verdict = json_read(decide(json_stringify(facts)))
  assert verdict.refused.nil?
  assert length(verdict.warnings) == 1
end

test "a node may refuse local builds too"
  facts = {
    "mode" => "enforce",
    "local_builds" => "refuse",
    "closure" => "/c",
    "paths" => [{"path" => "/c", "ultimate" => true}]
  }
  assert json_read(decide(json_stringify(facts))).refused != nil
end

test "a content-addressed path proves itself"
  facts = {
    "mode" => "enforce",
    "closure" => "/c",
    "paths" => [{"path" => "/c", "content_addressed" => true}]
  }
  verdict = json_read(decide(json_stringify(facts)))
  assert verdict.refused.nil?
  assert verdict.warnings == []
end

test "one bad path in a closure refuses it and counts the rest"
  facts = {
    "closure" => "/c",
    "paths" => [
      {"path" => "/a", "ultimate" => true},
      {"path" => "/b"},
      {"path" => "/d"}
    ]
  }
  assert refusal(facts) ==
    "closure /c: /b has no signature and was not built on this node (and 1 more)"
end

test "a store it cannot read refuses under enforce and warns under audit"
  facts = {"mode" => "enforce", "closure" => "/c", "blind" => "no nix"}
  assert json_read(decide(json_stringify(facts))).refused ==
    "closure /c cannot be checked: no nix"
  audit = {"mode" => "audit", "closure" => "/c", "blind" => "no nix"}
  verdict = json_read(decide(json_stringify(audit)))
  assert verdict.refused.nil?
  assert verdict.warnings ==
    ["would refuse under enforce: closure /c cannot be checked: no nix"]
end
