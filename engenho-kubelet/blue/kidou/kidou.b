@moduledoc """
kidou decides how engenho's kubelet runs a pod of the wasm runtime class: which component it loads, what the component is granted, how a crash is supervised and how an exec probe is answered.

Rust hands each word the facts as JSON text and applies the JSON verdict; no word reads a clock, a file or the network. The grant follows the service's access declaration (blue SERVICE-ACCESS): a pod with no access annotation gets the default access, so no writable path, no egress, and its declared container ports to listen on.

    restart_delay_ms(2) #=> 800
"""

@doc """
access_annotation: The pod annotation that carries the service's rendered access declaration, as JSON.

    access_annotation #=> "blue.pleme.io/access"
"""
def access_annotation()
  "blue.pleme.io/access"
end

@doc """
component_annotation: The pod annotation naming the component inside a nix closure, when the image names a directory.

    component_annotation #=> "wasm.pleme.io/component"
"""
def component_annotation()
  "wasm.pleme.io/component"
end

@doc """
restart_budget: How many crashes in a row the backend restarts in place before it surfaces the exit to the kubelet.

    restart_budget #=> 3
"""
def restart_budget()
  3
end

@doc """
restart_delay_ms: The wait before in-place restart number n, from 0: 200 ms, doubling.

    restart_delay_ms(0) #=> 200
"""
def restart_delay_ms(n)
  200 * expt(2, n)
end

def default_effects()
  ["network", "clock", "entropy", "environment"]
end

def after(prefix, s)
  join(drop(length(prefix), chars(s)), "")
end

def field(m, key, default)
  if not map?(m)
    default
  elsif get(m, key).nil?
    default
  else
    get(m, key)
  end
end

def annotation(facts, key)
  field(field(facts, "annotations", {}), key, nil)
end

@doc """
component_of: The component a pod runs, read from its image: a nix closure path, a file path when the node allows local components, never an OCI reference.

    component_of({"image" => "nix:/nix/store/abc-svc/lib/svc.wasm"}).path #=> "/nix/store/abc-svc/lib/svc.wasm"
"""
def component_of(facts)
  image = field(facts, "image", "")
  if starts_with?(image, "nix:")
    closure_component(facts, after("nix:", image))
  elsif starts_with?(image, "file:")
    local_component(facts, after("file:", image))
  else
    {
      path: nil,
      refused: "image #{image} is an OCI reference; a wasm pod runs a component from a nix closure (nix:/nix/store/...)"
    }
  end
end

def closure_component(facts, path)
  if not starts_with?(path, "/nix/store/")
    {path: nil, refused: "nix:#{path} is not under /nix/store/"}
  elsif contains?(path, "..")
    {path: nil, refused: "nix:#{path} contains .."}
  elsif ends_with?(path, ".wasm")
    {path:, refused: nil}
  else
    inside(path, annotation(facts, component_annotation) or "component.wasm")
  end
end

def inside(closure, name)
  if starts_with?(name, "/") or contains?(name, "..")
    {
      path: nil,
      refused: "#{component_annotation} #{name} must be a path inside the closure"
    }
  else
    {path: "#{closure}/#{name}", refused: nil}
  end
end

def local_component(facts, path)
  if not field(facts, "local_components", false)
    {
      path: nil,
      refused: "file:#{path} names a mutable path, and local components are off on this node"
    }
  elsif not starts_with?(path, "/") or not ends_with?(path, ".wasm")
    {
      path: nil,
      refused: "file:#{path} is not an absolute path to a .wasm component"
    }
  else
    {path:, refused: nil}
  end
end

@doc """
access_of: The pod's access declaration, read from its annotation, or the default access when there is none.

    access_of({"ports" => [8080]}).listen #=> [8080]
"""
def access_of(facts)
  text = annotation(facts, access_annotation)
  if text.nil?
    default_access(facts)
  else
    declared = json_read(text) rescue nil
    if not map?(declared)
      {refused: "the #{access_annotation} annotation is not a JSON object"}
    else
      declared_access(facts, declared)
    end
  end
end

def default_access(facts)
  reads = map(
    (m) -> field(m, "path", ""),
    filter((m) -> field(m, "read_only", false), field(facts, "mounts", []))
  )
  {
    refused: nil,
    effects: default_effects,
    egress: [],
    reads:,
    writes: [],
    listen: field(facts, "ports", []),
    exec: []
  }
end

def declared_access(facts, d)
  {
    refused: nil,
    effects: map(
      (e) -> replace(to_s(e), ":", ""),
      field(d, "effects", default_effects)
    ),
    egress: field(d, "egress", []),
    reads: field(d, "reads", []),
    writes: field(d, "writes", []),
    listen: field(d, "listen", field(facts, "ports", [])),
    exec: field(d, "exec", [])
  }
end

def effect?(access, name)
  member?(name, access.effects)
end

@doc """
peer_of: One egress entry read as a peer, or nil when it is not host:port.

    peer_of("db.data.svc:5432") #=> {"host" => "db.data.svc", "port" => 5432}
"""
def peer_of(entry)
  parts = split(to_s(entry), ":")
  port = if count(parts) == 2
    to_int(second(parts))
  end
  if port.nil? or first(parts) == ""
    nil
  else
    {"host" => first(parts), "port" => port}
  end
end

def network_of(access)
  if effect?(access, "network")
    {
      "bind" => access.listen,
      "connect" => remove((p) -> p.nil?, map(peer_of, access.egress))
    }
  else
    {"bind" => [], "connect" => []}
  end
end

def network_withheld(access)
  unparsed = filter((e) -> peer_of(e).nil?, access.egress)
  named = map((e) -> "egress #{e} is not host:port", unparsed)
  if effect?(access, "network")
    named
  elsif empty?(access.egress) and empty?(access.listen)
    []
  else
    ["egress and listen are withheld: the access does not declare the :network effect"]
  end
end

def writable?(access, m)
  member?(field(m, "path", ""), access.writes) and
    effect?(access, "filesystem") and
    not field(m, "read_only", false)
end

def declared?(access, m)
  path = field(m, "path", "")
  member?(path, access.reads) or member?(path, access.writes)
end

def preopen_of(access, m)
  {
    "hostPath" => field(m, "host_path", ""),
    "guestPath" => field(m, "path", ""),
    "writable" => writable?(access, m)
  }
end

def mountable?(access, m)
  declared?(access, m) and not field(m, "host_path", nil).nil?
end

def mount_withheld(access, m)
  path = field(m, "path", "")
  if not declared?(access, m)
    "mount #{path} is in neither the access reads nor its writes"
  elsif field(m, "host_path", nil).nil?
    "mount #{path} has no host path to map"
  elsif member?(path, access.writes) and not writable?(access, m)
    "mount #{path} is read-only: a write needs the :filesystem effect and a writable mount"
  end
end

@doc """
grant_of: What the component is granted: its output, its environment, the declared mounts and the declared network.

    grant_of({}, access_of({})) #=> {"env" => true, "network" => {"bind" => [], "connect" => []}, "preopens" => [], "stderr" => true, "stdout" => true}
"""
def grant_of(facts, access)
  mounts = field(facts, "mounts", [])
  {
    "stdout" => true,
    "stderr" => true,
    "env" => effect?(access, "environment"),
    "preopens" => map(
      (m) -> preopen_of(access, m),
      filter((m) -> mountable?(access, m), mounts)
    ),
    "network" => network_of(access)
  }
end

def withheld_of(facts, access)
  mounts = map((m) -> mount_withheld(access, m), field(facts, "mounts", []))
  append(remove((w) -> w.nil?, mounts), network_withheld(access))
end

def limits_of(facts)
  seconds = field(facts, "active_deadline_seconds", nil)
  deadline = if seconds.nil?
    nil
  else
    seconds * 1000
  end
  {
    "memoryBytes" => field(facts, "memory_limit_bytes", nil),
    "deadlineMs" => deadline,
    "cpuMillis" => field(facts, "cpu_limit_milli", nil)
  }
end

def serve_port(access)
  if effect?(access, "network") and not empty?(access.listen)
    first(access.listen)
  end
end

def refusal_of(component, access)
  if not component.refused.nil?
    component.refused
  elsif not access.refused.nil?
    access.refused
  elsif not empty?(access.exec)
    "the access declares exec #{join(access.exec, ", ")}, and a wasm component cannot run a program"
  end
end

@doc """
admit: The verdict for one container of a wasm pod: the component to load, its grant, its limits, the port a proxy component is served on and what was withheld, or the reason it is refused.

    json_read(admit(json_stringify({"image" => "docker.io/x:1"}))).component #=> nil
"""
def admit(facts_text)
  facts = json_read(facts_text)
  component = component_of(facts)
  access = access_of(facts)
  refused = refusal_of(component, access)
  if refused.nil?
    json_stringify(
      {
        "refused" => nil,
        "component" => component.path,
        "grant" => grant_of(facts, access),
        "limits" => limits_of(facts),
        "serveHttp" => serve_port(access),
        "withheld" => withheld_of(facts, access)
      }
    )
  else
    json_stringify({"refused" => refused, "component" => nil})
  end
end

@doc """
disposition_of: How an exit is reported to the kubelet: a code, or the signal its cause corresponds to.

    disposition_of({"kind" => "memoryLimit"}) #=> {"signal" => 9}
"""
def disposition_of(exit)
  kind = field(exit, "kind", "")
  if kind == "code"
    {"code" => field(exit, "code", 1)}
  elsif kind == "stopped"
    {"signal" => 15}
  elsif kind == "trapped"
    {"signal" => 6}
  else
    {"signal" => 9}
  end
end

def crashed?(exit)
  kind = field(exit, "kind", "")
  kind == "trapped" or kind == "code" and field(exit, "code", 1) != 0
end

def describe(exit)
  kind = field(exit, "kind", "")
  if kind == "code"
    "exit code #{field(exit, "code", 1)}"
  else
    kind
  end
end

@doc """
supervise: What the backend does when a component ends: restart it in place after a delay, or surface the exit to the kubelet, whose restartPolicy then applies.

    json_read(supervise(json_stringify({"exit" => {"kind" => "stopped"}, "restarts" => 0}))).action #=> "surface"
"""
def supervise(facts_text)
  facts = json_read(facts_text)
  exit = field(facts, "exit", {})
  restarts = field(facts, "restarts", 0)
  policy = field(facts, "restart_policy", "Always")
  disposition = disposition_of(exit)
  if not crashed?(exit)
    verdict(
      "surface",
      nil,
      disposition,
      "#{describe(exit)}: surfaced to the kubelet"
    )
  elsif policy == "Never"
    verdict(
      "surface",
      nil,
      disposition,
      "#{describe(exit)}: restartPolicy Never, surfaced"
    )
  elsif restarts >= restart_budget
    verdict(
      "surface",
      nil,
      disposition,
      "#{describe(exit)}: #{restarts} in-place restarts spent, surfaced to the kubelet"
    )
  else
    delay = restart_delay_ms(restarts)
    verdict(
      "restart",
      delay,
      disposition,
      "#{describe(exit)}: restart #{restarts + 1} of #{restart_budget} in #{delay} ms"
    )
  end
end

def verdict(action, delay, disposition, note)
  json_stringify(
    {
      "action" => action,
      "delayMs" => delay,
      "disposition" => disposition,
      "note" => note
    }
  )
end

@doc """
probe: The answer to an exec probe. A component cannot run a program, so the probe is answered from the component's state: 0 while it runs, 1 once it has ended.

    json_read(probe(json_stringify({"argv" => ["true"], "running" => true}))).exitCode #=> 0
"""
def probe(facts_text)
  facts = json_read(facts_text)
  argv = join(field(facts, "argv", []), " ")
  if field(facts, "running", false)
    json_stringify({"exitCode" => 0, "stdout" => "", "stderr" => ""})
  else
    json_stringify(
      {
        "exitCode" => 1,
        "stdout" => "",
        "stderr" => "wasm: the component has ended; #{argv} was not run, a component cannot run a program"
      }
    )
  end
end

test "an OCI image is refused and a closure directory resolves its component"
  oci = json_read(admit(json_stringify({"image" => "ghcr.io/pleme-io/svc:1"})))
  assert contains?(oci.refused, "OCI reference")
  dir = component_of(
    {
      "image" => "nix:/nix/store/abc-svc",
      "annotations" => {"wasm.pleme.io/component" => "lib/svc.wasm"}
    }
  )
  assert dir.path == "/nix/store/abc-svc/lib/svc.wasm"
  assert component_of({"image" => "nix:/nix/store/abc-svc"}).path ==
    "/nix/store/abc-svc/component.wasm"
  assert not component_of({"image" => "nix:/tmp/x.wasm"}).refused.nil?
  assert not component_of(
    {"image" => "nix:/nix/store/abc/../../etc"}
  ).refused.nil?
end

test "a file component runs only where the node allows local components"
  off = component_of({"image" => "file:/tmp/probe.wasm"})
  assert contains?(off.refused, "local components are off")
  on = component_of(
    {"image" => "file:/tmp/probe.wasm", "local_components" => true}
  )
  assert on.path == "/tmp/probe.wasm"
end

test "the default access grants no write, no egress and the declared ports"
  facts = {
    "image" => "nix:/nix/store/abc-svc/svc.wasm",
    "ports" => [8080],
    "mounts" => [
      {
        "path" => "/etc/svc",
        "host_path" => "/var/lib/pod/cfg",
        "read_only" => true
      },
      {
        "path" => "/data",
        "host_path" => "/var/lib/pod/data",
        "read_only" => false
      }
    ]
  }
  v = json_read(admit(json_stringify(facts)))
  assert v.refused.nil?
  assert v.grant.network.bind == [8080]
  assert v.grant.network.connect == []
  assert count(v.grant.preopens) == 1
  assert first(v.grant.preopens).guestPath == "/etc/svc"
  assert first(v.grant.preopens).writable == false
  assert member?(
    "mount /data is in neither the access reads nor its writes",
    v.withheld
  )
  assert v.serveHttp == 8080
end

test "a declared access grants exactly its peers, its writes and its listen ports"
  access = json_stringify(
    {
      "effects" => ["network", "filesystem", "clock"],
      "egress" => ["db.data.svc:5432", "cache"],
      "reads" => [],
      "writes" => ["/data"],
      "listen" => [9000]
    }
  )
  facts = {
    "image" => "nix:/nix/store/abc-svc/svc.wasm",
    "ports" => [8080],
    "annotations" => {"blue.pleme.io/access" => access},
    "mounts" => [
      {
        "path" => "/data",
        "host_path" => "/var/lib/pod/data",
        "read_only" => false
      }
    ]
  }
  v = json_read(admit(json_stringify(facts)))
  assert v.grant.network.bind == [9000]
  assert v.grant.network.connect == [{"host" => "db.data.svc", "port" => 5432}]
  assert first(v.grant.preopens).writable == true
  assert v.grant.env == false
  assert member?("egress cache is not host:port", v.withheld)
end

test "no network effect withholds every peer and port"
  access = json_stringify(
    {"effects" => ["clock"], "egress" => ["db:5432"], "listen" => [80]}
  )
  v = json_read(
    admit(
      json_stringify(
        {
          "image" => "nix:/nix/store/a/a.wasm",
          "annotations" => {"blue.pleme.io/access" => access}
        }
      )
    )
  )
  assert v.grant.network.bind == []
  assert v.grant.network.connect == []
  assert v.serveHttp.nil?
  assert count(v.withheld) == 1
end

test "an access that declares exec, or is not JSON, refuses the pod alone"
  exec = json_stringify({"exec" => ["/bin/sh"]})
  a = json_read(
    admit(
      json_stringify(
        {
          "image" => "nix:/nix/store/a/a.wasm",
          "annotations" => {"blue.pleme.io/access" => exec}
        }
      )
    )
  )
  assert contains?(a.refused, "cannot run a program")
  b = json_read(
    admit(
      json_stringify(
        {
          "image" => "nix:/nix/store/a/a.wasm",
          "annotations" => {"blue.pleme.io/access" => "not json"}
        }
      )
    )
  )
  assert contains?(b.refused, "not a JSON object")
end

test "limits come from the container memory and cpu limits and the pod deadline"
  v = json_read(
    admit(
      json_stringify(
        {
          "image" => "nix:/nix/store/a/a.wasm",
          "memory_limit_bytes" => 1024,
          "cpu_limit_milli" => 250,
          "active_deadline_seconds" => 5
        }
      )
    )
  )
  assert v.limits.memoryBytes == 1024
  assert v.limits.deadlineMs == 5000
  assert v.limits.cpuMillis == 250
end

test "a crash restarts in place within the budget, then surfaces"
  crash = fn(n, policy)
    json_read(
      supervise(
        json_stringify(
          {
            "exit" => {"kind" => "code", "code" => 1},
            "restarts" => n,
            "restart_policy" => policy
          }
        )
      )
    )
  end
  assert crash(0, "Always").action == "restart"
  assert crash(0, "Always").delayMs == 200
  assert crash(2, "OnFailure").delayMs == 800
  assert crash(3, "Always").action == "surface"
  assert crash(0, "Never").action == "surface"
  assert crash(0, "Always").disposition == {"code" => 1}
end

test "a clean exit, a stop and a limit are never restarted in place"
  end_of = fn(exit)
    json_read(
      supervise(
        json_stringify(
          {"exit" => exit, "restarts" => 0, "restart_policy" => "Always"}
        )
      )
    )
  end
  assert end_of({"kind" => "code", "code" => 0}).action == "surface"
  assert end_of({"kind" => "stopped"}).disposition == {"signal" => 15}
  assert end_of({"kind" => "memoryLimit", "limitBytes" => 1}).action ==
    "surface"
  assert end_of({"kind" => "deadline"}).action == "surface"
  assert end_of({"kind" => "trapped", "message" => "unreachable"}).action ==
    "restart"
end

test "an exec probe is answered from the component's state"
  assert json_read(
    probe(json_stringify({"argv" => ["cat", "/ready"], "running" => true}))
  ).exitCode == 0
  ended = json_read(
    probe(json_stringify({"argv" => ["cat", "/ready"], "running" => false}))
  )
  assert ended.exitCode == 1
  assert contains?(ended.stderr, "cat /ready was not run")
end
