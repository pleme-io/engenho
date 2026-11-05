@moduledoc """
kakoi (囲い) decides the enclosure of one native container: which identity it runs as and what it keeps.

Secure is what a pod gets by saying nothing: a non-root account, no new privileges, no capabilities, a Landlock domain and default bounds. Root is a grant the pod's declaration names with a reason. A demand the node cannot honour is refused for that one pod, or reported where the node cannot enforce anything yet. Rust hands the facts as JSON text and applies the JSON verdict; no word reaches the host.

    default_uid #=> 65532
"""

use("retsu", %i[last without])

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
    /nix/var/nix/daemon-socket
    /run/dbus
    /run/user
    /run/podman
    /run/docker
    /run/containerd
    /run/engenho
    /run/tailscale
    /run/systemd
    /run/udev
    /run/dhcpcd
    /run/ssh-unix-local
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

    hide_list(["/var/lib/app"]) == without(hidden_paths, "/var/lib") #=> true
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

    scratch_list([]) #=> ["/tmp", "/var/tmp", "/dev/shm"]
    scratch_list(["/var/tmp/job"]) #=> ["/tmp", "/dev/shm"]
"""
def scratch_list(volumes)
  filter(
    (dir) -> not any?((v) -> under?(v, dir) or under?(dir, v), volumes),
    %w[/tmp /var/tmp /dev/shm]
  )
end

@doc """
access_annotation: The pod annotation that carries the access value a Bluefile declares, rendered by haibi. It only narrows, so writing it takes no grant.

    access_annotation #=> "kakoi.pleme.io/access"
"""
def access_annotation()
  "kakoi.pleme.io/access"
end

@doc """
effect?: Whether an access allows an effect. A pod with no access declares none of this, so nothing narrows.

    effect?(nil, "network") #=> true
    effect?({"effects" => ["clock"]}, "network") #=> false
    effect?({"effects" => ["network"]}, "network") #=> true
    effect?({"writes" => []}, "network") #=> true
"""
def effect?(access, word)
  if access.nil? or field(access, "effects", nil).nil?
    true
  else
    member?(word, access["effects"])
  end
end

@doc """
namespaces: The kernel namespaces an enclosed container gets of its own. An access without the network effect gets an empty network.

    namespaces(nil) #=> ["mount", "ipc", "uts", "cgroup"]
    last(namespaces({"effects" => ["clock"]})) #=> "net"
"""
def namespaces(access)
  own = %w[mount ipc uts cgroup]
  if effect?(access, "network")
    own
  else
    append(own, ["net"])
  end
end

@doc """
narrowed: The container's volumes its access does not let it write, which stay read-only. A pod with no access keeps what its spec declares.

    narrowed(nil, ["/data"]) #=> []
    narrowed({"writes" => ["/data"]}, ["/data", "/cfg"]) #=> ["/cfg"]
    narrowed({"writes" => ["/data"], "effects" => ["clock"]}, ["/data"]) #=> ["/data"]
    narrowed({"effects" => ["clock"]}, ["/x"]) #=> ["/x"]
"""
def narrowed(access, volumes)
  if access.nil?
    []
  elsif not effect?(access, "filesystem")
    volumes
  else
    writes = field(access, "writes", [])
    filter((v) -> not any?((w) -> under?(v, w), writes), volumes)
  end
end

@doc """
access_refusal: Why an access cannot be true, or nil, as blue judges the same fields in a Bluefile.

    access_refusal({"writes" => ["/x"], "effects" => ["clock"]}) #=> "access writes /x without the filesystem effect"
    access_refusal({"listen" => [80], "effects" => ["clock"]}) #=> "access listens without the network effect"
    access_refusal({"exec" => ["/x"], "effects" => ["clock"]}) #=> "access runs /x without the process effect"
    access_refusal({"reads" => ["x"]}) #=> "access names x, which is relative"
    access_refusal({"writes" => ["/nix/store/x"], "effects" => ["filesystem"]}) #=> "access names /nix/store/x, which is in the store"
    access_refusal({"effects" => ["clock"]}) #=> nil
    access_refusal(nil) #=> nil
"""
def access_refusal(access)
  writes = field(access, "writes", [])
  runs = field(access, "exec", [])
  paths = append(field(access, "reads", []), writes, runs)
  relative = filter((p) -> not starts_with?(p, "/"), paths)
  stored = filter((p) -> under?(p, "/nix/store"), paths)
  if length(writes) != 0 and not effect?(access, "filesystem")
    "access writes #{join(writes, ", ")} without the filesystem effect"
  elsif length(field(access, "listen", [])) != 0 and
      not effect?(access, "network")
    "access listens without the network effect"
  elsif length(runs) != 0 and not effect?(access, "process")
    "access runs #{join(runs, ", ")} without the process effect"
  elsif length(relative) != 0
    "access names #{join(relative, ", ")}, which is relative"
  elsif length(stored) != 0
    "access names #{join(stored, ", ")}, which is in the store"
  end
end

@doc """
fs_rights_since: Each right Landlock can handle on a path, with the ABI that brought it.

    retsu::first(fs_rights_since) #=> [1, "execute"]
    last(fs_rights_since) #=> [9, "resolve-unix"]
"""
def fs_rights_since()
  [
    [1, "execute"],
    [1, "write-file"],
    [1, "read-file"],
    [1, "read-dir"],
    [1, "remove-dir"],
    [1, "remove-file"],
    [1, "make-char"],
    [1, "make-dir"],
    [1, "make-reg"],
    [1, "make-sock"],
    [1, "make-fifo"],
    [1, "make-block"],
    [1, "make-sym"],
    [2, "refer"],
    [3, "truncate"],
    [5, "ioctl-dev"],
    [9, "resolve-unix"]
  ]
end

@doc """
handled_rights: The rights a kernel of this ABI can hold. A right left out is a right the kernel does not mediate, which landlock_gaps says.

    member?("refer", handled_rights(1)) #=> false
    member?("refer", handled_rights(2)) #=> true
    member?("resolve-unix", handled_rights(6)) #=> false
    length(handled_rights(9)) #=> 17
    handled_rights(0) #=> []
"""
def handled_rights(abi)
  map(last, filter((row) -> row[0] <= abi, fs_rights_since))
end

@doc """
scopes_of: What a process of this ABI is cut off from outside its Landlock domain. The main process and its children hold both. A command run into the pod holds only the sockets: its domain is its own, so a signal scope would stop `kill -HUP 1` and `nginx -s reload`, which is what an exec is for.

    scopes_of(6, "start") #=> ["abstract-unix", "signal"]
    scopes_of(6, "join") #=> ["abstract-unix"]
    scopes_of(5, "start") #=> []
"""
def scopes_of(abi, door)
  if abi < 6
    []
  elsif door == "join"
    ["abstract-unix"]
  else
    %w[abstract-unix signal]
  end
end

@doc """
landlock_gaps: What this kernel's ABI leaves unheld for an enclosed pod, each by name, for an event a reader sees.

    landlock_gaps(9) #=> []
    landlock_gaps(6) #=> ["this node's kernel has Landlock ABI 6, so pathname unix sockets outside this pod stay reachable: that needs ABI 9"]
    length(landlock_gaps(5)) #=> 2
    landlock_gaps(0) #=> ["this node's kernel has no Landlock, so this pod's file rules, exec gate, abstract sockets, signals and pathname sockets are not held, and a read under a hidden path stays hidden"]
"""
def landlock_gaps(abi)
  if abi < 1
    ["this node's kernel has no Landlock, so this pod's file rules, exec gate, abstract sockets, signals and pathname sockets are not held, and a read under a hidden path stays hidden"]
  else
    append(
      named_if(
        abi < 6,
        [
          "this node's kernel has Landlock ABI #{to_s(abi)}, so abstract sockets and signals outside this pod stay reachable: that needs ABI 6"
        ]
      ),
      named_if(
        abi < 9,
        [
          "this node's kernel has Landlock ABI #{to_s(abi)}, so pathname unix sockets outside this pod stay reachable: that needs ABI 9"
        ]
      )
    )
  end
end

@doc """
reading: The rights to read a path and what lies under it.

    reading #=> ["read-file", "read-dir"]
"""
def reading()
  %w[read-file read-dir]
end

@doc """
writing: The rights to change what lies under a path. No device can be made, and a socket there can be reached.

    member?("make-char", writing) #=> false
    member?("make-block", writing) #=> false
    member?("write-file", writing) #=> true
    member?("resolve-unix", writing) #=> true
"""
def writing()
  append(
    reading,
    %w[
      write-file
      truncate
      remove-dir
      remove-file
      make-dir
      make-reg
      make-sock
      make-fifo
      make-sym
      refer
      resolve-unix
    ]
  )
end

@doc """
safe_devices: The device nodes every program expects, the only ones an enclosed pod opens for writing.

    member?("/dev/null", safe_devices) #=> true
    member?("/dev/sda", safe_devices) #=> false
"""
def safe_devices()
  %w[
    /dev/null
    /dev/zero
    /dev/full
    /dev/random
    /dev/urandom
    /dev/tty
    /dev/ptmx
    /dev/pts
  ]
end

@doc """
runtime_reads: What a program needs to read at all when its access narrows reads: names, certificates, the time zone, and its own limits.

    member?("/etc/resolv.conf", runtime_reads) #=> true
    member?("/etc/shadow", runtime_reads) #=> false
"""
def runtime_reads()
  %w[
    /etc/resolv.conf
    /etc/hosts
    /etc/nsswitch.conf
    /etc/passwd
    /etc/group
    /etc/ssl
    /etc/pki
    /etc/localtime
    /etc/services
    /etc/protocols
    /sys/fs/cgroup
    /sys/devices/system/cpu
  ]
end

@doc """
read_paths: Where a pod reads. With no `reads` declared the host stays read-only and visible, less what is hidden; `reads` narrows that to itself and what a program needs at all.

    read_paths(nil) #=> ["/"]
    read_paths({"effects" => ["clock"]}) #=> ["/"]
    last(read_paths({"reads" => ["/run/secrets/x"]})) #=> "/run/secrets/x"
    member?("/", read_paths({"reads" => []})) #=> false
"""
def read_paths(access)
  reads = field(access, "reads", nil)
  if reads.nil?
    ["/"]
  else
    append(runtime_reads, reads)
  end
end

@doc """
exec_paths: Where a pod may start a program. The store always, or nothing runs. A pod with no access keeps its volumes and scratch, since a chart's script in a ConfigMap and a tool an init container copied are real workloads. An access names what it runs with `exec`, and without the process effect or an `exec` list nothing writable runs.

    exec_paths(nil, ["/v"], ["/tmp"]) #=> ["/nix/store", "/v", "/tmp"]
    exec_paths({"effects" => ["clock"]}, ["/v"], ["/tmp"]) #=> ["/nix/store"]
    exec_paths({"effects" => ["process"], "exec" => ["/opt/x"]}, ["/v"], ["/tmp"]) #=> ["/nix/store", "/opt/x"]
    exec_paths({"effects" => ["process"]}, ["/v"], ["/tmp"]) #=> ["/nix/store"]
"""
def exec_paths(access, own, scratch)
  store = ["/nix/store"]
  if access.nil?
    append(store, own, scratch)
  elsif effect?(access, "process")
    append(store, field(access, "exec", []))
  else
    store
  end
end

@doc """
landlock_rules: Each path a pod's Landlock domain allows, with the rights this kernel can hold there. A path with no right left is dropped, since the kernel refuses an empty rule.

    length(filter((r) -> r["path"] == "/nix/store", landlock_rules({"landlock_abi" => 6}, [], []))) #=> 2
    landlock_rules({"landlock_abi" => 1, "volumes" => []}, [], []) == [] #=> false
"""
def landlock_rules(facts, own, scratch)
  access = field(facts, "access", nil)
  handled = handled_rights(field(facts, "landlock_abi", 0))
  read_only = field(facts, "read_only_volumes", [])
  shut = narrowed(access, own)
  writable = filter(
    (v) -> not member?(v, read_only) and not member?(v, shut),
    own
  )
  writes = if effect?(access, "filesystem")
    field(access, "writes", [])
  else
    []
  end
  reached = if field(access, "reads", nil).nil?
    own
  else
    filter((v) -> any?((r) -> under?(v, r), field(access, "reads", [])), own)
  end
  rule = fn(path, rights)
    {"path" => path, "rights" => filter((r) -> member?(r, handled), rights)}
  end
  all = append(
    map((p) -> rule(p, reading), read_paths(access)),
    map((p) -> rule(p, reading), ["/nix/store"]),
    map((p) -> rule(p, ["execute"]), exec_paths(access, own, scratch)),
    [rule("/proc", %w[read-file read-dir write-file truncate])],
    map(
      (p) -> rule(p, %w[read-file write-file truncate ioctl-dev]),
      safe_devices
    ),
    map((p) -> rule(p, writing), append(scratch, writable, writes)),
    map((p) -> rule(p, ["resolve-unix"]), reached)
  )
  filter((r) -> length(r["rights"]) != 0, all)
end

@doc """
landlock_of: The Landlock domain of an enclosed pod, or nil on a kernel with none.

    landlock_of({"landlock_abi" => 0}, [], []) #=> nil
    landlock_of({"landlock_abi" => 6}, [], []).scopes #=> ["abstract-unix", "signal"]
    landlock_of({"landlock_abi" => 6}, [], []).join_scopes #=> ["abstract-unix"]
    member?("resolve-unix", landlock_of({"landlock_abi" => 6}, [], []).handled) #=> false
"""
def landlock_of(facts, own, scratch)
  abi = field(facts, "landlock_abi", 0)
  if abi >= 1
    {
      "handled" => handled_rights(abi),
      "rules" => landlock_rules(facts, own, scratch),
      "scopes" => scopes_of(abi, "start"),
      "join_scopes" => scopes_of(abi, "join")
    }
  end
end

@doc """
visible_paths: What a pod's enclosure must not hide: its volumes, and what it declares it reads, which only a kernel with Landlock can narrow back to exactly that.

    visible_paths({"landlock_abi" => 6, "access" => {"reads" => ["/run/secrets/x"]}}, ["/v"]) #=> ["/v", "/run/secrets/x"]
    visible_paths({"landlock_abi" => 0, "access" => {"reads" => ["/run/secrets/x"]}}, ["/v"]) #=> ["/v"]
"""
def visible_paths(facts, own)
  if field(facts, "landlock_abi", 0) >= 1
    append(own, field(field(facts, "access", nil), "reads", []))
  else
    own
  end
end

@doc """
capability_rendering: What the enclosure does with each of blue's host effects. A row may say the kernel has no door; it may not be absent, and engenho's build fails on an effect with no row.

    retsu::first(capability_rendering) #=> ["network", "an empty network namespace when refused"]
"""
def capability_rendering()
  [
    ["network", "an empty network namespace when refused"],
    ["filesystem", "no writable volume when refused"],
    ["process", "Landlock runs nothing outside the store when refused"],
    ["environment", "no kernel door: engine only"],
    [
      "clock",
      "setting the clock is always denied; reading it has no kernel door"
    ],
    ["entropy", "no kernel door: engine only"],
    ["comm-flash", "no device until a row says so"],
    ["comm-scavenger", "no device until a row says so"],
    ["mpc-threshold", "no kernel door: engine only"],
    ["storage-append", "follows filesystem"],
    ["audio-out", "no device until a row says so"],
    ["audio-in", "no device until a row says so"],
    ["display", "no device until a row says so"],
    ["kernel-bpf", "named capabilities by grant"]
  ]
end

@doc """
rendered_capabilities: The effects capability_rendering has a row for, as an answer the seam can read.

    member?("network", json_read(rendered_capabilities("{}"))) #=> true
"""
def rendered_capabilities(_facts_text)
  json_stringify(map(retsu::first, capability_rendering))
end

@doc """
named_if: The words when the condition holds, else none.

    named_if(true, ["a"]) #=> ["a"]
    named_if(false, ["a"]) #=> []
"""
def named_if(condition, words)
  if condition
    words
  else
    []
  end
end

@doc """
unhonoured: What a granted pod declares that a grant cannot apply, since a grant keeps no enclosure.

    unhonoured({"read_only_root_fs" => true}) #=> ["readOnlyRootFilesystem"]
    unhonoured({"cap_drop" => ["ALL"]}) #=> ["capabilities.drop"]
    unhonoured({"no_new_privileges" => true}) #=> ["allowPrivilegeEscalation: false"]
    unhonoured({"seccomp_profile" => "RuntimeDefault"}) #=> ["seccompProfile RuntimeDefault"]
    unhonoured({}) #=> []
"""
def unhonoured(declared)
  append(
    named_if(
      declared["seccomp_profile"] == "RuntimeDefault",
      ["seccompProfile RuntimeDefault"]
    ),
    if field(declared, "read_only_root_fs", false)
      ["readOnlyRootFilesystem"]
    else
      []
    end,
    if length(field(declared, "cap_drop", [])) != 0
      ["capabilities.drop"]
    else
      []
    end,
    if field(declared, "no_new_privileges", false)
      ["allowPrivilegeEscalation: false"]
    else
      []
    end
  )
end

@doc """
any?: Whether f accepts some element; false for [].
"""
def any?(f, xs)
  length(filter(f, xs)) != 0
end

@doc """
denied_syscalls: What an enclosed container may never call: changing what it can see, reaching other processes or the kernel's own machinery. Each is a name and its number on x86_64 and aarch64, nil where the architecture has none.

    retsu::first(denied_syscalls) #=> ["mount", 165, 40]
"""
def denied_syscalls()
  [
    ["mount", 165, 40],
    ["umount2", 166, 39],
    ["pivot_root", 155, 41],
    ["chroot", 161, 51],
    ["unshare", 272, 97],
    ["setns", 308, 268],
    ["ptrace", 101, 117],
    ["process_vm_readv", 310, 270],
    ["process_vm_writev", 311, 271],
    ["bpf", 321, 280],
    ["perf_event_open", 298, 241],
    ["kexec_load", 246, 104],
    ["kexec_file_load", 320, 294],
    ["init_module", 175, 105],
    ["finit_module", 313, 273],
    ["delete_module", 176, 106],
    ["reboot", 169, 142],
    ["swapon", 167, 224],
    ["swapoff", 168, 225],
    ["acct", 163, 89],
    ["add_key", 248, 217],
    ["request_key", 249, 218],
    ["keyctl", 250, 219],
    ["userfaultfd", 323, 282],
    ["open_by_handle_at", 304, 265],
    ["quotactl", 179, 60],
    ["settimeofday", 164, 170],
    ["clock_settime", 227, 112],
    ["adjtimex", 159, 171],
    ["iopl", 172, nil],
    ["ioperm", 173, nil],
    ["io_uring_setup", 425, 425],
    ["io_uring_enter", 426, 426],
    ["io_uring_register", 427, 427],
    ["open_tree", 428, 428],
    ["move_mount", 429, 429],
    ["fsopen", 430, 430],
    ["fsconfig", 431, 431],
    ["fsmount", 432, 432],
    ["fspick", 433, 433],
    ["mount_setattr", 442, 442],
    ["pidfd_getfd", 438, 438],
    ["process_madvise", 440, 440],
    ["quotactl_fd", 443, 443],
    ["kcmp", 312, 272],
    ["fanotify_init", 300, 262],
    ["syslog", 103, 116],
    ["clock_adjtime", 305, 266],
    ["sethostname", 170, 161],
    ["setdomainname", 171, 162],
    ["vhangup", 153, 58],
    ["lookup_dcookie", 212, 18],
    ["nfsservctl", 180, 42],
    ["mbind", 237, 235],
    ["set_mempolicy", 238, 237],
    ["migrate_pages", 256, 238],
    ["move_pages", 279, 239],
    ["uselib", 134, nil],
    ["ustat", 136, nil],
    ["sysfs", 139, nil]
  ]
end

@doc """
withheld_syscalls: Calls answered as though this kernel lacked them, so a C library falls back to one the filter can read. clone3 keeps its flags in user memory, where no filter can see them.

    retsu::first(withheld_syscalls) #=> ["clone3", 435, 435]
"""
def withheld_syscalls()
  [["clone3", 435, 435]]
end

@doc """
newest_allowed_syscall: The highest call number an enclosure may make. A call the kernel adds later is refused until this is raised on purpose, and so is every number in the x32 range, which shares the x86_64 architecture value and would miss each entry above.

    newest_allowed_syscall #=> 466
"""
def newest_allowed_syscall()
  466
end

@doc """
arch_column: Which column of a syscall table holds this architecture's numbers, or nil where it is not known.

    arch_column("x86_64") #=> 1
    arch_column("aarch64") #=> 2
    arch_column("riscv64") #=> nil
"""
def arch_column(arch)
  if arch == "x86_64"
    1
  elsif arch == "aarch64"
    2
  end
end

@doc """
numbers_in: One architecture's numbers from a syscall table, none where the architecture is not known, since a wrong number would deny the wrong call.

    numbers_in(withheld_syscalls, "x86_64") #=> [435]
    numbers_in(withheld_syscalls, "riscv64") #=> []
"""
def numbers_in(table, arch)
  column = arch_column(arch)
  if column.nil?
    []
  else
    filter((n) -> n != nil, map((row) -> row[column], table))
  end
end

@doc """
denied_numbers: The numbers of the denied calls on one architecture.

    member?(165, denied_numbers("x86_64")) #=> true
    member?(40, denied_numbers("aarch64")) #=> true
    denied_numbers("riscv64") #=> []
"""
def denied_numbers(arch)
  numbers_in(denied_syscalls, arch)
end

@doc """
guarded_calls: Calls allowed except for what their first argument asks: clone with any namespace flag, which would reach what unshare is denied, and a socket of the kernel crypto or vsock families.

    retsu::first(guarded_calls("x86_64"))["mask"] #=> 2114060288
    last(guarded_calls("aarch64"))["number"] #=> 198
    guarded_calls("riscv64") #=> []
"""
def guarded_calls(arch)
  column = arch_column(arch)
  if column.nil?
    []
  else
    [
      {"number" => ["clone", 56, 220][column], "mask" => 2_114_060_288},
      {"number" => ["socket", 41, 198][column], "values" => [38, 40]}
    ]
  end
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
keepable_capabilities: The capabilities an enclosed pod may keep by naming them. The Restricted standard of Kubernetes allows exactly this one, and a port below 1024 reaches nothing but its own listener. Any other capability needs a grant.

    keepable_capabilities #=> ["NET_BIND_SERVICE"]
"""
def keepable_capabilities()
  ["NET_BIND_SERVICE"]
end

@doc """
capability_word: A capability as a pod may spell it, in the one spelling the rest of this program reads.

    capability_word("CAP_NET_BIND_SERVICE") #=> "NET_BIND_SERVICE"
    capability_word("net_bind_service") #=> "NET_BIND_SERVICE"
    capability_word("NET_ADMIN") #=> "NET_ADMIN"
"""
def capability_word(name)
  replace(upcase(name), "CAP_", "")
end

@doc """
kept_capabilities: What an enclosed container keeps of what it adds, as the seam's words.

    kept_capabilities({"cap_add" => ["CAP_NET_BIND_SERVICE", "NET_BIND_SERVICE", "NET_ADMIN"]}) #=> ["NET_BIND_SERVICE"]
    kept_capabilities({}) #=> []
"""
def kept_capabilities(declared)
  distinct(
    filter(
      (c) -> member?(c, keepable_capabilities),
      map(capability_word, field(declared, "cap_add", []))
    )
  )
end

@doc """
ungranted_capabilities: What a container adds that only a grant allows.

    ungranted_capabilities(["NET_BIND_SERVICE", "SYS_ADMIN", "ALL"]) #=> ["SYS_ADMIN", "ALL"]
    ungranted_capabilities(["cap_net_bind_service"]) #=> []
"""
def ungranted_capabilities(caps)
  filter((c) -> not member?(capability_word(c), keepable_capabilities), caps)
end

@doc """
capability_conflict: The capabilities a container both adds and drops by name. `ALL` in drop is how a pod says the rest, so it conflicts with nothing.

    capability_conflict({"cap_add" => ["NET_BIND_SERVICE"], "cap_drop" => ["ALL"]}) #=> []
    capability_conflict({"cap_add" => ["NET_BIND_SERVICE"], "cap_drop" => ["CAP_NET_BIND_SERVICE"]}) #=> ["NET_BIND_SERVICE"]
"""
def capability_conflict(declared)
  dropped = map(capability_word, field(declared, "cap_drop", []))
  filter(
    (c) -> member?(c, dropped),
    map(capability_word, field(declared, "cap_add", []))
  )
end

@doc """
demands: Each thing the container declares that only a grant allows, as text naming it.

    demands({"run_as_user" => 0}) #=> ["runAsUser 0"]
    demands({"run_as_user" => 1000}) #=> []
    demands({"privileged" => true}) #=> ["privileged"]
    demands({"allow_escalation" => true}) #=> ["allowPrivilegeEscalation"]
    demands({"cap_add" => ["NET_ADMIN"]}) #=> ["capabilities.add NET_ADMIN"]
    demands({"cap_add" => ["NET_BIND_SERVICE"]}) #=> []
    demands({"cap_add" => ["NET_BIND_SERVICE", "SYS_ADMIN"]}) #=> ["capabilities.add SYS_ADMIN"]
    demands({"host_pid" => true}) #=> ["hostPID"]
    demands({"host_ipc" => true}) #=> ["hostIPC"]
    demands({"seccomp_profile" => "Unconfined"}) #=> ["seccompProfile Unconfined"]
    demands({"seccomp_profile" => "RuntimeDefault"}) #=> []
    demands({"devices" => ["/dev/ttyUSB0"]}) #=> ["a device mount of /dev/ttyUSB0"]
"""
def demands(declared)
  caps = ungranted_capabilities(field(declared, "cap_add", []))
  devices = field(declared, "devices", [])
  append(
    named_if(field(declared, "host_pid", false), ["hostPID"]),
    named_if(field(declared, "host_ipc", false), ["hostIPC"]),
    named_if(
      declared["seccomp_profile"] == "Unconfined",
      ["seccompProfile Unconfined"]
    ),
    named_if(
      length(devices) != 0,
      ["a device mount of #{join(devices, ", ")}"]
    ),
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
    contradiction({"cap_add" => ["NET_BIND_SERVICE"], "cap_drop" => ["NET_BIND_SERVICE"]}) #=> "capabilities.add and capabilities.drop both name NET_BIND_SERVICE"
"""
def contradiction(declared)
  both = capability_conflict(declared)
  if field(declared, "run_as_non_root", false) and
      field(declared, "run_as_user", -1) == 0
    "runAsNonRoot and runAsUser 0 contradict"
  elsif length(both) != 0
    "capabilities.add and capabilities.drop both name #{join(both, ", ")}"
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
unappliable: What a pod declares that this node has no way to apply, with or without a grant.

    unappliable({"seccomp_profile" => "Localhost"}) #=> ["seccompProfile Localhost, since this node loads no profile from a file"]
    unappliable({"seccomp_profile" => "Odd"}) #=> ["seccompProfile Odd, which this node does not know"]
    unappliable({"sysctls" => ["kernel.shm_rmid_forced"]}) #=> ["securityContext.sysctls kernel.shm_rmid_forced, since this node sets none for a pod"]
    unappliable({"volume_devices" => ["/dev/xvda"]}) #=> ["volumeDevices /dev/xvda, since this node attaches no block device"]
    unappliable({"seccomp_profile" => "RuntimeDefault"}) #=> []
    unappliable({"seccomp_profile" => "Unconfined"}) #=> []
    unappliable({}) #=> []
"""
def unappliable(declared)
  profile = declared["seccomp_profile"]
  sysctls = field(declared, "sysctls", [])
  block = field(declared, "volume_devices", [])
  append(
    named_if(
      profile == "Localhost",
      ["seccompProfile Localhost, since this node loads no profile from a file"]
    ),
    named_if(
      profile != nil and
        not member?(profile, %w[RuntimeDefault Unconfined Localhost]),
      ["seccompProfile #{profile}, which this node does not know"]
    ),
    named_if(
      length(sysctls) != 0,
      [
        "securityContext.sysctls #{join(sysctls, ", ")}, since this node sets none for a pod"
      ]
    ),
    named_if(
      length(block) != 0,
      [
        "volumeDevices #{join(block, ", ")}, since this node attaches no block device"
      ]
    )
  )
end

@doc """
namespace_account_base: Where the accounts of namespaces begin. namespace_uid adds four hex digits to it, so 65536 accounts follow.

    namespace_account_base #=> 100000
"""
def namespace_account_base()
  100_000
end

@doc """
foreign_account: Why a declared id is another namespace's, or nil. The ids from namespace_account_base on belong to the namespaces that hash to them, so a pod may name only its own.

    foreign_account({"namespace" => "a", "declared" => {"run_as_user" => 100001}}) #=> "runAsUser 100001 is in the range kept for namespace accounts, so it may be another namespace's identity"
    foreign_account({"namespace" => "a", "declared" => {"run_as_group" => 165535}}) #=> "runAsGroup 165535 is in the range kept for namespace accounts, so it may be another namespace's identity"
    foreign_account({"namespace" => "a", "declared" => {"run_as_user" => namespace_uid("a")}}) #=> nil
    foreign_account({"declared" => {"run_as_user" => 65534}}) #=> nil
    foreign_account({"declared" => {"run_as_user" => 165536}}) #=> nil
    foreign_account({"declared" => {}}) #=> nil
"""
def foreign_account(facts)
  declared = field(facts, "declared", {})
  own = if field(facts, "namespace", "") != ""
    namespace_uid(facts["namespace"])
  end
  foreign? = fn(id)
    id != nil and
      id >= namespace_account_base and
      id < namespace_account_base + 65536 and
      id != own
  end
  if foreign?(declared["run_as_user"])
    "runAsUser #{to_s(declared["run_as_user"])} is in the range kept for namespace accounts, so it may be another namespace's identity"
  elsif foreign?(declared["run_as_group"])
    "runAsGroup #{to_s(declared["run_as_group"])} is in the range kept for namespace accounts, so it may be another namespace's identity"
  end
end

@doc """
system_account_warning: What a declared id below the first login account means. The pod keeps it, since a volume's owner may rest on it, and shares it with any host service that runs as it. An id from 1000 on is the pod's own to name.

    system_account_warning({"run_as_user" => 71}) #=> ["runAsUser 71 is in the host's system range, so this pod shares that account with any host service that runs as it"]
    system_account_warning({"run_as_user" => 999}) == [] #=> false
    system_account_warning({"run_as_user" => 1000}) #=> []
    system_account_warning({"run_as_user" => 0}) #=> []
    system_account_warning({}) #=> []
"""
def system_account_warning(declared)
  uid = declared["run_as_user"]
  named_if(
    uid != nil and uid > 0 and uid < 1000,
    [
      "runAsUser #{to_s(uid)} is in the host's system range, so this pod shares that account with any host service that runs as it"
    ]
  )
end

@doc """
network_conflict: Why a pod's hostNetwork and its access disagree, or nil. Every pod with the network effect shares the host's network today, so hostNetwork alone changes nothing and is not refused.

    network_conflict({"declared" => {"host_network" => true}, "access" => {"effects" => ["clock"]}}) #=> "hostNetwork and an access without the network effect contradict"
    network_conflict({"declared" => {"host_network" => true}}) #=> nil
    network_conflict({"declared" => {}, "access" => {"effects" => ["clock"]}}) #=> nil
"""
def network_conflict(facts)
  declared = field(facts, "declared", {})
  if field(declared, "host_network", false) and
      not effect?(field(facts, "access", nil), "network")
    "hostNetwork and an access without the network effect contradict"
  end
end

@doc """
refusal: Why this container may not start under an enforcing node, or nil.

    refusal({"declared" => {"run_as_user" => 0}}) #=> "this pod asks for runAsUser 0 and names no grant: add the annotation kakoi.pleme.io/root with its reason"
    refusal({"declared" => {"run_as_user" => 0}, "grant" => "needed"}) #=> nil
    refusal({"declared" => {"sysctls" => ["a"]}, "grant" => "needed"}) #=> "this pod asks for securityContext.sysctls a, since this node sets none for a pod"
"""
def refusal(facts)
  declared = field(facts, "declared", {})
  asked = demands(declared)
  impossible = contradiction(declared)
  gate = field(facts, "runtime_gate", false)
  cannot = if gate
    []
  else
    unappliable(declared)
  end
  if impossible != nil
    impossible
  elsif length(cannot) != 0
    "this pod asks for #{join(cannot, "; ")}"
  elsif network_conflict(facts) != nil
    network_conflict(facts)
  elsif not gate and not granted?(facts) and foreign_account(facts) != nil
    foreign_account(facts)
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

def verdict_text(refused, ids, confine, warnings, facts?)
  own = field(facts, "volumes", [])
  arch = field(facts, "arch", "x86_64")
  access = field(facts, "access", nil)
  json_stringify(
    {
      "refused" => refused,
      "uid" => ids[0],
      "gid" => ids[1],
      "no_new_privs" => confine,
      "clear_caps" => confine,
      "limits" => if confine
        default_limits(facts)
      else
        {}
      end,
      "keep" => if confine
        kept_capabilities(field(facts, "declared", {}))
      else
        []
      end,
      "enclosed" => confine,
      "landlock" => if confine
        landlock_of(facts, own, scratch_list(own))
      end,
      "hide" => if confine
        hide_list(visible_paths(facts, own))
      else
        []
      end,
      "scratch" => if confine
        scratch_list(own)
      else
        []
      end,
      "deny" => if confine
        denied_numbers(arch)
      else
        []
      end,
      "absent" => if confine
        numbers_in(withheld_syscalls, arch)
      else
        []
      end,
      "newest" => if confine
        newest_allowed_syscall
      end,
      "guards" => if confine
        guarded_calls(arch)
      else
        []
      end,
      "namespaces" => if confine
        namespaces(access)
      else
        []
      end,
      "narrowed" => if confine
        narrowed(access, own)
      else
        []
      end,
      "warnings" => warnings
    }
  )
end

@doc """
larger: The larger of two numbers.

    larger(1, 2) #=> 2
    larger(2, 1) #=> 2
"""
def larger(a, b)
  if a > b
    a
  else
    b
  end
end

@doc """
default_pids_max: The tasks a pod that declares nothing may hold, which stops a fork bomb from taking the node. Kubernetes has no field for it, so every enclosed pod gets it. A postgres with a connection per task and a JVM with its thread pools sit far below this.

    default_pids_max #=> 4096
"""
def default_pids_max()
  4096
end

@doc """
default_memory_bytes: The memory a pod that declares no limit may hold: a quarter of the node, so one runaway pod cannot exhaust it and a real workload still has room, and never under 1 GiB. A node that states no size gets 4 GiB.

    default_memory_bytes(68_719_476_736) #=> 17_179_869_184
    default_memory_bytes(2_147_483_648) #=> 1_073_741_824
    default_memory_bytes(0) #=> 4_294_967_296
"""
def default_memory_bytes(node_bytes)
  if node_bytes <= 0
    4_294_967_296
  else
    larger(floor(node_bytes / 4), 1_073_741_824)
  end
end

@doc """
default_limits: The bounds an enclosed pod gets for what it does not declare, among those this node can write. A declared limit stands, and a request above the default lifts it, since a pod is promised what it requests.

    default_limits({"bounds" => %w[memory pids], "node_memory_bytes" => 8_589_934_592, "declared" => {}}) == {"pids_max" => 4096, "memory_max" => 2_147_483_648} #=> true
    default_limits({"bounds" => %w[memory pids], "node_memory_bytes" => 8_589_934_592, "declared" => {"memory_limit" => 1}}).memory_max #=> nil
    default_limits({"bounds" => %w[memory pids], "node_memory_bytes" => 8_589_934_592, "declared" => {"memory_request" => 4_294_967_296}}).memory_max #=> 4_294_967_296
    default_limits({"bounds" => ["memory"], "declared" => {}}).pids_max #=> nil
    default_limits({"bounds" => [], "declared" => {}}).memory_max #=> nil
"""
def default_limits(facts)
  bounds = field(facts, "bounds", [])
  declared = field(facts, "declared", {})
  {
    "pids_max" => if member?("pids", bounds)
      default_pids_max
    end,
    "memory_max" => if member?("memory", bounds) and
        field(declared, "memory_limit", 0) <= 0
      larger(
        default_memory_bytes(field(facts, "node_memory_bytes", 0)),
        field(declared, "memory_request", 0)
      )
    end
  }
end

@doc """
limit_gaps: What an enclosed pod goes without on a node that cannot write its default bounds.

    limit_gaps(%w[memory pids]) #=> []
    limit_gaps(["memory"]) #=> ["this node cannot write pids.max, so this pod's tasks have no default bound"]
    length(limit_gaps([])) #=> 2
"""
def limit_gaps(bounds)
  append(
    named_if(
      not member?("memory", bounds),
      ["this node cannot write memory.max, so this pod's memory has no default bound"]
    ),
    named_if(
      not member?("pids", bounds),
      ["this node cannot write pids.max, so this pod's tasks have no default bound"]
    )
  )
end

@doc """
not_applied_warning: What the pod sets that this node reads and does not apply, for an event a reader sees.

    not_applied_warning({"not_applied" => ["fsGroup", "procMount"]}) #=> ["this pod sets fsGroup, procMount, which this node does not apply"]
    not_applied_warning({}) #=> []
"""
def not_applied_warning(declared)
  unread = field(declared, "not_applied", [])
  named_if(
    length(unread) != 0,
    ["this pod sets #{join(unread, ", ")}, which this node does not apply"]
  )
end

@doc """
enclose: The verdict for one container: refused or its identity, what it keeps, and what the node could not enforce.

    json_read(enclose(json_stringify({"mode" => "off", "can_enforce" => true, "declared" => {"run_as_user" => 0}}))).refused #=> nil
"""
def enclose(facts_text)
  facts = json_read(facts_text)
  mode = field(facts, "mode", "enforce")
  access = field(facts, "access", nil)
  why = if field(facts, "access_invalid", false)
    "the annotation #{access_annotation} does not read as an access value"
  elsif access_refusal(access) != nil
    access_refusal(access)
  else
    refusal(facts)
  end
  outside = outside_range(field(facts, "declared", {}))
  if outside != nil
    verdict_text(outside, [nil, nil], false, [])
  elsif mode == "off"
    verdict_text(nil, declared_only(facts), false, [])
  elsif not field(facts, "can_enforce", false)
    if field(facts, "kernel_decides", false)
      verdict_text(
        nil,
        declared_only(facts),
        false,
        ["this node's daemon cannot change identity or enclose: the kernel refuses a uid the pod declares that is not the daemon's, and a pod that declares none runs as the daemon's own account"]
      )
    else
      verdict_text(
        nil,
        [nil, nil],
        false,
        ["this node's daemon cannot change identity, so the container runs as the daemon's own account"]
      )
    end
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
    arch = field(facts, "arch", "x86_64")
    if not granted and arch_column(arch).nil?
      verdict_text(
        "this node's architecture #{arch} has no syscall table, so the pod cannot be enclosed",
        [nil, nil],
        false,
        []
      )
    else
      declared = field(facts, "declared", {})
      lost = if granted
        unhonoured(declared)
      else
        []
      end
      verdict_text(
        nil,
        identity(facts),
        not granted,
        append(
          named_if(
            length(lost) != 0,
            [
              "this pod's root grant keeps no enclosure, so #{join(lost, ", ")} is not applied"
            ]
          ),
          not_applied_warning(declared),
          named_if(not granted, system_account_warning(declared)),
          named_if(not granted, limit_gaps(field(facts, "bounds", []))),
          named_if(not granted, landlock_gaps(field(facts, "landlock_abi", 0)))
        ),
        facts
      )
    end
  end
end

@doc """
dig: The value at a path of keys, or nil where the path ends early.

    dig({"a" => {"b" => 1}}, ["a", "b"]) #=> 1
    dig({"a" => 1}, ["a", "b"]) #=> nil
    dig(nil, ["a"]) #=> nil
"""
def dig(value, path)
  if length(path) == 0
    value
  else
    dig(field(value, retsu::first(path), nil), retsu::rest(path))
  end
end

@doc """
grant_in: The root grant an object carries for itself or for the pods it makes. A controller's own writes pass no admission, so a grant in a template has to be caught where the template arrives.

    grant_in({"metadata" => {"annotations" => {"kakoi.pleme.io/root" => "x"}}}) #=> "x"
    grant_in({"spec" => {"jobTemplate" => {"spec" => {"template" => {"metadata" => {"annotations" => {"kakoi.pleme.io/root" => "y"}}}}}}}) #=> "y"
    grant_in({}) #=> nil
"""
def grant_in(object)
  found = filter(
    (place) -> place != nil,
    map(
      fn(path)
        dig(object, append(path, ["metadata", "annotations", grant_annotation]))
      end,
      [[], %w[spec template], %w[spec jobTemplate spec template]]
    )
  )
  if length(found) == 0
    nil
  else
    retsu::first(found)
  end
end

@doc """
grant_holders: The groups whose members may write a root grant.

    grant_holders #=> ["system:masters"]
"""
def grant_holders()
  ["system:masters"]
end

@doc """
admit: Whether a write may carry the root grant it carries. Only a holder may add or change one; a write that leaves an existing grant as it was passes, so a status update from anyone stays possible.

    json_read(admit(json_stringify({"object" => {}}))).refused #=> nil
"""
def admit(facts_text)
  facts = json_read(facts_text)
  mode = field(facts, "mode", "enforce")
  claimed = grant_in(field(facts, "object", {}))
  groups = field(facts, "groups", [])
  allowed = claimed.nil? or
    claimed == grant_in(field(facts, "current", {})) or
    any?((group) -> member?(group, grant_holders), groups)
  why = if allowed
    nil
  else
    "only #{join(grant_holders, ", ")} may write the annotation #{grant_annotation}, since it runs a pod as root on the node"
  end
  json_stringify(
    {
      "refused" => if mode == "enforce"
        why
      end,
      "warning" => if mode == "audit" and why != nil
        "would refuse under enforce: #{why}"
      end
    }
  )
end

def answer(facts)
  json_read(enclose(json_stringify(facts)))
end

def granting_pod(reason)
  {"metadata" => {"annotations" => {grant_annotation => reason}}}
end

def enforced(extra)
  {
    "mode" => "enforce",
    "can_enforce" => true,
    "declared" => field(extra, "declared", {}),
    "volumes" => field(extra, "volumes", []),
    "access" => field(extra, "access", nil),
    "access_invalid" => field(extra, "access_invalid", false),
    "bounds" => %w[memory pids],
    "landlock_abi" => field(extra, "landlock_abi", 9),
    "read_only_volumes" => field(extra, "read_only_volumes", []),
    "grant" => field(extra, "grant", nil)
  }
end

test "an access narrows the enclosure and never widens it"
  plain = answer(enforced({"volumes" => ["/data"]}))
  assert plain.namespaces == %w[mount ipc uts cgroup]
  assert plain.narrowed == []
  closed = answer(
    enforced(
      {
        "volumes" => %w[/data /cfg],
        "access" => {"effects" => %w[filesystem clock], "writes" => ["/data"]}
      }
    )
  )
  assert member?("net", closed.namespaces)
  assert closed.narrowed == ["/cfg"]
  assert closed.enclosed == true
  granted = answer(
    enforced({"grant" => "x", "access" => {"effects" => ["clock"]}})
  )
  assert granted.namespaces == []
end

test "an access that cannot be true refuses that pod under enforce and warns under audit"
  bad = {"access" => {"effects" => ["clock"], "writes" => ["/x"]}}
  assert contains?(answer(enforced(bad)).refused, "filesystem effect")
  unread = {"access_invalid" => true}
  assert contains?(answer(enforced(unread)).refused, access_annotation)
  audit = answer(assoc(enforced(bad), "mode", "audit"))
  assert audit.refused.nil?
  assert contains?(retsu::first(audit.warnings), "would refuse")
end

test "hostPID and hostIPC are refused by name without a grant and honoured with one"
  each = [
    [{"host_pid" => true}, "hostPID"],
    [{"host_ipc" => true}, "hostIPC"],
    [{"seccomp_profile" => "Unconfined"}, "seccompProfile Unconfined"],
    [{"devices" => ["/dev/ttyUSB0"]}, "/dev/ttyUSB0"]
  ]
  assert every?(
    fn(row)
      refused = answer(enforced({"declared" => row[0]})).refused
      refused != nil and
        contains?(refused, row[1]) and
        contains?(refused, grant_annotation)
    end,
    each
  )
  assert every?(
    fn(row)
      answer(enforced({"grant" => "x", "declared" => row[0]})).refused.nil?
    end,
    each
  )
end

test "hostNetwork stays honoured because every pod with the network effect already shares the host, and an access without the effect contradicts it"
  plain = answer(enforced({"declared" => {"host_network" => true}}))
  assert plain.refused.nil?
  assert plain.enclosed == true
  closed = answer(
    enforced(
      {
        "declared" => {"host_network" => true},
        "access" => {"effects" => ["clock"]}
      }
    )
  )
  assert contains?(closed.refused, "hostNetwork")
end

test "what the node cannot apply is refused with or without a grant"
  each = [
    {"seccomp_profile" => "Localhost"},
    {"seccomp_profile" => "Future"},
    {"sysctls" => ["net.core.somaxconn"]},
    {"volume_devices" => ["/dev/xvda"]}
  ]
  assert every?((d) -> answer(enforced({"declared" => d})).refused != nil, each)
  assert every?(
    (d) -> answer(enforced({"grant" => "x", "declared" => d})).refused != nil,
    each
  )
end

test "RuntimeDefault is what an enclosure already is, and a grant says it cannot apply it"
  declared = {"seccomp_profile" => "RuntimeDefault"}
  enclosed = answer(enforced({"declared" => declared}))
  assert enclosed.refused.nil?
  assert length(enclosed.warnings) == 0
  granted = answer(enforced({"grant" => "x", "declared" => declared}))
  assert granted.refused.nil?
  assert contains?(
    retsu::first(granted.warnings),
    "seccompProfile RuntimeDefault"
  )
end

test "audit names what enforce would refuse among the new fields"
  v = answer(
    assoc(enforced({"declared" => {"host_pid" => true}}), "mode", "audit")
  )
  assert v.refused.nil?
  assert contains?(retsu::first(v.warnings), "hostPID")
end

test "a field the node reads and does not apply reaches a reader, enclosed or granted"
  declared = {"not_applied" => ["fsGroup"]}
  assert contains?(
    retsu::first(answer(enforced({"declared" => declared})).warnings),
    "fsGroup"
  )
  assert contains?(
    retsu::first(
      answer(enforced({"grant" => "x", "declared" => declared})).warnings
    ),
    "fsGroup"
  )
end

test "a declared id keeps its account, inside the system range with a warning, and never another namespace's"
  on = fn(uid)
    answer(
      assoc(
        enforced({"declared" => {"run_as_user" => uid}}),
        "namespace",
        "own"
      )
    )
  end
  assert on(1000).uid == 1000
  assert length(on(1000).warnings) == 0
  assert on(71).uid == 71
  assert contains?(retsu::first(on(71).warnings), "system range")
  assert contains?(on(namespace_uid("other")).refused, "namespace accounts")
  assert on(namespace_uid("own")).refused.nil?
  granted = answer(
    assoc(
      enforced({"grant" => "x", "declared" => {"run_as_user" => 100_001}}),
      "namespace",
      "own"
    )
  )
  assert granted.refused.nil?
  granted_system = answer(
    enforced({"grant" => "x", "declared" => {"run_as_user" => 71}})
  )
  assert length(granted_system.warnings) == 0
end

test "the sockets a host service answers on are hidden, and a pod's own volume still shows through"
  assert every?(
    (p) -> member?(p, hidden_paths),
    %w[/run/engenho /run/tailscale /run/systemd /run/udev]
  )
  assert not member?("/run/engenho", hide_list(["/run/engenho/x"]))
end

test "a kept capability stays enclosed and filtered, and anything else still needs a grant"
  kept = answer(
    enforced(
      {"declared" => {"cap_add" => ["NET_BIND_SERVICE"], "cap_drop" => ["ALL"]}}
    )
  )
  assert kept.refused.nil?
  assert kept.enclosed == true
  assert kept.clear_caps == true
  assert kept.no_new_privs == true
  assert kept.keep == ["NET_BIND_SERVICE"]
  assert length(kept.deny) != 0
  mixed = answer(
    enforced({"declared" => {"cap_add" => %w[NET_BIND_SERVICE NET_RAW]}})
  )
  assert contains?(mixed.refused, "capabilities.add NET_RAW")
  assert contains?(mixed.refused, grant_annotation)
  assert length(answer(enforced({"declared" => {}})).keep) == 0
  granted = answer(
    enforced(
      {"grant" => "x", "declared" => {"cap_add" => ["NET_BIND_SERVICE"]}}
    )
  )
  assert granted.enclosed == false
  assert length(granted.keep) == 0
  audit = answer(
    assoc(
      enforced({"declared" => {"cap_add" => ["NET_BIND_SERVICE"]}}),
      "mode",
      "audit"
    )
  )
  assert length(audit.keep) == 0
end

test "a capability added and dropped by name cannot be true, even with a grant"
  both = {"cap_add" => ["NET_ADMIN"], "cap_drop" => ["NET_ADMIN"]}
  assert contains?(answer(enforced({"declared" => both})).refused, "NET_ADMIN")
  assert answer(enforced({"grant" => "x", "declared" => both})).refused != nil
end

test "an enclosed pod that declares nothing is bounded, a declared limit stands, and a grant and audit stay unbounded"
  plain = answer(assoc(enforced({}), "node_memory_bytes", 68_719_476_736))
  assert plain.limits.pids_max == default_pids_max
  assert plain.limits.memory_max == 17_179_869_184
  declared = answer(enforced({"declared" => {"memory_limit" => 67_108_864}}))
  assert declared.limits.memory_max.nil?
  assert declared.limits.pids_max == default_pids_max
  requested = answer(
    assoc(
      enforced({"declared" => {"memory_request" => 34_359_738_368}}),
      "node_memory_bytes",
      68_719_476_736
    )
  )
  assert requested.limits.memory_max == 34_359_738_368
  granted = answer(enforced({"grant" => "x"}))
  assert granted.limits == {}
  audit = answer(assoc(enforced({}), "mode", "audit"))
  assert audit.limits == {}
end

test "a node that cannot write a default bound says so and never refuses the pod"
  v = answer(assoc(enforced({}), "bounds", []))
  assert v.refused.nil?
  assert v.enclosed == true
  assert v.limits.memory_max.nil?
  assert length(filter((w) -> contains?(w, "default bound"), v.warnings)) == 2
  granted = answer(assoc(enforced({"grant" => "x"}), "bounds", []))
  assert length(granted.warnings) == 0
end

test "a gate in front of a runtime judges who may ask, and leaves to the runtime what only the native seam cannot apply"
  gate = fn(declared)
    answer(assoc(enforced({"declared" => declared}), "runtime_gate", true))
  end
  assert gate({"sysctls" => ["a.b"]}).refused.nil?
  assert gate({"seccomp_profile" => "Localhost"}).refused.nil?
  assert gate({"volume_devices" => ["/dev/x"]}).refused.nil?
  assert gate({"run_as_user" => 100_001}).refused.nil?
  assert gate({"run_as_user" => 0}).refused != nil
  assert gate({"host_pid" => true}).refused != nil
  assert gate({"cap_add" => ["SYS_ADMIN"]}).refused != nil
  assert gate({"cap_add" => ["NET_BIND_SERVICE"]}).refused.nil?
end

def rule_rights(v, path)
  retsu::flat_map(
    filter((r) -> r["path"] == path, v.landlock.rules),
    (r) -> r["rights"]
  )
end

test "a pod's Landlock rules follow its volumes, its scratch and the store"
  v = answer(
    enforced({"volumes" => %w[/data /cfg], "read_only_volumes" => ["/cfg"]})
  )
  assert member?("write-file", rule_rights(v, "/data"))
  assert not member?("write-file", rule_rights(v, "/cfg"))
  assert member?("resolve-unix", rule_rights(v, "/cfg"))
  assert member?("write-file", rule_rights(v, "/tmp"))
  assert member?("execute", rule_rights(v, "/nix/store"))
  assert member?("read-file", rule_rights(v, "/nix/store"))
  assert not member?("write-file", rule_rights(v, "/nix/store"))
  assert member?("read-dir", rule_rights(v, "/"))
  assert not member?("write-file", rule_rights(v, "/"))
  assert not member?("execute", rule_rights(v, "/"))
  assert not member?("write-file", rule_rights(v, "/etc"))
  assert member?("write-file", rule_rights(v, "/dev/null"))
  assert member?("execute", rule_rights(v, "/data"))
end

test "reads and writes declared narrow the rules, and exec narrows what runs"
  v = answer(
    enforced(
      {
        "volumes" => %w[/data /cfg],
        "access" => {
          "effects" => ["filesystem"],
          "writes" => ["/data"],
          "reads" => ["/run/secrets/x"]
        }
      }
    )
  )
  assert rule_rights(v, "/") == []
  assert member?("read-file", rule_rights(v, "/run/secrets/x"))
  assert member?("read-file", rule_rights(v, "/etc/resolv.conf"))
  assert member?("write-file", rule_rights(v, "/data"))
  assert rule_rights(v, "/cfg") == []
  assert not member?("execute", rule_rights(v, "/data"))
  assert not member?("execute", rule_rights(v, "/tmp"))
  assert member?("execute", rule_rights(v, "/nix/store"))
  assert not member?("/run/secrets", v.hide)
  closed = answer(
    enforced(
      {
        "volumes" => ["/data"],
        "access" => {"effects" => ["filesystem"], "writes" => []}
      }
    )
  )
  assert not member?("write-file", rule_rights(closed, "/data"))
  assert member?("/run/secrets", closed.hide)
end

test "a kernel without an ABI holds what it can and says what it cannot, and no pod is refused"
  on = fn(abi)
    answer(enforced({"landlock_abi" => abi, "volumes" => ["/data"]}))
  end
  none = on(0)
  assert none.refused.nil?
  assert none.enclosed == true
  assert none.landlock.nil?
  assert contains?(retsu::first(none.warnings), "no Landlock")
  old = on(1)
  assert old.refused.nil?
  assert old.landlock.scopes == []
  assert not member?("refer", old.landlock.handled)
  assert not member?("truncate", rule_rights(old, "/data"))
  assert length(old.warnings) == 2
  six = on(6)
  assert six.landlock.scopes == %w[abstract-unix signal]
  assert not member?("resolve-unix", rule_rights(six, "/data"))
  assert rule_rights(six, "/data") != []
  assert length(six.warnings) == 1
  assert contains?(retsu::first(six.warnings), "ABI 9")
  nine = on(9)
  assert member?("resolve-unix", rule_rights(nine, "/data"))
  assert length(nine.warnings) == 0
end

test "a read under a hidden path stays hidden on a kernel that could not narrow it back"
  v = answer(
    enforced({"landlock_abi" => 0, "access" => {"reads" => ["/run/secrets/x"]}})
  )
  assert member?("/run/secrets", v.hide)
end

test "a grant keeps no Landlock domain, and audit none either"
  assert answer(enforced({"grant" => "x"})).landlock.nil?
  assert answer(assoc(enforced({}), "mode", "audit")).landlock.nil?
  assert answer(assoc(enforced({}), "mode", "off")).landlock.nil?
end

test "every right the rules name is one the seam knows, and no rule is empty"
  v = answer(enforced({"volumes" => ["/data"], "read_only_volumes" => []}))
  known = map(last, fs_rights_since)
  assert every?(
    fn(r)
      length(r["rights"]) != 0 and every?((w) -> member?(w, known), r["rights"])
    end,
    v.landlock.rules
  )
  assert every?((w) -> member?(w, known), v.landlock.handled)
end

test "a grant says what it cannot apply"
  v = answer(
    enforced({"grant" => "x", "declared" => {"read_only_root_fs" => true}})
  )
  assert v.refused.nil?
  assert contains?(retsu::first(v.warnings), "readOnlyRootFilesystem")
  enclosed = answer(enforced({"declared" => {"read_only_root_fs" => true}}))
  assert length(enclosed.warnings) == 0
end

test "only a holder may add or change a root grant, anywhere it would reach a pod"
  on = (facts) -> json_read(admit(json_stringify(facts)))
  assert on(
    {"object" => granting_pod("x"), "groups" => ["system:authenticated"]}
  ).refused != nil
  assert on({"object" => granting_pod("x"), "groups" => []}).refused != nil
  assert on(
    {
      "object" => granting_pod("x"),
      "groups" => %w[system:masters system:authenticated]
    }
  ).refused.nil?
  template = {"spec" => {"template" => granting_pod("x")}}
  cron = {
    "spec" => {"jobTemplate" => {"spec" => {"template" => granting_pod("x")}}}
  }
  assert on({"object" => template, "groups" => []}).refused != nil
  assert on({"object" => cron, "groups" => []}).refused != nil
  assert on(
    {
      "object" => {"metadata" => {"annotations" => {"a" => "b"}}},
      "groups" => []
    }
  ).refused.nil?
  assert on(
    {
      "object" => granting_pod("x"),
      "current" => granting_pod("y"),
      "groups" => []
    }
  ).refused != nil
  assert on(
    {
      "object" => granting_pod("x"),
      "current" => granting_pod("x"),
      "groups" => []
    }
  ).refused.nil?
end

test "audit warns where enforce refuses, and off says nothing"
  on = fn(mode)
    json_read(
      admit(json_stringify({"mode" => mode, "object" => granting_pod("x")}))
    )
  end
  assert on("enforce").refused != nil
  assert on("enforce").warning.nil?
  assert on("audit").refused.nil?
  assert contains?(on("audit").warning, "would refuse")
  assert on("off").refused.nil?
  assert on("off").warning.nil?
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

test "an enclosed container is denied the calls that reach past it, on its own architecture only"
  on = fn(arch)
    answer(
      {
        "mode" => "enforce",
        "can_enforce" => true,
        "arch" => arch,
        "declared" => {}
      }
    ).deny
  end
  assert member?(101, on("x86_64"))
  assert member?(117, on("aarch64"))
  assert length(on("x86_64")) == length(denied_syscalls)
  unknown = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "arch" => "mips",
      "declared" => {}
    }
  )
  assert contains?(unknown.refused, "mips")
  assert unknown.enclosed == false
  granted_unknown = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "arch" => "mips",
      "grant" => "x",
      "declared" => {}
    }
  )
  assert granted_unknown.refused.nil?
  granted = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "grant" => "x",
      "arch" => "x86_64",
      "declared" => {}
    }
  )
  assert length(granted.deny) == 0
end

test "an enclosure withholds clone3, guards clone and socket, and stops at the newest call"
  v = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "arch" => "aarch64",
      "declared" => {}
    }
  )
  assert v.absent == [435]
  assert v.newest == newest_allowed_syscall
  assert retsu::first(v.guards)["number"] == 220
  assert retsu::first(v.guards)["mask"] == 2_114_060_288
  assert last(v.guards)["values"] == [38, 40]
  granted = answer(
    {
      "mode" => "enforce",
      "can_enforce" => true,
      "arch" => "aarch64",
      "grant" => "x",
      "declared" => {}
    }
  )
  assert granted.absent == []
  assert granted.newest.nil?
  assert granted.guards == []
end

test "no denied call sits above the newest allowed call, where it could not be reached"
  assert every?(
    (n) -> n <= newest_allowed_syscall,
    append(denied_numbers("x86_64"), denied_numbers("aarch64"))
  )
end

test "every denied call names a number on some architecture and none repeats"
  assert every?((row) -> row[1] != nil or row[2] != nil, denied_syscalls)
  assert length(distinct(map(retsu::first, denied_syscalls))) ==
    length(denied_syscalls)
  assert length(distinct(denied_numbers("x86_64"))) ==
    length(denied_numbers("x86_64"))
  assert length(distinct(denied_numbers("aarch64"))) ==
    length(denied_numbers("aarch64"))
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
  assert contains?(retsu::first(v.warnings), "would refuse under enforce")
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
  assert contains?(retsu::first(v.warnings), "cannot change identity")
end

test "an unprivileged daemon on Linux hands a declared uid to the kernel to refuse, never its own account in its place"
  v = answer(
    {
      "mode" => "enforce",
      "can_enforce" => false,
      "kernel_decides" => true,
      "declared" => {"run_as_user" => 65534}
    }
  )
  assert v.refused.nil?
  assert v.uid == 65534
  assert v.enclosed == false
  assert contains?(retsu::first(v.warnings), "the kernel refuses")
  silent = answer(
    {
      "mode" => "enforce",
      "can_enforce" => false,
      "kernel_decides" => true,
      "declared" => {}
    }
  )
  assert silent.uid.nil?
  elsewhere = answer(
    {
      "mode" => "enforce",
      "can_enforce" => false,
      "declared" => {"run_as_user" => 65534}
    }
  )
  assert elsewhere.uid.nil?
end
