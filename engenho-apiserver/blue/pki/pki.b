@moduledoc """
pki decides how long a leaf certificate lives and when it is renewed.

A leaf belongs to the period the clock falls in, so a restart inside one period issues the same bytes and a kubeconfig does not churn. Its span outlives its period, so a renewal that keeps failing leaves time to be seen before anything expires. The CA is not a leaf and is not decided here. Rust issues what the lease names; no word reaches the host.

    window("server") #=> [2592000, 7776000, 4]
"""

@doc """
day: Seconds in a day.

    day #=> 86400
"""
def day()
  86400
end

@doc """
window: One kind of leaf as [period, span, serial base], in seconds. The server leaf renews monthly and lasts three months; the admin client leaf, which a kubeconfig embeds and an operator may have copied, renews quarterly and lasts a year.

    window("client") #=> [7776000, 31536000, 5]
    window("other") #=> nil
"""
def window(kind)
  if kind == "server"
    [30 * day, 90 * day, 4]
  elsif kind == "client"
    [90 * day, 365 * day, 5]
  end
end

@doc """
lease_of: The lease a leaf of `kind` holds at `now`.

    lease_of("server", 0)["not_after"] #=> 7776000
    lease_of("server", 2592000)["serial"] #=> 6
    lease_of("server", 2591999)["renew_at"] #=> 2592000
"""
def lease_of(kind, now)
  w = window(kind)
  period = w[0]
  epoch = floor(now / period)
  start = epoch * period
  {
    "not_before" => start - day,
    "not_after" => start + w[1],
    "renew_at" => start + period,
    "serial" => w[2] + epoch * 2
  }
end

@doc """
lease: `lease_of` over JSON text.

    json_read(lease(json_stringify({"kind" => "client", "now" => 0}))).serial #=> 5
"""
def lease(facts_text)
  facts = json_read(facts_text)
  json_stringify(lease_of(facts.kind, facts.now))
end

test "a restart inside one period gets the same lease"
  assert lease_of("server", 2_592_000) == lease_of("server", 5_183_999)
end

test "the next period gets a new serial, for each kind apart"
  assert lease_of("server", 0)["serial"] !=
    lease_of("server", 2_592_000)["serial"]
  assert lease_of("client", 0)["serial"] != lease_of("server", 0)["serial"]
end

test "a lease holds now and outlives its renewal"
  now = 1_790_000_000
  each = map((kind) -> lease_of(kind, now), %w[server client])
  assert every?((l) -> l["not_before"] < now and now < l["renew_at"], each)
  assert every?((l) -> l["renew_at"] < l["not_after"], each)
end

test "no span reaches past a year"
  assert every?((kind) -> window(kind)[1] <= 365 * day, %w[server client])
end
