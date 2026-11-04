@moduledoc """
audit decides how much the API audit log records about one request.

The answer is `Metadata` or `None`, and no word here names a body, so no answer can put a Secret or a token in the log. Rust hands the facts as JSON text and writes what the verdict names; no word reaches the host. What no rule names is recorded.

    level_of({"verb" => "patch", "resource" => "widgets", "subresource" => nil, "refused" => false}) #=> "Metadata"
"""

@doc """
reads: The verbs that change nothing. They are high in volume and bury the writes.

    member?("watch", reads) #=> true
    member?("delete", reads) #=> false
"""
def reads()
  %w[get list watch]
end

@doc """
credentials: The resources whose every touch is recorded, reads too, because they hold or mint a credential.

    member?("secrets", credentials) #=> true
"""
def credentials()
  ["secrets"]
end

@doc """
minting: The subresources that hand out a credential.

    member?("token", minting) #=> true
"""
def minting()
  ["token"]
end

@doc """
sessions: The subresources that open a session into a workload or a node. A client may open one with a GET, so the verb says nothing about what it does.

    member?("exec", sessions) #=> true
    member?("status", sessions) #=> false
"""
def sessions()
  %w[exec attach portforward proxy]
end

@doc """
sensitive?: Whether a request touches a credential or opens a session, whatever its verb.

    sensitive?("secrets", nil) #=> true
    sensitive?("pods", "exec") #=> true
    sensitive?("serviceaccounts", "token") #=> true
    sensitive?("pods", nil) #=> false
"""
def sensitive?(resource, subresource)
  member?(resource, credentials) or
    member?(subresource, sessions) or
    member?(subresource, minting)
end

@doc """
level_of: The level of one request. A refused request is recorded whatever it asked for, since an investigator looks for exactly that.

    level_of({"verb" => "get", "resource" => "pods", "subresource" => nil, "refused" => false}) #=> "None"
    level_of({"verb" => "get", "resource" => "pods", "subresource" => nil, "refused" => true}) #=> "Metadata"
"""
def level_of(facts)
  recorded = facts.refused or
    sensitive?(facts.resource, facts.subresource) or
    not member?(facts.verb, reads)
  if recorded
    "Metadata"
  else
    "None"
  end
end

@doc """
level: `level_of` over JSON text, the word Rust calls.

    level("{\"verb\":\"create\",\"resource\":\"pods\",\"subresource\":null,\"refused\":false}") #=> "\"Metadata\""
"""
def level(facts_text)
  json_stringify(level_of(json_read(facts_text)))
end

def facts(verb, resource, subresource, refused)
  {
    "verb" => verb,
    "resource" => resource,
    "subresource" => subresource,
    "refused" => refused
  }
end

test "every write is recorded"
  each_write = map(
    (verb) -> level_of(facts(verb, "pods", nil, false)),
    %w[create update patch delete deletecollection]
  )
  assert every?((l) -> l == "Metadata", each_write)
end

test "a read of an ordinary resource is not"
  assert every?(
    (verb) -> level_of(facts(verb, "pods", nil, false)) == "None",
    reads
  )
end

test "a read of a Secret is recorded"
  assert every?(
    (verb) -> level_of(facts(verb, "secrets", nil, false)) == "Metadata",
    reads
  )
end

test "a denied read is recorded"
  assert every?(
    (verb) -> level_of(facts(verb, "pods", nil, true)) == "Metadata",
    reads
  )
end

test "a session opened with a GET is recorded"
  assert every?(
    (sub) -> level_of(facts("get", "pods", sub, false)) == "Metadata",
    sessions
  )
end

test "a verb no rule names is recorded"
  assert level_of(facts("impersonate", "", nil, false)) == "Metadata"
end

test "no answer names a level that would carry a body"
  answers = map(
    level_of,
    [
      facts("create", "secrets", nil, false),
      facts("get", "pods", nil, false),
      facts("create", "serviceaccounts", "token", true)
    ]
  )
  assert every?((l) -> l == "Metadata" or l == "None", answers)
end
