@moduledoc """
sealed_kinds decides which core-group kinds a store encrypts at rest.

The store's write path is hot and holds the catalog lock, so it reads a static Rust list. `engenho-store/src/at_rest.rs` carries a test that fails when that list and this word disagree.

    sealed_kinds #=> ["Secret"]
"""

@doc """
sealed_kinds: The core-group kinds whose values are encrypted at rest.

    member?("Secret", sealed_kinds) #=> true
    member?("ConfigMap", sealed_kinds) #=> false
"""
def sealed_kinds()
  ["Secret"]
end
