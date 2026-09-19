# RBAC `ResourceMatches`: upstream oracle notes (kubernetes v1.34.0)

Fixture: `rbac-resource-matches.json`, 47 cases. 27 come from upstream test tables and 20 are derived from the upstream code.
Sources: fetched raw from `raw.githubusercontent.com/kubernetes/kubernetes/v1.34.0/...` on 2026-09-19.

## The rule in plain words

The authorizer builds one string, then checks each entry in `rule.Resources` in order. The first entry that matches wins.

1. **Build the request string.** `combined = resource` when there is no subresource, otherwise `resource + "/" + subresource`. The two parts are joined even when `resource` is empty, which gives `"/status"` (rbac.go:180-183).
2. **An entry of exactly `"*"` matches everything**, subresources included.
3. **An entry equal to `combined` matches.** This is byte equality: case-sensitive, with no globbing and no normalisation.
4. **An entry of the form `"*/" + subresource` matches** when the request has a non-empty subresource and the entry is exactly `"*/"` followed by the whole subresource string.
5. Nothing else matches. An empty `Resources` list matches nothing.

Answers to the questions in the task:

| Question | Answer | Evidence |
|---|---|---|
| Does bare `pods` match `pods/status`? | **No** | rbac_test.go L222/L231 (shouldFail) |
| Does `pods/status` match bare `pods`? | **No** | helpers_test.go "matches exact rule 02" L102 |
| Is `resource/*` supported? | **No.** `pods/*` only matches a subresource literally named `*` | evaluation_helpers.go L59, L69-71 (code-derived, verified by execution) |
| Is `*/subresource` supported? | **Yes**, for any parent and in any API group (the group is checked separately) | rbac_test.go L239, L247-249; helpers_test.go L108 |
| Does `*` match subresources? | **Yes.** It is checked before any subresource logic | evaluation_helpers.go L55-57 |
| Is `*/*` "all subresources"? | **No.** It only matches the literal subresource `*` | evaluation_helpers.go L69 |

## Key upstream lines (pkg/apis/rbac/v1/evaluation_helpers.go@v1.34.0, L52-78)

```go
func ResourceMatches(rule *rbacv1.PolicyRule, combinedRequestedResource, requestedSubresource string) bool {
	for _, ruleResource := range rule.Resources {
		// if everything is allowed, we match
		if ruleResource == rbacv1.ResourceAll {
			return true
		}
		// if we have an exact match, we match
		if ruleResource == combinedRequestedResource {
			return true
		}
		// We can also match a */subresource.
		// if there isn't a subresource, then continue
		if len(requestedSubresource) == 0 {
			continue
		}
		// if the rule isn't in the format */subresource, then we don't match, continue
		if len(ruleResource) == len(requestedSubresource)+2 &&
			strings.HasPrefix(ruleResource, "*/") &&
			strings.HasSuffix(ruleResource, requestedSubresource) {
			return true
		}
	}
	return false
}
```

The caller is plugin/pkg/auth/authorizer/rbac/rbac.go:RuleAllows, L180-188:

```go
combinedResource := requestAttributes.GetResource()
if len(requestAttributes.GetSubresource()) > 0 {
	combinedResource = requestAttributes.GetResource() + "/" + requestAttributes.GetSubresource()
}
return rbacv1helpers.VerbMatches(...) && rbacv1helpers.APIGroupMatches(...) &&
	rbacv1helpers.ResourceMatches(rule, combinedResource, requestAttributes.GetSubresource()) &&
	rbacv1helpers.ResourceNameMatches(rule, requestAttributes.GetName())
```

The API docs are thinner than the code. `Resources` is documented only as "'*' represents all resources" (staging/src/k8s.io/api/rbac/v1/types.go L59). The `*/sub` form is not mentioned there.

## Surprises and traps for a reimplementation

- **The length check is load-bearing.** `len(rule) == len(sub)+2 && HasPrefix("*/") && HasSuffix(sub)` is the same as `rule == "*/" + sub`. Without the length check, `*/status` would match the subresources `s` and `tatus`. Two cases cover this.
- **There is no `parent/*` form, and `*/*` is not all subresources.** A per-segment glob implementation gets both wrong. To grant every pod subresource you must list each one, or use `*`.
- **Matching works on the joined string, not on a (resource, subresource) tuple.** A SubjectAccessReview with `resource: "pods/status"` and an empty subresource matches the rule `pods/status`, but does not match `*/status`. A tuple-parsing implementation inverts both results. URLs cannot produce this, because requestinfo.go L213-221 takes `Parts[0]` and `Parts[2]`, so a subresource from a URL is always one segment. SubjectAccessReview can produce it: `ResourceAttributesFrom` (pkg/registry/authorization/util/helpers.go L43-44) copies `Resource` and `Subresource` verbatim. Multi-slash subresources such as `other/segment` also come only from that path.
- **An empty resource still joins**, giving `"/status"`, and it matches `*/status`. The empty entry `""` matches an empty resource.
- **Everything is case-sensitive.** `*` matching `Pods` (rbac_test.go L165) holds only because `*` short-circuits.
- **Resource matching has no prefix glob.** `NonResourceURLMatches` in the same file (L102) does support a trailing `*` prefix, but `ResourceMatches` does not: `pod*` is literal.
- **There are two copies with the same body**: `pkg/apis/rbac/v1/evaluation_helpers.go` (used by the authorizer) and `pkg/apis/rbac/helpers.go` (internal types, exercised by `helpers_test.go:TestResourceMatches`). Treat them as one derivation, not two independent confirmations.
- **The escalation check uses a different algorithm.** `staging/src/k8s.io/component-helpers/auth/rbac/validation/policy_comparator.go:resourceCoversAll` (L108-131) splits the servant string at its first `/` and looks for `"*/" + rest`. It also requires a literal `"*"` in the owner to cover a servant `"*"`. Do not reuse `ResourceMatches` for role-escalation checks. `TestCoversSubresourceWildcard` (L48) shows owner `*/scale` covering `foo/scale`. The comparator is not part of this fixture.
- **The namespaces URL quirk is a requestinfo concern, not a ResourceMatches one.** `/api/v1/namespaces/ns/status` and `/finalize` parse as resource `namespaces`, subresource `status` or `finalize` (requestinfo.go L87, L200). Every other `namespaces/<ns>/<x>` path re-roots at `<x>`.

## How the expectations were verified

- The `ResourceMatches` bodies were extracted mechanically from both downloaded upstream files with awk, from `func ResourceMatches(` to the first `}`. The only textual change was stripping the `rbacv1.` prefix. All 47 cases were run through both copies, with `combined` recomputed as in rbac.go L180-183. Result: `cases=47 failures=0`, and the two copies agreed on every case. Harness: `harness-rbac/`.
- **Negative control:** two expectations were flipped (`pods/*` and `*/*` set to true), and the harness went red on exactly those two (exit 1).
- **kube-rs:** a GitHub code search for `PolicyRule` in `kube-rs/kube` returned 0 hits (default branch, 2026-09-19). Its `subresource` hits are client-side request builders. kube-rs has no RBAC evaluator to use as a second oracle, so it contributes no cases.
