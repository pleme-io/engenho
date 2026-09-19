# GC owner resolution: upstream oracle notes

Sources: kubernetes/kubernetes@v1.34.0 (`pkg/controller/garbagecollector/*`, its unit tests, `test/integration/garbagecollector`, `cmd/kube-controller-manager/app/core.go`, client-go `restmapper/discovery.go`, apimachinery `api/meta`, `runtime/schema` and `api/validation/objectmeta.go`), plus kube-rs/kube@4.2.0 `kube-runtime/src/reflector/object_ref.rs`. The fixtures are in `gc-owner-resolution.json` (64 cases, 20 with a `naive_trap`).

## The rules

1. **Resolving a kind.** `apiResource` sends `FromAPIVersionAndKind(ref.apiVersion, ref.kind)` to `restMapper.RESTMapping(groupKind, version)`. In production that mapper is client-go's `DeferredDiscoveryRESTMapper` (core.go:660-678). Discovery refreshes every 30s, and on any mapping miss while the cache is not fresh (discovery.go:285-296).
   - The lookup is pinned to the ref's exact group and version. There is **no fallback** to another served version of the same kind.
   - The discovery mapper also registers `strings.ToLower(Kind)` and `Kind+"List"`. So kind `pod` resolves, `PodList` resolves to a guessed resource `podlists`, and `pOd` fails.
   - Any mapping failure becomes `restMappingError` ("unable to get REST mapping for <apiVersion>/<kind>.").
2. **Choosing the namespace.** An owner reference has no namespace field. The owner is looked up in the **dependent's namespace** if the owner kind is namespaced, and cluster-wide otherwise. It is never searched by UID and never looked up in another namespace.
3. **Dangling.** An owner reference is dangling when either:
   - the live metadata GET returns 404, or
   - the GET returns an object whose `metadata.uid != ref.uid`.

   Both outcomes are cached in `absentOwnerCache`. Any other GET error is returned, and the item is requeued.
4. **The absence cache.** It is an LRU (500 entries in production). The key is `{apiVersion, kind, name, uid}` plus a namespace. `controller` and `blockOwnerDeletion` are stripped.
   - `isDangling` probes the key with namespace `""` first, then the key with the dependent's namespace, and only then consults the RESTMapper.
   - Because apiVersion and uid are part of the key, an absence recorded via `rbac/v1` does not cover a ref via `rbac/v1beta1`. An absence for uid 1 does not cover uid 2.
5. **Kinds that cannot be resolved.** `isDangling` returns an error, and `classifyReferences` stops at the **first** error. The dependent is not deleted and not patched, even if every other ref is verifiably dangling. The worker requeues it with rate limiting and no cap. The TODO at garbagecollector.go:402-407 (ignore or strip such refs) is not implemented.
6. **Cluster-scoped dependent with a namespaced owner kind.** This returns `namespacedOwnerOfClusterScopedObjectErr` before any GET. The worker **forgets** the item: it is not deleted and not retried. A virtual node of a namespaced kind with no namespace hits the same error in `getObject` and is also forgotten.
7. **Classification** uses the live owner (`isDangling` returns the owner object):
   - dangling: covered by rule 3;
   - waiting: the owner has a `deletionTimestamp` **and** the `foregroundDeletion` finalizer;
   - everything else is **solid**. That includes an owner that is terminating with the orphan finalizer or with no finalizer.
8. **What attemptToDeleteItem does**, working from the **live** item's ownerReferences rather than the graph's:
   - no owners: nothing;
   - any solid owner: patch out the dangling and waiting refs, matched by uid (SMP `$patch: delete` with `metadata.uid` in the patch body);
   - waiting owners and the item has dependents: delete with Foreground;
   - otherwise: delete with Orphan if the item has the orphan finalizer, Foreground if it has `foregroundDeletion`, and Background in every other case. Deletes carry a uid precondition, plus a resourceVersion precondition when the RV is non-empty.
   - On a 409, the GC does a live GET. If the object is gone, that counts as success. If only the RV changed and `ownerReferences` are identical, the delete is retried without the RV precondition. Otherwise the conflict is returned.
9. **Virtual owner nodes.** The graph creates a virtual node for an owner it has not seen, with the dependent's namespace, even when the owner kind is cluster-scoped.
   - A 404 or uid mismatch on the virtual node produces a virtual delete event.
   - If the node's dependents disagree on its coordinates, the node survives under an alternate identity: the first alternate that sorts after the absent one by (kind, apiVersion, namespace, name, uid), or else the first alternate.
   - A virtual node that exists but has not been observed is requeued until an informer sees it.

## Key upstream lines (quoted)

```go
// garbagecollector.go:382-411 isDangling: cache BEFORE mapper
absentOwnerCacheKey := objectReference{OwnerReference: ownerReferenceCoordinates(reference)}
if gc.absentOwnerCache.Has(absentOwnerCacheKey) { ... return true, nil, nil }
absentOwnerCacheKey.Namespace = item.identity.Namespace
if gc.absentOwnerCache.Has(absentOwnerCacheKey) { ... return true, nil, nil }
// TODO: we need to verify the reference resource is supported by the
// system. If it's not a valid resource, the garbage collector should i)
// ignore the reference when decide if the object should be deleted, and
// ii) should update the object to remove such references. ...
resource, namespaced, err := gc.apiResource(reference.APIVersion, reference.Kind)
if err != nil {
    return false, nil, err
}
```
```go
// garbagecollector.go:442-448
if owner.GetUID() != reference.UID {
    logger.V(5).Info("item's owner is not found, UID mismatch", ...)
    gc.absentOwnerCache.Add(absentOwnerCacheKey)
    return true, nil, nil
}
```
```go
// garbagecollector.go:350-363 attemptToDeleteWorker
if _, ok := err.(*restMappingError); ok {
    // There are at least two ways this can happen:
    // 1. The reference is to an object of a custom type that has not yet been
    //    recognized by gc.restMapper (this is a transient error).
    // 2. The reference is to an invalid group/version. We don't currently
    //    have a way to distinguish this from a valid type we will recognize
    //    after the next discovery sync.
    // For now, record the error and retry.
```
```go
// garbagecollector.go:475
if ownerAccessor.GetDeletionTimestamp() != nil && hasDeleteDependentsFinalizer(ownerAccessor) {
    waitingForDependentsDeletion = append(...)
} else { solid = append(...) }
```
```go
// client-go restmapper/discovery.go:114-117
versionMapper.AddSpecific(gv.WithKind(strings.ToLower(resource.Kind)), plural, singular, scope)
versionMapper.AddSpecific(gv.WithKind(resource.Kind), plural, singular, scope)
// TODO this is producing unsafe guesses that don't actually work, but it matches previous behavior
versionMapper.Add(gv.WithKind(resource.Kind+"List"), scope)
```
Upstream test comment, garbagecollector_test.go:1472: *"final state: child with unresolveable ownerRef remains, queued in pendingAttemptToDelete"*.

## Surprises (where a naive port goes wrong)

- **One unresolvable ref pins the dependent forever.** This holds even when the real owner is gone (the `extensions/v1beta1` Deployment test) and even when the object's other refs are all dangling. There is no give-up. The requeue backoff is the only limit.
- **A cached absence beats the RESTMapper.** An owner whose CRD was deleted still counts as dangling if its absence is cached, because the cache check comes before kind resolution. That is how `TestCRDDeletionCascading` can finish. The mechanism is inferred from the code order; the test only asserts the outcome.
- **A terminating owner is solid** unless it is foreground-deleting.
- **Identity is (apiVersion, kind, name, uid, namespace), never uid alone.** A ref that reuses a live Deployment's uid under kind `Secret` is dangling, and its child is deleted. A ref that reuses an owner's name with an old uid is dangling.
- **Cross-namespace refs** are dangling, and the graph emits `OwnerRefInvalidNamespace`. A cluster-scoped child that names a namespaced owner is the opposite: it is left alone permanently.
- **Kind matching is lenient in one specific way.** Exact case and all-lowercase resolve, `*List` resolves to a bogus resource, and mixed case fails.
- **Validation bounds the input** (objectmeta.go:69-108). An empty version (`""`, `"apps/"`, `"a/b/c"`), an empty kind, name or uid, core `v1 Event` as owner, and more than one `controller=true` are all rejected at write time. `events.k8s.io/v1 Event` is **not** banned.
- **kube-rs `ObjectRef` Eq and Hash ignore uid** (`#[educe(Hash(ignore), PartialEq(ignore))] extra`). `from_owner_ref` needs an exact-case apiVersion and kind match. Neither is safe as the GC's owner identity or as its absence-cache key.

## Not verified (left out of the fixtures)

- Strategic-merge `$patch: delete` keyed by uid, for an item that holds two refs with the **same uid** but different kinds (for example, one solid and one dangling). The patch probably removes both, but I did not check the strategicpatch code.
- What the apiserver returns for a GET on the guessed `podlists` resource. `IsNotFound` would make the ref dangling, but that was not checked.
