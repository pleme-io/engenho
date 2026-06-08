;;;; rbac_authz.lisp — the authored canonical spec for the RBAC Authorizer.
;;;;
;;;; The AUTHORED-LISP-SPEC half of the TYPED-SPEC + INTERPRETER TRIPLET (Brick
;;;; B). The typed border lives in `src/authz/mod.rs` (Attributes / Decision /
;;;; RbacStoreEnv / Authorizer); the working interpreter is `RbacAuthorizer`;
;;;; this file is the canonical statement of the algorithm + canonical instances
;;;; both the prose + the tests are kept honest against. RBAC is an ALLOW-only
;;;; authorizer: a matching rule => Allow, no match => NoOpinion (no deny rules);
;;;; the single-authorizer chain default-denies a NoOpinion.

(defrbac-authorizer rbac
  :doc "Kubernetes RBAC authorization, ALLOW-only. Walks the bindings that
        match the requesting identity, resolves their roles, and grants on the
        first matching PolicyRule. The system:masters short-circuit is the
        behavior-preservation lever: the admin kubeconfig stays allow-all."

  ;; The typed border (Attributes) — the request reduced to authz inputs.
  (border
    (attributes
      (user        :doc "the authenticated principal (UserInfo: username + groups)")
      (verb        :doc "get|list|watch|create|update|patch|delete|deletecollection (resource); lowercased HTTP method (non-resource)")
      (group       :doc "API group (\"\" core)")
      (version     :doc "API version (not matched by RBAC)")
      (resource    :doc "resource plural; empty for non-resource")
      (subresource :doc "status|scale; resource match key becomes resource/subresource")
      (namespace   :doc "namespace for a namespaced resource request")
      (name        :doc "instance name; matched against PolicyRule.resourceNames")
      (non-resource-url :doc "nonResourceURL (/healthz, /api, …) for a non-resource request")))

  ;; The typed verdict.
  (decision
    (allow      :doc "a matching rule granted the request")
    (deny       :doc "explicit deny — reserved; no RBAC producer")
    (no-opinion :doc "no rule matched; the chain default-denies this"))

  ;; The interpreter phases, in order. First matching rule => Allow.
  (phases
    (a short-circuit-masters
       :when "attrs.user.groups contains \"system:masters\""
       :do   "return Allow"
       :reads-store nil
       :doc  "ZERO store reads — the admin kubeconfig keeps allow-all so every existing live proof passes")
    (b gather-bindings
       :do "collect ClusterRoleBindings whose subjects match the identity (User name==username | Group name in groups | ServiceAccount system:serviceaccount:ns:name==username); + (when namespaced) the namespace's RoleBindings"
       :doc "cheap subject filter before any role resolution")
    (c resolve-roles
       :do "for each matched binding, resolve roleRef => ClusterRole (cluster) or Role (in the binding's namespace). RoleBinding may reference a ClusterRole (applied in-ns). Dangling roleRef => skip + warn (NoOpinion contribution, NOT a hard error)")
    (d match-rules
       :resource     "verb in rule.verbs|* AND group in rule.apiGroups|* AND resource-key in rule.resources|* (resource|resource/sub|resource/* match) AND (rule.resourceNames empty OR name in rule.resourceNames) => Allow"
       :non-resource "verb in rule.verbs|* AND path matches a rule.nonResourceURLs entry (exact | trailing /* prefix-glob) => Allow")
    (e default
       :do "no rule matched => NoOpinion"))

  ;; Canonical instances — the cases the unit tests + the live bar exercise.
  (cases
    (system-masters-allow-all
      :attrs (user (groups "system:masters") :verb "delete" :resource "secrets" :namespace "kube-system")
      :decision allow
      :store-reads 0)
    (bound-role-granted-verb
      :given  (role "default/pod-reader" :verbs (get list) :resources (pods))
      :given  (rolebinding "default/bind" :role-ref (Role "pod-reader") :subjects ((User "test-user")))
      :attrs  (user (name "test-user") :verb "get" :resource "pods" :namespace "default")
      :decision allow)
    (bound-role-ungranted-verb
      :attrs  (user (name "test-user") :verb "delete" :resource "pods" :namespace "default")
      :decision no-opinion)
    (resource-names-restriction
      :given  (clusterrole "named" :verbs (get) :resources (configmaps) :resource-names (foo))
      :attrs  (user (name "bob") :verb "get" :resource "configmaps" :name "bar")
      :decision no-opinion)
    (subresource-match
      :given  (clusterrole "status-writer" :verbs (patch) :resources ("pods/status"))
      :attrs  (user (name "carol") :verb "patch" :resource "pods" :subresource "status")
      :decision allow)
    (non-resource-url-glob
      :given  (clusterrole "system:discovery" :verbs (get) :non-resource-urls ("/api" "/apis/*"))
      :attrs  (user (group "system:authenticated") :verb "get" :non-resource-url "/apis/apps/v1")
      :decision allow)
    (default-deny-unbound
      :attrs  (user (name "nobody") :verb "get" :resource "pods" :namespace "default")
      :decision no-opinion)
    (rolebinding-to-clusterrole
      :given  (clusterrole "view" :verbs (get list) :resources (pods))
      :given  (rolebinding "team-a/bind" :role-ref (ClusterRole "view") :subjects ((User "erin")))
      :attrs  (user (name "erin") :verb "list" :resource "pods" :namespace "team-a")
      :decision allow)
    (dangling-role-ref-no-panic
      :given  (clusterrolebinding "ghost" :role-ref (ClusterRole "does-not-exist") :subjects ((User "frank")))
      :attrs  (user (name "frank") :verb "get" :resource "pods" :namespace "default")
      :decision no-opinion)))

;;; The bootstrap policy seeded at boot (engenho-runtime::seed_bootstrap_rbac).
;;; ClusterRole + ClusterRoleBinding pairs guaranteeing admin allow-all (via a
;;; real binding too), anonymous + authenticated discovery, the basic-user
;;; self-review surface, and the public health/version surface.
(defrbac-bootstrap rbac-bootstrap
  (cluster-role "cluster-admin"
    (rule :verbs (*) :api-groups (*) :resources (*))
    (rule :verbs (*) :non-resource-urls (*)))
  (cluster-role-binding "cluster-admin"
    :role-ref (ClusterRole "cluster-admin")
    :subjects ((Group "system:masters")))

  (cluster-role "system:discovery"
    (rule :verbs (get)
          :non-resource-urls ("/api" "/api/*" "/apis" "/apis/*"
                              "/openapi" "/openapi/*" "/version" "/version/*"
                              "/healthz" "/livez" "/readyz")))
  (cluster-role-binding "system:discovery"
    :role-ref (ClusterRole "system:discovery")
    :subjects ((Group "system:authenticated")))

  (cluster-role "system:basic-user"
    (rule :verbs (create) :api-groups ("authorization.k8s.io")
          :resources (selfsubjectaccessreviews selfsubjectrulesreviews))
    (rule :verbs (create) :api-groups ("authentication.k8s.io")
          :resources (selfsubjectreviews)))
  (cluster-role-binding "system:basic-user"
    :role-ref (ClusterRole "system:basic-user")
    :subjects ((Group "system:authenticated")))

  (cluster-role "system:public-info-viewer"
    (rule :verbs (get)
          :non-resource-urls ("/healthz" "/livez" "/readyz" "/version" "/version/*"
                              "/api" "/api/*" "/apis" "/apis/*" "/openapi" "/openapi/*")))
  (cluster-role-binding "system:public-info-viewer"
    :role-ref (ClusterRole "system:public-info-viewer")
    :subjects ((Group "system:authenticated") (Group "system:unauthenticated"))))
