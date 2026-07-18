# webhook-certgen

Self-signed webhook TLS without cert-manager. Two
`kube-webhook-certgen` Jobs (the ingress-nginx pattern) mint a
serving certificate into the conventional
`buckety-controller-webhook-tls` Secret and patch the
`buckety-controller` ValidatingWebhookConfiguration's caBundle.
The controller Deployment already mounts that Secret; nothing
else changes.

Use it when your cluster does not run cert-manager, composed next
to the release base:

```yaml
resources:
- github.com/Yolean/buckety-controller/deploy/kustomize/release-tls-selfsigned?ref=<sha>
```

or as `../release` + `../webhook-certgen` separately. With
cert-manager present, skip this and create a Certificate named
`buckety-controller-webhook` with secretName
`buckety-controller-webhook-tls` instead (docs/SCAFFOLDING.md
"Webhook TLS"); the VWC's `cert-manager.io/inject-ca-from`
annotation is inert without cert-manager, so both paths share one
base.

Running with `--enable-webhook=false` is strongly discouraged now
that TLS needs no external infrastructure: without admission,
invalid parameters, immutability violations, bad resolved names
and refused adoptions surface as Ready=False conditions instead
of failing the apply.

Operational notes:

- Startup ordering self-heals: the controller's TLS volume is
  optional, so a Pod scheduled before the Secret exists restarts
  until the create Job has run; the patch Job retries until the
  Secret is readable.
- Completed Jobs self-delete (ttlSecondsAfterFinished) so a
  converge loop that re-applies this directory re-runs them:
  `create` keeps the existing Secret, `patch` re-writes the same
  caBundle. Idempotent by construction.
- certgen issues a long-lived certificate (no rotation). Fine for
  dev and internal clusters; where rotation policy matters,
  prefer the cert-manager path.
