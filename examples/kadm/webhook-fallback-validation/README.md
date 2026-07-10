# kadm / webhook-fallback-validation

**Scenario:** Reconcile-loop parameter validation, the fallback
that webhook-disabled deployments rely on (docs/SCAFFOLDING.md
"Webhook TLS": platforms without cert-manager drop webhook.yaml
and pass --enable-webhook=false).

The assert deletes the ValidatingWebhookConfiguration, which is
what --enable-webhook=false looks like from the API server's
side, applies a Buckety with an unknown parameter and a
BucketyAccess with unsupported parameters, and requires both to
surface Ready=False reason=InvalidParameters on status instead of
reaching the backend. The webhook configuration is restored (and
verified rejecting again) before the scenario ends.
