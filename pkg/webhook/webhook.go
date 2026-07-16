// Package webhook implements the single validating admission
// webhook for Buckety + BucketyAccess. SPEC §Non-goals says no
// cross-resource invariants; this webhook focuses on per-resource
// parameter validation and naming-template resolution.
//
// CRD-level CEL handles immutability of spec.backend / spec.name /
// bucketyRef / credentialsSecretName, so the webhook does not
// duplicate those checks.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/template"
)

// Validator is the single handler the controller registers at
// /validate. It dispatches by request Kind.
type Validator struct {
	Config *config.Loaded
}

// Register installs the handler at /validate on the manager's
// webhook server. The kustomize base's
// ValidatingWebhookConfiguration points at that path.
func (v *Validator) Register(mgr ctrl.Manager) {
	mgr.GetWebhookServer().Register("/validate", &admission.Webhook{Handler: v})
}

// Handle is the controller-runtime admission.Handler entry point.
func (v *Validator) Handle(ctx context.Context, req admission.Request) admission.Response {
	switch req.Kind.Kind {
	case "Buckety":
		return v.validateBuckety(ctx, req)
	case "BucketyAccess":
		return v.validateAccess(ctx, req)
	default:
		return admission.Errored(http.StatusBadRequest,
			fmt.Errorf("unsupported kind %q", req.Kind.Kind))
	}
}

func (v *Validator) validateBuckety(_ context.Context, req admission.Request) admission.Response {
	if req.Operation == admissionv1.Delete {
		return admission.Allowed("")
	}
	var bky bucketyv1.Buckety
	if err := json.Unmarshal(req.Object.Raw, &bky); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	backend, ok := v.Config.Lookup(bky.Spec.Backend)
	if !ok {
		// On UPDATE the backend may have been renamed away after
		// admission; spec.backend is immutable so we cannot
		// reject the patch here without orphaning the resource
		// (no way to clean it up, no way to fix the spec). The
		// reconciler surfaces BackendUnavailable on the status;
		// admission only enforces this on CREATE.
		if req.Operation == admissionv1.Create {
			return admission.Denied(fmt.Sprintf("spec.backend %q is not registered in buckety-controller.yaml", bky.Spec.Backend))
		}
		return admission.Allowed("backend missing but resource pre-exists; reconciler will surface BackendUnavailable")
	}

	// Template resolution and driver name rules are checked only
	// until the resolved name is frozen in status (first reconcile).
	// After that, label mutations that would change a hypothetical
	// re-resolution are irrelevant: status.backendResourceName is
	// sticky and admission must not reject unrelated updates.
	if bky.Status.BackendResourceName == "" {
		resolved, err := resolveName(&bky, backend)
		if err != nil {
			return admission.Denied(fmt.Sprintf("spec.name: %v", err))
		}
		if err := backend.Driver.ValidateResourceName(resolved); err != nil {
			return admission.Denied(fmt.Sprintf("spec.name: resolves to %q: %v", resolved, err))
		}
	}

	if req.Operation == admissionv1.Update && len(req.OldObject.Raw) > 0 {
		var old bucketyv1.Buckety
		if err := json.Unmarshal(req.OldObject.Raw, &old); err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
		// Merged views on both sides: dropping a CR key that a
		// backend default also defines falls back to the default
		// value, and that transition must pass immutability too.
		if err := backend.Driver.ValidateUpdateParameters(
			backend.EffectiveParameters(old.Spec.Parameters),
			backend.EffectiveParameters(bky.Spec.Parameters)); err != nil {
			return admission.Denied(fmt.Sprintf("spec.parameters: %v", err))
		}
	} else if err := backend.Driver.ValidateParameters(backend.EffectiveParameters(bky.Spec.Parameters)); err != nil {
		return admission.Denied(fmt.Sprintf("spec.parameters: %v", err))
	}

	// defaultAccess shape: enforce non-empty Secret name.
	if bky.Spec.DefaultAccess != nil && bky.Spec.DefaultAccess.CredentialsSecretName == "" {
		return admission.Denied("spec.defaultAccess.credentialsSecretName is required when defaultAccess is set")
	}

	return admission.Allowed("")
}

func (v *Validator) validateAccess(_ context.Context, req admission.Request) admission.Response {
	if req.Operation == admissionv1.Delete {
		return admission.Allowed("")
	}
	var access bucketyv1.BucketyAccess
	if err := json.Unmarshal(req.Object.Raw, &access); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if access.Spec.BucketyRef.Name == "" {
		return admission.Denied("spec.bucketyRef.name is required")
	}
	if access.Spec.CredentialsSecretName == "" {
		return admission.Denied("spec.credentialsSecretName is required")
	}
	// Parameter validation happens in the reconcile loop
	// (ValidateAccessParameters): resolving the driver requires the
	// referenced Buckety, which may legitimately not exist at
	// admission time, and admission webhooks should not read from
	// the API server. Role enum is enforced by the CRD schema.
	return admission.Allowed("")
}

func resolveName(bky *bucketyv1.Buckety, backend config.Backend) (string, error) {
	if bky.Spec.Name == "" {
		return bky.Name, nil
	}
	return template.Resolve(bky.Spec.Name, template.Inputs{
		Name:            bky.Name,
		Namespace:       bky.Namespace,
		Labels:          bky.Labels,
		BackendDefaults: backend.Defaults,
	})
}
