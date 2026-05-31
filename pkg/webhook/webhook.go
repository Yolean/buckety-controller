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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	if _, err := resolveName(&bky, backend); err != nil {
		return admission.Denied(fmt.Sprintf("spec.name: %v", err))
	}

	if req.Operation == admissionv1.Update && len(req.OldObject.Raw) > 0 {
		var old bucketyv1.Buckety
		if err := json.Unmarshal(req.OldObject.Raw, &old); err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
		if err := backend.Driver.ValidateUpdateParameters(old.Spec.Parameters, bky.Spec.Parameters); err != nil {
			return admission.Denied(fmt.Sprintf("spec.parameters: %v", err))
		}
	} else if err := backend.Driver.ValidateParameters(bky.Spec.Parameters); err != nil {
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
	// We can't resolve the driver without the Buckety, and a
	// BucketyAccess may be authored before its Buckety exists.
	// Defer parameter validation to the reconciler in that case.
	// If the Buckety+backend are present at admission time, we
	// can do early rejection.
	if access.Spec.Parameters != nil {
		// Look up via backend pointer from the spec, defaulting to
		// nothing if we cannot resolve. Without listing Bucketys
		// here (admission webhooks should not Get against the API)
		// we'd need the request's containing resources. Skip the
		// pre-flight; the reconciler surfaces clear conditions.
		_ = access
	}
	// Role enum is enforced by the CRD schema; no extra check
	// here (per SPEC: keep CRD enum open for forward compat).
	_ = metav1.Now()
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
