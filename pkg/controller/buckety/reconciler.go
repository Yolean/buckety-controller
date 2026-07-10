// Package buckety reconciles Buckety resources to the backing
// service via the resolved driver. Implicit BucketyAccess
// materialisation lives here too; the BucketyAccess reconciler
// in the sibling package mints Secrets but does not own the
// implicit creation/teardown.
package buckety

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
	"github.com/Yolean/buckety-controller/pkg/template"
)

// Reconciler reconciles Buckety resources. Backends are resolved
// through the loaded config (immutable since startup; rotation
// requires re-rolling the Pod per SPEC §controller config file).
type Reconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	Config         *config.Loaded
	RequeueAfter   func() ctrl.Result // periodic re-check cadence; injected so tests can stub
	ControllerName string             // for ownership references on implicit BucketyAccess
}

// SetupWithManager registers the controller with the supplied
// manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bucketyv1.Buckety{}).
		Owns(&bucketyv1.BucketyAccess{}).
		Complete(r)
}

// Reconcile is the controller-runtime entrypoint.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("buckety", req.NamespacedName)

	var bky bucketyv1.Buckety
	if err := r.Get(ctx, req.NamespacedName, &bky); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Resolve the backend up front; nearly every branch needs it.
	backend, backendOK := r.Config.Lookup(bky.Spec.Backend)

	// Deletion path: handle finalizer before anything else.
	if !bky.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &bky, backend, backendOK)
	}

	// Ensure finalizer. Patch (no optimistic lock) instead of
	// Update so we don't conflict with whatever just modified
	// the resource (e.g. our own controller's previous reconcile
	// pass, the access reconciler reading the Buckety, etc.).
	// Return immediately so the next reconcile sees the
	// finalizer in place.
	if !controllerutil.ContainsFinalizer(&bky, bucketyv1.FinalizerCleanup) {
		patch := client.MergeFrom(bky.DeepCopy())
		controllerutil.AddFinalizer(&bky, bucketyv1.FinalizerCleanup)
		if err := r.Patch(ctx, &bky, patch); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Snapshot for status patching; tolerates concurrent RV bumps
	// (the access reconciler's reads, the implicit-access write
	// re-enqueueing this Buckety via the Owns watch, etc.) that a
	// .Status().Update() would conflict on.
	baseBky := bky.DeepCopy()

	if !backendOK {
		return r.surfaceBackendUnavailable(ctx, &bky, baseBky)
	}

	// First reconcile: stamp sticky fields.
	if bky.Status.Backend == "" {
		resolved, err := resolveName(&bky, backend)
		if err != nil {
			return r.surfaceCondition(ctx, &bky, baseBky, "Ready", metav1.ConditionFalse, "NameTemplate", err.Error())
		}
		major, _ := majorOf(backend.Driver.Version())
		bky.Status.Backend = backend.Name
		bky.Status.Driver = backend.Driver.Name()
		bky.Status.DriverMajor = major
		bky.Status.DriverBuildVersion = backend.Driver.Version()
		bky.Status.BackendResourceName = resolved
		// Update, not Patch: driverMajor stamped as 0 (any 0.x
		// driver) is invisible to a merge diff against the
		// zero-valued base, so a patch would never persist it.
		if err := r.Status().Update(ctx, &bky); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Drift on driver major after stickiness. Stampedness is
	// signalled by status.backend (set together with driverMajor at
	// first reconcile): 0 is a legitimate stamped major for 0.x
	// drivers, so `driverMajor != 0` cannot be the guard - it would
	// exempt every pre-1.0 resource from the compatibility check.
	runningMajor, _ := majorOf(backend.Driver.Version())
	if bky.Status.Backend != "" && runningMajor != bky.Status.DriverMajor {
		setCond(&bky.Status.Conditions, "DriverVersionIncompatible", metav1.ConditionTrue,
			"DriverMajorBump",
			fmt.Sprintf("stamped major=%d, running=%d; pin a compatible binary or migrate the resource", bky.Status.DriverMajor, runningMajor),
			bky.Generation)
		setCond(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "DriverVersionIncompatible", "reconcile paused", bky.Generation)
		_ = r.Status().Patch(ctx, &bky, client.MergeFrom(baseBky))
		return ctrl.Result{}, nil
	}
	// Compatible: update running build version, clear the
	// incompatible condition if it was set.
	bky.Status.DriverBuildVersion = backend.Driver.Version()
	meta.RemoveStatusCondition(&bky.Status.Conditions, "DriverVersionIncompatible")
	meta.RemoveStatusCondition(&bky.Status.Conditions, "BackendUnavailable")

	// Reconcile the backend resource itself.
	setCond(&bky.Status.Conditions, "Reconciling", metav1.ConditionTrue, "Ensuring", "calling driver.EnsureBuckety", bky.Generation)
	if err := backend.Driver.EnsureBuckety(ctx, registry.EnsureRequest{
		Name:       bky.Status.BackendResourceName,
		Parameters: bky.Spec.Parameters,
	}); err != nil {
		if registry.IsParameterDrift(err) {
			setCond(&bky.Status.Conditions, "ParameterDrift", metav1.ConditionTrue, "Unreconcilable", err.Error(), bky.Generation)
			setCond(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "ParameterDrift", err.Error(), bky.Generation)
			setCond(&bky.Status.Conditions, "Reconciling", metav1.ConditionFalse, "Paused", "drift requires human resolution", bky.Generation)
			return ctrl.Result{}, r.Status().Patch(ctx, &bky, client.MergeFrom(baseBky))
		}
		log.Error(err, "driver.EnsureBuckety failed")
		setCond(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "EnsureFailed", err.Error(), bky.Generation)
		_ = r.Status().Patch(ctx, &bky, client.MergeFrom(baseBky))
		return ctrl.Result{}, err
	}
	meta.RemoveStatusCondition(&bky.Status.Conditions, "ParameterDrift")

	// Implicit BucketyAccess materialisation / reclamation.
	if err := r.reconcileImplicitAccess(ctx, &bky); err != nil {
		log.Error(err, "implicit access reconcile failed")
		return ctrl.Result{}, err
	}

	// All done.
	setCond(&bky.Status.Conditions, "Reconciling", metav1.ConditionFalse, "Idle", "", bky.Generation)
	setCond(&bky.Status.Conditions, "Ready", metav1.ConditionTrue, "EnsuredOnBackend", "", bky.Generation)
	bky.Status.ObservedGeneration = bky.Generation
	if err := r.Status().Patch(ctx, &bky, client.MergeFrom(baseBky)); err != nil {
		return ctrl.Result{}, err
	}
	if r.RequeueAfter != nil {
		return r.RequeueAfter(), nil
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) reconcileDelete(ctx context.Context, bky *bucketyv1.Buckety, backend config.Backend, backendOK bool) (ctrl.Result, error) {
	base := bky.DeepCopy()
	// Block on explicit BucketyAccess children before we let the
	// resource go.
	accesses := &bucketyv1.BucketyAccessList{}
	if err := r.List(ctx, accesses, client.InNamespace(bky.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	var blocking []string
	for _, a := range accesses.Items {
		if a.Spec.BucketyRef.Name != bky.Name {
			continue
		}
		if a.Labels[bucketyv1.LabelImplicit] == "true" {
			// Implicit access is GC'd via owner-ref; not a blocker.
			continue
		}
		blocking = append(blocking, a.Name)
	}
	if len(blocking) > 0 {
		setCond(&bky.Status.Conditions, "BlockedByAccesses", metav1.ConditionTrue, "Pending",
			fmt.Sprintf("waiting on BucketyAccess: %s", strings.Join(blocking, ", ")),
			bky.Generation)
		if err := r.Status().Patch(ctx, bky, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		// Requeue periodically; the BucketyAccess deletions will
		// also re-enqueue this Buckety via the Owns watch.
		if r.RequeueAfter != nil {
			return r.RequeueAfter(), nil
		}
		return ctrl.Result{}, nil
	}

	// If we have a backend AND the policy says Delete, drop the
	// backing resource.
	if backendOK && bky.Spec.RetentionPolicy == bucketyv1.RetentionDelete && bky.Status.BackendResourceName != "" {
		if err := backend.Driver.DeleteBuckety(ctx, bky.Status.BackendResourceName); err != nil {
			setCond(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "DeleteFailed", err.Error(), bky.Generation)
			_ = r.Status().Patch(ctx, bky, client.MergeFrom(base))
			return ctrl.Result{}, err
		}
	}
	// Remove the finalizer and let GC proceed.
	controllerutil.RemoveFinalizer(bky, bucketyv1.FinalizerCleanup)
	if err := r.Update(ctx, bky); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileImplicitAccess materialises a BucketyAccess from
// spec.defaultAccess and reclaims it when an explicit access
// exists or defaultAccess is removed. See SPEC §Implicit access.
func (r *Reconciler) reconcileImplicitAccess(ctx context.Context, bky *bucketyv1.Buckety) error {
	accesses := &bucketyv1.BucketyAccessList{}
	if err := r.List(ctx, accesses, client.InNamespace(bky.Namespace)); err != nil {
		return err
	}
	var implicit *bucketyv1.BucketyAccess
	var explicitExists bool
	for i, a := range accesses.Items {
		if a.Spec.BucketyRef.Name != bky.Name {
			continue
		}
		if a.Labels[bucketyv1.LabelImplicit] == "true" {
			implicit = &accesses.Items[i]
			continue
		}
		explicitExists = true
	}

	wantImplicit := bky.Spec.DefaultAccess != nil && !explicitExists

	switch {
	case wantImplicit && implicit == nil:
		newAccess := &bucketyv1.BucketyAccess{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bky.Name,
				Namespace: bky.Namespace,
				Labels:    map[string]string{bucketyv1.LabelImplicit: "true"},
			},
			Spec: bucketyv1.BucketyAccessSpec{
				BucketyRef:            bucketyv1.BucketyRef{Name: bky.Name},
				CredentialsSecretName: bky.Spec.DefaultAccess.CredentialsSecretName,
				Role:                  bky.Spec.DefaultAccess.Role,
			},
		}
		if newAccess.Spec.Role == "" {
			newAccess.Spec.Role = bucketyv1.RoleReadWrite
		}
		if err := controllerutil.SetControllerReference(bky, newAccess, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, newAccess)
	case implicit != nil && !wantImplicit:
		// Reclaim. Owner-ref GC will sweep the Secret.
		return r.Delete(ctx, implicit)
	case implicit != nil && wantImplicit:
		// Field drift: update name/role if the user changed
		// defaultAccess. CredentialsSecretName on the access is
		// immutable, so we delete-and-recreate if it changed.
		if implicit.Spec.CredentialsSecretName != bky.Spec.DefaultAccess.CredentialsSecretName {
			if err := r.Delete(ctx, implicit); err != nil {
				return err
			}
			return nil // next reconcile will recreate
		}
		desiredRole := bky.Spec.DefaultAccess.Role
		if desiredRole == "" {
			desiredRole = bucketyv1.RoleReadWrite
		}
		if implicit.Spec.Role != desiredRole {
			implicit.Spec.Role = desiredRole
			return r.Update(ctx, implicit)
		}
	}
	return nil
}

func (r *Reconciler) surfaceBackendUnavailable(ctx context.Context, bky, base *bucketyv1.Buckety) (ctrl.Result, error) {
	setCond(&bky.Status.Conditions, "BackendUnavailable", metav1.ConditionTrue, "NotInConfig",
		fmt.Sprintf("backend %q is not registered in buckety-controller.yaml", bky.Spec.Backend),
		bky.Generation)
	setCond(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "BackendUnavailable", "reconcile paused", bky.Generation)
	return ctrl.Result{}, r.Status().Patch(ctx, bky, client.MergeFrom(base))
}

func (r *Reconciler) surfaceCondition(ctx context.Context, bky, base *bucketyv1.Buckety, condType string, status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	setCond(&bky.Status.Conditions, condType, status, reason, message, bky.Generation)
	return ctrl.Result{}, r.Status().Patch(ctx, bky, client.MergeFrom(base))
}

// resolveName runs the name template against the Buckety + backend.
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

// majorOf parses the major number out of a SemVer string.
// Returns 0 and the parse error if the input is malformed.
func majorOf(v string) (int, error) {
	dot := strings.IndexByte(v, '.')
	if dot < 0 {
		return strconv.Atoi(v)
	}
	return strconv.Atoi(v[:dot])
}

// setCond is a wrapper around meta.SetStatusCondition that stamps
// ObservedGeneration so consumers can tell which spec produced
// the condition.
func setCond(conds *[]metav1.Condition, t string, status metav1.ConditionStatus, reason, message string, observed int64) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               t,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observed,
	})
}
