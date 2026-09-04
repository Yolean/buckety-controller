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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/controller/status"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// Reconciler reconciles Buckety resources. Backends are resolved
// through the loaded config (immutable since startup; rotation
// requires re-rolling the Pod per SPEC §controller config file).
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config *config.Loaded
	// Recheck is the periodic re-check cadence returned from every
	// settled reconcile; zero disables it (tests).
	Recheck time.Duration
	// Recorder emits Events alongside condition changes so
	// `kubectl describe` tells the story; nil disables (tests).
	Recorder record.EventRecorder
	// Live reads straight from the apiserver, bypassing the
	// manager cache, for the one decision here that is
	// irreversible: whether a retentionPolicy=Delete teardown may
	// run, which depends on no explicit BucketyAccess existing.
	// Wired from mgr.GetAPIReader(); nil falls back to the cached
	// client (tests).
	Live client.Reader
}

// liveReader returns the uncached reader, or the cached client
// when tests have not wired one.
func (r *Reconciler) liveReader() client.Reader {
	if r.Live != nil {
		return r.Live
	}
	return r.Client
}

// surface sets a condition and, when that is a transition from
// base, emits an Event under the same reason.
func (r *Reconciler) surface(bky, base *bucketyv1.Buckety, typ string, st metav1.ConditionStatus, reason, msg, eventType string) {
	status.Event(r.Recorder, bky, base.Status.Conditions, typ, st, reason, eventType, reason, msg)
	status.Set(&bky.Status.Conditions, typ, st, reason, msg, bky.Generation)
}

// SetupWithManager registers the controller with the supplied
// manager. The BucketyAccess watch maps every access event
// (explicit or implicit) to its referenced Buckety; Owns() would
// only cover the implicit access, leaving implicit-access
// reclamation and deletion-unblock to the periodic requeue (up to
// 5 minutes at the production cadence).
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bucketyv1.Buckety{}).
		Watches(&bucketyv1.BucketyAccess{}, handler.EnqueueRequestsFromMapFunc(r.accessToBuckety)).
		Complete(r)
}

// accessToBuckety maps a BucketyAccess event to a reconcile request
// for the Buckety it references in the same namespace.
func (r *Reconciler) accessToBuckety(_ context.Context, obj client.Object) []reconcile.Request {
	a, ok := obj.(*bucketyv1.BucketyAccess)
	if !ok || a.Spec.BucketyRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{
		Namespace: a.Namespace, Name: a.Spec.BucketyRef.Name,
	}}}
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
	// The driver behind a backend name is part of the sticky
	// (backend, driver, driverMajor) triple (SPEC §Buckety shape),
	// so a backend re-pointed at another driver is as unavailable
	// as a missing one: reconciling a topic's resource as a bucket,
	// or deleting it as one under retentionPolicy=Delete, is worse
	// than pausing.
	backend, backendOK := r.Config.Lookup(bky.Spec.Backend)
	unavailableReason, unavailableMsg := "", ""
	switch {
	case !backendOK:
		unavailableReason = "NotInConfig"
		unavailableMsg = fmt.Sprintf("backend %q is not registered in buckety-controller.yaml", bky.Spec.Backend)
	case bky.Status.Driver != "" && bky.Status.Driver != backend.Driver.Name():
		unavailableReason = "DriverChanged"
		unavailableMsg = fmt.Sprintf("backend %q now runs driver %q but this resource was provisioned by driver %q; restore the backend's driver or migrate the resource", bky.Spec.Backend, backend.Driver.Name(), bky.Status.Driver)
	}

	// Deletion path: handle finalizer before anything else.
	if !bky.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &bky, backend, unavailableReason, unavailableMsg)
	}

	// Ensure finalizer. Patch (no optimistic lock) instead of
	// Update so we don't conflict with whatever just modified
	// the resource (e.g. our own controller's previous reconcile
	// pass, the access reconciler reading the Buckety, etc.).
	// Return immediately; the patch's own watch event brings the
	// next reconcile with the finalizer in place.
	if !controllerutil.ContainsFinalizer(&bky, bucketyv1.FinalizerCleanup) {
		patch := client.MergeFrom(bky.DeepCopy())
		controllerutil.AddFinalizer(&bky, bucketyv1.FinalizerCleanup)
		return ctrl.Result{}, r.Patch(ctx, &bky, patch)
	}

	// Snapshot for status patching; tolerates concurrent RV bumps
	// (the access reconciler's reads, the implicit-access write
	// re-enqueueing this Buckety via the access watch, etc.) that a
	// .Status().Update() would conflict on.
	baseBky := bky.DeepCopy()

	if unavailableReason != "" {
		return r.surfaceBackendUnavailable(ctx, &bky, baseBky, unavailableReason, unavailableMsg)
	}

	// First reconcile: stamp sticky fields.
	if bky.Status.Backend == "" {
		resolved, err := backend.ResolveName(bky.Spec.Name, bky.Name, bky.Namespace, bky.Labels)
		if err != nil {
			return r.surfaceCondition(ctx, &bky, baseBky, "Ready", metav1.ConditionFalse, "NameTemplate", err.Error())
		}
		// Admission rejects invalid resolved names when the webhook
		// runs; this is the webhook-disabled fallback, checked
		// before the name is frozen into status.
		if err := backend.Driver.ValidateResourceName(resolved); err != nil {
			return r.surfaceCondition(ctx, &bky, baseBky, "Ready", metav1.ConditionFalse, "NameInvalid", err.Error())
		}
		// Adoption gate (SPEC §Adoption): before claiming the name,
		// find out whether the backend resource already exists and
		// whether it holds content. The decision freezes into
		// status.provenance together with the other sticky fields;
		// a refused adoption stamps nothing, so the gate re-runs
		// until the spec or the backend changes.
		inspection, err := backend.Driver.InspectBuckety(ctx, resolved)
		if err != nil {
			r.surface(&bky, baseBky, "Ready", metav1.ConditionFalse, "InspectFailed", err.Error(), corev1.EventTypeWarning)
			_ = r.patchStatus(ctx, &bky, baseBky)
			return ctrl.Result{}, err
		}
		provenance := decideProvenance(inspection, bky.Spec.Adoption)
		if provenance == "" {
			msg := fmt.Sprintf(
				"backend resource %q already exists on backend %q and holds content; refusing to adopt it. Set spec.adoption=Adopt to claim it (adopted resources are never deleted from the backend), or change spec.name.",
				resolved, backend.Name)
			r.surface(&bky, baseBky, "Ready", metav1.ConditionFalse, "BackendResourceExists", msg, corev1.EventTypeWarning)
			status.Set(&bky.Status.Conditions, "Reconciling", metav1.ConditionFalse, "BackendResourceExists", "adoption refused", bky.Generation)
			if err := r.patchStatus(ctx, &bky, baseBky); err != nil {
				return ctrl.Result{}, err
			}
			// Spec edits re-enqueue via the watch; the periodic
			// cadence catches the backend resource being emptied
			// or removed out-of-band.
			return ctrl.Result{RequeueAfter: r.Recheck}, nil
		}
		if provenance == bucketyv1.ProvenanceAdopted && r.Recorder != nil {
			r.Recorder.Event(&bky, corev1.EventTypeNormal, "Adopted",
				fmt.Sprintf("adopted pre-existing backend resource %q (empty=%v); it will be retained when this Buckety is deleted", resolved, inspection.Empty))
		}
		major, _ := majorOf(backend.Driver.Version())
		bky.Status.Backend = backend.Name
		bky.Status.Driver = backend.Driver.Name()
		bky.Status.DriverMajor = major
		bky.Status.DriverBuildVersion = backend.Driver.Version()
		bky.Status.BackendResourceName = resolved
		bky.Status.Provenance = provenance
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
		status.Event(r.Recorder, &bky, baseBky.Status.Conditions, "DriverVersionIncompatible", metav1.ConditionTrue, "DriverMajorBump",
			corev1.EventTypeWarning, "DriverVersionIncompatible",
			fmt.Sprintf("stamped major=%d, running=%d; reconcile paused", bky.Status.DriverMajor, runningMajor))
		status.Set(&bky.Status.Conditions, "DriverVersionIncompatible", metav1.ConditionTrue,
			"DriverMajorBump",
			fmt.Sprintf("stamped major=%d, running=%d; pin a compatible binary or migrate the resource", bky.Status.DriverMajor, runningMajor),
			bky.Generation)
		status.Set(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "DriverVersionIncompatible", "reconcile paused", bky.Generation)
		_ = r.patchStatus(ctx, &bky, baseBky)
		return ctrl.Result{}, nil
	}
	// Compatible: update running build version, clear the
	// incompatible condition if it was set.
	bky.Status.DriverBuildVersion = backend.Driver.Version()
	meta.RemoveStatusCondition(&bky.Status.Conditions, "DriverVersionIncompatible")
	meta.RemoveStatusCondition(&bky.Status.Conditions, "BackendUnavailable")

	// Parameter validation, also done at admission when the webhook
	// runs. Platforms without cert-manager run --enable-webhook=false
	// (docs/SCAFFOLDING.md "Webhook TLS"); this is the promised
	// fallback that surfaces invalid parameters on status instead of
	// letting them travel to the backend as an opaque driver error.
	// Validation and Ensure both operate on the merged + resolved
	// view of backend parameter defaults + CR parameters (CR wins
	// per key, driver-declared templated keys resolved; see
	// config.Backend.ResolvedParameters).
	effective, perr := backend.ResolvedParameters(bky.Name, bky.Namespace, bky.Spec.Parameters)
	if perr != nil {
		r.surface(&bky, baseBky, "Ready", metav1.ConditionFalse, "ParameterTemplate", perr.Error(), corev1.EventTypeWarning)
		status.Set(&bky.Status.Conditions, "Reconciling", metav1.ConditionFalse, "ParameterTemplate", "spec change required", bky.Generation)
		return ctrl.Result{}, r.patchStatus(ctx, &bky, baseBky)
	}
	if err := backend.Driver.ValidateParameters(effective); err != nil {
		r.surface(&bky, baseBky, "Ready", metav1.ConditionFalse, "InvalidParameters", err.Error(), corev1.EventTypeWarning)
		status.Set(&bky.Status.Conditions, "Reconciling", metav1.ConditionFalse, "InvalidParameters", "spec change required", bky.Generation)
		return ctrl.Result{}, r.patchStatus(ctx, &bky, baseBky)
	}

	// Reconcile the backend resource itself.
	status.Set(&bky.Status.Conditions, "Reconciling", metav1.ConditionTrue, "Ensuring", "calling driver.EnsureBuckety", bky.Generation)
	if err := backend.Driver.EnsureBuckety(ctx, registry.EnsureRequest{
		Name:       bky.Status.BackendResourceName,
		Parameters: effective,
	}); err != nil {
		if registry.IsParameterDrift(err) {
			status.Event(r.Recorder, &bky, baseBky.Status.Conditions, "ParameterDrift", metav1.ConditionTrue, "Unreconcilable",
				corev1.EventTypeWarning, "ParameterDrift", err.Error())
			status.Set(&bky.Status.Conditions, "ParameterDrift", metav1.ConditionTrue, "Unreconcilable", err.Error(), bky.Generation)
			status.Set(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "ParameterDrift", err.Error(), bky.Generation)
			status.Set(&bky.Status.Conditions, "Reconciling", metav1.ConditionFalse, "Paused", "drift requires human resolution", bky.Generation)
			return ctrl.Result{}, r.patchStatus(ctx, &bky, baseBky)
		}
		if registry.IsProvisioningInProgress(err) && !provisioningOverdue(baseBky.Status.Conditions) {
			// The backend needs a moment, not a fix (freshly
			// created SA awaiting IAM propagation): Normal event,
			// prompt requeue - the DeletingContents posture, not
			// the EnsureFailed one, whose Warning + backoff would
			// misreport a merely-young resource as broken. Bounded
			// by provisioningPatience: past it, the same answer IS
			// a failure and gets the Warning + backoff below.
			r.surface(&bky, baseBky, "Ready", metav1.ConditionFalse, "Provisioning", err.Error(), corev1.EventTypeNormal)
			if perr := r.patchStatus(ctx, &bky, baseBky); perr != nil {
				return ctrl.Result{}, perr
			}
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		log.Error(err, "driver.EnsureBuckety failed")
		r.surface(&bky, baseBky, "Ready", metav1.ConditionFalse, "EnsureFailed", err.Error(), corev1.EventTypeWarning)
		_ = r.patchStatus(ctx, &bky, baseBky)
		return ctrl.Result{}, err
	}
	meta.RemoveStatusCondition(&bky.Status.Conditions, "ParameterDrift")

	// Implicit BucketyAccess materialisation / reclamation.
	if err := r.reconcileImplicitAccess(ctx, &bky); err != nil {
		log.Error(err, "implicit access reconcile failed")
		return ctrl.Result{}, err
	}

	// All done.
	status.Event(r.Recorder, &bky, baseBky.Status.Conditions, "Ready", metav1.ConditionTrue, "EnsuredOnBackend",
		corev1.EventTypeNormal, "Provisioned",
		fmt.Sprintf("backend resource %q ensured on backend %q", bky.Status.BackendResourceName, bky.Status.Backend))
	status.Set(&bky.Status.Conditions, "Reconciling", metav1.ConditionFalse, "Idle", "", bky.Generation)
	status.Set(&bky.Status.Conditions, "Ready", metav1.ConditionTrue, "EnsuredOnBackend", "", bky.Generation)
	bky.Status.ObservedGeneration = bky.Generation
	if err := r.patchStatus(ctx, &bky, baseBky); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.Recheck}, nil
}

// reconcileDelete runs the finalizer. A non-empty unavailableReason
// means backend cannot be used for this resource (missing from
// config, or now behind a different driver) and blocks a
// retentionPolicy=Delete teardown.
func (r *Reconciler) reconcileDelete(ctx context.Context, bky *bucketyv1.Buckety, backend config.Backend, unavailableReason, unavailableMsg string) (ctrl.Result, error) {
	base := bky.DeepCopy()
	// Block on explicit BucketyAccess children before we let the
	// resource go. Live read: this list gates DeleteBuckety, which
	// is irreversible, and an access created moments ago can be
	// missing from the informer cache.
	accesses := &bucketyv1.BucketyAccessList{}
	if err := r.liveReader().List(ctx, accesses, client.InNamespace(bky.Namespace)); err != nil {
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
		status.Event(r.Recorder, bky, base.Status.Conditions, "BlockedByAccesses", metav1.ConditionTrue, "Pending",
			corev1.EventTypeWarning, "BlockedByAccesses",
			fmt.Sprintf("deletion waits on BucketyAccess: %s", strings.Join(blocking, ", ")))
		status.Set(&bky.Status.Conditions, "BlockedByAccesses", metav1.ConditionTrue, "Pending",
			fmt.Sprintf("waiting on BucketyAccess: %s", strings.Join(blocking, ", ")),
			bky.Generation)
		if err := r.patchStatus(ctx, bky, base); err != nil {
			return ctrl.Result{}, err
		}
		// Requeue periodically; the BucketyAccess deletions also
		// re-enqueue this Buckety via the access watch.
		return ctrl.Result{RequeueAfter: r.Recheck}, nil
	}

	// The implicit access is not a deletion blocker, but it must
	// be revoked while this Buckety can still resolve its backend:
	// left to owner-ref GC it would be deleted AFTER the Buckety,
	// and its finalizer then has no backend to revoke against -
	// with gcs 0.2 per-access keys that orphans a live credential
	// on a Retain-surviving service account. So its deletion is
	// driven from here, and the Buckety waits for the access
	// finalizer (which performs the revocation) to finish.
	for i := range accesses.Items {
		a := &accesses.Items[i]
		if a.Spec.BucketyRef.Name != bky.Name || a.Labels[bucketyv1.LabelImplicit] != "true" {
			continue
		}
		if a.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, a); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		// The access deletion re-enqueues this Buckety via the
		// access watch; the short requeue covers a lost event.
		// Normally one pass - but a revocable principal with its
		// backend missing blocks the access (with its own
		// condition), so say what is being waited on.
		status.Set(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "RevokingAccesses",
			fmt.Sprintf("waiting for implicit BucketyAccess %q to revoke before teardown", a.Name),
			bky.Generation)
		if err := r.patchStatus(ctx, bky, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// Adopted resources are never deleted from the backend (SPEC
	// §Adoption): the content predates this CR, or the CR never
	// verified otherwise, so retentionPolicy=Delete degrades to
	// Retain. Resources stamped before provenance tracking carry
	// no value and keep the old behavior.
	if bky.Spec.RetentionPolicy == bucketyv1.RetentionDelete && bky.Status.Provenance == bucketyv1.ProvenanceAdopted {
		if r.Recorder != nil {
			r.Recorder.Event(bky, corev1.EventTypeNormal, "RetainedOnDelete",
				fmt.Sprintf("backend resource %q was adopted, not created; retained despite retentionPolicy=Delete", bky.Status.BackendResourceName))
		}
		controllerutil.RemoveFinalizer(bky, bucketyv1.FinalizerCleanup)
		return ctrl.Result{}, r.Update(ctx, bky)
	}

	// retentionPolicy=Delete blocks on DeleteBuckety succeeding
	// (SPEC "Lifecycle and deletion"). With the backend missing from
	// config that is impossible, and letting the finalizer go would
	// silently orphan the backend resource - so deletion blocks
	// until the maintainer restores the backend or the user flips
	// retentionPolicy to Retain (mutable).
	if bky.Spec.RetentionPolicy == bucketyv1.RetentionDelete && bky.Status.BackendResourceName != "" {
		if unavailableReason != "" {
			msg := fmt.Sprintf("retentionPolicy=Delete needs backend %q to remove %q, but %s; restore it or set retentionPolicy=Retain",
				bky.Spec.Backend, bky.Status.BackendResourceName, unavailableMsg)
			status.Event(r.Recorder, bky, base.Status.Conditions, "BackendUnavailable", metav1.ConditionTrue, unavailableReason,
				corev1.EventTypeWarning, "DeletionBlocked", msg)
			status.Set(&bky.Status.Conditions, "BackendUnavailable", metav1.ConditionTrue, unavailableReason, msg, bky.Generation)
			status.Set(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "BackendUnavailable", "deletion blocked", bky.Generation)
			if err := r.patchStatus(ctx, bky, base); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: r.Recheck}, nil
		}
		// The same resolved parameter view Ensure operated on, so
		// the driver can find per-resource principals (gcs
		// serviceAccount) at teardown. Resolution is deterministic
		// (name/namespace/backend defaults only), so a failure here
		// means the backend config changed underneath the resource;
		// deletion blocks rather than orphaning the principal.
		effective, perr := backend.ResolvedParameters(bky.Name, bky.Namespace, bky.Spec.Parameters)
		if perr != nil {
			status.Event(r.Recorder, bky, base.Status.Conditions, "Ready", metav1.ConditionFalse, "ParameterTemplate",
				corev1.EventTypeWarning, "DeleteFailed", perr.Error())
			status.Set(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "ParameterTemplate", perr.Error(), bky.Generation)
			_ = r.patchStatus(ctx, bky, base)
			return ctrl.Result{}, perr
		}
		if err := backend.Driver.DeleteBuckety(ctx, registry.DeleteRequest{
			Name:       bky.Status.BackendResourceName,
			Parameters: effective,
		}); err != nil {
			if registry.IsDeletionInProgress(err) {
				// Recursive contents deletion runs in bounded
				// slices; this is progress, not failure. The
				// (status, reason) event gate keeps the stream
				// quiet while the message advances.
				r.surface(bky, base, "Ready", metav1.ConditionFalse, "DeletingContents", err.Error(), corev1.EventTypeNormal)
				if perr := r.patchStatus(ctx, bky, base); perr != nil {
					return ctrl.Result{}, perr
				}
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			r.surface(bky, base, "Ready", metav1.ConditionFalse, "DeleteFailed", err.Error(), corev1.EventTypeWarning)
			_ = r.patchStatus(ctx, bky, base)
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
//
// It reads the cache, and the reconcile that follows its own
// create or delete (the status patch re-enqueues this Buckety)
// can run before the informer has caught up, so create tolerates
// AlreadyExists and delete tolerates NotFound: both mean the
// previous pass already did the work.
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
		return client.IgnoreAlreadyExists(r.Create(ctx, newAccess))
	case implicit != nil && !wantImplicit:
		// Reclaim. Owner-ref GC will sweep the Secret.
		return client.IgnoreNotFound(r.Delete(ctx, implicit))
	case implicit != nil && wantImplicit:
		// Field drift: update name/role if the user changed
		// defaultAccess. CredentialsSecretName on the access is
		// immutable, so we delete-and-recreate if it changed.
		if implicit.Spec.CredentialsSecretName != bky.Spec.DefaultAccess.CredentialsSecretName {
			return client.IgnoreNotFound(r.Delete(ctx, implicit)) // next reconcile recreates
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

func (r *Reconciler) surfaceBackendUnavailable(ctx context.Context, bky, base *bucketyv1.Buckety, reason, msg string) (ctrl.Result, error) {
	status.Event(r.Recorder, bky, base.Status.Conditions, "BackendUnavailable", metav1.ConditionTrue, reason,
		corev1.EventTypeWarning, "BackendUnavailable", msg)
	status.Set(&bky.Status.Conditions, "BackendUnavailable", metav1.ConditionTrue, reason, msg, bky.Generation)
	status.Set(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "BackendUnavailable", "reconcile paused", bky.Generation)
	return ctrl.Result{}, r.patchStatus(ctx, bky, base)
}

// surfaceCondition is surface for failures that end the reconcile:
// Warning event, then the status patch.
func (r *Reconciler) surfaceCondition(ctx context.Context, bky, base *bucketyv1.Buckety, typ string, st metav1.ConditionStatus, reason, msg string) (ctrl.Result, error) {
	r.surface(bky, base, typ, st, reason, msg, corev1.EventTypeWarning)
	return ctrl.Result{}, r.patchStatus(ctx, bky, base)
}

// provisioningPatience bounds how long an ErrProvisioningInProgress
// answer is taken at its word. IAM propagation takes seconds; a
// resource still "provisioning" minutes later is stuck (an org
// policy, a disabled account, a permission), and a Normal event
// with a 2s requeue would hide that indefinitely while hammering
// the backend.
const provisioningPatience = 2 * time.Minute

// provisioningOverdue reports whether Ready has carried the
// Provisioning reason for longer than provisioningPatience.
func provisioningOverdue(conds []metav1.Condition) bool {
	c := meta.FindStatusCondition(conds, "Ready")
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == "Provisioning" &&
		time.Since(c.LastTransitionTime.Time) > provisioningPatience
}

// decideProvenance is the adoption gate's pure core (SPEC
// §Adoption): the provenance to freeze at first reconcile, or ""
// when adoption must be refused (exists with content and no
// explicit opt-in).
func decideProvenance(insp registry.Inspection, policy bucketyv1.AdoptionPolicy) bucketyv1.Provenance {
	switch {
	case !insp.Exists:
		return bucketyv1.ProvenanceCreated
	case insp.Empty, policy == bucketyv1.AdoptionAdopt:
		return bucketyv1.ProvenanceAdopted
	default:
		return ""
	}
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

// patchStatus writes status only when something OTHER than
// condition messages changed since base
// (status.ChangedBeyondMessages has the rationale; the loop it
// prevents is ISSUE_status_message_reconcile_loop.md).
func (r *Reconciler) patchStatus(ctx context.Context, bky, base *bucketyv1.Buckety) error {
	if !status.ChangedBeyondMessages(&base.Status, &bky.Status, func(s *bucketyv1.BucketyStatus) *[]metav1.Condition { return &s.Conditions }) {
		return nil
	}
	return r.Status().Patch(ctx, bky, client.MergeFrom(base))
}
