// Package bucketyaccess reconciles BucketyAccess to a flat
// `secretKeyRef`-friendly Secret. Implicit access creation lives
// in the Buckety reconciler; this one only sees real
// BucketyAccess resources (implicit or explicit).
package bucketyaccess

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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

	"k8s.io/client-go/tools/record"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/controller/status"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// Reconciler reconciles BucketyAccess resources.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config *config.Loaded
	// Recheck is the periodic re-check cadence returned from every
	// settled reconcile; zero disables it (tests).
	Recheck time.Duration
	// Recorder emits Events alongside failure conditions so
	// `kubectl describe` tells the story; nil disables (tests).
	Recorder record.EventRecorder
	// Live reads straight from the apiserver, bypassing the
	// manager cache. The cache only carries Secrets labelled
	// LabelOwnedSecret, so Secret existence checks MUST use this
	// reader: an unlabelled Secret (pre-existing foreign one, or
	// one minted before the label existed) is invisible to the
	// cache, and a cached lookup would wrongly report it absent.
	// Wired from mgr.GetAPIReader(); nil falls back to the cached
	// client (only acceptable in tests without cache scoping).
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
func (r *Reconciler) surface(access, base *bucketyv1.BucketyAccess, typ string, st metav1.ConditionStatus, reason, msg, eventType string) {
	status.Event(r.Recorder, access, base.Status.Conditions, typ, st, reason, eventType, reason, msg)
	status.Set(&access.Status.Conditions, typ, st, reason, msg, access.Generation)
}

// SetupWithManager registers this controller with the manager.
// Buckety updates re-enqueue every BucketyAccess that references
// the changed Buckety in the same namespace; without this, an
// access created at the same instant as its Buckety races against
// the Buckety reaching Ready and sits at WaitingForBuckety until
// the periodic requeue fires.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bucketyv1.BucketyAccess{}).
		Owns(&corev1.Secret{}).
		Watches(&bucketyv1.Buckety{}, handler.EnqueueRequestsFromMapFunc(r.bucketyToAccesses)).
		Complete(r)
}

// bucketyToAccesses returns Reconcile requests for every
// BucketyAccess in the same namespace as bky that references it.
func (r *Reconciler) bucketyToAccesses(ctx context.Context, obj client.Object) []reconcile.Request {
	bky, ok := obj.(*bucketyv1.Buckety)
	if !ok {
		return nil
	}
	accesses := &bucketyv1.BucketyAccessList{}
	if err := r.List(ctx, accesses, client.InNamespace(bky.Namespace)); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, a := range accesses.Items {
		if a.Spec.BucketyRef.Name == bky.Name {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: a.Namespace, Name: a.Name,
			}})
		}
	}
	return out
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("bucketyaccess", req.NamespacedName)

	var access bucketyv1.BucketyAccess
	if err := r.Get(ctx, req.NamespacedName, &access); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Resolve the Buckety to find the backend + driver early.
	var bky bucketyv1.Buckety
	bkyErr := r.Get(ctx, types.NamespacedName{Namespace: access.Namespace, Name: access.Spec.BucketyRef.Name}, &bky)

	// Deletion path. SPEC: deletion blocks on RevokeAccess
	// succeeding - and since gcs 0.2 a principal can be a live
	// key, so the skip paths here must never silently orphan one.
	if !access.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&access, bucketyv1.FinalizerCleanup) {
			switch {
			case bkyErr != nil && !apierrors.IsNotFound(bkyErr):
				// Transient Buckety read failure: retry instead of
				// falling through to a finalizer removal that would
				// skip revocation.
				return ctrl.Result{}, bkyErr
			case bkyErr == nil:
				backend, ok := r.Config.Lookup(bky.Spec.Backend)
				if !ok && access.Status.PrincipalRevocable {
					// A minted credential exists but no backend to
					// revoke it against. Letting the finalizer go
					// would orphan it, so deletion blocks with the
					// same remedy as Buckety deletion under
					// retentionPolicy=Delete: restore the backend in
					// buckety-controller.yaml. Static shared
					// principals (Revocable=false) release as in
					// v1alpha1 - their revoke is a no-op, and
					// blocking them would wedge scenarios like
					// backend renames.
					base := access.DeepCopy()
					msg := fmt.Sprintf("cannot revoke principal %q: backend %q is not registered in buckety-controller.yaml; restore it to let this BucketyAccess go", access.Status.Principal, bky.Spec.Backend)
					status.Event(r.Recorder, &access, base.Status.Conditions, "Ready", metav1.ConditionFalse, "BackendUnavailable",
						corev1.EventTypeWarning, "DeletionBlocked", msg)
					status.Set(&access.Status.Conditions, "Ready", metav1.ConditionFalse, "BackendUnavailable", msg, access.Generation)
					if err := r.patchStatus(ctx, &access, base); err != nil {
						return ctrl.Result{}, err
					}
					return ctrl.Result{RequeueAfter: r.Recheck}, nil
				}
				if ok {
					if err := backend.Driver.RevokeAccess(ctx, access.Status.Principal); err != nil {
						log.Error(err, "RevokeAccess failed")
						return ctrl.Result{}, err
					}
				}
			default:
				// Buckety already gone (NotFound): no backend can be
				// resolved and no operator remedy would ever unblock,
				// so the finalizer is released. The systematic route
				// here - the implicit access, owner-ref-GC'd after
				// its Buckety - is prevented by the Buckety
				// reconciler deleting it BEFORE releasing its own
				// finalizer, so revocation ran with the Buckety
				// still present.
			}
			controllerutil.RemoveFinalizer(&access, bucketyv1.FinalizerCleanup)
			if err := r.Update(ctx, &access); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer. Patch (no optimistic lock) tolerates a
	// concurrent modification of the resource between our Get and
	// our write - common when a Buckety just created this implicit
	// access and several reconcile events fire in quick succession.
	// Return immediately; the patch's own watch event brings the
	// next reconcile with the finalizer in place.
	if !controllerutil.ContainsFinalizer(&access, bucketyv1.FinalizerCleanup) {
		patch := client.MergeFrom(access.DeepCopy())
		controllerutil.AddFinalizer(&access, bucketyv1.FinalizerCleanup)
		return ctrl.Result{}, r.Patch(ctx, &access, patch)
	}

	// Snapshot for status patching; tolerates concurrent RV bumps
	// that .Update can't.
	baseAccess := access.DeepCopy()

	if bkyErr != nil {
		if apierrors.IsNotFound(bkyErr) {
			r.surface(&access, baseAccess, "Ready", metav1.ConditionFalse, "BucketyNotFound",
				fmt.Sprintf("Buckety %q not found in namespace %q", access.Spec.BucketyRef.Name, access.Namespace), corev1.EventTypeWarning)
			return ctrl.Result{}, r.patchStatus(ctx, &access, baseAccess)
		}
		return ctrl.Result{}, bkyErr
	}

	// Buckety must be Ready and have its backend resource name
	// stamped before we can mint a Secret.
	if bky.Status.BackendResourceName == "" || !isReady(&bky) {
		status.Set(&access.Status.Conditions, "Ready", metav1.ConditionFalse,
			"WaitingForBuckety",
			"Buckety is not Ready yet; will retry",
			access.Generation)
		_ = r.patchStatus(ctx, &access, baseAccess)
		return ctrl.Result{RequeueAfter: r.Recheck}, nil
	}

	backend, ok := r.Config.Lookup(bky.Status.Backend)
	if !ok {
		r.surface(&access, baseAccess, "Ready", metav1.ConditionFalse, "BackendUnavailable",
			fmt.Sprintf("backend %q is not registered in buckety-controller.yaml", bky.Status.Backend), corev1.EventTypeWarning)
		return ctrl.Result{}, r.patchStatus(ctx, &access, baseAccess)
	}

	// Parameter validation, also done at admission when the webhook
	// runs; admission additionally cannot resolve the driver when
	// the Buckety does not exist yet, so this is the authoritative
	// check for BucketyAccess parameters.
	if err := backend.Driver.ValidateAccessParameters(access.Spec.Parameters); err != nil {
		r.surface(&access, baseAccess, "Ready", metav1.ConditionFalse, "InvalidParameters", err.Error(), corev1.EventTypeWarning)
		return ctrl.Result{}, r.patchStatus(ctx, &access, baseAccess)
	}

	// The Buckety's resolved parameter view rides along on the
	// grant so drivers can find per-resource principals (gcs
	// serviceAccount) without knowing about CRDs.
	bkyParams, err := backend.ResolvedParameters(bky.Name, bky.Namespace, bky.Spec.Parameters)
	if err != nil {
		r.surface(&access, baseAccess, "Ready", metav1.ConditionFalse, "ParameterTemplate", err.Error(), corev1.EventTypeWarning)
		return ctrl.Result{}, r.patchStatus(ctx, &access, baseAccess)
	}

	// Refuse to touch a Secret this BucketyAccess does not control:
	// adopting an orphan Secret in place would clobber its data and
	// later garbage-collect it with the access. This also covers a
	// Secret still owned by a deleted predecessor access (GC lag):
	// the conflict clears on a later requeue once the old Secret is
	// gone. Live read, not cache: foreign Secrets carry no
	// LabelOwnedSecret and are invisible to the scoped informer.
	//
	// The gate runs BEFORE GrantAccess: drivers may mint live
	// credentials there (a GCS SA key), and minting for a Secret we
	// then refuse to write would leak an unrecorded credential. The
	// read also feeds ExistingSecretData, which is what lets such
	// drivers return the already-minted credential unchanged
	// instead of minting on every reconcile.
	var existing corev1.Secret
	getErr := r.liveReader().Get(ctx, types.NamespacedName{Namespace: access.Namespace, Name: access.Spec.CredentialsSecretName}, &existing)
	switch {
	case getErr == nil && !metav1.IsControlledBy(&existing, &access):
		msg := fmt.Sprintf("secret %q exists and is not managed by this BucketyAccess; delete it or pick another credentialsSecretName", access.Spec.CredentialsSecretName)
		r.surface(&access, baseAccess, "Ready", metav1.ConditionFalse, "SecretConflict", msg, corev1.EventTypeWarning)
		if err := r.patchStatus(ctx, &access, baseAccess); err != nil {
			return ctrl.Result{}, err
		}
		// Periodic requeue notices when the conflicting Secret goes
		// away (an unowned Secret's deletion maps to no watch).
		return ctrl.Result{RequeueAfter: r.Recheck}, nil
	case getErr != nil && !apierrors.IsNotFound(getErr):
		return ctrl.Result{}, getErr
	}
	var existingData map[string][]byte
	if getErr == nil {
		existingData = existing.Data
	}

	res, err := backend.Driver.GrantAccess(ctx, registry.GrantRequest{
		BucketyName:        bky.Status.BackendResourceName,
		Role:               string(access.Spec.Role),
		Parameters:         access.Spec.Parameters,
		BucketyParameters:  bkyParams,
		ExistingSecretData: existingData,
	})
	if err != nil {
		r.surface(&access, baseAccess, "Ready", metav1.ConditionFalse, "GrantFailed", err.Error(), corev1.EventTypeWarning)
		_ = r.patchStatus(ctx, &access, baseAccess)
		return ctrl.Result{}, err
	}

	// Mint/update the Secret with this BucketyAccess as owner.
	// Built on the live read above instead of
	// controllerutil.CreateOrUpdate, whose cached read would miss
	// an owned-but-unlabelled Secret (minted by an older version)
	// and dead-end in AlreadyExists on the Create attempt. The
	// update path stamps LabelOwnedSecret, which is what migrates
	// pre-label Secrets into the scoped cache.
	err = r.writeSecret(ctx, &access, &existing, apierrors.IsNotFound(getErr), res.SecretData)
	if err != nil {
		// A credential minted in this pass and not written anywhere
		// is held by nobody and recorded nowhere: status.principal
		// still names its predecessor, so neither a retry (which
		// reuses the Secret's key) nor deletion (which revokes the
		// recorded principal) would ever reach it. Revoke it now;
		// the retry mints again. Best effort - a failure here is
		// logged, and the write error is what the status carries.
		if res.Minted && res.Revocable {
			if rerr := backend.Driver.RevokeAccess(ctx, res.Principal); rerr != nil {
				log.Error(rerr, "revoking the credential minted for a failed secret write", "principal", res.Principal)
			}
		}
		if apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
			// The namespace is going away and will take this
			// BucketyAccess with it; retrying with backoff only
			// churns the workqueue and floods the log.
			log.Info("secret write skipped; namespace is terminating")
			return ctrl.Result{}, nil
		}
		r.surface(&access, baseAccess, "Ready", metav1.ConditionFalse, "SecretWriteFailed", err.Error(), corev1.EventTypeWarning)
		_ = r.patchStatus(ctx, &access, baseAccess)
		return ctrl.Result{}, err
	}

	// A changed principal means GrantAccess re-minted (the Secret
	// was lost, hand-edited, or its key invalidated): revoke the
	// replaced credential now that the Secret carries its
	// successor. Without this, every re-mint orphans a live key
	// on the gcs SA until GCP's 10-keys-per-SA cap wedges
	// Keys.Create permanently.
	// Ordering makes it self-healing: status.principal keeps
	// naming the old key until revocation succeeds, so a failure
	// retries here while GrantAccess keeps returning the
	// already-written replacement.
	if old := access.Status.Principal; old != "" && old != res.Principal {
		if rerr := backend.Driver.RevokeAccess(ctx, old); rerr != nil {
			r.surface(&access, baseAccess, "Ready", metav1.ConditionFalse, "RevokeFailed", rerr.Error(), corev1.EventTypeWarning)
			_ = r.patchStatus(ctx, &access, baseAccess)
			return ctrl.Result{}, rerr
		}
		// A replaced credential is invisible from the resource
		// otherwise: Ready stays True and the Secret looks fine,
		// while a consumer that loaded the old key once is now
		// broken. The event is what tells an operator to restart
		// such consumers, or that an unrequested rotation happened.
		if r.Recorder != nil {
			r.Recorder.Event(&access, corev1.EventTypeNormal, "PrincipalReplaced",
				fmt.Sprintf("credential re-minted: %q revoked, secret %q now carries %q; restart consumers that loaded the secret once", old, access.Spec.CredentialsSecretName, res.Principal))
		}
	}
	access.Status.Principal = res.Principal
	access.Status.PrincipalRevocable = res.Revocable

	// ScopingNotImplemented if the driver is not actually
	// scoping per role and the user asked for something other
	// than ReadWrite.
	if !res.Scoped && access.Spec.Role != "" && access.Spec.Role != bucketyv1.RoleReadWrite {
		status.Set(&access.Status.Conditions, "ScopingNotImplemented", metav1.ConditionTrue,
			"DriverIgnoresRole",
			fmt.Sprintf("driver %q v1alpha1 does not scope credentials per role; got root creds despite role=%q", backend.Driver.Name(), access.Spec.Role),
			access.Generation)
	} else {
		meta.RemoveStatusCondition(&access.Status.Conditions, "ScopingNotImplemented")
	}

	status.Event(r.Recorder, &access, baseAccess.Status.Conditions, "Ready", metav1.ConditionTrue, "SecretMinted",
		corev1.EventTypeNormal, "SecretMinted",
		fmt.Sprintf("secret %q minted for backend resource %q", access.Spec.CredentialsSecretName, bky.Status.BackendResourceName))
	status.Set(&access.Status.Conditions, "Ready", metav1.ConditionTrue, "SecretMinted", "", access.Generation)
	access.Status.ObservedGeneration = access.Generation
	if err := r.patchStatus(ctx, &access, baseAccess); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.Recheck}, nil
}

// writeSecret creates or updates the access's credentials Secret
// with owner-ref, LabelOwnedSecret and the driver's data. existing
// is the live-read state (ignored when notFound); a no-op update
// is skipped so steady-state requeues do not bump
// resourceVersion.
func (r *Reconciler) writeSecret(ctx context.Context, access *bucketyv1.BucketyAccess, existing *corev1.Secret, notFound bool, data map[string][]byte) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      access.Spec.CredentialsSecretName,
			Namespace: access.Namespace,
		},
	}
	if !notFound {
		secret = existing.DeepCopy()
	}
	if err := controllerutil.SetControllerReference(access, secret, r.Scheme); err != nil {
		return err
	}
	if secret.Labels == nil {
		secret.Labels = map[string]string{}
	}
	secret.Labels[bucketyv1.LabelOwnedSecret] = "true"
	secret.Type = corev1.SecretTypeOpaque
	secret.Data = data
	if notFound {
		return r.Create(ctx, secret)
	}
	if equality.Semantic.DeepEqual(existing, secret) {
		return nil
	}
	return r.Update(ctx, secret)
}

// patchStatus writes status only when something OTHER than
// condition messages changed since base
// (status.ChangedBeyondMessages has the rationale; the loop it
// prevents is ISSUE_status_message_reconcile_loop.md).
func (r *Reconciler) patchStatus(ctx context.Context, access, base *bucketyv1.BucketyAccess) error {
	if !status.ChangedBeyondMessages(&base.Status, &access.Status, func(s *bucketyv1.BucketyAccessStatus) *[]metav1.Condition { return &s.Conditions }) {
		return nil
	}
	return r.Status().Patch(ctx, access, client.MergeFrom(base))
}

func isReady(bky *bucketyv1.Buckety) bool {
	return meta.IsStatusConditionTrue(bky.Status.Conditions, "Ready")
}
