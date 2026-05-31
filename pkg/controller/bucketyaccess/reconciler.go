// Package bucketyaccess reconciles BucketyAccess to a flat
// `secretKeyRef`-friendly Secret. Implicit access creation lives
// in the Buckety reconciler; this one only sees real
// BucketyAccess resources (implicit or explicit).
package bucketyaccess

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
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

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// Reconciler reconciles BucketyAccess resources.
type Reconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Config       *config.Loaded
	RequeueAfter func() ctrl.Result
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

	// Deletion path.
	if !access.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&access, bucketyv1.FinalizerCleanup) {
			if bkyErr == nil {
				if backend, ok := r.Config.Lookup(bky.Spec.Backend); ok {
					if err := backend.Driver.RevokeAccess(ctx, access.Status.Principal); err != nil {
						log.Error(err, "RevokeAccess failed")
						return ctrl.Result{}, err
					}
				}
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
	// Return immediately so the next reconcile sees the finalizer
	// in place.
	if !controllerutil.ContainsFinalizer(&access, bucketyv1.FinalizerCleanup) {
		patch := client.MergeFrom(access.DeepCopy())
		controllerutil.AddFinalizer(&access, bucketyv1.FinalizerCleanup)
		if err := r.Patch(ctx, &access, patch); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Snapshot for status patching; tolerates concurrent RV bumps
	// that .Update can't.
	baseAccess := access.DeepCopy()

	if bkyErr != nil {
		if apierrors.IsNotFound(bkyErr) {
			setCond(&access.Status.Conditions, "Ready", metav1.ConditionFalse,
				"BucketyNotFound",
				fmt.Sprintf("Buckety %q not found in namespace %q", access.Spec.BucketyRef.Name, access.Namespace),
				access.Generation)
			return ctrl.Result{}, r.Status().Patch(ctx, &access, client.MergeFrom(baseAccess))
		}
		return ctrl.Result{}, bkyErr
	}

	// Buckety must be Ready and have its backend resource name
	// stamped before we can mint a Secret.
	if bky.Status.BackendResourceName == "" || !isReady(&bky) {
		setCond(&access.Status.Conditions, "Ready", metav1.ConditionFalse,
			"WaitingForBuckety",
			"Buckety is not Ready yet; will retry",
			access.Generation)
		_ = r.Status().Patch(ctx, &access, client.MergeFrom(baseAccess))
		if r.RequeueAfter != nil {
			return r.RequeueAfter(), nil
		}
		return ctrl.Result{}, nil
	}

	backend, ok := r.Config.Lookup(bky.Status.Backend)
	if !ok {
		setCond(&access.Status.Conditions, "Ready", metav1.ConditionFalse,
			"BackendUnavailable",
			fmt.Sprintf("backend %q is not registered in buckety-controller.yaml", bky.Status.Backend),
			access.Generation)
		return ctrl.Result{}, r.Status().Patch(ctx, &access, client.MergeFrom(baseAccess))
	}

	res, err := backend.Driver.GrantAccess(ctx, registry.GrantRequest{
		BucketyName: bky.Status.BackendResourceName,
		Role:        string(access.Spec.Role),
		Parameters:  access.Spec.Parameters,
	})
	if err != nil {
		setCond(&access.Status.Conditions, "Ready", metav1.ConditionFalse, "GrantFailed", err.Error(), access.Generation)
		_ = r.Status().Patch(ctx, &access, client.MergeFrom(baseAccess))
		return ctrl.Result{}, err
	}

	// Mint/update the Secret with this BucketyAccess as owner.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      access.Spec.CredentialsSecretName,
			Namespace: access.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if err := controllerutil.SetControllerReference(&access, secret, r.Scheme); err != nil {
			return err
		}
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = res.SecretData
		return nil
	})
	if err != nil {
		setCond(&access.Status.Conditions, "Ready", metav1.ConditionFalse, "SecretWriteFailed", err.Error(), access.Generation)
		_ = r.Status().Patch(ctx, &access, client.MergeFrom(baseAccess))
		return ctrl.Result{}, err
	}

	access.Status.Principal = res.Principal

	// ScopingNotImplemented if the driver is not actually
	// scoping per role and the user asked for something other
	// than ReadWrite.
	if !res.Scoped && access.Spec.Role != "" && access.Spec.Role != bucketyv1.RoleReadWrite {
		setCond(&access.Status.Conditions, "ScopingNotImplemented", metav1.ConditionTrue,
			"DriverIgnoresRole",
			fmt.Sprintf("driver %q v1alpha1 does not scope credentials per role; got root creds despite role=%q", backend.Driver.Name(), access.Spec.Role),
			access.Generation)
	} else {
		meta.RemoveStatusCondition(&access.Status.Conditions, "ScopingNotImplemented")
	}

	setCond(&access.Status.Conditions, "Ready", metav1.ConditionTrue, "SecretMinted", "", access.Generation)
	access.Status.ObservedGeneration = access.Generation
	if err := r.Status().Patch(ctx, &access, client.MergeFrom(baseAccess)); err != nil {
		return ctrl.Result{}, err
	}
	if r.RequeueAfter != nil {
		return r.RequeueAfter(), nil
	}
	return ctrl.Result{}, nil
}

func isReady(bky *bucketyv1.Buckety) bool {
	for _, c := range bky.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}

func setCond(conds *[]metav1.Condition, t string, status metav1.ConditionStatus, reason, message string, observed int64) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               t,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observed,
	})
}
