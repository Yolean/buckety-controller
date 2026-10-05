package buckety

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
)

// parameterDrift mirrors each Buckety's ParameterDrift condition:
// 1 while it is True, 0 otherwise, series removed once the Buckety
// is gone. Driver-independent: any driver's ErrParameterDrift sets
// the condition. spec.backend is immutable, so the label set of a
// Buckety never changes. Registered with controller-runtime's
// registry, which is what --metrics-addr serves.
var parameterDrift = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "buckety_parameter_drift",
	Help: "1 while the Buckety's ParameterDrift condition is True (backend state the driver cannot reconcile in place, e.g. a Kafka partition shrink request), 0 otherwise.",
}, []string{"namespace", "buckety", "backend"})

func init() {
	crmetrics.Registry.MustRegister(parameterDrift)
}

// recordDrift sets the drift gauge from the conditions the
// reconcile leaves on bky, or drops the series once the finalizer
// is released and the Buckety is about to disappear.
func recordDrift(bky *bucketyv1.Buckety) {
	if !bky.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(bky, bucketyv1.FinalizerCleanup) {
		forgetDrift(bky.Namespace, bky.Name)
		return
	}
	v := 0.0
	if meta.IsStatusConditionTrue(bky.Status.Conditions, "ParameterDrift") {
		v = 1
	}
	parameterDrift.WithLabelValues(bky.Namespace, bky.Name, bky.Spec.Backend).Set(v)
}

// forgetDrift removes the drift series of a Buckety that no longer
// exists.
func forgetDrift(namespace, name string) {
	parameterDrift.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "buckety": name})
}
