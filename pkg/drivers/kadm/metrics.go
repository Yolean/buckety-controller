package kadm

import (
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Event reasons for registry.Change values this driver reports.
const (
	ReasonPartitionsAdded    = "PartitionsAdded"
	ReasonTopicConfigChanged = "TopicConfigChanged"
)

// Counters for the changes alignTopic makes to existing topics.
// Both move only after the broker acknowledged the change, never
// for values that already matched. Labels are identities, never
// values: one series per Buckety (and per managed config key), so
// cardinality follows the number of Bucketies, not their history.
// Registered with controller-runtime's registry, which is what
// --metrics-addr serves.
var (
	partitionsAdded = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "buckety_kadm_partitions_added_total",
		Help: "Partitions the kadm driver added to existing topics because the Buckety requested more than the topic had.",
	}, []string{"namespace", "buckety", "topic"})

	configChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "buckety_kadm_config_changes_total",
		Help: "Topic config keys the kadm driver set on existing topics because the broker's value differed from the Buckety's config.* parameter.",
	}, []string{"namespace", "buckety", "topic", "key"})
)

func init() {
	crmetrics.Registry.MustRegister(partitionsAdded, configChanges)
}
