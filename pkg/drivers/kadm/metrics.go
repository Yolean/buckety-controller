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

// Gauges for the topic configuration the broker reports as in
// effect (effective.go): configuration only, never the topic's
// state (sizes, offsets, partitions, replicas), which Kafka
// exporters already measure. Set from the DescribeConfigs answer
// every reconcile of the Buckety already reads, so they cost no
// extra broker call, and are as fresh as the re-check cadence.
var (
	topicConfig = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "buckety_kadm_topic_config",
		Help: "Effective value of a topic config key as the broker reports it, whether set on the topic or inherited from the broker's config or built-in default (source: topic, broker, default, unknown); -1 means unlimited for retention.ms and retention.bytes. Configuration only, nothing about the topic's state.",
	}, []string{"namespace", "buckety", "topic", "key", "source"})

	topicCleanupPolicy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "buckety_kadm_topic_cleanup_policy",
		Help: "1 for the topic's effective cleanup.policy (policy: delete, compact or compact,delete as the broker reports it), with the same source label as buckety_kadm_topic_config.",
	}, []string{"namespace", "buckety", "topic", "policy", "source"})
)

func init() {
	crmetrics.Registry.MustRegister(partitionsAdded, configChanges, topicConfig, topicCleanupPolicy)
}
