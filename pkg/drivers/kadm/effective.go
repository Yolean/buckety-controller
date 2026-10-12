package kadm

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// effectiveKeys are the topic config keys exported as
// buckety_kadm_topic_config: retention, compaction and size limits,
// whose effective value depends on how the broker merges topic
// overrides with broker config and defaults, so the Buckety's own
// parameters cannot tell it. cleanup.policy is a string and has its
// own gauge.
var effectiveKeys = []string{
	"retention.ms",
	"retention.bytes",
	"delete.retention.ms",
	"min.compaction.lag.ms",
	"max.compaction.lag.ms",
	"min.cleanable.dirty.ratio",
	"segment.ms",
	"segment.bytes",
	"max.message.bytes",
	"min.insync.replicas",
}

const cleanupPolicyKey = "cleanup.policy"

// sourceLabel names where the broker says a config value comes
// from: the topic's own override, any broker-level config, or the
// built-in default.
func sourceLabel(s kmsg.ConfigSource) string {
	switch s {
	case kmsg.ConfigSourceDynamicTopicConfig:
		return "topic"
	case kmsg.ConfigSourceDynamicBrokerConfig,
		kmsg.ConfigSourceDynamicDefaultBrokerConfig,
		kmsg.ConfigSourceStaticBrokerConfig:
		return "broker"
	case kmsg.ConfigSourceDefaultConfig:
		return "default"
	}
	return "unknown"
}

// effectiveExport remembers, per Buckety, the label values of the
// series last exported for it, so that a value whose source or
// policy changed replaces its old series instead of leaving it
// behind, and ForgetBuckety can drop them all.
type effectiveExport struct {
	mu        sync.Mutex
	byBuckety map[bucketyID]*exported
}

type bucketyID struct{ namespace, name string }

type exported struct {
	topic string
	// source per effectiveKeys key; absent when not exported
	source map[string]string
	// policy and its source, empty when not exported
	policy, policySource string
}

// recordEffective exports the effective config in view for the
// Buckety's topic. A key the broker does not report (or reports
// as a non-number) has no series.
func (d *Driver) recordEffective(namespace, buckety, topic string, view *topicView) {
	d.effective.mu.Lock()
	defer d.effective.mu.Unlock()
	e := d.effective.entry(namespace, buckety, topic)
	for _, k := range effectiveKeys {
		v, ok := view.configs[k]
		var f float64
		if ok {
			var err error
			f, err = strconv.ParseFloat(v, 64)
			ok = err == nil
		}
		if !ok {
			if src, had := e.source[k]; had {
				topicConfig.DeleteLabelValues(namespace, buckety, topic, k, src)
				delete(e.source, k)
			}
			continue
		}
		e.setConfig(namespace, buckety, k, f, sourceLabel(view.sources[k]))
	}
	if p, ok := view.configs[cleanupPolicyKey]; ok {
		e.setPolicy(namespace, buckety, p, sourceLabel(view.sources[cleanupPolicyKey]))
	} else if e.policy != "" {
		topicCleanupPolicy.DeleteLabelValues(namespace, buckety, topic, e.policy, e.policySource)
		e.policy, e.policySource = "", ""
	}
}

// recordAltered exports the values of an alter the broker
// acknowledged: they are now topic overrides.
func (d *Driver) recordAltered(namespace, buckety, topic string, alters []kadm.AlterConfig) {
	d.effective.mu.Lock()
	defer d.effective.mu.Unlock()
	e := d.effective.entry(namespace, buckety, topic)
	for _, a := range alters {
		if a.Value == nil {
			continue
		}
		if a.Name == cleanupPolicyKey {
			e.setPolicy(namespace, buckety, *a.Value, "topic")
			continue
		}
		if f, err := strconv.ParseFloat(*a.Value, 64); err == nil && isEffectiveKey(a.Name) {
			e.setConfig(namespace, buckety, a.Name, f, "topic")
		}
	}
}

// ForgetBuckety drops every series this driver exports for the
// Buckety: the effective config gauges and the change counters.
// The reconciler calls it once the Buckety is gone (registry
// ForgetBuckety).
func (d *Driver) ForgetBuckety(namespace, name string) {
	labels := prometheus.Labels{"namespace": namespace, "buckety": name}
	topicConfig.DeletePartialMatch(labels)
	topicCleanupPolicy.DeletePartialMatch(labels)
	configChanges.DeletePartialMatch(labels)
	partitionsAdded.DeletePartialMatch(labels)
	d.effective.mu.Lock()
	defer d.effective.mu.Unlock()
	delete(d.effective.byBuckety, bucketyID{namespace, name})
}

// entry returns the Buckety's export record, starting over (and
// dropping its series) if its topic changed. Callers hold mu.
func (x *effectiveExport) entry(namespace, buckety, topic string) *exported {
	if x.byBuckety == nil {
		x.byBuckety = map[bucketyID]*exported{}
	}
	id := bucketyID{namespace, buckety}
	e := x.byBuckety[id]
	if e != nil && e.topic != topic {
		labels := prometheus.Labels{"namespace": namespace, "buckety": buckety, "topic": e.topic}
		topicConfig.DeletePartialMatch(labels)
		topicCleanupPolicy.DeletePartialMatch(labels)
		e = nil
	}
	if e == nil {
		e = &exported{topic: topic, source: map[string]string{}}
		x.byBuckety[id] = e
	}
	return e
}

func isEffectiveKey(key string) bool {
	for _, k := range effectiveKeys {
		if k == key {
			return true
		}
	}
	return false
}

func (e *exported) setConfig(namespace, buckety, key string, value float64, source string) {
	if old, had := e.source[key]; had && old != source {
		topicConfig.DeleteLabelValues(namespace, buckety, e.topic, key, old)
	}
	e.source[key] = source
	topicConfig.WithLabelValues(namespace, buckety, e.topic, key, source).Set(value)
}

func (e *exported) setPolicy(namespace, buckety, policy, source string) {
	if e.policy != "" && (e.policy != policy || e.policySource != source) {
		topicCleanupPolicy.DeleteLabelValues(namespace, buckety, e.topic, e.policy, e.policySource)
	}
	e.policy, e.policySource = policy, source
	topicCleanupPolicy.WithLabelValues(namespace, buckety, e.topic, policy, source).Set(1)
}
