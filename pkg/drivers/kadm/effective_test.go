package kadm

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// series returns the series vec exports for Buckety ns/buckety, as
// sorted "label=value,... value" lines (labels in Prometheus' order,
// which is alphabetical) without the namespace and buckety labels.
func series(t *testing.T, vec prometheus.Collector, ns, buckety string) []string {
	t.Helper()
	ch := make(chan prometheus.Metric, 100)
	vec.Collect(ch)
	close(ch)
	var out []string
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatal(err)
		}
		var labels []string
		match := 0
		for _, l := range d.GetLabel() {
			switch {
			case l.GetName() == "namespace" && l.GetValue() == ns, l.GetName() == "buckety" && l.GetValue() == buckety:
				match++
			default:
				labels = append(labels, l.GetName()+"="+l.GetValue())
			}
		}
		if match == 2 {
			out = append(out, fmt.Sprintf("%s %g", strings.Join(labels, ","), d.GetGauge().GetValue()))
		}
	}
	sort.Strings(out)
	return out
}

func viewOf(cfgs map[string]string, srcs map[string]kmsg.ConfigSource) *topicView {
	return &topicView{partitions: 1, rf: 1, configs: cfgs, sources: srcs}
}

func wantSeries(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s:\n got %q\nwant %q", what, got, want)
	}
}

// The effective value and its source are exported for every listed
// key the broker reports; a source or policy change replaces the
// series, and a key that stops being reported loses it.
func TestEffectiveConfigFollowsTheBroker(t *testing.T) {
	const ns, bky, topic = "eff1", "orders", "eff1.orders"
	d := &Driver{}

	d.recordEffective(ns, bky, topic, viewOf(
		map[string]string{"retention.ms": "-1", "retention.bytes": "-1", "segment.bytes": "1073741824", "cleanup.policy": "delete", "compression.type": "producer"},
		map[string]kmsg.ConfigSource{
			"retention.ms":    kmsg.ConfigSourceStaticBrokerConfig,
			"retention.bytes": kmsg.ConfigSourceDefaultConfig,
			"segment.bytes":   kmsg.ConfigSourceDynamicDefaultBrokerConfig,
			"cleanup.policy":  kmsg.ConfigSourceDefaultConfig,
		}))
	wantSeries(t, "first pass", series(t, topicConfig, ns, bky),
		"key=retention.bytes,source=default,topic=eff1.orders -1",
		"key=retention.ms,source=broker,topic=eff1.orders -1",
		"key=segment.bytes,source=broker,topic=eff1.orders 1.073741824e+09",
	)
	wantSeries(t, "first pass policy", series(t, topicCleanupPolicy, ns, bky),
		"policy=delete,source=default,topic=eff1.orders 1")

	// An override appears out of band, the policy changes, and the
	// broker stops reporting segment.bytes.
	d.recordEffective(ns, bky, topic, viewOf(
		map[string]string{"retention.ms": "604800000", "retention.bytes": "-1", "cleanup.policy": "compact,delete"},
		map[string]kmsg.ConfigSource{
			"retention.ms":    kmsg.ConfigSourceDynamicTopicConfig,
			"retention.bytes": kmsg.ConfigSourceDefaultConfig,
			"cleanup.policy":  kmsg.ConfigSourceDynamicTopicConfig,
		}))
	wantSeries(t, "second pass", series(t, topicConfig, ns, bky),
		"key=retention.bytes,source=default,topic=eff1.orders -1",
		"key=retention.ms,source=topic,topic=eff1.orders 6.048e+08",
	)
	wantSeries(t, "second pass policy", series(t, topicCleanupPolicy, ns, bky),
		"policy=compact,delete,source=topic,topic=eff1.orders 1")
}

// An alter the broker acknowledged is exported at once as a topic
// override, replacing the inherited value's series.
func TestEffectiveConfigAfterAlter(t *testing.T) {
	const ns, bky, topic = "eff2", "orders", "eff2.orders"
	m := &fakeMutator{}
	d := &Driver{mutator: m}
	existing := viewOf(
		map[string]string{"retention.ms": "-1", "cleanup.policy": "delete"},
		map[string]kmsg.ConfigSource{"retention.ms": kmsg.ConfigSourceStaticBrokerConfig, "cleanup.policy": kmsg.ConfigSourceDefaultConfig})
	d.recordEffective(ns, bky, topic, existing)
	err := d.alignTopic(context.Background(), registry.EnsureRequest{Name: topic, Namespace: ns, BucketyName: bky}, existing, 0, 0,
		map[string]*string{"retention.ms": strp("86400000"), "cleanup.policy": strp("compact")})
	if err != nil {
		t.Fatal(err)
	}
	wantSeries(t, "after alter", series(t, topicConfig, ns, bky),
		"key=retention.ms,source=topic,topic=eff2.orders 8.64e+07")
	wantSeries(t, "after alter policy", series(t, topicCleanupPolicy, ns, bky),
		"policy=compact,source=topic,topic=eff2.orders 1")

	// A rejected alter changes nothing.
	m.alterErr = errInvalidConfigForTest
	_ = d.alignTopic(context.Background(), registry.EnsureRequest{Name: topic, Namespace: ns, BucketyName: bky}, existing, 0, 0,
		map[string]*string{"retention.ms": strp("1")})
	wantSeries(t, "after a rejected alter", series(t, topicConfig, ns, bky),
		"key=retention.ms,source=topic,topic=eff2.orders 8.64e+07")
}

// Once the Buckety is gone, every series the driver exports for it
// goes, change counters included; other Bucketies keep theirs.
func TestForgetBucketyDropsItsSeries(t *testing.T) {
	const ns, topic = "eff3", "eff3.a"
	m := &fakeMutator{}
	d := &Driver{mutator: m}
	view := func() *topicView {
		return viewOf(map[string]string{"retention.ms": "-1", "cleanup.policy": "delete"},
			map[string]kmsg.ConfigSource{"retention.ms": kmsg.ConfigSourceDefaultConfig, "cleanup.policy": kmsg.ConfigSourceDefaultConfig})
	}
	for _, b := range []string{"a", "b"} {
		d.recordEffective(ns, b, topic+b, view())
		if err := d.alignTopic(context.Background(), registry.EnsureRequest{Name: topic + b, Namespace: ns, BucketyName: b}, view(), 0, 0,
			map[string]*string{"retention.bytes": strp("1")}); err != nil {
			t.Fatal(err)
		}
	}

	registry.ForgetBuckety(d, ns, "a")
	for _, c := range []struct {
		name string
		vec  prometheus.Collector
	}{{"config", topicConfig}, {"policy", topicCleanupPolicy}, {"changes", configChanges}, {"partitions", partitionsAdded}} {
		if got := series(t, c.vec, ns, "a"); len(got) != 0 {
			t.Errorf("%s series left for a forgotten Buckety: %q", c.name, got)
		}
		if got := series(t, c.vec, ns, "b"); len(got) == 0 {
			t.Errorf("%s series of another Buckety dropped", c.name)
		}
	}
	if _, ok := d.effective.byBuckety[bucketyID{ns, "a"}]; ok {
		t.Error("export record kept for a forgotten Buckety")
	}
}

func TestSourceLabel(t *testing.T) {
	for s, want := range map[kmsg.ConfigSource]string{
		kmsg.ConfigSourceDynamicTopicConfig:         "topic",
		kmsg.ConfigSourceDynamicBrokerConfig:        "broker",
		kmsg.ConfigSourceDynamicDefaultBrokerConfig: "broker",
		kmsg.ConfigSourceStaticBrokerConfig:         "broker",
		kmsg.ConfigSourceDefaultConfig:              "default",
		kmsg.ConfigSourceUnknown:                    "unknown",
	} {
		if got := sourceLabel(s); got != want {
			t.Errorf("%v: %q, want %q", s, got, want)
		}
	}
}
