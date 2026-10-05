package kadm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// Validation methods never touch the broker clients, so a zero
// Driver is sufficient.
var d = &Driver{}

func TestValidateParameters(t *testing.T) {
	cases := []struct {
		name    string
		params  map[string]string
		wantErr string // substring; empty means accept
	}{
		{"empty", nil, ""},
		{"partitions ok", map[string]string{"partitions": "12"}, ""},
		{"partitions zero", map[string]string{"partitions": "0"}, "positive integer"},
		{"partitions negative", map[string]string{"partitions": "-3"}, "positive integer"},
		{"partitions garbage", map[string]string{"partitions": "many"}, "positive integer"},
		{"rf ok", map[string]string{"replicationFactor": "3"}, ""},
		{"rf broker default", map[string]string{"replicationFactor": "-1"}, ""},
		{"rf zero", map[string]string{"replicationFactor": "0"}, "non-zero"},
		{"config passthrough", map[string]string{"config.retention.ms": "not-even-a-number"}, ""},
		{"unknown key", map[string]string{"retentionMs": "1"}, "unknown parameter"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := d.ValidateParameters(c.params)
			checkErr(t, err, c.wantErr)
		})
	}
}

func TestValidateUpdateParametersRejectsRFChange(t *testing.T) {
	old := map[string]string{"replicationFactor": "1"}
	err := d.ValidateUpdateParameters(old, map[string]string{"replicationFactor": "3"})
	checkErr(t, err, "immutable")
	if err := d.ValidateUpdateParameters(old, old); err != nil {
		t.Fatalf("unchanged replicationFactor rejected: %v", err)
	}
	// Partition shrink is deliberately NOT rejected at admission;
	// it surfaces as ParameterDrift at reconcile time.
	if err := d.ValidateUpdateParameters(
		map[string]string{"partitions": "3"},
		map[string]string{"partitions": "2"}); err != nil {
		t.Fatalf("partition shrink should pass admission (handled as drift): %v", err)
	}
}

func TestValidateResourceName(t *testing.T) {
	cases := []struct {
		name    string
		topic   string
		wantErr string
	}{
		{"plain", "orders", ""},
		{"dotted", "tenant1.orders.v003", ""},
		{"mixed case ok", "Orders_2024-v1", ""},
		{"empty", "", "empty"},
		{"dot reserved", ".", "reserved"},
		{"dotdot reserved", "..", "reserved"},
		{"too long", strings.Repeat("a", 250), "249"},
		{"max length ok", strings.Repeat("a", 249), ""},
		{"bad char", "orders,v1", "characters outside"},
		{"space", "or ders", "characters outside"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			checkErr(t, d.ValidateResourceName(c.topic), c.wantErr)
		})
	}
}

func TestTranslateParameters(t *testing.T) {
	parts, rf, cfgs, err := translateParameters(map[string]string{
		"partitions":          "12",
		"replicationFactor":   "3",
		"config.retention.ms": " 604800000 ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if parts != 12 || rf != 3 {
		t.Fatalf("parts=%d rf=%d", parts, rf)
	}
	// config. prefix stripped, value trimmed.
	v, ok := cfgs["retention.ms"]
	if !ok || v == nil || *v != "604800000" {
		t.Fatalf("cfgs=%v", cfgs)
	}
	if _, _, _, err := translateParameters(map[string]string{"nope": "1"}); err == nil {
		t.Fatal("unknown key accepted")
	}
}

func checkErr(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not contain %q", err, want)
	}
}

// Pins the published parameters schema to ValidateParameters in
// both directions; see the gcs twin for rationale. The generated
// whole-CR schemas (schema/) compose from this file.
func TestParametersSchemaInSync(t *testing.T) {
	raw, err := os.ReadFile("schema/v0.1/parameters.schema.json")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var s struct {
		Properties        map[string]json.RawMessage `json:"properties"`
		PatternProperties map[string]json.RawMessage `json:"patternProperties"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("parse schema: %v", err)
	}

	for key := range s.Properties {
		if verr := d.ValidateParameters(map[string]string{key: ""}); verr != nil && strings.Contains(verr.Error(), "unknown parameter") {
			t.Errorf("schema property %q is unknown to ValidateParameters: %v", key, verr)
		}
	}
	// The config.* pass-through advertised in the error message is
	// published as patternProperties.
	if len(s.PatternProperties) != 1 {
		t.Errorf("expected exactly one patternProperties (config.*), got %d", len(s.PatternProperties))
	}
	if verr := d.ValidateParameters(map[string]string{"config.retention.ms": "1000"}); verr != nil {
		t.Errorf("config.* pass-through rejected: %v", verr)
	}

	verr := d.ValidateParameters(map[string]string{"definitely-not-a-parameter": "x"})
	if verr == nil {
		t.Fatal("expected an unknown-parameter error")
	}
	msg := verr.Error()
	i := strings.Index(msg, "accepts: ")
	if i < 0 {
		t.Fatalf("no 'accepts:' enumeration in %q", msg)
	}
	for _, tok := range strings.Split(strings.TrimSuffix(msg[i+len("accepts: "):], ")"), " ") {
		tok = strings.Trim(tok, ",")
		if tok == "" || tok == "and" || strings.Contains(tok, "*") {
			continue
		}
		if _, ok := s.Properties[tok]; !ok {
			t.Errorf("ValidateParameters advertises %q but the schema does not list it", tok)
		}
	}
}

// fakeMutator records the broker mutations alignTopic issues and
// answers with the configured errors.
type fakeMutator struct {
	updates   []int
	alters    [][]kadm.AlterConfig
	updateErr error // request-level
	topicErr  error // per-topic, in the response
	alterErr  error // per-resource, in the response
}

func (f *fakeMutator) UpdatePartitions(_ context.Context, set int, topics ...string) (kadm.CreatePartitionsResponses, error) {
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	f.updates = append(f.updates, set)
	rs := kadm.CreatePartitionsResponses{}
	for _, t := range topics {
		rs[t] = kadm.CreatePartitionsResponse{Topic: t, Err: f.topicErr}
	}
	return rs, nil
}

func (f *fakeMutator) AlterTopicConfigs(_ context.Context, cfgs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
	f.alters = append(f.alters, cfgs)
	var rs kadm.AlterConfigsResponses
	for _, t := range topics {
		rs = append(rs, kadm.AlterConfigsResponse{Name: t, Err: f.alterErr})
	}
	return rs, nil
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func strp(s string) *string { return &s }

// alignCase runs alignTopic for Buckety ns/orders on topic and
// returns the changes it reported.
func alignCase(t *testing.T, m *fakeMutator, ns, topic string, existing *topicView, wantParts int32, wantRF int16, wantCfgs map[string]*string) ([]registry.Change, error) {
	t.Helper()
	var got []registry.Change
	drv := &Driver{mutator: m}
	err := drv.alignTopic(context.Background(), registry.EnsureRequest{
		Name: topic, Namespace: ns, BucketyName: "orders",
		OnChange: func(c registry.Change) { got = append(got, c) },
	}, existing, wantParts, wantRF, wantCfgs)
	return got, err
}

// Partition increases and config alters move the counters by what
// was actually changed and report one Change each; a topic already
// in shape moves nothing and reports nothing.
func TestAlignTopicCountsChanges(t *testing.T) {
	const ns, topic = "align-counts", "align-counts.orders"
	parts := partitionsAdded.WithLabelValues(ns, "orders", topic)
	retention := configChanges.WithLabelValues(ns, "orders", topic, "retention.ms")
	cleanup := configChanges.WithLabelValues(ns, "orders", topic, "cleanup.policy")
	segment := configChanges.WithLabelValues(ns, "orders", topic, "segment.bytes")

	m := &fakeMutator{}
	existing := &topicView{partitions: 3, rf: 3, configs: map[string]string{
		"retention.ms":   "604800000",
		"cleanup.policy": "delete",
		"segment.bytes":  "1073741824",
	}}
	changes, err := alignCase(t, m, ns, topic, existing, 6, 3, map[string]*string{
		"retention.ms":   strp("86400000"),
		"cleanup.policy": strp("delete"), // equal: not altered, not counted
		"segment.bytes":  strp("536870912"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.updates) != 1 || m.updates[0] != 6 {
		t.Errorf("UpdatePartitions calls %v, want [6]", m.updates)
	}
	if got := counterValue(t, parts); got != 3 {
		t.Errorf("partitions added = %v, want 3", got)
	}
	if got := counterValue(t, retention); got != 1 {
		t.Errorf("retention.ms changes = %v, want 1", got)
	}
	if got := counterValue(t, segment); got != 1 {
		t.Errorf("segment.bytes changes = %v, want 1", got)
	}
	if got := counterValue(t, cleanup); got != 0 {
		t.Errorf("cleanup.policy (equal) changes = %v, want 0", got)
	}
	want := []registry.Change{
		{Reason: ReasonPartitionsAdded, Parameter: "partitions", Old: "3", New: "6"},
		{Reason: ReasonTopicConfigChanged, Parameter: "config.retention.ms", Old: "604800000", New: "86400000"},
		{Reason: ReasonTopicConfigChanged, Parameter: "config.segment.bytes", Old: "1073741824", New: "536870912"},
	}
	if fmt.Sprint(changes) != fmt.Sprint(want) {
		t.Errorf("changes\n got %v\nwant %v", changes, want)
	}

	// Next reconcile sees the topic in shape: no calls, no counts.
	inShape := &topicView{partitions: 6, rf: 3, configs: map[string]string{
		"retention.ms":   "86400000",
		"cleanup.policy": "delete",
		"segment.bytes":  "536870912",
	}}
	changes, err = alignCase(t, m, ns, topic, inShape, 6, 3, map[string]*string{
		"retention.ms":   strp("86400000"),
		"cleanup.policy": strp("delete"),
		"segment.bytes":  strp("536870912"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 || len(m.updates) != 1 || len(m.alters) != 1 {
		t.Errorf("no-op align: changes=%v updates=%v alters=%d", changes, m.updates, len(m.alters))
	}
	if counterValue(t, parts) != 3 || counterValue(t, retention) != 1 || counterValue(t, segment) != 1 {
		t.Error("no-op align moved a counter")
	}

	// A key the broker does not report counts as a change from unset.
	changes, err = alignCase(t, m, ns, topic, &topicView{partitions: 6, rf: 3, configs: map[string]string{}}, 0, 0,
		map[string]*string{"cleanup.policy": strp("compact")})
	if err != nil {
		t.Fatal(err)
	}
	if got := counterValue(t, cleanup); got != 1 {
		t.Errorf("cleanup.policy changes = %v, want 1", got)
	}
	if len(changes) != 1 || changes[0].Old != "" || changes[0].New != "compact" {
		t.Errorf("changes %v, want one from unset to compact", changes)
	}
}

// Nothing is counted or reported unless the broker accepted the
// change, and a shrink request stays ParameterDrift.
func TestAlignTopicCountsOnlyAcknowledgedChanges(t *testing.T) {
	const ns, topic = "align-failures", "align-failures.orders"
	parts := partitionsAdded.WithLabelValues(ns, "orders", topic)
	retention := configChanges.WithLabelValues(ns, "orders", topic, "retention.ms")
	existing := &topicView{partitions: 3, rf: 3, configs: map[string]string{"retention.ms": "1"}}
	want := map[string]*string{"retention.ms": strp("2")}

	cases := []struct {
		name    string
		m       *fakeMutator
		parts   int32
		cfgs    map[string]*string
		wantErr string
	}{
		{"request fails", &fakeMutator{updateErr: errors.New("broker down")}, 6, nil, "update partitions"},
		{"topic rejected", &fakeMutator{topicErr: kerr.PolicyViolation}, 6, nil, ""},
		{"alter rejected", &fakeMutator{alterErr: kerr.InvalidConfig}, 0, want, "alter configs"},
		{"shrink", &fakeMutator{}, 2, want, "cannot shrink"},
	}
	for _, c := range cases {
		changes, err := alignCase(t, c.m, ns, topic, existing, c.parts, 3, c.cfgs)
		if c.wantErr == "" && err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
		if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
			t.Errorf("%s: err %v, want %q", c.name, err, c.wantErr)
		}
		if len(changes) != 0 {
			t.Errorf("%s: reported %v", c.name, changes)
		}
	}
	if _, err := alignCase(t, &fakeMutator{}, ns, topic, existing, 2, 3, nil); !registry.IsParameterDrift(err) {
		t.Errorf("shrink: %v, want ParameterDrift", err)
	}
	if got := counterValue(t, parts); got != 0 {
		t.Errorf("partitions added = %v, want 0", got)
	}
	if got := counterValue(t, retention); got != 0 {
		t.Errorf("retention.ms changes = %v, want 0", got)
	}
}

// An in-shape topic still exports its counters, at 0, so a change
// on a later reconcile reads as an increase.
func TestAlignTopicExportsZeroSeries(t *testing.T) {
	const ns, topic = "align-zero", "align-zero.orders"
	if _, err := alignCase(t, &fakeMutator{}, ns, topic,
		&topicView{partitions: 3, rf: 3, configs: map[string]string{"retention.ms": "1"}}, 3, 3,
		map[string]*string{"retention.ms": strp("1")}); err != nil {
		t.Fatal(err)
	}
	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "namespace" && l.GetValue() == ns {
					seen[f.GetName()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	for _, name := range []string{"buckety_kadm_partitions_added_total", "buckety_kadm_config_changes_total"} {
		if v, ok := seen[name]; !ok || v != 0 {
			t.Errorf("%s for %s: value %v present %v, want 0 present", name, ns, v, ok)
		}
	}
}
