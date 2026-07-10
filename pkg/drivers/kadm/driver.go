// Package kadm is the Kafka-protocol driver.
//
// Backing services in scope: Apache Kafka, Redpanda, Confluent
// (anything that speaks the Kafka admin protocol). v1alpha1 has
// no SASL/SCRAM; the controller treats all BucketyAccess as
// receiving identical credentials and surfaces
// ScopingNotImplemented.
package kadm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DriverName is the name backends reference under
// backends[].driver in buckety-controller.yaml.
const DriverName = "kadm"

// version is the driver SemVer. Injected at build time via
//
//	-ldflags '-X github.com/Yolean/buckety-controller/pkg/drivers/kadm.version=0.1.0'
//
// per SPEC §Driver versioning. Default keeps tests building.
var version = "0.1.0"

func init() {
	registry.Register(DriverName, version, factory)
}

func factory(raw json.RawMessage) (registry.Driver, error) {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.SeedBrokers...),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kadm: build kgo client: %w", err)
	}
	return &Driver{
		cfg:     cfg,
		kclient: kc,
		aclient: kadm.NewClient(kc),
	}, nil
}

// Driver implements registry.Driver for Kafka-protocol topics.
type Driver struct {
	cfg     *Config
	kclient *kgo.Client
	aclient *kadm.Client
}

func (d *Driver) Name() string    { return DriverName }
func (d *Driver) Version() string { return version }

// Bootstrap returns the resolved seedBrokers value to embed in
// minted Secrets. Comma-joined.
func (d *Driver) Bootstrap() string {
	return strings.Join(d.cfg.SeedBrokers, ",")
}

// EnsureBuckety create-or-updates the topic to match req.
// Idempotent. Partition shrink is rejected with ErrParameterDrift.
func (d *Driver) EnsureBuckety(ctx context.Context, req registry.EnsureRequest) error {
	wantParts, wantRF, wantCfgs, err := translateParameters(req.Parameters)
	if err != nil {
		return err
	}

	existing, err := d.describeTopic(ctx, req.Name)
	if err != nil {
		return err
	}
	if existing == nil {
		return d.createTopic(ctx, req.Name, wantParts, wantRF, wantCfgs)
	}
	return d.alignTopic(ctx, req.Name, existing, wantParts, wantRF, wantCfgs)
}

// DeleteBuckety removes the topic. Idempotent on NotFound.
func (d *Driver) DeleteBuckety(ctx context.Context, name string) error {
	resp, err := d.aclient.DeleteTopics(ctx, name)
	if err != nil {
		return fmt.Errorf("kadm: delete topic %q: %w", name, err)
	}
	for _, r := range resp {
		if r.Err == nil || errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
			continue
		}
		return fmt.Errorf("kadm: delete topic %q: %w", name, r.Err)
	}
	return nil
}

// GrantAccess returns the kadm Secret payload for a BucketyAccess.
// v1alpha1: identical credentials for all roles (no SASL/SCRAM).
// Scoped=false signals to the reconciler to surface
// ScopingNotImplemented for non-ReadWrite roles.
func (d *Driver) GrantAccess(_ context.Context, req registry.GrantRequest) (registry.GrantResult, error) {
	return registry.GrantResult{
		SecretData: map[string][]byte{
			"bootstrap": []byte(d.Bootstrap()),
			"topic":     []byte(req.BucketyName),
		},
		Principal: "kadm-root",
		Scoped:    false,
	}, nil
}

// RevokeAccess is a no-op in v1alpha1 (nothing to remove since
// there is no per-access principal).
func (d *Driver) RevokeAccess(_ context.Context, _ string) error { return nil }

// ValidateParameters enforces the kadm parameters schema at the
// admission layer. The pattern is intentionally aligned with
// pkg/drivers/kadm/schema/v0.1/parameters.schema.json: a future
// JSON-Schema validator can replace this method and the contract
// stays the same.
func (d *Driver) ValidateParameters(params map[string]string) error {
	for k, v := range params {
		switch {
		case k == "partitions":
			if _, err := strconv.ParseInt(v, 10, 32); err != nil || v == "0" || strings.HasPrefix(v, "-") {
				return fmt.Errorf("parameters.partitions must be a positive integer, got %q", v)
			}
		case k == "replicationFactor":
			if _, err := strconv.ParseInt(v, 10, 16); err != nil || v == "0" {
				return fmt.Errorf("parameters.replicationFactor must be a non-zero integer, got %q", v)
			}
		case strings.HasPrefix(k, "config."):
			// Kafka topic-config keys; broker validates content.
			_ = v
		default:
			return fmt.Errorf("unknown parameter %q (kadm v0.1 accepts: partitions, replicationFactor, config.*)", k)
		}
	}
	return nil
}

// ValidateUpdateParameters enforces immutability of
// replicationFactor and partitions-shrink in advance of the
// reconcile loop's ErrParameterDrift. Webhook-level rejection
// gives consumers a clear error at apply time.
func (d *Driver) ValidateUpdateParameters(oldParams, newParams map[string]string) error {
	if err := d.ValidateParameters(newParams); err != nil {
		return err
	}
	// Only strict immutables are rejected at admission.
	// Partition shrink lives in reconcile-time drift detection
	// (alignTopic returns ErrParameterDrift) per SPEC
	// §End-to-end coverage #4: the controller surfaces
	// ParameterDrift=True for the user to resolve, rather than
	// failing the apply.
	if o, n := oldParams["replicationFactor"], newParams["replicationFactor"]; o != n {
		return fmt.Errorf("parameters.replicationFactor is immutable post-create (current=%q, requested=%q); Kafka cannot reassign partitions without explicit kafka-reassign-partitions tooling", o, n)
	}
	return nil
}

// topicNameRE is the Kafka-legal topic charset. Kafka additionally
// warns about mixing '.' and '_' (metric-name collisions) but does
// not reject it; neither do we.
var topicNameRE = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// ValidateResourceName enforces Kafka topic naming rules on the
// resolved spec.name template result.
func (d *Driver) ValidateResourceName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name is empty")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("topic name %q is reserved", name)
	}
	if len(name) > 249 {
		return fmt.Errorf("topic name is %d characters; Kafka's limit is 249", len(name))
	}
	if !topicNameRE.MatchString(name) {
		return fmt.Errorf("topic name %q contains characters outside [a-zA-Z0-9._-]", name)
	}
	return nil
}

// ValidateAccessParameters: v1alpha1 kadm accepts no
// access-level parameters.
func (d *Driver) ValidateAccessParameters(params map[string]string) error {
	if len(params) == 0 {
		return nil
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	return fmt.Errorf("kadm v0.1 accepts no BucketyAccess parameters; got: %s",
		strings.Join(keys, ", "))
}

// ---- internals ----

type topicView struct {
	partitions int32
	rf         int16
	configs    map[string]string
}

func (d *Driver) describeTopic(ctx context.Context, name string) (*topicView, error) {
	td, err := d.aclient.ListTopics(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("kadm: list topic %q: %w", name, err)
	}
	t, ok := td[name]
	if !ok {
		return nil, nil
	}
	if t.Err != nil {
		if errors.Is(t.Err, kerr.UnknownTopicOrPartition) {
			return nil, nil
		}
		return nil, fmt.Errorf("kadm: list topic %q: %w", name, t.Err)
	}
	view := &topicView{
		partitions: int32(len(t.Partitions)),
	}
	if len(t.Partitions) > 0 {
		// All partitions share the replication factor in a healthy topic.
		view.rf = int16(len(t.Partitions[0].Replicas))
	}

	cfgs, err := d.aclient.DescribeTopicConfigs(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("kadm: describe configs %q: %w", name, err)
	}
	view.configs = make(map[string]string)
	for _, rc := range cfgs {
		if rc.Err != nil {
			continue
		}
		for _, c := range rc.Configs {
			if c.Value != nil {
				view.configs[c.Key] = *c.Value
			}
		}
	}
	return view, nil
}

func (d *Driver) createTopic(ctx context.Context, name string, parts int32, rf int16, cfgs map[string]*string) error {
	if parts == 0 {
		parts = -1 // ask broker to use its default
	}
	if rf == 0 {
		rf = -1
	}
	// kadm.CreateTopic surfaces the broker-level per-topic Err as
	// the function's error too (the response.Err and err are the
	// same value). TopicAlreadyExists is idempotent here:
	// describeTopic should have caught existence but can miss it
	// against a freshly-built kgo metadata cache on controller
	// restart; treat both pathways as success.
	resp, err := d.aclient.CreateTopic(ctx, parts, rf, cfgs, name)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("kadm: create topic %q: %w", name, err)
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("kadm: create topic %q: %w", name, resp.Err)
	}
	return nil
}

func (d *Driver) alignTopic(ctx context.Context, name string, existing *topicView, wantParts int32, wantRF int16, wantCfgs map[string]*string) error {
	if wantParts > 0 && existing.partitions != wantParts {
		if wantParts < existing.partitions {
			return &registry.ErrParameterDrift{
				Reason: fmt.Sprintf("partitions: current=%d requested=%d (Kafka cannot shrink partition counts in place)", existing.partitions, wantParts),
			}
		}
		// Add partitions to reach the requested count.
		if _, err := d.aclient.UpdatePartitions(ctx, int(wantParts), name); err != nil {
			return fmt.Errorf("kadm: update partitions %q: %w", name, err)
		}
	}
	if wantRF > 0 && existing.rf != 0 && existing.rf != wantRF {
		return &registry.ErrParameterDrift{
			Reason: fmt.Sprintf("replicationFactor: current=%d requested=%d (cannot be changed in place; needs partition reassignment)", existing.rf, wantRF),
		}
	}
	// Apply config delta.
	alters := []kadm.AlterConfig{}
	for k, vp := range wantCfgs {
		cur, hadCur := existing.configs[k]
		switch {
		case vp == nil && hadCur:
			alters = append(alters, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
		case vp != nil && (!hadCur || cur != *vp):
			alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: vp})
		}
	}
	if len(alters) > 0 {
		resp, err := d.aclient.AlterTopicConfigs(ctx, alters, name)
		if err != nil {
			return fmt.Errorf("kadm: alter configs %q: %w", name, err)
		}
		for _, r := range resp {
			if r.Err != nil {
				return fmt.Errorf("kadm: alter configs %q: %w", name, r.Err)
			}
		}
	}
	return nil
}

// translateParameters splits spec.parameters into (partitions,
// replicationFactor, config-keys). config.* keys are passed
// through with the "config." prefix stripped (Kafka's wire-level
// names).
func translateParameters(params map[string]string) (int32, int16, map[string]*string, error) {
	var parts int32
	var rf int16
	cfgs := map[string]*string{}
	for k, v := range params {
		switch {
		case k == "partitions":
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return 0, 0, nil, fmt.Errorf("parameters.partitions: %w", err)
			}
			parts = int32(n)
		case k == "replicationFactor":
			n, err := strconv.ParseInt(v, 10, 16)
			if err != nil {
				return 0, 0, nil, fmt.Errorf("parameters.replicationFactor: %w", err)
			}
			rf = int16(n)
		case strings.HasPrefix(k, "config."):
			val := strings.TrimSpace(v)
			key := strings.TrimPrefix(k, "config.")
			cfgs[key] = &val
		default:
			return 0, 0, nil, fmt.Errorf("unknown parameter %q", k)
		}
	}
	return parts, rf, cfgs, nil
}
