// Package gcs is the Google Cloud Storage driver.
//
// Unlike the s3 driver, which speaks the S3 XML protocol to any
// compatible endpoint, this driver provisions buckets through the
// native GCS JSON API: bucket creation needs the project, and
// native parameters (location, uniform bucket-level access,
// versioning, lifecycle) have no S3-interop equivalent.
//
// Control plane and data plane use separate credentials:
//
//   - The controller authenticates to the JSON API via Application
//     Default Credentials (GOOGLE_APPLICATION_CREDENTIALS pointing
//     at a service-account JSON key mounted into the Deployment;
//     Workload Identity works wherever it exists but is not
//     assumed). STORAGE_EMULATOR_HOST is honoured natively by the
//     client library, which is how e2e runs against
//     fake-gcs-server.
//   - Access Secrets carry a static HMAC key pair from the backend
//     config, minted out of band for a service account with access
//     to this backend's buckets (`gcloud storage hmac create`).
//     HMAC keys work with SigV4 against storage.googleapis.com, so
//     consumers use the same S3-protocol client libraries as with
//     the s3 driver.
//
// The static pair is copied identically to every BucketyAccess and
// the reconciler surfaces ScopingNotImplemented for non-ReadWrite
// roles - the same v1alpha1 posture as the s3 driver's root keys.
// Driver-minted per-access credentials (an HMAC key or a
// bucket-scoped service account per access) are deliberately NOT
// in v0.1: GrantAccess runs on every reconcile and rewrites the
// Secret from its result, and an HMAC secret is only retrievable
// at creation, so per-access minting needs the v1alpha2 scoping
// design (grant-once semantics, access identity in GrantRequest)
// before it can be idempotent.
package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
	"github.com/Yolean/y-cluster/pkg/envsubst"
	yaml "sigs.k8s.io/yaml"
)

// DriverName matches backends[].driver in buckety-controller.yaml.
const DriverName = "gcs"

// version is the driver SemVer. Injected at build time via
//
//	-ldflags '-X github.com/Yolean/buckety-controller/pkg/drivers/gcs.version=0.1.0'
//
// per SPEC §Driver versioning. Default keeps tests building.
var version = "0.1.0"

// defaultEndpoint is the public S3-interop endpoint written to
// access Secrets when the backend config does not override it.
// This is connection metadata for consumers, not a parameter
// default: the control plane never uses it.
const defaultEndpoint = "https://storage.googleapis.com"

func init() {
	registry.Register(DriverName, version, factory)
}

// Config is the typed shape of the `config:` block under a gcs
// backend. Mirrors pkg/drivers/gcs/schema/v0.1/config.schema.json.
// Credential fields carry envsubst:"true" so ${VAR} interpolation
// works at controller startup.
type Config struct {
	// Project is the GCP project buckets are created in.
	Project string `json:"project"`
	// Endpoint overrides the S3-interop endpoint written to access
	// Secrets. Defaults to https://storage.googleapis.com. e2e sets
	// it to the fake-gcs-server Service so consumers reach the same
	// emulator the controller provisions against.
	Endpoint string `json:"endpoint,omitempty"`
	// AccessKeyID / SecretAccessKey are the static HMAC pair copied
	// into every access Secret (see the package comment for why
	// v0.1 does not mint per-access keys).
	AccessKeyID     string `json:"accessKeyID" envsubst:"true"`
	SecretAccessKey string `json:"secretAccessKey" envsubst:"true"`
}

func factory(raw json.RawMessage) (registry.Driver, error) {
	var c Config
	if err := yaml.UnmarshalStrict(raw, &c); err != nil {
		return nil, fmt.Errorf("gcs config: %w", err)
	}
	if err := envsubst.Apply(&c, envsubst.OSEnv); err != nil {
		return nil, fmt.Errorf("gcs config: %w", err)
	}
	if c.Project == "" {
		return nil, fmt.Errorf("gcs config: missing required field %q", "project")
	}
	if c.AccessKeyID == "" {
		return nil, fmt.Errorf("gcs config: missing required field %q", "accessKeyID")
	}
	if c.SecretAccessKey == "" {
		return nil, fmt.Errorf("gcs config: missing required field %q", "secretAccessKey")
	}
	if c.Endpoint == "" {
		c.Endpoint = defaultEndpoint
	}

	// ADC resolution happens here, so a controller without
	// GOOGLE_APPLICATION_CREDENTIALS (or STORAGE_EMULATOR_HOST)
	// fails at startup with the library's diagnostic instead of at
	// first reconcile.
	cl, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("gcs config: client init (is GOOGLE_APPLICATION_CREDENTIALS set?): %w", err)
	}

	return &Driver{cfg: &c, client: cl}, nil
}

// Driver implements registry.Driver for Google Cloud Storage.
type Driver struct {
	cfg    *Config
	client *storage.Client
}

func (d *Driver) Name() string    { return DriverName }
func (d *Driver) Version() string { return version }

// EnsureBuckety creates the bucket in the configured project, or
// reconciles mutable parameters on an existing one. Parameters not
// present on the Buckety are unmanaged: the driver never touches
// those knobs on the backend, so buckets it did not create keep
// their configuration (the coexistence invariant).
//
// Location cannot be changed in place; a location parameter that
// disagrees with the backend surfaces ErrParameterDrift and waits
// for human resolution.
func (d *Driver) EnsureBuckety(ctx context.Context, req registry.EnsureRequest) error {
	bkt := d.client.Bucket(req.Name)
	attrs, err := bkt.Attrs(ctx)
	switch {
	case err == nil:
		// Exists and is readable with our credentials: ours to
		// manage.
		return d.reconcileExisting(ctx, bkt, attrs, req)
	case errors.Is(err, storage.ErrBucketNotExist):
		createAttrs, aerr := attrsForCreate(req.Parameters)
		if aerr != nil {
			return fmt.Errorf("gcs: bucket %q: %w", req.Name, aerr)
		}
		cerr := bkt.Create(ctx, d.cfg.Project, createAttrs)
		if cerr == nil {
			return nil
		}
		if isConflict(cerr) {
			// Raced another creator, or the globally-unique name is
			// taken by a bucket we cannot see. Re-fetch decides.
			if attrs, err = bkt.Attrs(ctx); err == nil {
				return d.reconcileExisting(ctx, bkt, attrs, req)
			}
			return fmt.Errorf("gcs: bucket %q exists but is not accessible with this backend's credentials (name likely taken by another project): %w", req.Name, cerr)
		}
		return fmt.Errorf("gcs: create bucket %q: %w", req.Name, cerr)
	case isForbidden(err):
		return fmt.Errorf("gcs: bucket %q is not accessible with this backend's credentials (name likely taken by another project): %w", req.Name, err)
	default:
		return fmt.Errorf("gcs: get bucket %q: %w", req.Name, err)
	}
}

func (d *Driver) reconcileExisting(ctx context.Context, bkt *storage.BucketHandle, attrs *storage.BucketAttrs, req registry.EnsureRequest) error {
	if want, ok := req.Parameters["location"]; ok && !strings.EqualFold(want, attrs.Location) {
		return &registry.ErrParameterDrift{Reason: fmt.Sprintf(
			"bucket %q is in location %q but parameters.location wants %q; GCS buckets cannot move in place",
			req.Name, attrs.Location, want)}
	}
	update, err := updateForDrift(req.Parameters, attrs)
	if err != nil {
		return fmt.Errorf("gcs: bucket %q: %w", req.Name, err)
	}
	if update == nil {
		return nil
	}
	if _, err := bkt.Update(ctx, *update); err != nil {
		return fmt.Errorf("gcs: update bucket %q: %w", req.Name, err)
	}
	return nil
}

// DeleteBuckety removes the bucket. Idempotent on NotFound.
// Buckets must be empty for deletion to succeed; v1alpha1 does not
// empty the bucket on the operator's behalf - same posture as the
// s3 driver.
func (d *Driver) DeleteBuckety(ctx context.Context, name string) error {
	err := d.client.Bucket(name).Delete(ctx)
	if err == nil || errors.Is(err, storage.ErrBucketNotExist) || isNotFound(err) {
		return nil
	}
	return fmt.Errorf("gcs: delete bucket %q: %w", name, err)
}

// GrantAccess returns the gcs Secret payload for a BucketyAccess:
// the backend's static HMAC pair, identical for all roles.
// Scoped=false signals the reconciler to surface
// ScopingNotImplemented for non-ReadWrite roles.
//
// Secret keys per SPEC §Secret output > gcs driver:
//
//	endpoint, bucket, project, accessKeyID, secretAccessKey
//
// `bucket` is the resource-type key per the SPEC's stable
// per-driver convention.
func (d *Driver) GrantAccess(_ context.Context, req registry.GrantRequest) (registry.GrantResult, error) {
	return registry.GrantResult{
		SecretData: map[string][]byte{
			"endpoint":        []byte(d.cfg.Endpoint),
			"bucket":          []byte(req.BucketyName),
			"project":         []byte(d.cfg.Project),
			"accessKeyID":     []byte(d.cfg.AccessKeyID),
			"secretAccessKey": []byte(d.cfg.SecretAccessKey),
		},
		Principal: "gcs-static",
		Scoped:    false,
	}, nil
}

// RevokeAccess is a no-op in v0.1 (nothing to remove since there
// is no per-access principal).
func (d *Driver) RevokeAccess(_ context.Context, _ string) error { return nil }

// ValidateParameters accepts the driver-known keys. No internal
// defaults per SPEC §Parameters: omitted keys leave the backend
// knob unmanaged (GCS's own defaults apply on create: location US,
// uniform bucket-level access off, versioning off, no lifecycle).
func (d *Driver) ValidateParameters(params map[string]string) error {
	for k, v := range params {
		switch k {
		case "location":
			if v == "" {
				return fmt.Errorf("parameters.location must not be empty when set")
			}
		case "uniformBucketLevelAccess", "versioning":
			if _, err := strconv.ParseBool(v); err != nil {
				return fmt.Errorf("parameters.%s: want \"true\" or \"false\", got %q", k, v)
			}
		case "lifecycle":
			if _, err := parseLifecycle(v); err != nil {
				return fmt.Errorf("parameters.lifecycle: %w", err)
			}
		default:
			return fmt.Errorf("unknown parameter %q (gcs v0.1 accepts: location, uniformBucketLevelAccess, versioning, lifecycle)", k)
		}
	}
	return nil
}

// ValidateUpdateParameters: location is set-at-create and
// immutable; any change (including adding or removing the key) is
// a rejection, because the backend cannot move a bucket in place.
func (d *Driver) ValidateUpdateParameters(oldParams, newParams map[string]string) error {
	if err := d.ValidateParameters(newParams); err != nil {
		return err
	}
	if o, n := oldParams["location"], newParams["location"]; o != n {
		return fmt.Errorf("parameters.location is immutable post-create (current=%q, requested=%q)", o, n)
	}
	return nil
}

func (d *Driver) ValidateAccessParameters(params map[string]string) error {
	if len(params) == 0 {
		return nil
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Errorf("gcs v0.1 accepts no BucketyAccess parameters; got: %s",
		strings.Join(keys, ", "))
}

// bucketNameRE covers GCS's core charset rule: lowercase
// alphanumerics, dashes, underscores and dots, starting and ending
// alphanumeric. Note underscores are legal in GCS, unlike S3.
var bucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*[a-z0-9]$`)

// ValidateResourceName enforces the core GCS bucket naming rules on
// the resolved spec.name template result. Rules the backend layers
// on top (dotted names require domain verification and allow up to
// 222 characters, "close misspellings of google" are rejected)
// surface as EnsureBuckety errors rather than being duplicated
// here.
func (d *Driver) ValidateResourceName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name %q is %d characters; GCS requires 3-63 (longer dotted names need domain verification and are out of scope for gcs v0.1)", name, len(name))
	}
	if !bucketNameRE.MatchString(name) {
		return fmt.Errorf("bucket name %q must be lowercase alphanumerics, dashes, underscores and dots, starting and ending alphanumeric", name)
	}
	if strings.HasPrefix(name, "goog") {
		return fmt.Errorf("bucket name %q must not begin with the %q prefix", name, "goog")
	}
	if strings.Contains(name, "google") {
		return fmt.Errorf("bucket name %q must not contain %q", name, "google")
	}
	if net.ParseIP(name) != nil {
		return fmt.Errorf("bucket name %q must not be formatted as an IP address", name)
	}
	return nil
}

// ---- parameter translation ----

// attrsForCreate translates Buckety parameters into creation-time
// BucketAttrs. Only parameters present on the resource are set;
// GCS's own defaults fill the rest.
func attrsForCreate(params map[string]string) (*storage.BucketAttrs, error) {
	attrs := &storage.BucketAttrs{}
	if v, ok := params["location"]; ok {
		attrs.Location = v
	}
	if v, ok := params["uniformBucketLevelAccess"]; ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.uniformBucketLevelAccess: %w", err)
		}
		attrs.UniformBucketLevelAccess = storage.UniformBucketLevelAccess{Enabled: b}
	}
	if v, ok := params["versioning"]; ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.versioning: %w", err)
		}
		attrs.VersioningEnabled = b
	}
	if v, ok := params["lifecycle"]; ok {
		lc, err := parseLifecycle(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.lifecycle: %w", err)
		}
		attrs.Lifecycle = *lc
	}
	return attrs, nil
}

// updateForDrift compares the managed parameters against the
// backend's current attributes and returns the update that
// reconciles them, or nil when nothing differs. Location is
// handled by the caller (unreconcilable → ErrParameterDrift).
func updateForDrift(params map[string]string, attrs *storage.BucketAttrs) (*storage.BucketAttrsToUpdate, error) {
	var update storage.BucketAttrsToUpdate
	dirty := false
	if v, ok := params["uniformBucketLevelAccess"]; ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.uniformBucketLevelAccess: %w", err)
		}
		if attrs.UniformBucketLevelAccess.Enabled != b {
			update.UniformBucketLevelAccess = &storage.UniformBucketLevelAccess{Enabled: b}
			dirty = true
		}
	}
	if v, ok := params["versioning"]; ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.versioning: %w", err)
		}
		if attrs.VersioningEnabled != b {
			update.VersioningEnabled = b
			dirty = true
		}
	}
	if v, ok := params["lifecycle"]; ok {
		lc, err := parseLifecycle(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.lifecycle: %w", err)
		}
		if !reflect.DeepEqual(attrs.Lifecycle, *lc) {
			update.Lifecycle = lc
			dirty = true
		}
	}
	if !dirty {
		return nil, nil
	}
	return &update, nil
}

// ---- lifecycle JSON (gsutil `lifecycle set` shape) ----

// The parameter value is the same document `gsutil lifecycle set`
// takes, so operators can move an existing policy file into the
// Buckety verbatim:
//
//	{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]}
//
// Decoding is strict: unknown action types and condition fields
// are admission errors, not silent drops.
type lifecycleDoc struct {
	Rule []lifecycleRule `json:"rule"`
}

type lifecycleRule struct {
	Action    lifecycleAction    `json:"action"`
	Condition lifecycleCondition `json:"condition"`
}

type lifecycleAction struct {
	Type         string `json:"type"`
	StorageClass string `json:"storageClass,omitempty"`
}

type lifecycleCondition struct {
	Age                     *int64   `json:"age,omitempty"`
	CreatedBefore           string   `json:"createdBefore,omitempty"`
	CustomTimeBefore        string   `json:"customTimeBefore,omitempty"`
	DaysSinceCustomTime     int64    `json:"daysSinceCustomTime,omitempty"`
	DaysSinceNoncurrentTime int64    `json:"daysSinceNoncurrentTime,omitempty"`
	IsLive                  *bool    `json:"isLive,omitempty"`
	MatchesPrefix           []string `json:"matchesPrefix,omitempty"`
	MatchesStorageClass     []string `json:"matchesStorageClass,omitempty"`
	MatchesSuffix           []string `json:"matchesSuffix,omitempty"`
	NoncurrentTimeBefore    string   `json:"noncurrentTimeBefore,omitempty"`
	NumNewerVersions        int64    `json:"numNewerVersions,omitempty"`
}

func parseLifecycle(doc string) (*storage.Lifecycle, error) {
	dec := json.NewDecoder(strings.NewReader(doc))
	dec.DisallowUnknownFields()
	var parsed lifecycleDoc
	if err := dec.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("not a valid gsutil lifecycle document: %w", err)
	}
	if parsed.Rule == nil {
		return nil, fmt.Errorf("not a valid gsutil lifecycle document: missing \"rule\" list (use {\"rule\": []} to clear all rules)")
	}
	out := &storage.Lifecycle{}
	for i, r := range parsed.Rule {
		rule := storage.LifecycleRule{}
		switch r.Action.Type {
		case "Delete", "AbortIncompleteMultipartUpload":
			if r.Action.StorageClass != "" {
				return nil, fmt.Errorf("rule[%d].action.storageClass is only valid with type SetStorageClass", i)
			}
			rule.Action.Type = r.Action.Type
		case "SetStorageClass":
			if r.Action.StorageClass == "" {
				return nil, fmt.Errorf("rule[%d].action.storageClass is required with type SetStorageClass", i)
			}
			rule.Action.Type = r.Action.Type
			rule.Action.StorageClass = r.Action.StorageClass
		case "":
			return nil, fmt.Errorf("rule[%d].action.type is required", i)
		default:
			return nil, fmt.Errorf("rule[%d].action.type %q is not one of Delete, SetStorageClass, AbortIncompleteMultipartUpload", i, r.Action.Type)
		}
		c := r.Condition
		if c.Age != nil {
			if *c.Age == 0 {
				// The client library drops a zero AgeInDays; the
				// documented way to match all objects is AllObjects.
				rule.Condition.AllObjects = true
			} else {
				rule.Condition.AgeInDays = *c.Age
			}
		}
		var err error
		if rule.Condition.CreatedBefore, err = parseLifecycleDate(c.CreatedBefore, i, "createdBefore"); err != nil {
			return nil, err
		}
		if rule.Condition.CustomTimeBefore, err = parseLifecycleDate(c.CustomTimeBefore, i, "customTimeBefore"); err != nil {
			return nil, err
		}
		if rule.Condition.NoncurrentTimeBefore, err = parseLifecycleDate(c.NoncurrentTimeBefore, i, "noncurrentTimeBefore"); err != nil {
			return nil, err
		}
		rule.Condition.DaysSinceCustomTime = c.DaysSinceCustomTime
		rule.Condition.DaysSinceNoncurrentTime = c.DaysSinceNoncurrentTime
		if c.IsLive != nil {
			if *c.IsLive {
				rule.Condition.Liveness = storage.Live
			} else {
				rule.Condition.Liveness = storage.Archived
			}
		}
		rule.Condition.MatchesPrefix = c.MatchesPrefix
		rule.Condition.MatchesStorageClasses = c.MatchesStorageClass
		rule.Condition.MatchesSuffix = c.MatchesSuffix
		rule.Condition.NumNewerVersions = c.NumNewerVersions
		out.Rules = append(out.Rules, rule)
	}
	return out, nil
}

func parseLifecycleDate(v string, i int, field string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse("2006-01-02", v)
	if err != nil {
		return time.Time{}, fmt.Errorf("rule[%d].condition.%s: want YYYY-MM-DD, got %q", i, field, v)
	}
	return t, nil
}

// ---- error classification ----

func gapiCode(err error, code int) bool {
	var ge *googleapi.Error
	return errors.As(err, &ge) && ge.Code == code
}

func isConflict(err error) bool  { return gapiCode(err, 409) }
func isForbidden(err error) bool { return gapiCode(err, 403) }
func isNotFound(err error) bool  { return gapiCode(err, 404) }
