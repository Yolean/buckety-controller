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
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

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

// globalEndpoint is the S3-interop host written to access
// Secrets for buckets whose location has no locational endpoint
// (multi-regions like EU/US, dual-region codes). Endpoint values
// are BARE hosts per s3-config convention - the scheme is the
// consumer's choice (issue #14: a scheme'd value produced
// https://https://... at a consumer that prepends).
const globalEndpoint = "storage.googleapis.com"

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
	// Endpoint overrides the S3-interop endpoint written to
	// access Secrets, as a BARE host (no scheme). When unset -
	// the normal case - the driver derives the endpoint per
	// bucket from its location: storage.<region>.rep.googleapis.com
	// for regional buckets, storage.googleapis.com otherwise.
	// Override for emulators (fake-gcs-server) so consumers reach
	// the same emulator the controller provisions against.
	Endpoint string `json:"endpoint,omitempty"`
	// Region overrides the SigV4 signing region written to access
	// Secrets under the region key. When unset it is derived from
	// each bucket's location alongside the endpoint.
	Region string `json:"region,omitempty"`
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
	if strings.Contains(c.Endpoint, "://") {
		return nil, fmt.Errorf("gcs config: endpoint %q must be a bare host without scheme; consumers choose the scheme", c.Endpoint)
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

// DeleteBuckety removes the bucket and its contents -
// PersistentVolume reclaimPolicy=Delete semantics per SPEC
// §Lifecycle and deletion. Idempotent on NotFound.
//
// Contents (including archived generations on versioned buckets)
// are emptied in bounded slices with a small worker pool;
// ErrDeletionInProgress tells the controller to requeue promptly.
// Objects the data plane protected with holds or retention refuse
// deletion and block the Buckety with an error naming them - that
// protection is the design's guard against a bad Delete, not an
// obstacle to work around. Soft-deleted objects do not block
// bucket deletion; with a soft delete policy the bucket itself
// remains restorable for the configured window.
func (d *Driver) DeleteBuckety(ctx context.Context, name string) error {
	noList := false
	live, err := d.emptyBucketSlice(ctx, name)
	switch {
	case errors.Is(err, errNoListPermission):
		// A provisioning credential without storage.objects.list
		// (the bucket-CRUD-only day-one role) cannot empty; plain
		// bucket delete below still handles the empty-bucket case,
		// and the non-empty case gets an actionable error instead
		// of an in-progress loop that never converges.
		noList = true
	case err != nil:
		if errors.Is(err, storage.ErrBucketNotExist) || isNotFound(err) {
			return nil
		}
		return fmt.Errorf("gcs: empty bucket %q for deletion: %w", name, err)
	case live > 0:
		return &registry.ErrDeletionInProgress{Progress: fmt.Sprintf("bucket %q: deleted %d objects, checking for more", name, live)}
	}
	err = d.client.Bucket(name).Delete(ctx)
	if err == nil || errors.Is(err, storage.ErrBucketNotExist) || isNotFound(err) {
		return nil
	}
	if isConflict(err) {
		if noList {
			return fmt.Errorf("gcs: bucket %q is not empty and the controller's credentials lack storage.objects.list, so it cannot be emptied; grant storage.objects.list + storage.objects.delete (retentionPolicy=Delete is recursive) or drain the bucket out of band", name)
		}
		// 409 conflict: not empty - archived generations remain (the
		// slice attempts them best-effort) or a writer raced; the
		// next pass picks up whatever the backend still lists.
		return &registry.ErrDeletionInProgress{Progress: fmt.Sprintf("bucket %q: still not empty after emptying pass", name)}
	}
	return fmt.Errorf("gcs: delete bucket %q: %w", name, err)
}

// errNoListPermission marks an emptying pass refused at the
// object listing itself.
var errNoListPermission = errors.New("no storage.objects.list permission")

// emptyDeleteSliceSize bounds objects deleted per DeleteBuckety
// call, and emptyDeleteWorkers bounds concurrent DeleteObject
// calls (GCS has no batch-delete API).
const (
	emptyDeleteSliceSize = 256
	emptyDeleteWorkers   = 16
)

// emptyBucketSlice deletes up to emptyDeleteSliceSize object
// generations (Versions: true covers live and archived) and
// returns how many LIVE generations were deleted. Only live
// generations count as remaining work: deleting an archived
// generation is best-effort because backends diverge - real GCS
// removes it permanently (and requires this for the bucket to
// become deletable), while fake-gcs-server keeps a tombstone
// listed forever and 404s repeat deletes. Counting those
// tombstones would spin the in-progress loop without converging;
// the bucket delete that follows is the arbiter of actually
// empty.
func (d *Driver) emptyBucketSlice(ctx context.Context, name string) (int, error) {
	bkt := d.client.Bucket(name)
	it := bkt.Objects(ctx, &storage.Query{Versions: true})

	type target struct {
		key  string
		gen  int64
		live bool
	}
	var targets []target
	live := 0
	for len(targets) < emptyDeleteSliceSize {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if isForbidden(err) {
			return 0, fmt.Errorf("%w: %w", errNoListPermission, err)
		}
		if err != nil {
			return 0, err
		}
		isLive := attrs.Deleted.IsZero()
		if isLive {
			live++
		}
		targets = append(targets, target{key: attrs.Name, gen: attrs.Generation, live: isLive})
	}
	if len(targets) == 0 {
		return 0, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(emptyDeleteWorkers)
	for _, t := range targets {
		g.Go(func() error {
			err := bkt.Object(t.key).Generation(t.gen).Delete(gctx)
			if err == nil || errors.Is(err, storage.ErrObjectNotExist) {
				return nil
			}
			if isForbidden(err) {
				return fmt.Errorf("object %q (generation %d) refuses deletion - likely under a hold or retention placed by the data plane; release it to let the Buckety go: %w", t.key, t.gen, err)
			}
			return fmt.Errorf("delete object %q: %w", t.key, err)
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	return live, nil
}

// GrantAccess returns the gcs Secret payload for a BucketyAccess:
// the backend's static HMAC pair, identical for all roles.
// Scoped=false signals the reconciler to surface
// ScopingNotImplemented for non-ReadWrite roles.
//
// Secret keys per SPEC §Secret output > gcs driver:
//
//	endpoint, bucket, project, region (when known),
//	accessKeyID, secretAccessKey
//
// `bucket` is the resource-type key per the SPEC's stable
// per-driver convention. endpoint/region are derived from the
// bucket's location unless the backend config overrides them
// (issue #14: signing for a EUROPE-WEST4 bucket against the
// global host breaks SigV4 and data residency).
func (d *Driver) GrantAccess(ctx context.Context, req registry.GrantRequest) (registry.GrantResult, error) {
	endpoint, region := d.cfg.Endpoint, d.cfg.Region
	if endpoint == "" {
		attrs, err := d.client.Bucket(req.BucketyName).Attrs(ctx)
		if err != nil {
			return registry.GrantResult{}, fmt.Errorf("gcs: resolve endpoint for bucket %q: %w", req.BucketyName, err)
		}
		endpoint, region = locationEndpoint(attrs.Location)
		if d.cfg.Region != "" {
			region = d.cfg.Region
		}
	}
	data := map[string][]byte{
		"endpoint":        []byte(endpoint),
		"bucket":          []byte(req.BucketyName),
		"project":         []byte(d.cfg.Project),
		"accessKeyID":     []byte(d.cfg.AccessKeyID),
		"secretAccessKey": []byte(d.cfg.SecretAccessKey),
	}
	if region != "" {
		data["region"] = []byte(region)
	}
	return registry.GrantResult{
		SecretData: data,
		Principal:  "gcs-static",
		Scoped:     false,
	}, nil
}

// locationEndpoint maps a bucket location to its S3-interop bare
// host and SigV4 signing region. Regional locations (the ones
// containing a dash, e.g. EUROPE-WEST4) have locational
// endpoints, storage.<region>.rep.googleapis.com, which keep
// requests in-region. Multi-regions (EU, US, ASIA) and
// dual-region codes have none; those fall back to the global
// host with no region key, leaving the signing region the
// consumer's choice.
func locationEndpoint(location string) (endpoint, region string) {
	l := strings.ToLower(location)
	if strings.Contains(l, "-") {
		return "storage." + l + ".rep.googleapis.com", l
	}
	return globalEndpoint, ""
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
		case "softDeleteRetentionSeconds":
			if _, err := parseSoftDeleteRetention(v); err != nil {
				return fmt.Errorf("parameters.softDeleteRetentionSeconds: %w", err)
			}
		case "labels":
			if _, err := parseLabels(v); err != nil {
				return fmt.Errorf("parameters.labels: %w", err)
			}
		default:
			return fmt.Errorf("unknown parameter %q (gcs v0.1 accepts: location, uniformBucketLevelAccess, versioning, lifecycle, softDeleteRetentionSeconds, labels)", k)
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
	if v, ok := params["softDeleteRetentionSeconds"]; ok {
		d, err := parseSoftDeleteRetention(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.softDeleteRetentionSeconds: %w", err)
		}
		attrs.SoftDeletePolicy = &storage.SoftDeletePolicy{RetentionDuration: d}
	}
	if v, ok := params["labels"]; ok {
		labels, err := parseLabels(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.labels: %w", err)
		}
		attrs.Labels = labels
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
	if v, ok := params["softDeleteRetentionSeconds"]; ok {
		want, err := parseSoftDeleteRetention(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.softDeleteRetentionSeconds: %w", err)
		}
		current := time.Duration(0)
		if attrs.SoftDeletePolicy != nil {
			current = attrs.SoftDeletePolicy.RetentionDuration
		}
		if current != want {
			update.SoftDeletePolicy = &storage.SoftDeletePolicy{RetentionDuration: want}
			dirty = true
		}
	}
	if v, ok := params["labels"]; ok {
		want, err := parseLabels(v)
		if err != nil {
			return nil, fmt.Errorf("parameters.labels: %w", err)
		}
		// Listed labels are converged to their declared values;
		// labels absent from the parameter are unmanaged and never
		// deleted - same posture as unlisted parameters.
		for k2, v2 := range want {
			if attrs.Labels[k2] != v2 {
				update.SetLabel(k2, v2)
				dirty = true
			}
		}
	}
	if !dirty {
		return nil, nil
	}
	return &update, nil
}

// parseSoftDeleteRetention maps the parameter to
// softDeletePolicy.retentionDurationSeconds. "0" disables soft
// delete; GCS accepts enabled windows of 7 to 90 days only, so
// out-of-range values are rejected here instead of surfacing as a
// backend error after admission.
func parseSoftDeleteRetention(v string) (time.Duration, error) {
	secs, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("want seconds as an integer (\"0\" disables), got %q", v)
	}
	const week, ninetyDays = 7 * 24 * 60 * 60, 90 * 24 * 60 * 60
	if secs != 0 && (secs < week || secs > ninetyDays) {
		return 0, fmt.Errorf("GCS requires 0 (disabled) or %d..%d seconds (7 to 90 days), got %d", week, ninetyDays, secs)
	}
	return time.Duration(secs) * time.Second, nil
}

// parseLabels decodes the labels parameter, a JSON object of
// string values. Key/value charset rules are the backend's to
// enforce.
func parseLabels(v string) (map[string]string, error) {
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields()
	var out map[string]string
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("want a JSON object of string values, got %q: %w", v, err)
	}
	return out, nil
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
