// Package s3 is the S3-protocol driver.
//
// Backing services in scope: any S3-compatible API. The v1alpha1
// CI matrix exercises VersityGW and MinIO; AWS S3, R2, Hetzner
// and GCS interop are covered by the SPEC's client-library
// compatibility bet (see SPEC.md §Drivers in v1alpha1). v1alpha1
// has no per-access IAM minting; all BucketyAccess instances
// against the same Buckety receive identical credentials drawn
// from the backend's configured root keys and the reconciler
// surfaces ScopingNotImplemented for non-ReadWrite roles.
package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"

	"github.com/Yolean/buckety-controller/pkg/drivers/objectstore"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
	"github.com/Yolean/y-cluster/pkg/envsubst"
	yaml "sigs.k8s.io/yaml"
)

// DriverName matches backends[].driver in buckety-controller.yaml.
const DriverName = "s3"

// version is the driver SemVer. Injected at build time via
//
//	-ldflags '-X github.com/Yolean/buckety-controller/pkg/drivers/s3.version=0.1.0'
//
// per SPEC §Driver versioning. Default keeps tests building.
var version = "0.1.0"

func init() {
	registry.Register(DriverName, version, factory)
}

// Config is the typed shape of the `config:` block under an s3
// backend. Mirrors pkg/drivers/s3/schema/v0.1/config.schema.json.
// Credential fields carry envsubst:"true" so ${VAR} interpolation
// works at controller startup.
type Config struct {
	Implementation  string `json:"implementation,omitempty"`
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"forcePathStyle,omitempty"`
	AccessKeyID     string `json:"accessKeyID" envsubst:"true"`
	SecretAccessKey string `json:"secretAccessKey" envsubst:"true"`
}

func factory(raw json.RawMessage) (registry.Driver, error) {
	var c Config
	if err := yaml.UnmarshalStrict(raw, &c); err != nil {
		return nil, fmt.Errorf("s3 config: %w", err)
	}
	if err := envsubst.Apply(&c, envsubst.OSEnv); err != nil {
		return nil, fmt.Errorf("s3 config: %w", err)
	}
	if c.Endpoint == "" {
		return nil, fmt.Errorf("s3 config: missing required field %q", "endpoint")
	}
	if c.AccessKeyID == "" {
		return nil, fmt.Errorf("s3 config: missing required field %q", "accessKeyID")
	}
	if c.SecretAccessKey == "" {
		return nil, fmt.Errorf("s3 config: missing required field %q", "secretAccessKey")
	}
	switch c.Implementation {
	case "", "aws", "r2", "minio", "versitygw":
		// ok
	default:
		return nil, fmt.Errorf("s3 config: unknown implementation %q", c.Implementation)
	}

	// The AWS SDK requires a region even when BaseEndpoint is set;
	// "auto" is the documented value R2 expects, "us-east-1" is the
	// MinIO/VersityGW default. The empty case is normalised here so
	// the eventual signer has something to fill into Authorization
	// headers; this matches the AWS CLI's behaviour when --region is
	// omitted with --endpoint-url.
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	cl := awss3.NewFromConfig(aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, ""),
	}, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
		o.UsePathStyle = c.ForcePathStyle
	})

	return &Driver{cfg: &c, client: cl}, nil
}

// Driver implements registry.Driver for S3-compatible backends.
type Driver struct {
	cfg    *Config
	client *awss3.Client
}

func (d *Driver) Name() string    { return DriverName }
func (d *Driver) Version() string { return version }

// InspectBuckety probes existence and content ahead of the
// adoption decision (SPEC §Adoption). Emptiness counts current
// objects and noncurrent object versions; delete markers alone do
// not count as content. A backend without versioning support
// (NotImplemented on the versions listing) has no hidden versions
// to find, so the current-objects check decides. AccessDenied on
// either listing degrades to non-empty: unknown content is
// content.
func (d *Driver) InspectBuckety(ctx context.Context, name string) (registry.Inspection, error) {
	if _, err := d.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(name)}); err != nil {
		if isNotFound(err) {
			return registry.Inspection{}, nil
		}
		return registry.Inspection{}, fmt.Errorf("s3: bucket %q exists but is not accessible with this backend's credentials (name likely taken by another account): %w", name, err)
	}
	objs, err := d.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(name), MaxKeys: aws.Int32(1),
	})
	switch {
	case isAccessDenied(err):
		return registry.Inspection{Exists: true, Empty: false}, nil
	case err != nil:
		return registry.Inspection{}, fmt.Errorf("s3: list bucket %q: %w", name, err)
	case len(objs.Contents) > 0:
		return registry.Inspection{Exists: true, Empty: false}, nil
	}
	vers, err := d.client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String(name), MaxKeys: aws.Int32(1),
	})
	switch {
	case isNotImplemented(err):
		return registry.Inspection{Exists: true, Empty: true}, nil
	case isAccessDenied(err):
		return registry.Inspection{Exists: true, Empty: false}, nil
	case err != nil:
		return registry.Inspection{}, fmt.Errorf("s3: list versions of %q: %w", name, err)
	}
	return registry.Inspection{Exists: true, Empty: len(vers.Versions) == 0}, nil
}

// EnsureBuckety creates the backend bucket. Idempotent on
// BucketAlreadyOwnedByYou. BucketAlreadyExists is ambiguous: most
// non-AWS implementations surface it for a caller-owned bucket,
// but on AWS proper it can mean a global-namespace collision with
// a foreign account. HeadBucket disambiguates: accessible with
// this backend's credentials means ours, anything else is a hard
// error rather than a false Ready=True over a bucket the consumer
// cannot use.
//
// After create-or-noop, the object-store family's mutable
// parameters (versioning, lifecycle) are reconciled. A backend
// that does not implement a family capability fails SAFE per SPEC
// §Driver families: the bucket provisions, the knob is skipped
// (e.g. MinIO without erasure coding has no versioning; retention
// rules simply do not expire objects there). Capability-gated
// parameters (R2's jurisdiction) are immutable at the admission
// layer and are stamped via CreateBucketConfiguration below.
func (d *Driver) EnsureBuckety(ctx context.Context, req registry.EnsureRequest) error {
	input := &awss3.CreateBucketInput{Bucket: aws.String(req.Name)}
	if cfg := bucketCreationConfig(d.cfg.Implementation, d.cfg.Region, req.Parameters); cfg != nil {
		input.CreateBucketConfiguration = cfg
	}
	_, err := d.client.CreateBucket(ctx, input)
	switch {
	case err == nil || isAlreadyOwnedByYou(err):
		// ours
	case isAlreadyExists(err):
		if _, herr := d.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(req.Name)}); herr != nil {
			return fmt.Errorf("s3: bucket %q exists but is not accessible with this backend's credentials (name likely taken by another account): %w", req.Name, herr)
		}
	default:
		return fmt.Errorf("s3: create bucket %q: %w", req.Name, err)
	}
	if err := d.reconcileVersioning(ctx, req.Name, req.Parameters); err != nil {
		return err
	}
	return d.reconcileLifecycle(ctx, req.Name, req.Parameters)
}

// DeleteBuckety removes the backend bucket and its contents -
// PersistentVolume reclaimPolicy=Delete semantics per SPEC
// §Lifecycle and deletion. Idempotent on NoSuchBucket.
//
// Contents are emptied in bounded slices (one list page of up to
// 1000 keys per call, then one page of versions/delete-markers on
// versioned buckets); ErrDeletionInProgress tells the controller
// to requeue promptly. A bucket under sustained concurrent writes
// is chased rather than declared failed.
func (d *Driver) DeleteBuckety(ctx context.Context, name string) error {
	deleted, err := d.emptyBucketSlice(ctx, name)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("s3: empty bucket %q for deletion: %w", name, err)
	}
	if deleted > 0 {
		return &registry.ErrDeletionInProgress{Progress: fmt.Sprintf("bucket %q: deleted %d objects, checking for more", name, deleted)}
	}
	_, err = d.client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(name)})
	if err == nil || isNotFound(err) {
		return nil
	}
	if isBucketNotEmpty(err) {
		// Raced a writer (or an implementation that hides keys from
		// our listing); keep chasing instead of failing.
		return &registry.ErrDeletionInProgress{Progress: fmt.Sprintf("bucket %q: still not empty after emptying pass", name)}
	}
	return fmt.Errorf("s3: delete bucket %q: %w", name, err)
}

// emptyBucketSlice deletes up to one list page of current objects,
// or - once no current objects remain - up to one page of object
// versions and delete markers (versioned buckets refuse deletion
// while any version exists). Returns how many were deleted.
func (d *Driver) emptyBucketSlice(ctx context.Context, name string) (int, error) {
	list, err := d.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String(name)})
	if err != nil {
		return 0, err
	}
	var ids []s3types.ObjectIdentifier
	for _, o := range list.Contents {
		ids = append(ids, s3types.ObjectIdentifier{Key: o.Key})
	}
	if len(ids) == 0 {
		ids, err = d.listVersionsPage(ctx, name)
		if err != nil {
			return 0, err
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	out, err := d.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
		Bucket: aws.String(name),
		Delete: &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return 0, err
	}
	if len(out.Errors) > 0 {
		e := out.Errors[0]
		return 0, fmt.Errorf("delete of %q refused (%s): %s (and %d more)",
			aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message), len(out.Errors)-1)
	}
	return len(ids), nil
}

// listVersionsPage returns one page of version + delete-marker
// identifiers. Implementations without versioning support surface
// NotImplemented, which is treated as "no versions".
func (d *Driver) listVersionsPage(ctx context.Context, name string) ([]s3types.ObjectIdentifier, error) {
	vlist, err := d.client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{Bucket: aws.String(name)})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && api.ErrorCode() == "NotImplemented" {
			return nil, nil
		}
		return nil, err
	}
	var ids []s3types.ObjectIdentifier
	for _, v := range vlist.Versions {
		ids = append(ids, s3types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
	}
	for _, m := range vlist.DeleteMarkers {
		ids = append(ids, s3types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
	}
	return ids, nil
}

// GrantAccess returns the s3 Secret payload for a BucketyAccess.
// v1alpha1: identical credentials for all roles (the backend's
// root keys). Scoped=false signals the reconciler to surface
// ScopingNotImplemented for non-ReadWrite roles.
//
// Secret keys per SPEC §Secret output > s3 driver:
//
//	endpoint, bucket, region (if non-empty), accessKeyID, secretAccessKey
//
// `bucket` is the resource-type key per the SPEC's stable
// per-driver convention.
func (d *Driver) GrantAccess(_ context.Context, req registry.GrantRequest) (registry.GrantResult, error) {
	data := map[string][]byte{
		"endpoint":        []byte(d.cfg.Endpoint),
		"bucket":          []byte(req.BucketyName),
		"accessKeyID":     []byte(d.cfg.AccessKeyID),
		"secretAccessKey": []byte(d.cfg.SecretAccessKey),
	}
	if d.cfg.Region != "" {
		data["region"] = []byte(d.cfg.Region)
	}
	return registry.GrantResult{
		SecretData: data,
		Principal:  "s3-root",
		Scoped:     false,
	}, nil
}

// RevokeAccess is a no-op in v1alpha1 (nothing to remove since
// there is no per-access principal).
func (d *Driver) RevokeAccess(_ context.Context, _ string) error { return nil }

// ValidateParameters accepts the object-store family's common
// parameters (versioning, lifecycle - see pkg/drivers/objectstore
// and SPEC §Driver families) plus the capability-gated
// jurisdiction (r2 only).
func (d *Driver) ValidateParameters(params map[string]string) error {
	for k, v := range params {
		switch k {
		case "jurisdiction":
			if d.cfg.Implementation != "r2" {
				return fmt.Errorf("parameters.jurisdiction is r2-only; this backend's implementation is %q", d.cfg.Implementation)
			}
			if v != "eu" {
				return fmt.Errorf("parameters.jurisdiction: only %q is supported in v0.1, got %q", "eu", v)
			}
		case "versioning":
			if _, err := strconv.ParseBool(v); err != nil {
				return fmt.Errorf("parameters.versioning: want \"true\" or \"false\", got %q", v)
			}
		case "lifecycle":
			if _, err := translateLifecycle(v); err != nil {
				return fmt.Errorf("parameters.lifecycle: %w", err)
			}
		default:
			return fmt.Errorf("unknown parameter %q (s3 accepts: versioning, lifecycle, and jurisdiction when implementation=r2)", k)
		}
	}
	return nil
}

// ValidateUpdateParameters: jurisdiction is set-at-create and
// immutable; any change is a rejection.
func (d *Driver) ValidateUpdateParameters(oldParams, newParams map[string]string) error {
	if err := d.ValidateParameters(newParams); err != nil {
		return err
	}
	if o, n := oldParams["jurisdiction"], newParams["jurisdiction"]; o != n {
		return fmt.Errorf("parameters.jurisdiction is immutable post-create (current=%q, requested=%q)", o, n)
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
	return fmt.Errorf("s3 v0.1 accepts no BucketyAccess parameters; got: %s",
		strings.Join(keys, ", "))
}

// bucketNameRE covers S3's core charset rule: lowercase
// alphanumerics, dots and hyphens, starting and ending
// alphanumeric.
var bucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`)

// ipLikeRE matches dotted-quad shapes, which S3 forbids as bucket
// names.
var ipLikeRE = regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`)

// ValidateResourceName enforces the core S3 bucket naming rules on
// the resolved spec.name template result. Individual backends may
// impose more (AWS reserves prefixes like xn-- and suffixes like
// --ol-s3); those surface as EnsureBuckety errors rather than being
// duplicated here.
func (d *Driver) ValidateResourceName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name %q is %d characters; S3 requires 3-63", name, len(name))
	}
	if !bucketNameRE.MatchString(name) {
		return fmt.Errorf("bucket name %q must be lowercase alphanumerics, dots and hyphens, starting and ending alphanumeric", name)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("bucket name %q must not contain two adjacent periods", name)
	}
	if ipLikeRE.MatchString(name) {
		return fmt.Errorf("bucket name %q must not be formatted as an IP address", name)
	}
	return nil
}

// ---- internals ----

// bucketCreationConfig translates per-Buckety parameters into the
// implementation-specific CreateBucketConfiguration. Returns nil
// when no implementation-specific bucket-creation knobs apply.
//
// For AWS S3 the LocationConstraint follows the bucket's region
// unless the region is us-east-1 (which uses an empty
// LocationConstraint per AWS API rules).
//
// For R2 the jurisdiction parameter, when present, maps to the
// LocationConstraint slot per Cloudflare's documented S3-interop
// surface ("eu" places the bucket in the EU jurisdiction).
//
// For MinIO and VersityGW we deliberately omit
// CreateBucketConfiguration entirely; both reject unexpected
// LocationConstraint values on some versions.
func bucketCreationConfig(impl, region string, params map[string]string) *s3types.CreateBucketConfiguration {
	switch impl {
	case "r2":
		if j, ok := params["jurisdiction"]; ok && j != "" {
			return &s3types.CreateBucketConfiguration{
				LocationConstraint: s3types.BucketLocationConstraint(j),
			}
		}
	case "aws":
		if region != "" && region != "us-east-1" {
			return &s3types.CreateBucketConfiguration{
				LocationConstraint: s3types.BucketLocationConstraint(region),
			}
		}
	}
	return nil
}

// isAlreadyOwnedByYou reports the unambiguous caller-owned case.
func isAlreadyOwnedByYou(err error) bool {
	var ae *s3types.BucketAlreadyOwnedByYou
	if errors.As(err, &ae) {
		return true
	}
	var api smithy.APIError
	return errors.As(err, &api) && api.ErrorCode() == "BucketAlreadyOwnedByYou"
}

// isAlreadyExists reports the ambiguous name-taken case; the caller
// must disambiguate ownership (see EnsureBuckety's HeadBucket).
func isAlreadyExists(err error) bool {
	var ex *s3types.BucketAlreadyExists
	if errors.As(err, &ex) {
		return true
	}
	var api smithy.APIError
	return errors.As(err, &api) && api.ErrorCode() == "BucketAlreadyExists"
}

// isBucketNotEmpty reports the backend refusing DeleteBucket on
// remaining contents.
func isBucketNotEmpty(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && api.ErrorCode() == "BucketNotEmpty"
}

// isAccessDenied reports a permission refusal on an operation the
// caller treats as optional (InspectBuckety's emptiness listings
// degrade to non-empty rather than failing the reconcile).
func isAccessDenied(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && api.ErrorCode() == "AccessDenied"
}

// isNotFound reports whether err is the S3 service signalling a
// missing bucket. NoSuchBucket is the documented code; some
// implementations (VersityGW, MinIO older releases) return
// NotFound or a 404 status without a typed error, so we fall
// back to APIError code matching.
func isNotFound(err error) bool {
	var nsb *s3types.NoSuchBucket
	if errors.As(err, &nsb) {
		return true
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchBucket", "NotFound":
			return true
		}
	}
	return false
}

// ---- object-store family parameters (versioning, lifecycle) ----

// reconcileVersioning converges the versioning parameter when
// present. NotImplemented from the backend is the family's
// fail-safe: skip, do not fail the resource.
func (d *Driver) reconcileVersioning(ctx context.Context, name string, params map[string]string) error {
	v, ok := params["versioning"]
	if !ok {
		return nil
	}
	want, err := strconv.ParseBool(v)
	if err != nil {
		return fmt.Errorf("s3: parameters.versioning: %w", err)
	}
	got, err := d.client.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{Bucket: aws.String(name)})
	if err != nil {
		if isNotImplemented(err) {
			return nil
		}
		return fmt.Errorf("s3: get versioning for %q: %w", name, err)
	}
	current := got.Status == s3types.BucketVersioningStatusEnabled
	if current == want {
		return nil
	}
	if !want && got.Status == "" {
		// Never-versioned bucket: S3 rejects an explicit Suspended
		// on some implementations, and there is nothing to do.
		return nil
	}
	status := s3types.BucketVersioningStatusSuspended
	if want {
		status = s3types.BucketVersioningStatusEnabled
	}
	_, err = d.client.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket:                  aws.String(name),
		VersioningConfiguration: &s3types.VersioningConfiguration{Status: status},
	})
	if err != nil && !isNotImplemented(err) {
		return fmt.Errorf("s3: set versioning=%v on %q: %w", want, name, err)
	}
	return nil
}

// reconcileLifecycle converges the lifecycle parameter when
// present. {"rule": []} clears the configuration; NotImplemented
// is the family's fail-safe skip.
func (d *Driver) reconcileLifecycle(ctx context.Context, name string, params map[string]string) error {
	doc, ok := params["lifecycle"]
	if !ok {
		return nil
	}
	desired, err := translateLifecycle(doc)
	if err != nil {
		return fmt.Errorf("s3: parameters.lifecycle: %w", err)
	}
	current, err := d.client.GetBucketLifecycleConfiguration(ctx, &awss3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(name)})
	var currentRules []s3types.LifecycleRule
	switch {
	case err == nil:
		currentRules = current.Rules
	case isNoLifecycle(err):
		// no configuration yet
	case isNotImplemented(err):
		return nil
	default:
		return fmt.Errorf("s3: get lifecycle for %q: %w", name, err)
	}
	if lifecycleEqual(currentRules, desired) {
		return nil
	}
	if len(desired) == 0 {
		if _, err := d.client.DeleteBucketLifecycle(ctx, &awss3.DeleteBucketLifecycleInput{Bucket: aws.String(name)}); err != nil && !isNotImplemented(err) && !isNoLifecycle(err) {
			return fmt.Errorf("s3: clear lifecycle on %q: %w", name, err)
		}
		return nil
	}
	_, err = d.client.PutBucketLifecycleConfiguration(ctx, &awss3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(name),
		LifecycleConfiguration: &s3types.BucketLifecycleConfiguration{Rules: desired},
	})
	if err != nil && !isNotImplemented(err) {
		return fmt.Errorf("s3: set lifecycle on %q: %w", name, err)
	}
	return nil
}

// translateLifecycle maps the family's lifecycle shape to S3
// rules. S3 expresses less than GCS, so the s3 driver supports
// the PORTABLE SUBSET a cross-backend CR can rely on: action
// Delete or AbortIncompleteMultipartUpload, condition age
// (required) plus at most one matchesPrefix. Anything else is an
// admission error naming the subset - silently dropping
// conditions would delete more than the operator asked.
func translateLifecycle(doc string) ([]s3types.LifecycleRule, error) {
	rules, err := objectstore.ParseLifecycle(doc)
	if err != nil {
		return nil, err
	}
	out := make([]s3types.LifecycleRule, 0, len(rules))
	for i, r := range rules {
		c := r.Condition
		if c.Age == nil || *c.Age == 0 {
			return nil, fmt.Errorf("rule[%d]: the s3 driver's portable subset requires condition.age >= 1", i)
		}
		if len(c.MatchesPrefix) > 1 {
			return nil, fmt.Errorf("rule[%d]: S3 supports one prefix per rule; split into one rule per prefix", i)
		}
		if !c.CreatedBefore.IsZero() || !c.CustomTimeBefore.IsZero() || !c.NoncurrentTimeBefore.IsZero() ||
			c.DaysSinceCustomTime != 0 || c.DaysSinceNoncurrentTime != 0 || c.IsLive != nil ||
			len(c.MatchesStorageClass) != 0 || len(c.MatchesSuffix) != 0 || c.NumNewerVersions != 0 {
			return nil, fmt.Errorf("rule[%d]: only age + matchesPrefix conditions are in the s3 driver's portable subset", i)
		}
		prefix := ""
		if len(c.MatchesPrefix) == 1 {
			prefix = c.MatchesPrefix[0]
		}
		rule := s3types.LifecycleRule{
			ID:     aws.String(fmt.Sprintf("buckety-%d", i)),
			Status: s3types.ExpirationStatusEnabled,
			Filter: &s3types.LifecycleRuleFilter{Prefix: aws.String(prefix)},
		}
		switch r.Action.Type {
		case "Delete":
			rule.Expiration = &s3types.LifecycleExpiration{Days: aws.Int32(int32(*c.Age))}
		case "AbortIncompleteMultipartUpload":
			rule.AbortIncompleteMultipartUpload = &s3types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(int32(*c.Age))}
		default:
			return nil, fmt.Errorf("rule[%d]: action %q is not in the s3 driver's portable subset (Delete, AbortIncompleteMultipartUpload)", i, r.Action.Type)
		}
		out = append(out, rule)
	}
	return out, nil
}

// lifecycleEqual compares configurations on the normalized tuples
// the portable subset can express, ignoring rule IDs and ordering.
func lifecycleEqual(a, b []s3types.LifecycleRule) bool {
	if len(a) != len(b) {
		return false
	}
	key := func(r s3types.LifecycleRule) string {
		prefix := ""
		if r.Filter != nil && r.Filter.Prefix != nil {
			prefix = *r.Filter.Prefix
		} else if r.Prefix != nil {
			prefix = *r.Prefix
		}
		exp, abort := int32(0), int32(0)
		if r.Expiration != nil && r.Expiration.Days != nil {
			exp = *r.Expiration.Days
		}
		if r.AbortIncompleteMultipartUpload != nil && r.AbortIncompleteMultipartUpload.DaysAfterInitiation != nil {
			abort = *r.AbortIncompleteMultipartUpload.DaysAfterInitiation
		}
		return fmt.Sprintf("%s|%d|%d|%s", prefix, exp, abort, r.Status)
	}
	ka, kb := make([]string, len(a)), make([]string, len(b))
	for i := range a {
		ka[i] = key(a[i])
	}
	for i := range b {
		kb[i] = key(b[i])
	}
	sort.Strings(ka)
	sort.Strings(kb)
	for i := range ka {
		if ka[i] != kb[i] {
			return false
		}
	}
	return true
}

// isNotImplemented reports a backend refusing an operation it
// does not support - the family's fail-safe skip trigger. Error
// codes vary per implementation (versitygw answers HTTP 501 with
// its own VersioningNotConfigured code, for instance), so any
// 501 response counts alongside the AWS-documented codes.
func isNotImplemented(err error) bool {
	// Interface target: the SDK wraps the smithy http response
	// error in its own type, so a concrete *ResponseError match
	// would miss it.
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotImplemented {
		return true
	}
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "NotImplemented" || api.ErrorCode() == "NotSupported")
}

// isNoLifecycle reports the absent-configuration case of
// GetBucketLifecycleConfiguration.
func isNoLifecycle(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && api.ErrorCode() == "NoSuchLifecycleConfiguration"
}
