package s3

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func r2Driver() *Driver   { return &Driver{cfg: &Config{Implementation: "r2"}} }
func gwDriver() *Driver   { return &Driver{cfg: &Config{Implementation: "versitygw"}} }
func bareDriver() *Driver { return &Driver{cfg: &Config{}} }

func TestValidateParametersJurisdictionGating(t *testing.T) {
	if err := r2Driver().ValidateParameters(map[string]string{"jurisdiction": "eu"}); err != nil {
		t.Fatalf("jurisdiction=eu on r2 rejected: %v", err)
	}
	if err := r2Driver().ValidateParameters(map[string]string{"jurisdiction": "us"}); err == nil {
		t.Fatal("jurisdiction=us accepted; only eu is supported in v0.1")
	}
	// Capability gating: any non-r2 implementation, including the
	// unmarked backend, rejects the parameter.
	for _, d := range []*Driver{gwDriver(), bareDriver()} {
		if err := d.ValidateParameters(map[string]string{"jurisdiction": "eu"}); err == nil {
			t.Fatalf("jurisdiction accepted on implementation %q", d.cfg.Implementation)
		}
	}
	if err := gwDriver().ValidateParameters(map[string]string{"whatever": "x"}); err == nil {
		t.Fatal("unknown parameter accepted")
	}
	if err := gwDriver().ValidateParameters(nil); err != nil {
		t.Fatalf("empty parameters rejected: %v", err)
	}
}

func TestValidateUpdateParametersJurisdictionImmutable(t *testing.T) {
	d := r2Driver()
	if err := d.ValidateUpdateParameters(
		map[string]string{"jurisdiction": "eu"},
		map[string]string{"jurisdiction": "eu"}); err != nil {
		t.Fatalf("unchanged jurisdiction rejected: %v", err)
	}
	if err := d.ValidateUpdateParameters(map[string]string{"jurisdiction": "eu"}, nil); err == nil {
		t.Fatal("jurisdiction removal accepted; set-at-create means immutable both ways")
	}
	if err := d.ValidateUpdateParameters(nil, map[string]string{"jurisdiction": "eu"}); err == nil {
		t.Fatal("late jurisdiction addition accepted")
	}
}

func TestValidateResourceName(t *testing.T) {
	d := bareDriver()
	cases := []struct {
		name    string
		bucket  string
		wantErr string
	}{
		{"plain", "orders", ""},
		{"hyphens and dots", "tenant1-orders.v3", ""},
		{"too short", "ab", "3-63"},
		{"too long", strings.Repeat("a", 64), "3-63"},
		{"max ok", strings.Repeat("a", 63), ""},
		{"uppercase", "Orders", "lowercase"},
		{"underscore", "or_ders", "lowercase"},
		{"leading hyphen", "-orders", "lowercase"},
		{"trailing dot", "orders.", "lowercase"},
		{"adjacent periods", "or..ders", "adjacent periods"},
		{"ip-like", "192.168.1.1", "IP address"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := d.ValidateResourceName(c.bucket)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("error %v does not contain %q", err, c.wantErr)
			}
		})
	}
}

func TestBucketCreationConfig(t *testing.T) {
	// MinIO and VersityGW must not receive a CreateBucketConfiguration
	// at all; some versions reject unexpected LocationConstraint.
	for _, impl := range []string{"minio", "versitygw", ""} {
		if cfg := bucketCreationConfig(impl, "us-east-1", nil); cfg != nil {
			t.Fatalf("impl %q: expected nil config, got %+v", impl, cfg)
		}
	}
	// R2 maps jurisdiction to LocationConstraint.
	cfg := bucketCreationConfig("r2", "auto", map[string]string{"jurisdiction": "eu"})
	if cfg == nil || cfg.LocationConstraint != s3types.BucketLocationConstraint("eu") {
		t.Fatalf("r2 jurisdiction config: %+v", cfg)
	}
	if cfg := bucketCreationConfig("r2", "auto", nil); cfg != nil {
		t.Fatalf("r2 without jurisdiction should send no config, got %+v", cfg)
	}
	// AWS: LocationConstraint follows region except us-east-1.
	if cfg := bucketCreationConfig("aws", "eu-north-1", nil); cfg == nil ||
		cfg.LocationConstraint != s3types.BucketLocationConstraint("eu-north-1") {
		t.Fatalf("aws eu-north-1 config: %+v", cfg)
	}
	if cfg := bucketCreationConfig("aws", "us-east-1", nil); cfg != nil {
		t.Fatalf("aws us-east-1 must omit LocationConstraint, got %+v", cfg)
	}
}

func TestErrorClassification(t *testing.T) {
	owned := &s3types.BucketAlreadyOwnedByYou{}
	exists := &s3types.BucketAlreadyExists{}
	nsb := &s3types.NoSuchBucket{}
	genericOwned := &smithy.GenericAPIError{Code: "BucketAlreadyOwnedByYou"}
	genericExists := &smithy.GenericAPIError{Code: "BucketAlreadyExists"}
	genericNotFound := &smithy.GenericAPIError{Code: "NotFound"}
	other := &smithy.GenericAPIError{Code: "AccessDenied"}

	if !isAlreadyOwnedByYou(owned) || !isAlreadyOwnedByYou(genericOwned) {
		t.Fatal("BucketAlreadyOwnedByYou not classified")
	}
	if isAlreadyOwnedByYou(exists) || isAlreadyOwnedByYou(other) {
		t.Fatal("isAlreadyOwnedByYou over-matches")
	}
	if !isAlreadyExists(exists) || !isAlreadyExists(genericExists) {
		t.Fatal("BucketAlreadyExists not classified")
	}
	if isAlreadyExists(owned) || isAlreadyExists(other) {
		t.Fatal("isAlreadyExists over-matches")
	}
	if !isNotFound(nsb) || !isNotFound(genericNotFound) {
		t.Fatal("NoSuchBucket/NotFound not classified")
	}
	if isNotFound(other) {
		t.Fatal("isNotFound over-matches")
	}
}

// The object-store family parameters on the s3 driver: portable
// subset translation and its rejections (SPEC §Driver families).
func TestTranslateLifecyclePortableSubset(t *testing.T) {
	rules, err := translateLifecycle(`{"rule": [
	  {"action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": ["board-prints/"]}},
	  {"action": {"type": "AbortIncompleteMultipartUpload"}, "condition": {"age": 2}}
	]}`)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("rules: %d", len(rules))
	}
	if *rules[0].Expiration.Days != 7 || *rules[0].Filter.Prefix != "board-prints/" {
		t.Errorf("rule[0]: %+v", rules[0])
	}
	if *rules[1].AbortIncompleteMultipartUpload.DaysAfterInitiation != 2 || *rules[1].Filter.Prefix != "" {
		t.Errorf("rule[1]: %+v", rules[1])
	}

	for name, doc := range map[string]string{
		"age required":  `{"rule": [{"action": {"type": "Delete"}, "condition": {"matchesPrefix": ["x/"]}}]}`,
		"multi prefix":  `{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 1, "matchesPrefix": ["a/", "b/"]}}]}`,
		"gcs-only cond": `{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 1, "isLive": false}}]}`,
		"storage class": `{"rule": [{"action": {"type": "SetStorageClass", "storageClass": "COLDLINE"}, "condition": {"age": 1}}]}`,
	} {
		if _, err := translateLifecycle(doc); err == nil {
			t.Errorf("%s: accepted outside portable subset", name)
		}
	}
}

func TestValidateParametersFamily(t *testing.T) {
	d := &Driver{cfg: &Config{}}
	if err := d.ValidateParameters(map[string]string{
		"versioning": "true",
		"lifecycle":  `{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": [".staging/"]}}]}`,
	}); err != nil {
		t.Fatalf("family parameters rejected: %v", err)
	}
	if err := d.ValidateParameters(map[string]string{"versioning": "maybe"}); err == nil {
		t.Error("bad versioning bool accepted")
	}
}

func TestLifecycleEqual(t *testing.T) {
	a, _ := translateLifecycle(`{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": ["p/"]}}]}`)
	b, _ := translateLifecycle(`{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": ["p/"]}}]}`)
	c, _ := translateLifecycle(`{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 8, "matchesPrefix": ["p/"]}}]}`)
	if !lifecycleEqual(a, b) {
		t.Error("identical configs unequal")
	}
	if lifecycleEqual(a, c) {
		t.Error("different days equal")
	}
	if lifecycleEqual(a, nil) {
		t.Error("nil equal to non-empty")
	}
}

// versitygw answers HTTP 501 with its own error code
// (VersioningNotConfigured) rather than NotImplemented; the
// fail-safe keys on the status code (seen live on run 29507386876).
func TestIsNotImplemented(t *testing.T) {
	versitygw501 := fmt.Errorf("operation error S3: GetBucketVersioning: %w",
		&awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 501}},
			Err:      &smithy.GenericAPIError{Code: "VersioningNotConfigured", Message: "versioning has not been configured for the gateway"},
		}})
	if !isNotImplemented(versitygw501) {
		t.Error("501 VersioningNotConfigured not treated as fail-safe skip")
	}
	if !isNotImplemented(&smithy.GenericAPIError{Code: "NotImplemented"}) {
		t.Error("NotImplemented code not matched")
	}
	denied := fmt.Errorf("wrap: %w", &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 403}},
		Err:      &smithy.GenericAPIError{Code: "AccessDenied"},
	}})
	if isNotImplemented(denied) {
		t.Error("403 AccessDenied wrongly treated as not-implemented")
	}
}
