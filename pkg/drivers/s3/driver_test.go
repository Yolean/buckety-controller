package s3

import (
	"strings"
	"testing"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
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
