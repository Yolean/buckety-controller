package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// fakeS3 answers bucket-level HEAD and PUT on a path-style
// endpoint and records them as "METHOD /bucket".
type fakeS3 struct {
	mu       sync.Mutex
	calls    []string
	exists   bool
	headCode int    // non-zero overrides HEAD's answer
	putCode  string // non-empty: PUT answers 409 with this error code
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)
	switch r.Method {
	case http.MethodHead:
		switch {
		case f.headCode != 0:
			w.WriteHeader(f.headCode)
		case f.exists:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	case http.MethodPut:
		code := f.putCode
		if code == "" && f.exists {
			code = "BucketAlreadyOwnedByYou"
		}
		if code != "" {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>` + code + `</Code><Message>conflict</Message></Error>`))
			return
		}
		f.exists = true
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func (f *fakeS3) count(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, method+" ") {
			n++
		}
	}
	return n
}

// fakeDriver is a driver against f with SDK retries off, so a 5xx
// is one request and costs no backoff.
func fakeDriver(t *testing.T, f *fakeS3) *Driver {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	cfg := &Config{Implementation: "versitygw", Endpoint: srv.URL, ForcePathStyle: true}
	cl := awss3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""),
	}, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	return &Driver{cfg: cfg, client: cl}
}

// A re-check of an existing bucket - the steady state of every
// provisioned Buckety - reads only: no CreateBucket.
func TestEnsureExistingBucketMakesNoCreate(t *testing.T) {
	f := &fakeS3{exists: true}
	d := fakeDriver(t, f)
	for range 3 {
		if err := d.EnsureBuckety(context.Background(), registry.EnsureRequest{Name: "orders"}); err != nil {
			t.Fatal(err)
		}
	}
	if n := f.count(http.MethodPut); n != 0 {
		t.Errorf("existing bucket: %d PUT (CreateBucket) requests, want 0; calls %v", n, f.calls)
	}
	if n := f.count(http.MethodHead); n != 3 {
		t.Errorf("existing bucket: %d HEAD requests in 3 passes; calls %v", n, f.calls)
	}
}

func TestEnsureMissingBucketCreates(t *testing.T) {
	f := &fakeS3{}
	d := fakeDriver(t, f)
	if err := d.EnsureBuckety(context.Background(), registry.EnsureRequest{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 2 || f.calls[0] != "HEAD /orders" || f.calls[1] != "PUT /orders" {
		t.Errorf("missing bucket: calls %v, want [HEAD /orders PUT /orders]", f.calls)
	}
	if !f.exists {
		t.Error("bucket not created")
	}
}

// 403 on HeadBucket goes to CreateBucket. A name taken by another
// account answers BucketAlreadyExists there, and the second
// HeadBucket's 403 makes it the not-accessible error.
func TestEnsureForeignBucketFails(t *testing.T) {
	f := &fakeS3{exists: true, headCode: http.StatusForbidden, putCode: "BucketAlreadyExists"}
	d := fakeDriver(t, f)
	err := d.EnsureBuckety(context.Background(), registry.EnsureRequest{Name: "orders"})
	if err == nil || !strings.Contains(err.Error(), "not accessible with this backend's credentials") {
		t.Fatalf("want the not-accessible error, got %v", err)
	}
	want := []string{"HEAD /orders", "PUT /orders", "HEAD /orders"}
	if strings.Join(f.calls, ",") != strings.Join(want, ",") {
		t.Errorf("foreign bucket: calls %v, want %v", f.calls, want)
	}
}

// An implementation that answers HeadBucket 403 for a bucket these
// credentials own still passes: CreateBucket says
// BucketAlreadyOwnedByYou, as it did before HeadBucket came first.
func TestEnsureHeadForbiddenOwnedBucketPasses(t *testing.T) {
	f := &fakeS3{exists: true, headCode: http.StatusForbidden}
	d := fakeDriver(t, f)
	if err := d.EnsureBuckety(context.Background(), registry.EnsureRequest{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if n := f.count(http.MethodPut); n != 1 {
		t.Errorf("HEAD 403, owned: %d PUT requests, want 1; calls %v", n, f.calls)
	}
}

// A HeadBucket answer that says nothing about existence falls back
// to CreateBucket, which decides as it always did.
func TestEnsureHeadErrorFallsBackToCreate(t *testing.T) {
	f := &fakeS3{headCode: http.StatusInternalServerError}
	d := fakeDriver(t, f)
	if err := d.EnsureBuckety(context.Background(), registry.EnsureRequest{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if n := f.count(http.MethodPut); n != 1 {
		t.Errorf("HEAD 500: %d PUT requests, want 1; calls %v", n, f.calls)
	}
}
