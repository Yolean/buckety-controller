package s3

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// deleteServer answers the calls of one DeleteBuckety pass on
// bucket "b": a listing with `objects` current keys, no versions,
// and DeleteBucket refused with BucketNotEmpty.
func deleteServer(t *testing.T, objects int) *Driver {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == http.MethodGet && q.Has("versions"):
			_, _ = w.Write([]byte(`<ListVersionsResult><Name>b</Name></ListVersionsResult>`))
		case r.Method == http.MethodGet:
			body := `<ListBucketResult><Name>b</Name>`
			for range objects {
				body += `<Contents><Key>k</Key></Contents>`
			}
			_, _ = w.Write([]byte(body + `</ListBucketResult>`))
		case r.Method == http.MethodPost && q.Has("delete"):
			_, _ = w.Write([]byte(`<DeleteResult></DeleteResult>`))
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`<Error><Code>BucketNotEmpty</Code><Message>not empty</Message></Error>`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotImplemented)
		}
	}))
	t.Cleanup(srv.Close)
	raw, _ := json.Marshal(map[string]any{
		"endpoint": srv.URL, "accessKeyID": "a", "secretAccessKey": "s", "forcePathStyle": true,
	})
	d, err := factory(raw)
	if err != nil {
		t.Fatal(err)
	}
	return d.(*Driver)
}

// A pass that deleted objects is progress; a pass that found
// nothing to delete while the bucket still refuses deletion is
// stalled, so the controller backs off instead of re-listing
// every 2 s.
func TestDeleteBucketyStalledOnlyWithoutProgress(t *testing.T) {
	ctx := context.Background()

	err := deleteServer(t, 3).DeleteBuckety(ctx, registry.DeleteRequest{Name: "b"})
	var ip *registry.ErrDeletionInProgress
	if !errors.As(err, &ip) || ip.Stalled {
		t.Errorf("pass that deleted objects: want in progress, not stalled; got %v (%+v)", err, ip)
	}

	err = deleteServer(t, 0).DeleteBuckety(ctx, registry.DeleteRequest{Name: "b"})
	if !registry.IsDeletionStalled(err) {
		t.Errorf("pass that deleted nothing on a non-empty bucket: want stalled, got %v", err)
	}
}
