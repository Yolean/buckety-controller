package gcs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	iam "google.golang.org/api/iam/v1"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// fakeGCP fakes the two control-plane surfaces the serviceAccounts
// feature touches: iam.googleapis.com (SA + key lifecycle) and the
// storage JSON API's bucket IAM policy endpoints. fake-gcs-server
// implements neither, which is why this feature is unit-tested
// here and documented as not-e2e-gated (SPEC §E2E harness).
type fakeGCP struct {
	mu             sync.Mutex
	project        string
	sas            map[string]*iam.ServiceAccount               // email -> SA
	keys           map[string]map[string]*iam.ServiceAccountKey // email -> key id -> key
	keySeq         int
	policies       map[string][]*policyBinding // bucket -> bindings
	setPolicyCalls int
	// tombstoned emails 404 on Get but still 409 on Create,
	// GCP's ~30-day soft-deletion name reservation.
	tombstoned map[string]bool
}

type policyBinding struct {
	Role    string   `json:"role"`
	Members []string `json:"members"`
}

func newFakeGCP(t *testing.T, project string) (*fakeGCP, *httptest.Server) {
	t.Helper()
	f := &fakeGCP{
		project:    project,
		sas:        map[string]*iam.ServiceAccount{},
		keys:       map[string]map[string]*iam.ServiceAccountKey{},
		policies:   map[string][]*policyBinding{},
		tombstoned: map[string]bool{},
	}
	mux := http.NewServeMux()

	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	writeErr := func(w http.ResponseWriter, code int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": code, "message": msg},
		})
	}

	mux.HandleFunc("POST /v1/projects/{proj}/serviceAccounts", func(w http.ResponseWriter, r *http.Request) {
		var req iam.CreateServiceAccountRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		email := req.AccountId + "@" + r.PathValue("proj") + ".iam.gserviceaccount.com"
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, exists := f.sas[email]; exists || f.tombstoned[email] {
			writeErr(w, 409, "already exists")
			return
		}
		sa := &iam.ServiceAccount{
			Name:  "projects/" + r.PathValue("proj") + "/serviceAccounts/" + email,
			Email: email,
		}
		if req.ServiceAccount != nil {
			sa.DisplayName = req.ServiceAccount.DisplayName
			sa.Description = req.ServiceAccount.Description
		}
		f.sas[email] = sa
		writeJSON(w, sa)
	})
	mux.HandleFunc("GET /v1/projects/{proj}/serviceAccounts/{email}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		sa, ok := f.sas[r.PathValue("email")]
		if !ok {
			writeErr(w, 404, "no such service account")
			return
		}
		writeJSON(w, sa)
	})
	mux.HandleFunc("DELETE /v1/projects/{proj}/serviceAccounts/{email}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		email := r.PathValue("email")
		if _, ok := f.sas[email]; !ok {
			writeErr(w, 404, "no such service account")
			return
		}
		delete(f.sas, email)
		delete(f.keys, email)
		writeJSON(w, map[string]any{})
	})
	mux.HandleFunc("GET /v1/projects/{proj}/serviceAccounts/{email}/keys", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		email := r.PathValue("email")
		if _, ok := f.sas[email]; !ok {
			writeErr(w, 404, "no such service account")
			return
		}
		resp := iam.ListServiceAccountKeysResponse{}
		for _, k := range f.keys[email] {
			resp.Keys = append(resp.Keys, k)
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("POST /v1/projects/{proj}/serviceAccounts/{email}/keys", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		email := r.PathValue("email")
		if _, ok := f.sas[email]; !ok {
			writeErr(w, 404, "no such service account")
			return
		}
		f.keySeq++
		id := fmt.Sprintf("key%d", f.keySeq)
		keyfile, _ := json.Marshal(map[string]string{
			"type":           "service_account",
			"project_id":     r.PathValue("proj"),
			"private_key_id": id,
			"private_key":    "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
			"client_email":   email,
		})
		key := &iam.ServiceAccountKey{
			Name:            "projects/" + r.PathValue("proj") + "/serviceAccounts/" + email + "/keys/" + id,
			PrivateKeyData:  base64.StdEncoding.EncodeToString(keyfile),
			ValidAfterTime:  time.Now().UTC().Format(time.RFC3339),
			ValidBeforeTime: "9999-12-31T23:59:59Z",
			KeyType:         "USER_MANAGED",
		}
		if f.keys[email] == nil {
			f.keys[email] = map[string]*iam.ServiceAccountKey{}
		}
		f.keys[email][id] = key
		writeJSON(w, key)
	})
	mux.HandleFunc("DELETE /v1/projects/{proj}/serviceAccounts/{email}/keys/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		email, id := r.PathValue("email"), r.PathValue("id")
		if _, ok := f.keys[email][id]; !ok {
			writeErr(w, 404, "no such key")
			return
		}
		delete(f.keys[email], id)
		writeJSON(w, map[string]any{})
	})

	mux.HandleFunc("GET /storage/v1/b/{bucket}/iam", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		writeJSON(w, map[string]any{
			"kind":       "storage#policy",
			"resourceId": "projects/_/buckets/" + r.PathValue("bucket"),
			"bindings":   f.policies[r.PathValue("bucket")],
			"etag":       "CAE=",
		})
	})
	mux.HandleFunc("PUT /storage/v1/b/{bucket}/iam", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Bindings []*policyBinding `json:"bindings"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.policies[r.PathValue("bucket")] = body.Bindings
		f.setPolicyCalls++
		writeJSON(w, map[string]any{
			"kind":     "storage#policy",
			"bindings": body.Bindings,
			"etag":     "CAI=",
		})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return f, srv
}

// saDriver builds a Driver through the factory, both clients
// pointed at the fake. The endpoint override keeps GrantAccess off
// the bucket-attrs lookup, matching TestGrantAccessPayload.
func saDriver(t *testing.T, srv *httptest.Server) *Driver {
	t.Helper()
	t.Setenv("STORAGE_EMULATOR_HOST", srv.URL)
	raw := fmt.Sprintf(`{
		"project": "bucket-proj", "endpoint": "fake-gcs:8000", "region": "r1",
		"accessKeyID": "id", "secretAccessKey": "sec",
		"serviceAccounts": {"project": "id-proj", "endpoint": %q, "insecure": true}
	}`, srv.URL)
	drv, err := factory(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	return drv.(*Driver)
}

func (f *fakeGCP) hasBinding(bucket, role, member string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range f.policies[bucket] {
		if b.Role != role {
			continue
		}
		for _, m := range b.Members {
			if m == member {
				return true
			}
		}
	}
	return false
}

func (f *fakeGCP) keyIDs(email string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for id := range f.keys[email] {
		out = append(out, id)
	}
	return out
}

func TestServiceAccountConfigValidation(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:1")
	if _, err := factory(json.RawMessage(`{"project": "p", "accessKeyID": "a", "secretAccessKey": "s", "serviceAccounts": {}}`)); err == nil {
		t.Error("serviceAccounts without project accepted")
	}
	// insecure is only meaningful together with endpoint; alone it
	// is a config error, not a silent no-op.
	if _, err := factory(json.RawMessage(`{"project": "p", "accessKeyID": "a", "secretAccessKey": "s", "serviceAccounts": {"project": "id-proj", "insecure": true}}`)); err == nil {
		t.Error("serviceAccounts.insecure without endpoint accepted")
	}
	drv, err := factory(json.RawMessage(`{"project": "p", "accessKeyID": "a", "secretAccessKey": "s", "serviceAccounts": {"project": "id-proj", "endpoint": "http://127.0.0.1:1", "insecure": true}}`))
	if err != nil {
		t.Fatalf("valid serviceAccounts config rejected: %v", err)
	}
	if got := registry.TemplatedParameters(drv); len(got) != 1 || got[0] != "serviceAccount" {
		t.Errorf("TemplatedParameters with feature on: %v", got)
	}

	// Feature off: parameter rejected, nothing declared templated.
	off, err := factory(json.RawMessage(`{"project": "p", "accessKeyID": "a", "secretAccessKey": "s"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := registry.TemplatedParameters(off); got != nil {
		t.Errorf("TemplatedParameters with feature off: %v", got)
	}
	if err := off.ValidateParameters(map[string]string{"serviceAccount": "orders-t1"}); err == nil || !strings.Contains(err.Error(), "serviceAccounts") {
		t.Errorf("parameter on non-enabled backend: %v", err)
	}
}

func TestValidateServiceAccountParameter(t *testing.T) {
	d := &Driver{cfg: &Config{Project: "p", ServiceAccounts: &ServiceAccountsConfig{Project: "id-proj"}}}

	valid := []string{"orders", "orders-tenant1", "a-b-c-1", "a" + strings.Repeat("b", 29)}
	for _, v := range valid {
		if err := d.ValidateParameters(map[string]string{"serviceAccount": v}); err != nil {
			t.Errorf("valid %q rejected: %v", v, err)
		}
	}
	invalid := []string{
		"short",                 // 5 chars, below GCP's 6 minimum
		strings.Repeat("a", 31), // above 30
		"1leading-digit",        // must start with a letter
		"trailing-dash-",        // must end alphanumeric
		"Upper-case",            // charset
		"under_score",           // underscores illegal in SA IDs
		"${name}-${namespace}",  // unresolved template must not reach the driver
	}
	for _, v := range invalid {
		if err := d.ValidateParameters(map[string]string{"serviceAccount": v}); err == nil {
			t.Errorf("invalid %q accepted", v)
		}
	}
}

func TestServiceAccountImmutable(t *testing.T) {
	d := &Driver{cfg: &Config{Project: "p", ServiceAccounts: &ServiceAccountsConfig{Project: "id-proj"}}}
	old := map[string]string{"serviceAccount": "orders-t1"}

	if err := d.ValidateUpdateParameters(old, map[string]string{"serviceAccount": "orders-t1", "versioning": "true"}); err != nil {
		t.Fatalf("unchanged serviceAccount rejected: %v", err)
	}
	if err := d.ValidateUpdateParameters(old, map[string]string{"serviceAccount": "other-name"}); err == nil {
		t.Error("serviceAccount change accepted")
	}
	if err := d.ValidateUpdateParameters(old, map[string]string{}); err == nil {
		t.Error("serviceAccount removal accepted")
	}
	if err := d.ValidateUpdateParameters(map[string]string{}, old); err == nil {
		t.Error("serviceAccount addition post-create accepted")
	}
}

func TestEnsureServiceAccount(t *testing.T) {
	f, srv := newFakeGCP(t, "id-proj")
	d := saDriver(t, srv)
	ctx := context.Background()
	email := "orders-t1@id-proj.iam.gserviceaccount.com"
	member := "serviceAccount:" + email

	if err := d.ensureServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	sa := f.sas[email]
	if sa == nil {
		t.Fatal("service account not created")
	}
	var m saMarker
	if err := json.Unmarshal([]byte(sa.Description), &m); err != nil || m.ManagedBy != "buckety" || m.Bucket != "bucket-x" {
		t.Errorf("ownership marker: %q", sa.Description)
	}
	if !f.hasBinding("bucket-x", saBucketRole, member) {
		t.Errorf("bucket binding missing: %+v", f.policies["bucket-x"])
	}
	if f.setPolicyCalls != 1 {
		t.Errorf("setPolicy calls after create: %d", f.setPolicyCalls)
	}

	// Steady state: no policy write.
	if err := d.ensureServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	if f.setPolicyCalls != 1 {
		t.Errorf("steady-state ensure wrote the policy: %d calls", f.setPolicyCalls)
	}

	// Out-of-band binding removal heals on the next pass.
	f.mu.Lock()
	f.policies["bucket-x"] = nil
	f.mu.Unlock()
	if err := d.ensureServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Fatalf("heal ensure: %v", err)
	}
	if !f.hasBinding("bucket-x", saBucketRole, member) {
		t.Error("binding not re-asserted after out-of-band removal")
	}

	// Foreign SA (no buckety marker): refused. This is the
	// cross-tenant hijack guard - a key for someone else's SA
	// would carry their bucket grants.
	f.mu.Lock()
	f.sas["victim@id-proj.iam.gserviceaccount.com"] = &iam.ServiceAccount{
		Email: "victim@id-proj.iam.gserviceaccount.com", Description: "hand-made",
	}
	f.mu.Unlock()
	if err := d.ensureServiceAccount(ctx, "victim", "bucket-x"); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Errorf("foreign SA: %v", err)
	}

	// Marker for a DIFFERENT bucket: also refused (two Bucketys
	// racing for one SA name).
	if err := d.ensureServiceAccount(ctx, "orders-t1", "bucket-y"); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Errorf("other bucket's SA: %v", err)
	}
}

func TestGrantAccessKeyLifecycle(t *testing.T) {
	f, srv := newFakeGCP(t, "id-proj")
	d := saDriver(t, srv)
	ctx := context.Background()
	email := "orders-t1@id-proj.iam.gserviceaccount.com"
	if err := d.ensureServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Fatal(err)
	}
	req := registry.GrantRequest{
		BucketyName:       "bucket-x",
		Role:              "ReadWrite",
		BucketyParameters: map[string]string{"serviceAccount": "orders-t1"},
	}

	// First grant mints.
	res1, err := d.GrantAccess(ctx, req)
	if err != nil {
		t.Fatalf("first grant: %v", err)
	}
	var keyfile struct {
		ClientEmail  string `json:"client_email"`
		PrivateKeyID string `json:"private_key_id"`
		Type         string `json:"type"`
	}
	if err := json.Unmarshal(res1.SecretData["serviceAccountKey"], &keyfile); err != nil {
		t.Fatalf("serviceAccountKey not JSON: %v", err)
	}
	if keyfile.ClientEmail != email || keyfile.Type != "service_account" {
		t.Errorf("key file: %+v", keyfile)
	}
	if string(res1.SecretData["serviceAccountEmail"]) != email {
		t.Errorf("serviceAccountEmail: %q", res1.SecretData["serviceAccountEmail"])
	}
	keyID := string(res1.SecretData["serviceAccountKeyId"])
	if keyID != keyfile.PrivateKeyID {
		t.Errorf("serviceAccountKeyId %q != key file id %q", keyID, keyfile.PrivateKeyID)
	}
	wantPrincipal := "projects/id-proj/serviceAccounts/" + email + "/keys/" + keyID
	if res1.Principal != wantPrincipal {
		t.Errorf("principal %q, want %q", res1.Principal, wantPrincipal)
	}
	if res1.Scoped {
		t.Error("bucket-scoped SA must still report Scoped=false (role scoping is not implemented)")
	}
	if !res1.Revocable {
		t.Error("minted key principal must report Revocable=true")
	}
	// HMAC pair still present (additive keys per SPEC).
	if string(res1.SecretData["accessKeyID"]) != "id" || string(res1.SecretData["bucket"]) != "bucket-x" {
		t.Errorf("base payload regressed: %v", keysOf(res1.SecretData))
	}
	if n := len(f.keyIDs(email)); n != 1 {
		t.Fatalf("keys after first grant: %d", n)
	}

	// Steady state: the existing Secret's key verifies against
	// keys.list and comes back byte-identical - no new key.
	req.ExistingSecretData = res1.SecretData
	res2, err := d.GrantAccess(ctx, req)
	if err != nil {
		t.Fatalf("steady-state grant: %v", err)
	}
	if string(res2.SecretData["serviceAccountKey"]) != string(res1.SecretData["serviceAccountKey"]) || res2.Principal != res1.Principal {
		t.Error("steady-state grant re-minted")
	}
	if n := len(f.keyIDs(email)); n != 1 {
		t.Fatalf("keys after steady-state grant: %d", n)
	}

	// Out-of-band revocation (also the manual rotation runbook):
	// next grant self-heals with a fresh key.
	f.mu.Lock()
	delete(f.keys[email], keyID)
	f.mu.Unlock()
	res3, err := d.GrantAccess(ctx, req)
	if err != nil {
		t.Fatalf("post-revocation grant: %v", err)
	}
	if string(res3.SecretData["serviceAccountKeyId"]) == keyID {
		t.Error("revoked key reused")
	}
	if n := len(f.keyIDs(email)); n != 1 {
		t.Fatalf("keys after re-mint: %d", n)
	}

	// Expired key (org-policy iam.serviceAccountKeyExpiryHours):
	// treated as invalid, re-minted.
	req.ExistingSecretData = res3.SecretData
	f.mu.Lock()
	f.keys[email][string(res3.SecretData["serviceAccountKeyId"])].ValidBeforeTime = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	f.mu.Unlock()
	res4, err := d.GrantAccess(ctx, req)
	if err != nil {
		t.Fatalf("post-expiry grant: %v", err)
	}
	if string(res4.SecretData["serviceAccountKeyId"]) == string(res3.SecretData["serviceAccountKeyId"]) {
		t.Error("expired key reused")
	}

	// A key file for some OTHER identity in the Secret (email
	// mismatch) is never trusted.
	req.ExistingSecretData = map[string][]byte{
		"serviceAccountKey": []byte(`{"type":"service_account","client_email":"other@id-proj.iam.gserviceaccount.com","private_key_id":"keyX"}`),
	}
	res5, err := d.GrantAccess(ctx, req)
	if err != nil {
		t.Fatalf("mismatched-email grant: %v", err)
	}
	var got struct {
		ClientEmail string `json:"client_email"`
	}
	_ = json.Unmarshal(res5.SecretData["serviceAccountKey"], &got)
	if got.ClientEmail != email {
		t.Errorf("mismatched-email grant kept foreign key: %q", got.ClientEmail)
	}
}

func TestRevokeAccessKey(t *testing.T) {
	f, srv := newFakeGCP(t, "id-proj")
	d := saDriver(t, srv)
	ctx := context.Background()
	email := "orders-t1@id-proj.iam.gserviceaccount.com"
	if err := d.ensureServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Fatal(err)
	}
	res, err := d.GrantAccess(ctx, registry.GrantRequest{
		BucketyName:       "bucket-x",
		BucketyParameters: map[string]string{"serviceAccount": "orders-t1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := d.RevokeAccess(ctx, res.Principal); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if n := len(f.keyIDs(email)); n != 0 {
		t.Errorf("keys after revoke: %d", n)
	}
	// Idempotent, and also covers the key having gone with its SA.
	if err := d.RevokeAccess(ctx, res.Principal); err != nil {
		t.Errorf("second revoke: %v", err)
	}
	// The static principal and empty principal are no-ops.
	if err := d.RevokeAccess(ctx, "gcs-static"); err != nil {
		t.Errorf("gcs-static revoke: %v", err)
	}
	if err := d.RevokeAccess(ctx, ""); err != nil {
		t.Errorf("empty principal revoke: %v", err)
	}
	// A key principal on a feature-disabled backend is an error,
	// not a silent leak: deletion blocks until the operator
	// restores the serviceAccounts config.
	off := &Driver{cfg: &Config{Project: "p"}}
	if err := off.RevokeAccess(ctx, res.Principal); err == nil {
		t.Error("key revoke with serviceAccounts disabled succeeded silently")
	}
}

func TestDeleteBucketyRemovesServiceAccount(t *testing.T) {
	f, srv := newFakeGCP(t, "id-proj")
	d := saDriver(t, srv)
	ctx := context.Background()
	email := "orders-t1@id-proj.iam.gserviceaccount.com"
	if err := d.ensureServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Fatal(err)
	}

	// Owned SA goes with the bucket.
	if err := d.deleteServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if f.sas[email] != nil {
		t.Error("service account not deleted")
	}
	// Idempotent on absent.
	if err := d.deleteServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Errorf("second delete: %v", err)
	}

	// Foreign SA is left alone WITHOUT blocking the finalizer.
	f.mu.Lock()
	f.sas["victim@id-proj.iam.gserviceaccount.com"] = &iam.ServiceAccount{
		Email: "victim@id-proj.iam.gserviceaccount.com", Description: "hand-made",
	}
	f.mu.Unlock()
	if err := d.deleteServiceAccount(ctx, "victim", "bucket-x"); err != nil {
		t.Errorf("foreign SA delete attempt errored: %v", err)
	}
	if f.sas["victim@id-proj.iam.gserviceaccount.com"] == nil {
		t.Error("foreign SA deleted")
	}
}

// A soft-deleted SA holds its name for ~30 days: Create conflicts
// while Get sees nothing. That state never converges on retries,
// so the driver must name the tombstone instead of wrapping the
// misleading create/get error (checkit review finding 2).
func TestEnsureServiceAccountTombstone(t *testing.T) {
	f, srv := newFakeGCP(t, "id-proj")
	d := saDriver(t, srv)
	f.mu.Lock()
	f.tombstoned["orders-t1@id-proj.iam.gserviceaccount.com"] = true
	f.mu.Unlock()
	err := d.ensureServiceAccount(context.Background(), "orders-t1", "bucket-x")
	if err == nil {
		t.Fatal("tombstoned SA creation succeeded")
	}
	if !strings.Contains(err.Error(), "30 days") || !strings.Contains(err.Error(), "reserved") {
		t.Errorf("tombstone diagnostic missing: %v", err)
	}
}

// serviceAccount works as a CR parameter, as a backend default,
// and - the piece this test pins - "" is the explicit per-CR
// opt-out of a backend default. Immutability covers the opt-out
// transition too.
func TestServiceAccountEmptyOptOut(t *testing.T) {
	d := &Driver{cfg: &Config{Project: "p", ServiceAccounts: &ServiceAccountsConfig{Project: "id-proj"}}}
	if err := d.ValidateParameters(map[string]string{"serviceAccount": ""}); err != nil {
		t.Errorf("explicit opt-out rejected: %v", err)
	}
	// Also valid on a backend without the feature: it asks for
	// nothing.
	off := &Driver{cfg: &Config{Project: "p"}}
	if err := off.ValidateParameters(map[string]string{"serviceAccount": ""}); err != nil {
		t.Errorf("opt-out on non-enabled backend rejected: %v", err)
	}
	// Effective transition default->"" is still a post-create
	// mutation and gets rejected like any other change.
	if err := d.ValidateUpdateParameters(
		map[string]string{"serviceAccount": "orders-t1"},
		map[string]string{"serviceAccount": ""}); err == nil {
		t.Error("opt-out transition accepted post-create")
	}
	// GrantAccess treats "" exactly like absent: HMAC-only Secret.
	// Endpoint override skips the bucket-attrs lookup, and the ""
	// opt-out must never reach the (nil here) IAM client.
	d = &Driver{cfg: &Config{
		Project: "p", Endpoint: "fake:8000",
		AccessKeyID: "id", SecretAccessKey: "sec",
		ServiceAccounts: &ServiceAccountsConfig{Project: "id-proj"},
	}}
	res, err := d.GrantAccess(t.Context(), registry.GrantRequest{
		BucketyName:       "bucket-x",
		BucketyParameters: map[string]string{"serviceAccount": ""},
	})
	if err != nil {
		t.Fatalf("grant with opt-out: %v", err)
	}
	if _, ok := res.SecretData["serviceAccountKey"]; ok {
		t.Error("opt-out minted a key")
	}
	if res.Principal != "gcs-static" {
		t.Errorf("principal: %q", res.Principal)
	}
	if res.Revocable {
		t.Error("static principal must report Revocable=false")
	}
}

// hmac="false" omits the backend-wide static pair from the
// Secret; driver default stays "true" (incumbent contract).
func TestHMACOptOut(t *testing.T) {
	f, srv := newFakeGCP(t, "id-proj")
	d := saDriver(t, srv)
	_ = f
	ctx := context.Background()

	if err := d.ValidateParameters(map[string]string{"hmac": "false"}); err != nil {
		t.Errorf("hmac false rejected: %v", err)
	}
	if err := d.ValidateParameters(map[string]string{"hmac": "sometimes"}); err == nil {
		t.Error("bad hmac value accepted")
	}

	// Default: pair present (compatibility).
	res, err := d.GrantAccess(ctx, registry.GrantRequest{BucketyName: "bucket-x"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.SecretData["accessKeyID"]; !ok {
		t.Error("default grant lost the HMAC pair")
	}

	// Opt-out with a serviceAccount: SA keys only.
	if err := d.ensureServiceAccount(ctx, "orders-t1", "bucket-x"); err != nil {
		t.Fatal(err)
	}
	res, err = d.GrantAccess(ctx, registry.GrantRequest{
		BucketyName:       "bucket-x",
		BucketyParameters: map[string]string{"hmac": "false", "serviceAccount": "orders-t1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.SecretData["accessKeyID"]; ok {
		t.Error("hmac=false Secret still carries accessKeyID")
	}
	if _, ok := res.SecretData["secretAccessKey"]; ok {
		t.Error("hmac=false Secret still carries secretAccessKey")
	}
	if _, ok := res.SecretData["serviceAccountKey"]; !ok {
		t.Error("hmac=false Secret missing the SA key")
	}
	// Coordinates survive either way.
	if string(res.SecretData["bucket"]) != "bucket-x" || string(res.SecretData["endpoint"]) == "" {
		t.Errorf("coordinates missing: %v", keysOf(res.SecretData))
	}

	// Opt-out without serviceAccount: coordinates-only Secret for
	// ambient-credential consumers.
	res, err = d.GrantAccess(ctx, registry.GrantRequest{
		BucketyName:       "bucket-x",
		BucketyParameters: map[string]string{"hmac": "false"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"accessKeyID", "secretAccessKey", "serviceAccountKey"} {
		if _, ok := res.SecretData[k]; ok {
			t.Errorf("coordinates-only Secret carries %s", k)
		}
	}
}
