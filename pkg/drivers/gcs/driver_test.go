package gcs

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

func TestValidateParameters(t *testing.T) {
	d := &Driver{cfg: &Config{Project: "p"}}

	ok := map[string]string{
		"location":                 "EUROPE-WEST4",
		"uniformBucketLevelAccess": "true",
		"versioning":               "false",
		"lifecycle":                `{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]}`,
	}
	if err := d.ValidateParameters(ok); err != nil {
		t.Fatalf("valid parameters rejected: %v", err)
	}
	if err := d.ValidateParameters(nil); err != nil {
		t.Fatalf("empty parameters rejected: %v", err)
	}

	okMore := map[string]string{
		"softDeleteRetentionSeconds": "604800",
		"labels":                     `{"site": "tenant1", "managed-by": "buckety"}`,
	}
	if err := d.ValidateParameters(okMore); err != nil {
		t.Fatalf("valid softDelete/labels rejected: %v", err)
	}
	if err := d.ValidateParameters(map[string]string{"softDeleteRetentionSeconds": "0"}); err != nil {
		t.Fatalf("softDeleteRetentionSeconds=0 (disabled) rejected: %v", err)
	}

	cases := map[string]map[string]string{
		"unknown key":     {"partitions": "3"},
		"bad bool":        {"versioning": "yes please"},
		"empty location":  {"location": ""},
		"bad lifecycle":   {"lifecycle": "not json"},
		"unknown action":  {"lifecycle": `{"rule": [{"action": {"type": "Recycle"}, "condition": {}}]}`},
		"unknown cond":    {"lifecycle": `{"rule": [{"action": {"type": "Delete"}, "condition": {"aeg": 30}}]}`},
		"no rule key":     {"lifecycle": `{}`},
		"class w/ delete": {"lifecycle": `{"rule": [{"action": {"type": "Delete", "storageClass": "COLDLINE"}, "condition": {}}]}`},
		"class missing":   {"lifecycle": `{"rule": [{"action": {"type": "SetStorageClass"}, "condition": {}}]}`},
		"softdel string":  {"softDeleteRetentionSeconds": "7d"},
		"softdel short":   {"softDeleteRetentionSeconds": "86400"}, // below the 7-day GCS minimum
		"softdel long":    {"softDeleteRetentionSeconds": "31536000"},
		"labels not obj":  {"labels": `["site"]`},
		"labels non-str":  {"labels": `{"site": 3}`},
	}
	for name, params := range cases {
		if err := d.ValidateParameters(params); err == nil {
			t.Errorf("%s: expected rejection for %v", name, params)
		}
	}
}

func TestValidateUpdateParametersLocationImmutable(t *testing.T) {
	d := &Driver{cfg: &Config{Project: "p"}}
	old := map[string]string{"location": "EU"}

	if err := d.ValidateUpdateParameters(old, map[string]string{"location": "EU", "versioning": "true"}); err != nil {
		t.Fatalf("unchanged location rejected: %v", err)
	}
	if err := d.ValidateUpdateParameters(old, map[string]string{"location": "US"}); err == nil {
		t.Fatal("location change accepted")
	}
	if err := d.ValidateUpdateParameters(old, map[string]string{}); err == nil {
		t.Fatal("location removal accepted")
	}
	if err := d.ValidateUpdateParameters(map[string]string{}, map[string]string{"location": "EU"}); err == nil {
		t.Fatal("location addition post-create accepted")
	}
}

func TestValidateResourceName(t *testing.T) {
	d := &Driver{cfg: &Config{Project: "p"}}

	valid := []string{
		"abc",
		"yo-site-tenant1-eu-west4",
		"with_underscore",   // legal in GCS, unlike S3
		"dotted.name.works", // dotted names are legal (domain verification is the backend's concern)
		"0numeric9",
	}
	for _, name := range valid {
		if err := d.ValidateResourceName(name); err != nil {
			t.Errorf("valid name %q rejected: %v", name, err)
		}
	}

	invalid := []string{
		"ab",                    // too short
		strings.Repeat("a", 64), // too long
		"Uppercase",             // charset
		"-leading-dash",         // must start alphanumeric
		"trailing-dash-",        // must end alphanumeric
		"goog-prefixed",         // reserved prefix
		"not-google-inside",     // reserved word
		"192.168.0.1",           // IP shape
	}
	for _, name := range invalid {
		if err := d.ValidateResourceName(name); err == nil {
			t.Errorf("invalid name %q accepted", name)
		}
	}
}

func TestParseLifecycle(t *testing.T) {
	doc := `{"rule": [
	  {"action": {"type": "Delete"}, "condition": {"age": 0, "isLive": false}},
	  {"action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
	   "condition": {"age": 90, "createdBefore": "2026-01-31", "matchesPrefix": ["tmp/"], "numNewerVersions": 3}}
	]}`
	lc, err := parseLifecycle(doc)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(lc.Rules) != 2 {
		t.Fatalf("want 2 rules, got %d", len(lc.Rules))
	}
	r0 := lc.Rules[0]
	if r0.Action.Type != "Delete" || !r0.Condition.AllObjects || r0.Condition.AgeInDays != 0 {
		t.Errorf("rule[0]: age 0 should map to AllObjects, got %+v", r0)
	}
	if r0.Condition.Liveness != storage.Archived {
		t.Errorf("rule[0]: isLive=false should map to Archived, got %v", r0.Condition.Liveness)
	}
	r1 := lc.Rules[1]
	if r1.Action.StorageClass != "COLDLINE" || r1.Condition.AgeInDays != 90 {
		t.Errorf("rule[1]: %+v", r1)
	}
	want := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	if !r1.Condition.CreatedBefore.Equal(want) {
		t.Errorf("rule[1].createdBefore: want %v, got %v", want, r1.Condition.CreatedBefore)
	}
	if len(r1.Condition.MatchesPrefix) != 1 || r1.Condition.MatchesPrefix[0] != "tmp/" {
		t.Errorf("rule[1].matchesPrefix: %v", r1.Condition.MatchesPrefix)
	}
	if r1.Condition.NumNewerVersions != 3 {
		t.Errorf("rule[1].numNewerVersions: %d", r1.Condition.NumNewerVersions)
	}

	// The blobs-per-org data plane's exact shape (checkit
	// cluster-g2/buckety-controller/GCS_DRIVER_REQUIREMENTS_FROM_BLOBS.md
	// must-have 3): multiple concurrent matchesPrefix rules.
	blobs := `{"rule": [
	  {"action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": ["board-prints/"]}},
	  {"action": {"type": "Delete"}, "condition": {"age": 1, "matchesPrefix": [".staging/"]}}
	]}`
	lc, err = parseLifecycle(blobs)
	if err != nil {
		t.Fatalf("blobs-per-org two-rule lifecycle rejected: %v", err)
	}
	if len(lc.Rules) != 2 ||
		lc.Rules[0].Condition.AgeInDays != 7 || lc.Rules[0].Condition.MatchesPrefix[0] != "board-prints/" ||
		lc.Rules[1].Condition.AgeInDays != 1 || lc.Rules[1].Condition.MatchesPrefix[0] != ".staging/" {
		t.Errorf("blobs-per-org lifecycle mistranslated: %+v", lc.Rules)
	}

	// Managed-empty clears rules; distinct from omitting the
	// parameter (unmanaged).
	empty, err := parseLifecycle(`{"rule": []}`)
	if err != nil {
		t.Fatalf("empty rule list rejected: %v", err)
	}
	if len(empty.Rules) != 0 {
		t.Fatalf("want 0 rules, got %d", len(empty.Rules))
	}
}

func TestAttrsForCreate(t *testing.T) {
	attrs, err := attrsForCreate(map[string]string{
		"location":                 "EUROPE-WEST4",
		"uniformBucketLevelAccess": "true",
		"versioning":               "true",
	})
	if err != nil {
		t.Fatalf("attrsForCreate: %v", err)
	}
	if attrs.Location != "EUROPE-WEST4" || !attrs.UniformBucketLevelAccess.Enabled || !attrs.VersioningEnabled {
		t.Errorf("attrs: %+v", attrs)
	}

	// Omitted parameters leave zero values so GCS's own defaults
	// apply (no driver-side defaults per SPEC §Parameters).
	bare, err := attrsForCreate(nil)
	if err != nil {
		t.Fatalf("attrsForCreate(nil): %v", err)
	}
	if bare.Location != "" || bare.UniformBucketLevelAccess.Enabled || bare.VersioningEnabled || len(bare.Lifecycle.Rules) != 0 {
		t.Errorf("bare attrs should be all zero, got %+v", bare)
	}
}

func TestUpdateForDrift(t *testing.T) {
	current := &storage.BucketAttrs{
		Location:                 "EUROPE-WEST4",
		VersioningEnabled:        true,
		UniformBucketLevelAccess: storage.UniformBucketLevelAccess{Enabled: true},
	}

	// In-sync managed parameters: no update.
	up, err := updateForDrift(map[string]string{
		"uniformBucketLevelAccess": "true",
		"versioning":               "true",
	}, current)
	if err != nil {
		t.Fatalf("updateForDrift: %v", err)
	}
	if up != nil {
		t.Errorf("in-sync parameters produced an update: %+v", up)
	}

	// Unmanaged (absent) parameters never produce an update even
	// when the backend has non-default state.
	if up, err = updateForDrift(nil, current); err != nil || up != nil {
		t.Errorf("unmanaged parameters produced update=%+v err=%v", up, err)
	}

	// Drift on a managed bool.
	up, err = updateForDrift(map[string]string{"versioning": "false"}, current)
	if err != nil {
		t.Fatalf("updateForDrift: %v", err)
	}
	if up == nil || up.VersioningEnabled != false {
		t.Errorf("versioning drift not reconciled: %+v", up)
	}
	if up.UniformBucketLevelAccess != nil {
		t.Errorf("unmanaged UBLA included in update: %+v", up)
	}

	// Lifecycle drift.
	up, err = updateForDrift(map[string]string{
		"lifecycle": `{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7}}]}`,
	}, current)
	if err != nil {
		t.Fatalf("updateForDrift: %v", err)
	}
	if up == nil || up.Lifecycle == nil || len(up.Lifecycle.Rules) != 1 {
		t.Errorf("lifecycle drift not reconciled: %+v", up)
	}

	// Lifecycle in sync: no update.
	current.Lifecycle = storage.Lifecycle{Rules: []storage.LifecycleRule{{
		Action:    storage.LifecycleAction{Type: "Delete"},
		Condition: storage.LifecycleCondition{AgeInDays: 7},
	}}}
	up, err = updateForDrift(map[string]string{
		"lifecycle": `{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7}}]}`,
	}, current)
	if err != nil {
		t.Fatalf("updateForDrift: %v", err)
	}
	if up != nil {
		t.Errorf("in-sync lifecycle produced an update: %+v", up)
	}

	// Soft delete: absent policy counts as 0, enabling produces an
	// update, matching policy does not.
	up, err = updateForDrift(map[string]string{"softDeleteRetentionSeconds": "604800"}, current)
	if err != nil {
		t.Fatalf("updateForDrift: %v", err)
	}
	if up == nil || up.SoftDeletePolicy == nil || up.SoftDeletePolicy.RetentionDuration != 7*24*time.Hour {
		t.Errorf("soft-delete enable not reconciled: %+v", up)
	}
	current.SoftDeletePolicy = &storage.SoftDeletePolicy{RetentionDuration: 7 * 24 * time.Hour}
	if up, err = updateForDrift(map[string]string{"softDeleteRetentionSeconds": "604800"}, current); err != nil || up != nil {
		t.Errorf("in-sync soft delete produced update=%+v err=%v", up, err)
	}
	// GCS's default-on 7d policy is reverted when the resource
	// declares 0.
	up, err = updateForDrift(map[string]string{"softDeleteRetentionSeconds": "0"}, current)
	if err != nil || up == nil || up.SoftDeletePolicy == nil || up.SoftDeletePolicy.RetentionDuration != 0 {
		t.Errorf("soft-delete disable not reconciled: update=%+v err=%v", up, err)
	}

	// Labels: listed keys converge, unlisted keys are unmanaged.
	current.Labels = map[string]string{"site": "old", "keepme": "untouched"}
	up, err = updateForDrift(map[string]string{"labels": `{"site": "tenant1"}`}, current)
	if err != nil {
		t.Fatalf("updateForDrift: %v", err)
	}
	if up == nil {
		t.Fatal("label drift not reconciled")
	}
	if up, err = updateForDrift(map[string]string{"labels": `{"site": "old"}`}, current); err != nil || up != nil {
		t.Errorf("in-sync labels produced update=%+v err=%v", up, err)
	}
}

// Once any knob drifts, the update payload carries every managed
// knob, drifted or not: a backend with replace-like update
// semantics (fake-gcs-server resets fields absent from the
// payload) must not un-converge settled values. Seen live as
// portable-blobs-cr flaking on versioning (run 29509508991): the
// emulator drops UBLA/lifecycle, the resulting perpetual drift
// update wiped the already-correct versioning flag.
func TestUpdateForDriftCarriesAllManagedKnobs(t *testing.T) {
	current := &storage.BucketAttrs{VersioningEnabled: true}
	up, err := updateForDrift(map[string]string{
		"versioning":               "true",
		"uniformBucketLevelAccess": "true",
	}, current)
	if err != nil {
		t.Fatalf("updateForDrift: %v", err)
	}
	if up == nil || up.UniformBucketLevelAccess == nil || !up.UniformBucketLevelAccess.Enabled {
		t.Fatalf("UBLA drift not reconciled: %+v", up)
	}
	if up.VersioningEnabled != true {
		t.Errorf("undrifted managed versioning missing from update payload: %+v", up)
	}
}

func TestFactoryValidation(t *testing.T) {
	// Client construction succeeds offline against an emulator
	// coordinate; ADC is not consulted when the env var is set.
	t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:1")

	cases := map[string]string{
		"missing project": `{"accessKeyID": "a", "secretAccessKey": "s"}`,
		"missing key id":  `{"project": "p", "secretAccessKey": "s"}`,
		"missing secret":  `{"project": "p", "accessKeyID": "a"}`,
		"unknown field":   `{"project": "p", "accessKeyID": "a", "secretAccessKey": "s", "reigon": "x"}`,
		"undefined var":   `{"project": "p", "accessKeyID": "${E2E_GCS_TEST_UNDEFINED_VAR}", "secretAccessKey": "s"}`,
	}
	for name, raw := range cases {
		if _, err := factory(json.RawMessage(raw)); err == nil {
			t.Errorf("%s: factory accepted %s", name, raw)
		}
	}

	d, err := factory(json.RawMessage(`{"project": "p", "accessKeyID": "a", "secretAccessKey": "s"}`))
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if got := d.(*Driver).cfg.Endpoint; got != "" {
		t.Errorf("endpoint should stay empty (derived per bucket), got %q", got)
	}

	// Endpoint overrides are bare hosts; a scheme is a config error
	// (issue #14: consumers prepend the scheme themselves).
	if _, err := factory(json.RawMessage(`{"project": "p", "endpoint": "http://fake-gcs:8000", "accessKeyID": "a", "secretAccessKey": "s"}`)); err == nil {
		t.Error("scheme'd endpoint accepted; want bare-host rejection")
	}
	d, err = factory(json.RawMessage(`{"project": "p", "endpoint": "fake-gcs:8000", "accessKeyID": "a", "secretAccessKey": "s"}`))
	if err != nil {
		t.Fatalf("valid bare-host endpoint rejected: %v", err)
	}
	if got := d.(*Driver).cfg.Endpoint; got != "fake-gcs:8000" {
		t.Errorf("endpoint override: got %q", got)
	}
}

func TestLocationEndpoint(t *testing.T) {
	cases := []struct {
		location, endpoint, region string
	}{
		{"EUROPE-WEST4", "storage.europe-west4.rep.googleapis.com", "europe-west4"},
		{"us-central1", "storage.us-central1.rep.googleapis.com", "us-central1"},
		{"EU", "storage.googleapis.com", ""},   // multi-region: no locational endpoint
		{"US", "storage.googleapis.com", ""},   // multi-region
		{"EUR4", "storage.googleapis.com", ""}, // dual-region code
		{"", "storage.googleapis.com", ""},
	}
	for _, c := range cases {
		e, r := locationEndpoint(c.location)
		if e != c.endpoint || r != c.region {
			t.Errorf("locationEndpoint(%q) = (%q, %q), want (%q, %q)", c.location, e, r, c.endpoint, c.region)
		}
	}
}

func TestGrantAccessPayload(t *testing.T) {
	// With an endpoint override the payload is config-only (no
	// backend lookup); the derived path is covered by e2e and the
	// fakestorage smoke, since it reads bucket attrs.
	d := &Driver{cfg: &Config{
		Project:         "yo-project",
		Endpoint:        "fake-gcs:8000",
		Region:          "e2e-region-1",
		AccessKeyID:     "GOOG1EXAMPLE",
		SecretAccessKey: "sekrit",
	}}
	res, err := d.GrantAccess(t.Context(), registry.GrantRequest{
		BucketyName: "tenant1-orders",
		Role:        "ReadWrite",
	})
	if err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}
	if res.Scoped {
		t.Error("v0.1 static credentials must report Scoped=false")
	}
	want := map[string]string{
		"endpoint":        "fake-gcs:8000",
		"bucket":          "tenant1-orders",
		"project":         "yo-project",
		"region":          "e2e-region-1",
		"accessKeyID":     "GOOG1EXAMPLE",
		"secretAccessKey": "sekrit",
	}
	for k, v := range want {
		if string(res.SecretData[k]) != v {
			t.Errorf("SecretData[%s]: want %q, got %q", k, v, res.SecretData[k])
		}
	}
	if len(res.SecretData) != len(want) {
		t.Errorf("unexpected extra Secret keys: %v", keysOf(res.SecretData))
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The published parameters schema is hand-maintained next to
// ValidateParameters; this pins the two together in both
// directions so neither can outdate silently. The generated
// whole-CR schemas (schema/) compose from this file, so this is
// also their sync guard.
func TestParametersSchemaInSync(t *testing.T) {
	props := schemaProperties(t, "schema/v0.1/parameters.schema.json")
	d := &Driver{cfg: &Config{Project: "p"}}

	// Every schema property must be a code-known key: probing with
	// an empty value may fail value validation, but never as
	// "unknown parameter".
	for key := range props {
		if err := d.ValidateParameters(map[string]string{key: ""}); err != nil && strings.Contains(err.Error(), "unknown parameter") {
			t.Errorf("schema property %q is unknown to ValidateParameters: %v", key, err)
		}
	}

	// Every key the unknown-parameter message advertises must be
	// in the schema; a parameter added in code but not published
	// fails here.
	for _, key := range acceptedKeysFromError(t, d.ValidateParameters(map[string]string{"definitely-not-a-parameter": "x"})) {
		if _, ok := props[key]; !ok {
			t.Errorf("ValidateParameters advertises %q but schema/v0.1/parameters.schema.json does not list it", key)
		}
	}

	// Family portability (SPEC "Driver families" rule 1): every
	// object-store family-common parameter must exist in this
	// driver's schema too.
	for key := range schemaProperties(t, "../objectstore/schema/v0.1/parameters.schema.json") {
		if _, ok := props[key]; !ok {
			t.Errorf("family parameter %q missing from the gcs parameters schema", key)
		}
	}
}

func schemaProperties(t *testing.T, path string) map[string]json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var s struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return s.Properties
}

// acceptedKeysFromError extracts parameter names from the
// "(<driver> ... accepts: a, b, c)" enumeration, skipping filler
// words and capability qualifiers.
func acceptedKeysFromError(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		t.Fatal("expected an unknown-parameter error to parse")
	}
	msg := err.Error()
	i := strings.Index(msg, "accepts: ")
	if i < 0 {
		t.Fatalf("no 'accepts:' enumeration in %q", msg)
	}
	seg := strings.TrimSuffix(msg[i+len("accepts: "):], ")")
	var keys []string
	for _, tok := range strings.Split(seg, " ") {
		tok = strings.Trim(tok, ",")
		switch {
		case tok == "" || tok == "and" || tok == "when":
		case strings.Contains(tok, "="): // capability qualifier, not a key
		case strings.Contains(tok, "*"): // pattern shorthand, checked separately
		default:
			keys = append(keys, tok)
		}
	}
	return keys
}
