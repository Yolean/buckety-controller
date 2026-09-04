// Per-bucket service accounts (Config.ServiceAccounts): the
// control-plane side of parameters.serviceAccount. EnsureBuckety
// maintains the SA and its bucket-level binding, GrantAccess
// mints one user-managed key per BucketyAccess (driver.go), and
// DeleteBuckety tears the SA down with the bucket.
//
// The ownership marker is the security boundary here: a tenant
// who names another Buckety's SA in parameters.serviceAccount
// must not get keys for it - a key equals impersonation, and the
// foreign SA holds bindings on the foreign bucket. Every
// mutating path therefore verifies the marker first and refuses
// SAs it did not create for exactly this bucket.
package gcs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	iam "google.golang.org/api/iam/v1"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// saNameRE is GCP's service account ID rule: 6-30 characters,
// lowercase letters, digits and hyphens, starting with a letter,
// ending alphanumeric.
var saNameRE = regexp.MustCompile(`^[a-z][-a-z0-9]{4,28}[a-z0-9]$`)

// saBucketRole is the bucket-level grant for the per-bucket SA.
// objectAdmin (get/list/create/delete objects), matching what the
// static HMAC pair's backing SA is documented to need; per-role
// scoping (ReadOnly -> objectViewer) is the per-access-SA design
// deferred with the rest of v1alpha2 scoping.
const saBucketRole = "roles/storage.objectAdmin"

// saMarker is the ownership stamp serialized into the service
// account's description at creation.
type saMarker struct {
	ManagedBy string `json:"managedBy"`
	Bucket    string `json:"bucket"`
}

func (d *Driver) saEmail(shortName string) string {
	return shortName + "@" + d.cfg.ServiceAccounts.Project + ".iam.gserviceaccount.com"
}

// saResource is the IAM API resource name for the SA. The
// explicit project (not the "-" wildcard) keeps every call
// confined to the configured identity project.
func (d *Driver) saResource(email string) string {
	return "projects/" + d.cfg.ServiceAccounts.Project + "/serviceAccounts/" + email
}

// ensureServiceAccount creates-or-verifies the bucket's SA and
// re-asserts its bucket-level binding. Re-asserting every
// reconcile is what heals the binding after out-of-band edits and
// after SA delete+recreate (a recreated SA is a new identity to
// IAM; stale bindings never revive on their own).
func (d *Driver) ensureServiceAccount(ctx context.Context, shortName, bucket string) error {
	if d.iamsvc == nil {
		return fmt.Errorf("gcs: bucket %q declares parameters.serviceAccount but this backend has serviceAccounts disabled", bucket)
	}
	email := d.saEmail(shortName)
	resource := d.saResource(email)

	sa, err := d.iamsvc.Projects.ServiceAccounts.Get(resource).Context(ctx).Do()
	if isNotFound(err) {
		marker, merr := json.Marshal(saMarker{ManagedBy: "buckety", Bucket: bucket})
		if merr != nil {
			return merr
		}
		sa, err = d.iamsvc.Projects.ServiceAccounts.Create("projects/"+d.cfg.ServiceAccounts.Project, &iam.CreateServiceAccountRequest{
			AccountId: shortName,
			ServiceAccount: &iam.ServiceAccount{
				DisplayName: "buckety bucket " + bucket,
				Description: string(marker),
			},
		}).Context(ctx).Do()
		if isConflict(err) {
			// Raced another creator; the re-fetched marker decides
			// whether it is ours.
			sa, err = d.iamsvc.Projects.ServiceAccounts.Get(resource).Context(ctx).Do()
			if isNotFound(err) {
				// Create conflicts while Get sees nothing: a
				// soft-deleted SA holds the name. GCP reserves a
				// deleted SA's ID for ~30 days, so this state does
				// not converge on retries and the generic create
				// error would hide the actual cause.
				return fmt.Errorf("gcs: service account ID %q is reserved by a recently deleted account; GCP holds deleted SA names for ~30 days. Wait out the window, undelete it if its numeric unique ID is known (gcloud iam service-accounts undelete), or use a different parameters.serviceAccount", email)
			}
		}
		if err != nil {
			return fmt.Errorf("gcs: create service account %q (needs roles/iam.serviceAccountAdmin on project %q): %w", email, d.cfg.ServiceAccounts.Project, err)
		}
	} else if err != nil {
		return fmt.Errorf("gcs: get service account %q: %w", email, err)
	}
	if err := verifySAMarker(sa, bucket); err != nil {
		return err
	}
	return d.ensureBucketBinding(ctx, bucket, email)
}

// verifySAMarker enforces the ownership rule: the driver touches
// (binds, mints keys for, deletes) only SAs whose description
// carries its marker for exactly this bucket.
func verifySAMarker(sa *iam.ServiceAccount, bucket string) error {
	var m saMarker
	if json.Unmarshal([]byte(sa.Description), &m) != nil || m.ManagedBy != "buckety" || m.Bucket != bucket {
		return fmt.Errorf("gcs: service account %q exists but was not created by buckety for bucket %q; refusing to touch it - choose another parameters.serviceAccount", sa.Email, bucket)
	}
	return nil
}

// ensureBucketBinding grants the SA saBucketRole on the bucket,
// skipping the write when the binding is already present.
// SetPolicy is etag-guarded read-modify-write; a concurrent
// policy edit surfaces as a conflict error and the next reconcile
// retries.
func (d *Driver) ensureBucketBinding(ctx context.Context, bucket, email string) error {
	handle := d.client.Bucket(bucket).IAM()
	policy, err := handle.Policy(ctx)
	if err != nil {
		return fmt.Errorf("gcs: read IAM policy of bucket %q (needs storage.buckets.getIamPolicy): %w", bucket, err)
	}
	member := "serviceAccount:" + email
	if policy.HasRole(member, saBucketRole) {
		return nil
	}
	policy.Add(member, saBucketRole)
	if err := handle.SetPolicy(ctx, policy); err != nil {
		if isForbidden(err) {
			// The permission hint belongs to 403 ONLY: attaching it
			// to the member-validation 400 below sent operators
			// auditing IAM roles for what is a timing condition.
			return fmt.Errorf("gcs: grant %s to %s on bucket %q (needs storage.buckets.setIamPolicy): %w", saBucketRole, member, bucket, err)
		}
		if isMemberNotPropagated(err) {
			// A just-created (or just-recreated) SA is not yet
			// usable as an IAM member; GCP refuses it as a
			// member-validation 400 "does not exist" - reading as a
			// missing account when the account is merely seconds
			// old. Typed so the reconciler requeues promptly
			// instead of surfacing a Warning failure; the
			// re-asserted binding lands once IAM has propagated.
			return &registry.ErrProvisioningInProgress{Progress: fmt.Sprintf(
				"service account %s was just created and is not yet bindable on bucket %q; IAM propagation takes a few seconds", email, bucket)}
		}
		return fmt.Errorf("gcs: grant %s to %s on bucket %q: %w", saBucketRole, member, bucket, err)
	}
	return nil
}

// isMemberNotPropagated matches setIamPolicy's member-validation
// refusal of a not-yet-propagated service account: HTTP 400 (not
// 404 - it is policy validation, not an account lookup) with the
// account named as nonexistent.
func isMemberNotPropagated(err error) bool {
	return gapiCode(err, 400) && strings.Contains(err.Error(), "does not exist")
}

// ensureAccessKey returns the access's SA key JSON and key id.
// The private material is only retrievable at creation, so the
// key carried in the existing Secret is reused, and a re-mint
// happens only on POSITIVE evidence that it is unusable: the JSON
// does not parse, it names another identity, or keys.list shows
// it expired (constraints/iam.serviceAccountKeyExpiryHours).
//
// Absence from keys.list is NOT such evidence. The listing is
// eventually consistent, and a key created a second earlier is
// routinely missing from it - which is exactly when the second
// reconcile runs, since the Secret write and the status patch of
// the first one both re-enqueue the access. Reading absence as
// "revoked out of band" made that second reconcile mint a
// replacement and revoke the key a consumer had just read from
// the Secret (11 of 19 first provisionings in one project;
// ISSUE_first_provisioning_mints_two_keys_and_revokes_the_one_a_consumer_read.md).
// A revoke is irreversible, so it needs evidence, not the lack of
// it. The cost is that a key deleted server-side is no longer
// replaced on its own; rotation is triggered by deleting the
// Secret instead, which mints a fresh key and (in the reconciler)
// revokes the previous one.
//
// Minting verifies the ownership marker first: a key is
// impersonation, and the reconciler-side ordering that normally
// keeps a foreign SA name from reaching here (the Buckety must be
// Ready, which required ensureServiceAccount to accept it) is not
// a check this function can rely on - with the webhook disabled
// nothing stops parameters.serviceAccount from changing under an
// access that reads the still-Ready cached status. fresh reports
// that a key was created.
func (d *Driver) ensureAccessKey(ctx context.Context, email, bucket string, existing map[string][]byte) (keyJSON []byte, keyID string, fresh bool, err error) {
	resource := d.saResource(email)
	if raw, ok := existing["serviceAccountKey"]; ok {
		var k struct {
			ClientEmail  string `json:"client_email"`
			PrivateKeyID string `json:"private_key_id"`
		}
		if json.Unmarshal(raw, &k) == nil && k.ClientEmail == email && k.PrivateKeyID != "" {
			resp, err := d.iamsvc.Projects.ServiceAccounts.Keys.List(resource).KeyTypes("USER_MANAGED").Context(ctx).Do()
			if err != nil {
				return nil, "", false, fmt.Errorf("gcs: list keys of %q: %w", email, err)
			}
			expired := false
			for _, key := range resp.Keys {
				if strings.HasSuffix(key.Name, "/keys/"+k.PrivateKeyID) {
					expired = keyExpired(key)
					break
				}
			}
			if !expired {
				return raw, k.PrivateKeyID, false, nil
			}
		}
		// Unparseable, mismatched email, or expired: mint fresh.
		// The reconciler revokes the replaced key once the Secret
		// carries its successor, so each access holds one key; GCP
		// caps user-managed keys at 10 per SA, bounding accesses
		// per bucket accordingly.
	}
	sa, err := d.iamsvc.Projects.ServiceAccounts.Get(resource).Context(ctx).Do()
	if err != nil {
		return nil, "", false, fmt.Errorf("gcs: get service account %q before minting a key: %w", email, err)
	}
	if err := verifySAMarker(sa, bucket); err != nil {
		return nil, "", false, err
	}
	key, err := d.iamsvc.Projects.ServiceAccounts.Keys.Create(resource, &iam.CreateServiceAccountKeyRequest{}).Context(ctx).Do()
	if err != nil {
		return nil, "", false, fmt.Errorf("gcs: create key for %q (needs roles/iam.serviceAccountKeyAdmin; the org policy constraints/iam.disableServiceAccountKeyCreation blocks user-managed keys entirely): %w", email, err)
	}
	keyJSON, err = base64.StdEncoding.DecodeString(key.PrivateKeyData)
	if err != nil {
		return nil, "", false, fmt.Errorf("gcs: decode created key for %q: %w", email, err)
	}
	return keyJSON, key.Name[strings.LastIndex(key.Name, "/")+1:], true, nil
}

// keyExpired reports whether the key's validity window has
// passed. Non-expiring keys carry a far-future (year 9999)
// validBeforeTime; an org-policy expiry
// (constraints/iam.serviceAccountKeyExpiryHours) shows up here,
// and treating such keys as invalid re-mints within one reconcile
// of expiry - degraded (the Secret holds a dead key for up to the
// requeue cadence) but converging. Proactive renewal is the
// scheduled-rotation roadmap item.
func keyExpired(key *iam.ServiceAccountKey) bool {
	if key.ValidBeforeTime == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339, key.ValidBeforeTime)
	if err != nil {
		return false
	}
	return time.Now().After(t)
}

// deleteServiceAccount removes the bucket's SA at bucket
// deletion. Idempotent on NotFound. An SA that fails the marker
// check is left alone WITHOUT blocking: it was never ours
// (ensureServiceAccount refused it too, so nothing was minted),
// and the immutable serviceAccount parameter would otherwise
// wedge the finalizer with no spec-side fix.
func (d *Driver) deleteServiceAccount(ctx context.Context, shortName, bucket string) error {
	resource := d.saResource(d.saEmail(shortName))
	sa, err := d.iamsvc.Projects.ServiceAccounts.Get(resource).Context(ctx).Do()
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("gcs: get service account for deletion: %w", err)
	}
	if verifySAMarker(sa, bucket) != nil {
		return nil
	}
	_, err = d.iamsvc.Projects.ServiceAccounts.Delete(resource).Context(ctx).Do()
	if err == nil || isNotFound(err) {
		return nil
	}
	return fmt.Errorf("gcs: delete service account %q: %w", sa.Email, err)
}
