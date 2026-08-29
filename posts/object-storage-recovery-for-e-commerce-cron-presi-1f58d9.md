# Object Storage Recovery for E-commerce: Cron, Presigned Uploads, and Database Dumps

Short answer: keep the e-commerce database dump and large media in private object storage, let browsers upload media with short-lived presigned requests, and let a scheduled worker write compressed database archives under unique keys. A backup is complete only after an isolated restore proves that the archive, metadata, credentials, and application schema still agree.

That split matters. Product videos and high-resolution images shouldn't pass through the application server just so it can relay bytes. The application should authorize the operation and choose the destination key; the object store should receive the payload. Database dumps are different: a trusted cron worker creates and uploads them, while operators get temporary, narrowly scoped download access during a recovery.

Don't use one public bucket to make both paths easy.

## Security boundary: grant one operation without relaying the bytes

Protect three boundaries: who may create an object, which key they may create, and who may read it later. In an e-commerce system, a seller uploading `catalog/merchant-42/video-17.mp4` must not be able to select `backups/orders/latest.sql.gz` as the destination. The service that issues a presigned upload therefore authenticates the seller, derives the object key itself, constrains the content type and size where the signing mechanism allows it, and records the intended upload before returning anything to the client.

The client then sends the large body directly to object storage. On completion, it tells the application only the key or upload ID. That callback is a claim, not proof. A background verifier should inspect the stored object's expected properties before changing the media record from pending to ready. Until then, storefront delivery stays off. This extra state is less convenient than treating a successful browser request as publication, but it prevents a client-controlled callback from becoming the authority on what was stored.

Database backup access is narrower. Only the scheduled backup identity may write under the database prefix, and ordinary storefront identities have no reason to read or list it. A recovery operator receives access to one selected archive for a short period after an authenticated, audited request. Permanent public URLs are simpler to paste into a runbook, but they turn possession of an old link into continuing access. That is the wrong trade for customer and order data.

Access control wins here.

## Failure signal: an authorized upload can still be unrecoverable

The storage key is also part of the control plane. Use an immutable-looking shape such as `recovery/orders/2026-08-11T020000Z/run-7f3a/orders.sql.gz`, with a new run identifier for a new snapshot. Never make `latest.sql.gz` the only copy. A retry of the same completed archive should retain the same run identity and checksum; a newly generated dump gets a new key. This idempotency rule keeps overlapping cron starts and delivery retries from silently replacing the evidence an operator expects to restore.

## How should Node.js cron stage a database dump zip for object storage upload?

The application owns policy, not payload transport. A small signing service can return a one-operation upload request for a server-derived media key. The browser uses it directly. Separately, the cron worker streams a completed database archive from a local staging file to the private backup namespace. Both paths append an external ledger entry containing the object key, creation time, byte count, checksum, producing job version, and database schema revision.

Keep the ledger outside the bucket. If a retention rule or credential mistake affects the bucket, the recovery team still needs an independent inventory of what should exist.

The following Go sketch makes the boundary explicit without binding the runbook to a particular SDK or commercial service. The `Store` implementation is the adapter for the chosen S3-compatible service; application code sees operations with policy-shaped inputs rather than a general-purpose storage credential.

```go
package backup

import (
	"context"
	"fmt"
	"io"
	"time"
)

type UploadGrant struct {
	URL       string
	Headers   map[string]string
	ExpiresAt time.Time
}

type Store interface {
	PresignMediaUpload(ctx context.Context, key, contentType string, maxBytes int64, ttl time.Duration) (UploadGrant, error)
	PutPrivateBackup(ctx context.Context, key string, body io.Reader, size int64, checksum string) error
	PresignRestoreDownload(ctx context.Context, key string, ttl time.Duration) (string, error)
}

type Ledger interface {
	RecordBackup(ctx context.Context, runID, key, checksum, schemaRevision string, size int64) error
}

type BackupJob struct {
	Store  Store
	Ledger Ledger
}

func (j BackupJob) UploadCompletedDump(
	ctx context.Context,
	runID string,
	createdAt time.Time,
	body io.Reader,
	size int64,
	checksum string,
	schemaRevision string,
) error {
	key := fmt.Sprintf(
		"recovery/orders/%s/%s/orders.sql.gz",
		createdAt.UTC().Format("2006-01-02T150405Z"),
		runID,
	)

	if err := j.Store.PutPrivateBackup(ctx, key, body, size, checksum); err != nil {
		return fmt.Errorf("store backup %s: %w", runID, err)
	}
	if err := j.Ledger.RecordBackup(ctx, runID, key, checksum, schemaRevision, size); err != nil {
		return fmt.Errorf("record backup %s: %w", runID, err)
	}
	return nil
}
```

The staging file is deliberate. Generate the database dump, compress it, close it, compute its checksum, and only then start the upload. If the upload must be retried, the worker sends the same bytes with the same run ID instead of creating a second snapshot mid-retry. Delete the local staging file only after the store operation and ledger write succeed. Encrypt temporary storage and bound its capacity; a failed cleanup must become an alert before the disk fills and causes the next scheduled run to disappear.

## Operational cost: two data paths require two completion signals

For media, the trade-off runs the other way. Direct upload removes application bandwidth and timeout pressure, but it introduces a pending state and a verifier. Proxying through the app can still be reasonable for small, highly transformed files where one synchronous request is operationally simpler. It is not suitable for large product videos when application instances would spend most of their time moving bytes they neither inspect nor retain.

| Path | Credential holder | Data path | Completion signal |
|---|---|---|---|
| Product media upload | Application issues a short-lived grant | Browser to private object storage | Verifier accepts expected object properties |
| Scheduled database archive | Trusted cron worker | Worker to private object storage | Object is recorded in the external ledger |
| Restore | Authorized recovery service issues a short-lived grant | Storage to isolated recovery host | Database opens and application checks pass |

Lifecycle policies can expire or transition objects according to rules, which is useful for retention automation. They do not define the business recovery objective, certify that a dump is usable, or replace a separately controlled copy. Set policy only after the team has written down recovery point and recovery time objectives. I'm not sure what those targets should be for your shop; order volume, regulatory duties, and the tolerated replay window resolve that question, not a generic storage default.

## Recovery evidence: test the artifact, schema, and application together

Even if Node.js triggers the cron workflow, verification belongs to a separate job and trust boundary. The production backup worker should not grade its own output. A green scheduler event proves that a callback ran; it doesn't prove the dump selected the intended database, the compressed stream is complete, the decryption material is available, or the current application can read the restored schema.

I've been paged for missed jobs and duplicate deliveries. The useful lesson is prosaic: monitor the artifact and the restore, not just the trigger. Record each expected schedule window, and alert when it closes without exactly one accepted run. A duplicate run is not automatically destructive when keys are unique, but it is still a signal that schedule ownership or retry semantics are unclear. A missing run is worse because there may be no storage-side error to inspect at all.

Run a restore drill on a cadence stricter than the time the organization is willing to remain uncertain. The drill selects a recent ledger record, requests temporary access to exactly that key, downloads into an isolated host, and compares the archive checksum before decompression. Stop on mismatch. After decompression, restore into a disposable database running a reviewed compatible engine version, apply no production traffic, and execute a compact set of application checks: expected migrations exist, representative orders and inventory rows can be read, foreign-key relationships hold where the schema defines them, and media references resolve to permitted object keys.

Then destroy the disposable environment and retain the drill result outside the backup bucket. Capture the selected run ID, object key, checksum result, schema revision, start and finish times, and the check that failed. Avoid a single "restore passed" counter; it hides which recovery assumption expired. Your mileage may vary on the exact application queries, but they should test business meaning rather than merely count tables.

Test restores.

The failure taxonomy should drive alerts and ownership. A missed schedule belongs to the scheduler or queue owner. An archive checksum mismatch belongs to the transfer path. A decompression failure belongs to artifact creation. A schema or application check failure belongs to the database and application owners together. Rate limiting, including HTTP `429`, should cause bounded retry with jitter and respect for server guidance; authorization failures should stop immediately and page the credential owner. Retrying every response with the same policy turns configuration errors into noisy load and delays the real diagnosis.

## Rollback decision: preserve evidence before changing production

Rollback starts before a restore command. Do not overwrite a questionable archive, edit its ledger record, or relabel it as successful. Mark the run rejected, preserve its identifiers for the postmortem, and create a new run with a new key if the source bytes change. If a retention policy is wrong, disable future deletion through the provider's reviewed configuration path and switch the recovery candidate to the independently controlled copy. Already expired data cannot be recovered by changing a future lifecycle rule.

Before replacing production state, require a named decision maker, a selected ledger record, checksum verification, enough isolated database capacity, a compatible engine and schema plan, validated encryption access, and written traffic-cutover steps. Restore alongside the primary first. Validate it. Pause writers only at the approved cutover point, account for changes after the chosen snapshot, and keep the old primary untouched until the acceptance checks finish.

This design has a real cost: two upload paths, a pending media state, an external ledger, and recurring restore drills. A small system with tiny files and no sensitive data may reasonably proxy uploads and use a simpler recovery process. Stick with direct presigned transfer when large e-commerce media would otherwise consume application bandwidth, and keep database archives on the trusted-worker path because delivery simplicity must not erase the access boundary.

## Further reading

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://firebase.google.com/docs/storage
