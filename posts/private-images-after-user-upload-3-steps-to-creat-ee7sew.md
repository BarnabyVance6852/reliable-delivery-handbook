# Private Images After User Upload: 3 Steps to Create Object Storage Thumbnails

Store the original first, acknowledge the upload second, and create the thumbnail asynchronously. For private logistics images, the upload request should never carry the full cost of downloading, decoding, resizing, and writing a large signed document.

Short answer: let a Next.js API route validate an object key and enqueue an idempotent job after the user upload; let a bounded Node.js worker use Sharp to read the private object, create the thumbnail, and write it under a deterministic private key with the same deletion deadline as the original.

This rule is about failure containment, not framework fashion. A proof-of-delivery scan may be needed for a dispute, yet it must disappear on an explicit date. Mixing upload, transformation, and retention into one request creates an awkward outcome: the user sees a timeout while some writes may already have succeeded. Keep the stages observable and independently retryable.

Treat the original as the record and the thumbnail as a reproducible derivative. A direct, time-limited upload flow keeps large image bytes away from the application process: the API authorizes a specific object operation, the client uploads to object storage, and a completion request carries metadata rather than the file body. Presigned URLs are bearer capabilities, so their lifetime and permitted object key deserve the same care as any other credential.

The completion request should identify an immutable upload attempt, not merely a mutable filename. For a logistics example, use fields such as `shipment_id`, `document_id`, `source_key`, `content_type`, and `delete_at`. The server verifies that the authenticated tenant owns the expected key, records the deletion deadline, and publishes a job whose identity is derived from the document plus transformation version. It doesn't accept a destination key chosen by the browser.

The alarming signal is not a slow web request. It is a derivative job whose queue age is consuming the time left before deletion. Consider `doc-73`, uploaded at 14:00 with `delete_at` at 18:00. Its first transform attempt starts at 14:02, writes `thumb-v3.jpg`, and loses its worker lease before the catalog update. A redelivery at 14:07 must inspect the same job identity and target the same key; creating a new name would leave an untracked private image behind. Now move that redelivery to 17:59:59. Even if the resize could finish quickly, the worker must treat the deadline as the stronger instruction, decline publication, and let deletion own both keys. This example has no exotic outage in it — only ordinary at-least-once delivery and two operations completing in an inconvenient order. The runbook therefore watches queue age beside expiry distance and routes close-to-expiry work toward deletion, not toward an increasingly desperate resize attempt.

Three states are enough for the public operational model:

| State | Required evidence | Allowed next action |
| --- | --- | --- |
| Original stored | Verified source key and `delete_at` | Enqueue one transform version |
| Derivative pending | Stable job ID and attempt record | Retry the same target key or expire |
| Derivative ready | Target key and transform version | Serve with authorization or delete |

Failed attempts remain pending with an attempt count and next eligible time; they don't become a new public state.

Boring survives retries.

Object access stays private at both keys. A reader asks the application for authorization, then receives a short-lived read capability for the exact original or thumbnail it may view. The thumbnail isn't harmless just because it is small: a signature, address, barcode, or loading-dock image can still expose sensitive shipment data.

## How does a Next.js API route hand Sharp thumbnail work to Node.js after upload?

It shouldn't do the resize before returning success. The route is the coordinator: authenticate, validate the completion claim against object metadata, insert or confirm the job, and answer. The Sharp process belongs in a worker with explicit concurrency and memory limits. This division also lets a serverless route stay short-lived while a large input is processed under a runtime chosen for image work.

The safe write rule is `(source identity, transform version) -> one derivative key`. For example, `tenant-a/documents/doc-73/original` can map to `tenant-a/documents/doc-73/thumb-v3.jpg`. A retry writes the same result, and a queue redelivery can't create `thumb-v3-copy-2.jpg`. If the transformation changes, increment the version rather than overwriting the meaning of an old key.

There is one important race. A retention task can delete the original while a delayed thumbnail job is starting. Make deletion state authoritative: the worker loads the document record before reading, refuses work at or beyond `delete_at`, and rechecks the deadline immediately before publishing the derivative. The deletion workflow removes both the original prefix and every known derivative, then records completion. Storage lifecycle rules are useful defense in depth, but the application record remains the place to explain why a document exists and when it must go.

The following Go sketch shows the coordinator contract around the Sharp worker. All storage and queue operations are interfaces on purpose; the reliability boundary matters more than a vendor SDK. The actual Node.js process receives `TransformRequest`, uses Sharp with a fixed output policy, and reports the object metadata it wrote.

```go
package thumbnails

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrExpired = errors.New("document retention deadline reached")

type Document struct {
	TenantID  string
	ID        string
	SourceKey string
	DeleteAt  time.Time
}

type TransformRequest struct {
	JobID      string
	SourceKey  string
	TargetKey  string
	DeleteAt   time.Time
	MaxWidth   int
	MaxHeight  int
	OutputType string
}

type Catalog interface {
	FindDocument(context.Context, string, string) (Document, error)
	MarkReady(context.Context, string, string, string) error
}

type Transformer interface {
	Create(context.Context, TransformRequest) error
}

type Worker struct {
	Catalog     Catalog
	Transformer Transformer
	Now         func() time.Time
}

func (w Worker) Run(ctx context.Context, tenantID, documentID string) error {
	doc, err := w.Catalog.FindDocument(ctx, tenantID, documentID)
	if err != nil {
		return fmt.Errorf("load document: %w", err)
	}
	if !w.Now().Before(doc.DeleteAt) {
		return ErrExpired
	}

	target := fmt.Sprintf("%s/thumb-v3.jpg", doc.SourceKey)
	req := TransformRequest{
		JobID:      tenantID + ":" + documentID + ":thumb-v3",
		SourceKey:  doc.SourceKey,
		TargetKey:  target,
		DeleteAt:   doc.DeleteAt,
		MaxWidth:   480,
		MaxHeight:  480,
		OutputType: "image/jpeg",
	}
	if err := w.Transformer.Create(ctx, req); err != nil {
		return fmt.Errorf("create derivative: %w", err)
	}
	if !w.Now().Before(doc.DeleteAt) {
		return ErrExpired
	}
	return w.Catalog.MarkReady(ctx, tenantID, documentID, target)
}
```

The catch is that a separate worker adds a queue, a job table, and deployment work. Inline resizing can be suitable for tiny, tightly bounded internal uploads where the request runtime, memory ceiling, and worst-case decode cost have been measured. Stick with the asynchronous design when files are large, bursts are normal, or a retry must not ask the user to upload a signed document again. I'm not sure what concurrency fits your fleet; only a load test with the real image distribution and memory limit can resolve that.

## Budget bandwidth, memory, and expiry slack

Limit work at admission and at execution. The API should cap accepted source size and content type before enqueueing, while the worker should cap concurrent transforms, decoded dimensions, download bytes, and elapsed time. File size alone is a weak proxy for decode memory. Rejecting an input after it has consumed the whole worker is too late.

Use a small initial concurrency, then raise it from evidence. Watch queue age, job duration, resident memory, source-read throughput, derivative-write throughput, retry counts, and the number of documents approaching `delete_at` without a completed deletion record. Queue depth alone lies during bursts — an old head-of-line job is the signal that customers are waiting.

Backpressure should be visible. When pending work exceeds the service's operating envelope, stop admitting optional reprocessing and preserve capacity for new signed documents and deletion. Don't let thumbnail freshness compete with legal retention execution in an unbounded shared queue. Separate priority or worker pools if one workload can starve the other.

Bandwidth also changes the decision. Sending a multi-megabyte original through the application and then back to storage doubles application-path transfer and holds connections open. Direct upload plus worker-side reads removes that application relay, though it does not eliminate storage traffic. Benchmark with the actual median and tail object sizes; a folder of polished sample JPEGs won't represent phone scans, multi-page conversions, or poorly compressed evidence photos.

## Deploy, verify, and roll back the thumbnail worker

Test the state machine, not just the happy-path pixels. A good fixture set includes the same completion event delivered twice, a worker termination after the derivative write but before `MarkReady`, a job starting one second before expiry, an already expired document, a source whose declared type disagrees with its bytes, and an image whose decoded dimensions exceed policy. For each case, assert the final keys, database state, and retention timestamp.

Then run a canary with the new transform version. Compare dimensions, orientation, output type, and a content digest for repeat runs of the same fixture. Track the version in both the target key and metadata so an operator can answer which policy produced an artifact without opening it. The canary should use private read credentials and should prove that unauthenticated reads fail.

Set two alerts with different owners: derivative queue age for the media path, and overdue deletion count for retention. The first affects convenience and browsing speed. The second affects the promise made about signed documents. They aren't the same severity.

Rollback changes the active transform version; it never asks for another original. Pause admission for the bad version, leave its deterministic objects quarantined from reads, deploy the prior worker policy, and enqueue the still-retained originals under the prior version key. Because the original is immutable and the job identity includes the version, this recovery is a controlled replay rather than guesswork.

Do not replay anything past its deadline.

If the source has already been deleted, mark the derivative request expired and finish the deletion record; restoring from a backup would violate the same retention boundary unless a separately authorized hold applies. The final runbook check is simple: no readable derivative without an authorized original record, and no original or derivative after the effective deletion workflow completes.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://cloud.google.com/storage/docs
