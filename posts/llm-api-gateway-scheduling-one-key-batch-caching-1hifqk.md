# LLM API Gateway Scheduling: One Key, Batch Caching, and Safe Catalog Replays

A catalog enrichment job can finish its model request and still leave the scheduler unsure whether the product update committed. That uncertainty changes how to compare a cheap LLM API gateway offering one key for batch work and caching. TL;DR: evaluate safe replay and provider replacement before optimizing token cost. Preserve product revisions and the publication contract across every route.

Consider this hypothetical production exercise: I schedule enrichment for product `SKU-418`, whose description reads “blue travel mug, 350 ml, lid included.” The worker receives extracted attributes, then loses its connection before recording completion. A replacement worker picks up the same job while a merchant changes the description to “green travel mug, 350 ml.” Which result may be published?

This is a design scenario, not an incident report. Its invariant is concrete: accept at most one publication for a particular enrichment operation, and accept it only while the source product revision is current. A repeated model call may still occur. A repeated catalog mutation must be prevented by the application.

## Can a cheap LLM API gateway with one key prevent duplicate catalog writes?

Keep the identity of the business operation separate from the identity of each execution attempt. For this design, derive the operation key from the tenant, product identifier, source revision, extraction schema version, and enrichment policy version. Give each attempt its own identifier for diagnostics. Changing the provider alone should not create a new business operation; intentionally rerunning an extraction with a different policy should.

That distinction determines what replay means. If the job for source revision 12 has already published, another execution of that operation returns an already-applied outcome. If the merchant has advanced the product to revision 13, a result for revision 12 is stale and cannot publish. If neither condition holds, a validated result can compete to commit. These are proposed application rules, not promises made by a gateway protocol.

Reject stale results.

The hard part is the commit boundary. Put the operation record and the attribute update in the same database transaction, enforce uniqueness on the operation key, and make publication conditional on the source revision. A worker checking “have I done this?” before a separate write leaves an interval in which another worker can reach the same answer. A lease can help distribute work; this design still requires the database to settle competing publications. Specify the ordering inside that transaction too. Check for an existing operation record first. Otherwise, verify the source revision, write the attributes, and insert the operation record atomically. An already-applied operation remains already applied even if the merchant subsequently edits the product. A stale operation with no publication record should trigger fresh scheduling against the current revision, not an endless retry of the old payload. For the example, publishing blue against revision 13 would violate the contract even if the extraction was accurate for revision 12. The scheduler must account for both the completed work and the merchant's intervening edit.

## Compare the execution paths by their recovery burden

For messy product descriptions, the useful comparison is where each execution path stores unfinished work and how an operator reconciles it. The following alternatives are architectural choices for this example. They do not describe any particular service's advertised capabilities.

| Execution path | Application responsibility | Reason to choose it |
| --- | --- | --- |
| Direct calls through provider adapters | Maintain adapter behavior, retry decisions, and result validation | Explicit control over provider differences |
| Calls through a shared gateway | Keep the operation ledger and verify each route's behavior | Centralize access while retaining application publication rules |
| Submitted batch work | Persist submission and item identifiers; reconcile each item separately | Process catalog backfills that can tolerate delayed completion |

For a scheduled catalog refresh, define a completion deadline before deciding between individual calls and batch submission. An overnight backfill and a merchant waiting for a corrected listing have different freshness requirements. Keep separate queues or scheduling policies if both workloads exist. Otherwise, a large backfill can consume the concurrency budget intended for urgent product edits.

One key should be treated as an access convenience in the evaluation.

Ask what happens to unfinished operations when a route changes, how attempt identifiers are exposed, and whether the application can retain its own deadlines. Verify the answers with a controlled replay exercise. Don't infer them from a compatible request shape.

Provider portability also needs a stable result contract. For the mug example, define allowed attributes, units, required evidence, and the treatment of unknown values. Preserve the original description alongside the operation record according to the application's retention policy. If one route returns “350 ml” and another returns an untyped string, the adapter must produce the same validated application representation or reject the result. Quiet coercion makes a provider change look successful while changing catalog meaning.

## Keep the preventative path close to the publication boundary

The Go example below sketches the worker boundary. Its interfaces deliberately leave transport and database implementation outside the example. `Commit` is a contract that must be implemented transactionally; a process-local map does not satisfy it across workers. The scheduler supplies the operation key under the identity rules above.

```go
package enrichment

import (
	"context"
	"fmt"
)

type Job struct {
	OperationKey  string
	Tenant        string
	SKU           string
	SourceRevision int64
	Description   string
}

type Attributes struct {
	Color      string
	CapacityML int
}

type Outcome uint8

const (
	Applied Outcome = iota
	AlreadyApplied
	StaleSource
)

type Extractor interface {
	Extract(context.Context, string) (Attributes, error)
}

type Publisher interface {
	// Commit atomically checks the operation key and source revision,
	// updates attributes, and records the operation when applying it.
	Commit(context.Context, Job, Attributes) (Outcome, error)
}

func Run(ctx context.Context, job Job, model Extractor, db Publisher) (Outcome, error) {
	attrs, err := model.Extract(ctx, job.Description)
	if err != nil {
		return 0, fmt.Errorf("extract %s: %w", job.OperationKey, err)
	}
	// This validator is deliberately limited to the travel-mug example.
	if attrs.Color == "" || attrs.CapacityML <= 0 {
		return 0, fmt.Errorf("invalid attributes for %s", job.OperationKey)
	}
	outcome, err := db.Commit(ctx, job, attrs)
	if err != nil {
		return 0, fmt.Errorf("publish %s: %w", job.OperationKey, err)
	}
	return outcome, nil
}
```

The caller must inspect the error before using the outcome. A publication error leaves the operation unresolved; the recovery path checks the durable operation record and can safely retry under the same key. A successful `AlreadyApplied` outcome completes the delivery. `StaleSource` completes the obsolete delivery and requests enrichment for the current revision under a new operation key.

Notice the deliberate limitation: this path can spend model work twice.

An optional durable lookup before extraction can avoid some redundant calls, but the transactional publication check remains necessary. If preventing duplicate external requests is itself a requirement, evaluate the endpoint's documented idempotency behavior separately and define recovery for an uncertain response. The catalog transaction alone cannot deduplicate an external service's computation.

For a broader catalog, replace the mug validator with category-specific validation and source-evidence checks. Positive capacity and a nonempty color are insufficient to prove an extraction matches a description. Validation policy belongs in the operation identity so a deliberate policy change can produce a new publication.

## Rehearse replay before changing a route

A release exercise should interrupt the workflow at specific boundaries: before the model response, after validation, and after the database commits but before the queue delivery is acknowledged. Also run two workers against the same operation and edit the product while an extraction is outstanding. The expected outcomes follow directly from the invariant: one accepted publication, an already-applied result for a replay, or rejection of an obsolete source revision. Keep these as acceptance conditions, not assumed test results.

Use a small fixture set that includes missing units, conflicting descriptions, and unsupported attributes. Compare validated outputs and rejection reasons across adapters. Exact text equality is a poor acceptance rule if different responses can express the same allowed attributes. Conversely, matching field names does not establish semantic equivalence.

Instrument the same boundaries. Record attempt count, accepted publication count, stale-result count, validation rejection count, and queue age separately. Keep the operation key available in controlled logs or traces so an operator can connect deliveries; use bounded dimensions for aggregate metrics.

A transport success isn't a published product update.

Streaming needs its own boundary. MDN describes server-sent events as event-stream messages with fields including `data`, `id`, and `retry`, and documents browser reconnection behavior. Those are transport concepts. In this design, partial streamed content stays outside the catalog, and publication waits for a complete, validated result. An event identifier is not the catalog operation key. If a Go adapter consumes such a stream, define its reconnect behavior explicitly rather than assuming browser behavior applies to that client.

For EU and US processing requirements, document the permitted locations for input descriptions, logs, caches, and retained batch output. Require evidence for the actual route under evaluation. An endpoint label or a shared credential is insufficient evidence for that deployment decision. Treat an unverified location requirement as an unresolved acceptance condition.

## Bound the advice before optimizing consumption

Token estimates, caching, and batching belong in the operating budget after the replay contract is defined. Keep each token estimate separate from reported usage and reconcile completed attempts, including attempts whose results were discarded. A cost comparison should count those discarded attempts too. For cached enrichment, include the source revision and extraction policy in the application cache identity. Validate a cache hit before publication. For submitted batches, preserve item-level operation keys so a partial rerun cannot become an untracked second catalog update. The same acceptance exercise belongs in a Node.js caller's release process; this note keeps the implementation example in Go.

The trade-off is additional state and reconciliation work. That burden may be unnecessary for a disposable preview whose output is never persisted, while workflows that trigger further external mutations need a stronger boundary than the catalog transaction shown here. Define those downstream effects explicitly before claiming the whole workflow is safe to replay.

For persisted catalog enrichment, make replay safety the first acceptance gate for a gateway change. Then compare validated output quality, completion deadlines, regional evidence, and measured consumption on the same fixture set. The durable decision is whether a provider can be replaced while the product publication rules remain enforceable.

## References

The source below supports the event-stream discussion. The scheduling rules, interfaces, fixture values, and acceptance conditions in this note are an illustrative application design; they are not claims about a commercial gateway's behavior.

## Sources

- MDN, Using server-sent events: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
