# Node.js PDF Dispatch: Password Verification and Template Approval Before Customer Delivery

For a B2B SaaS contract service, keep the release rule small: email becomes eligible only after the owned template revision, final artifact, and encryption verification are bound to the same dispatch record. Consider the page an on-call engineer should receive: a contract is due for customer delivery, but the record has no verified encrypted attachment. The useful evidence is the contract revision and blocked stage, with a link to restricted audit details. A queue depth alone cannot explain that page.

**Short answer:** To password-protect a PDF through an API before emailing a customer from Node.js, put encryption and verification before the mail release gate. Keep password access separate from attachment delivery. Make template ownership determine who can approve the document revision, then preserve that approval through signing, encryption, and dispatch. Treat every retry as a request to finish the same delivery intent.

This note proposes a control design, not an incident report or a claim about a particular service. ISO 32000-2 is the reference for the PDF format; the scheduling policy, example identifiers, and release checks below are design choices. Any implementation still needs evidence that its chosen PDF processor produces the required encrypted output and that the recipient can open it.

## What should have fired before the delivery page?

The earlier signal is a blocked transition: an approved contract has reached the point where it should be prepared for delivery, but no verified artifact exists for its current revision. Instrument that state directly. A scheduler reporting that a job ran only establishes that something was invoked. It doesn't establish that the attachment is ready.

Start with the contract.

Use a proposed example, contract `contract-1042`, tenant `tenant-17`, and template revision `msa-7`. Suppose the delivery intent names an approved snapshot of that template and a specific recipient binding. A worker produces an artifact, and the encryption stage reports success. Before release, require verification evidence tied to that artifact's digest and the intended password reference. If the worker's reply contains a digest for an earlier revision, keep the intent blocked even though the job itself completed. This is the failure to look for: success at the worker boundary with no valid evidence at the document boundary.

Separate deadline risk from ordinary preparation time. A warning can identify aging work while there is still time to inspect it; a page should correspond to a delivery commitment that needs human action. Choose those intervals from the service's actual commitments and measured preparation times. No universal timeout follows from the PDF standard.

## Template ownership decides which bytes may leave

Password protection answers an access question. Template ownership answers a different operational question: who can authorize the contract that the access control protects? Encrypting an unapproved revision gives the release process no evidence that the right contract was prepared.

For application-owned templates, require an application review to publish a new revision. For templates managed by a legal or operations team, provide an explicit approval transition and record its authority. Both arrangements can support this design. The trade-off is release coupling: application ownership makes changes travel with software deployment, while delegated ownership needs a separately enforced approval boundary. Pick the arrangement that matches who can accept responsibility for contract language. A convenient editor is insufficient evidence of that authority.

Freeze the approved input for a delivery intent. Record the tenant, contract revision, template revision, recipient binding, and approval event. A later template edit should create a new candidate revision rather than silently alter an already approved dispatch. Keep sensitive contract text out of general logs; store a restricted audit record and expose only the identifiers needed to investigate the transition.

The audit trail must make the relationship legible. An approval event points to a template snapshot. A signing event points to the applicable document revision. Encryption verification points to the final attachment. Dispatch points to that same attachment and recipient binding. These are proposed relationships to enforce, not properties to assume from an API success response.

Signing order deserves its own acceptance test. Specify exactly which artifact the signature covers and verify that the final encrypted deliverable retains the signature behavior the workflow requires. Don't assume that an arbitrary transformation after signing preserves it. Resolve that question with the chosen processor and signature validator before enabling dispatch.

## What should the API verify before emailing a password-protected PDF?

In a Node.js application, keep the orchestration boundary explicit even if separate workers perform document processing. Submit an approved revision to the processing API, retain its operation reference, collect the output into controlled artifact storage, and verify the result before creating a sendable dispatch. The following Go example defines the release predicate shared by that workflow; it deliberately leaves PDF processing and storage behind their own implementation boundaries.

```go
package dispatch

import "errors"

type Evidence struct {
	TemplateRevision string
	ArtifactDigest   string
	RecipientBinding string
	PasswordRef      string
	Verified         bool
}

type Intent struct {
	TemplateRevision string
	ArtifactDigest   string
	RecipientBinding string
	PasswordRef      string
	Approved         bool
	SignatureChecked bool
}

func CheckRelease(i Intent, e Evidence) error {
	if !i.Approved || !i.SignatureChecked {
		return errors.New("approval or signature check missing")
	}
	if i.TemplateRevision == "" || i.ArtifactDigest == "" ||
		i.RecipientBinding == "" || i.PasswordRef == "" {
		return errors.New("release binding incomplete")
	}
	if !e.Verified || e.TemplateRevision != i.TemplateRevision ||
		e.ArtifactDigest != i.ArtifactDigest ||
		e.RecipientBinding != i.RecipientBinding ||
		e.PasswordRef != i.PasswordRef {
		return errors.New("encryption evidence does not match intent")
	}
	return nil
}
```

The predicate checks 4 bindings: template revision, artifact digest, recipient binding, and password reference. It doesn't encrypt a PDF, validate a signature, or establish that a verification flag is trustworthy. Only the restricted verification stage should be allowed to write that evidence. Its acceptance checks should include opening the final artifact with the intended secret, rejecting an incorrect secret, and confirming the required document and signature behavior. Keep plaintext passwords out of the evidence record.

A successful predicate is also not a concurrency lock. Evaluate it while atomically reserving the dispatch intent, using the storage system's supported transaction or conditional-write mechanism. Otherwise two workers can both pass the check and proceed to send. Define the idempotency identity around the tenant, contract revision, recipient binding, and explicit delivery intent; a deliberate resend needs a new intent with a recorded reason.

A timeout is ambiguous.

If the mail handoff times out after submission, preserve the attempt reference and reconcile its outcome through whatever evidence the mail boundary actually provides. Don't automatically reinterpret an unknown outcome as permission for a fresh delivery. If reconciliation cannot establish the result, move the intent to a reviewable state. That costs operator time, but it avoids treating uncertainty as proof that no send occurred. Neither this predicate nor a queue acknowledgment establishes exactly one customer delivery.

For observability, emit counters for blocked transitions, verification outcomes, and reconciled attempts. Track the oldest eligible intent still waiting at each stage. Keep high-detail contract identifiers in restricted event records, so the on-call engineer can follow a single dispatch without placing document contents or password material in ordinary telemetry. The instrumentation change is concrete: replace a lone job-success signal with evidence of each transition required for release.

## Prove the boundary before changing production traffic

Use an acceptance matrix that targets the release decision:

| Test input | Required result |
| --- | --- |
| Approved revision with matching verification evidence | Eligible for dispatch reservation |
| Superseded template revision | Blocked pending a new approved intent |
| Mismatched recipient binding | Blocked |
| Incorrect opening password | Verification fails |
| Repeated dispatch attempt | Reconcile the existing intent |
| Timeout after mail submission | Unknown outcome goes to reconciliation |

These tests target business failures that a successful encryption response cannot rule out.

Deploy a new verifier in observation mode first, against restricted test artifacts, and compare its decisions with the existing release policy before allowing it to authorize customer sends. A rollback should stop new authorizations while preserving existing evidence and attempt records. Avoid regenerating every pending document as a rollback side effect: that would change the artifacts an operator is trying to reconcile.

There is a storage and operational cost to keeping immutable revisions and verification evidence. Define retention with the people responsible for the contract audit requirement, and test that an investigation can retrieve the needed relationships. Expensive encryption work also needs bounded concurrency; otherwise a backlog can consume the capacity needed for reconciliation. Budget those resources from measured workloads rather than a quoted per-document price.

## Page on an actionable delivery risk

Close the loop at the alert. Its runbook should identify the blocked intent, show the last trusted transition, and direct the operator to verify approval, artifact binding, verification evidence, and any unresolved send attempt before authorizing further action. A routine template approval wait belongs with the team that owns that approval. A delivery deadline at risk belongs with the responder who can restore progress or communicate the delay.

Keep the threshold honest. An alert that fires on every short encryption wait turns normal queue variation into interruptions; an alert that waits until every contract is late leaves no recovery window. Measure warning-to-resolution time, count pages that required no action, and adjust against actual delivery commitments. Raising a threshold to suppress noise is a trade-off only when the resulting delay is still acceptable.

The release rule remains simple: approved revision, verified attachment, traceable dispatch. Tune the page around that rule. Too low a threshold consumes attention on ordinary waiting; too high a threshold hides the first opportunity to intervene.

## References

ISO 32000-2, Portable Document Format, is the format reference. The release workflow and example code above are proposed application controls; this source does not prescribe their scheduling thresholds or audit retention policy.

## Further reading

- ISO 32000-2 — Portable Document Format: https://www.iso.org/standard/75839.html
