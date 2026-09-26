# Auditable Transactional Email Setup for Signup Domains (with Bounce Suppression)

Send a signup verification link only after the sending domain passes an automated SPF, DKIM, and DMARC readiness gate, and record every later state transition in an append-only delivery ledger. The deciding constraint is compliance evidence: a provider's accepted response proves intake, not delivery, while a click proves control of the mailbox but says little about how the message traveled. Treat submission, authentication, bounce handling, suppression, and verification as separate facts.

**TL;DR:** use a stable message ID and signup ID, suppress known bad recipients before enqueueing, make submission idempotent, poll only as a bounded reconciliation path, and retain normalized events with their raw-source digest. Do not use opens as the verification signal. Apple Mail Privacy Protection can download remote content in the background, so an open pixel is not reliable proof that a person read the message.

Evidence first.

## How should a Node.js transactional email setup verify its sending domain?

Start with the claim an auditor or incident commander may need to test. For an e-commerce signup, the useful claim is usually: the system sent one time-limited verification challenge to the address supplied for account A, authenticated the message under the controlled domain, stopped retries after a terminal failure, and activated the account only after the challenge was redeemed. That is narrower than claiming inbox placement. It is also defensible.

I use 4 evidence layers because their failure domains differ. Configuration evidence records the DNS names and observed authentication state at deployment time. Submission evidence records the stable internal message ID, recipient hash, template revision, challenge expiry, and remote receipt ID. Transport evidence records normalized delivered, delayed, bounced, complained, or suppressed events. Application evidence records challenge redemption without storing the token itself. This separation also makes a deliverability review useful: it shows whether a weak result belongs to DNS configuration, the submission API, transport, or the signup application instead of compressing every failure into “email did not arrive.”

Short records beat screenshots. A screenshot ages immediately and cannot be joined to a queue attempt during an incident. Store timestamps in UTC, preserve the provider event identifier, and hash the untouched payload before normalization. Restrict access to raw recipient data and define retention from the applicable policy; this article cannot choose that duration for your jurisdiction.

SPF alone is insufficient for the claim. DMARC evaluates alignment between the visible RFC 5322 From domain and an authenticated SPF or DKIM domain, then applies the published policy. In practice, DKIM alignment usually survives forwarding better than SPF because forwarding changes the SMTP path, but both authentication paths should be observable. Publish DMARC deliberately, collect reports, and move enforcement only after legitimate senders are accounted for.

## Gate the domain before traffic

Make domain readiness a release condition, not a dashboard somebody remembers to inspect. The gate should query authoritative DNS through a normal resolver path, confirm the expected records exist, and compare the mail system's reported verification state with the deployment's intended domain. Cache the evidence snapshot with the release identifier.

Do not infer DMARC success merely from the presence of a TXT record. The receiver performs DMARC evaluation on a message, using SPF and DKIM results plus identifier alignment. DNS inspection is configuration evidence; sampled authentication results from received mail are runtime evidence. Keep those labels distinct.

Those are different claims.

The operational sequence is small:

1. Publish and independently resolve the DKIM selector, SPF policy, and DMARC policy.
2. Send a canary through the same queue, signing path, and return path as production signup mail.
3. Inspect the received authentication results and correlate the canary's internal ID with its transport event.
4. Enable signup traffic only when configuration and canary evidence agree.

One trap is rotating a selector and removing the old public key as soon as new workers deploy. Queued mail may still carry the old signature. Keep the previous key resolvable until the maximum queue and retry window has elapsed, based on your own system's configured bounds.

## Build one idempotent path from signup to suppression

A verification request should enter a durable queue after the signup transaction commits. The worker checks a suppression store, claims an idempotency key, creates the remote submission, and records the receipt. Event ingestion updates the ledger and suppression state. A scheduled reconciler polls only submissions that lack a terminal event after the normal event-delivery window.

Retries are where duplicate links appear. Use a deterministic key derived from the signup and challenge generation, not from a worker attempt. If the submission call times out after the remote system accepted it, the next attempt must recover the prior receipt or repeat the same idempotent operation. Never mint a fresh challenge merely because transport acknowledgment was ambiguous.

One identity. Many attempts.

This focused Go sketch shows the boundary. The same contract can sit behind a Node.js signup service; keeping the queue worker isolated makes the language choice irrelevant to the evidence model.

```go
package mailflow

import (
    "context"
    "errors"
    "time"
)

type Job struct {
    SignupID   string
    MessageID  string
    Address    string
    Link       string
    ExpiresAt  time.Time
    TemplateID string
}

type Receipt struct {
    RemoteID   string
    AcceptedAt time.Time
}

type Sender interface {
    Submit(ctx context.Context, idempotencyKey string, job Job) (Receipt, error)
}

type Suppressions interface {
    Blocked(ctx context.Context, address string) (bool, error)
}

type Ledger interface {
    Begin(ctx context.Context, messageID string) (alreadyExists bool, err error)
    RecordSuppressed(ctx context.Context, job Job, at time.Time) error
    RecordAccepted(ctx context.Context, job Job, receipt Receipt) error
}

func Deliver(ctx context.Context, now time.Time, job Job, sender Sender, blocks Suppressions, log Ledger) error {
    if !now.Before(job.ExpiresAt) {
        return errors.New("verification challenge expired before submission")
    }
    blocked, err := blocks.Blocked(ctx, job.Address)
    if err != nil {
        return err // Fail closed until suppression state is readable.
    }
    exists, err := log.Begin(ctx, job.MessageID)
    if err != nil || exists {
        return err
    }
    if blocked {
        return log.RecordSuppressed(ctx, job, now)
    }
    receipt, err := sender.Submit(ctx, job.MessageID, job)
    if err != nil {
        return err
    }
    return log.RecordAccepted(ctx, job, receipt)
}
```

The minimal state machine is `queued -> accepted -> delivered`, with `delayed`, `bounced`, `complained`, `suppressed`, and `expired` handled explicitly according to source semantics. Do not pretend every source uses the same vocabulary. Preserve the original type, map it to an internal enum, and reject impossible regressions such as `bounced -> delivered` unless the source documents event reordering and the ledger keeps both observations.

A hard bounce or complaint should prevent another automated send to the same normalized address under the policy your organization has approved. A transient deferral should not automatically become permanent suppression. That trade-off matters: aggressive suppression can strand a legitimate signup after a temporary failure; weak suppression repeats delivery to an address already known to reject mail. Record the reason and source so an authorized recovery process can distinguish them.

## Poll for gaps, not as the primary event stream

Webhook-style delivery events are the fast path; polling is repair. The reconciler should select accepted records whose event deadline has passed, query the status API by remote receipt ID, append newly observed states, and stop at a terminal state or a fixed reconciliation deadline. Add jitter and a concurrency cap so a delayed event stream does not turn into a synchronized query spike. I choose bounded polling over permanent polling because an old ambiguous receipt must eventually become an explicit evidence gap rather than consume query capacity forever.

After pages for missed jobs and duplicate deliveries, my runbook question is blunt: can the on-call engineer tell whether the user needs a new challenge, the queue needs replay, or the evidence pipeline is merely late? One status field cannot answer that. Track queue age, submission age, challenge expiry, and last evidence time separately.

Keep the alert set small. Page on sustained inability to submit verification mail, a growing oldest-queued age, or failure to read suppression state. Ticket delayed evidence reconciliation when submission remains healthy and challenges are still redeemable. Delivery events can arrive out of order, so use event time plus ingestion time and never overwrite history with a single latest-status row.

There is another misleading signal: opens. Apple documents that Mail Privacy Protection prevents senders from learning Mail activity by downloading remote content privately in the background. An open event therefore must not activate an account, clear a retry, or serve as compliance proof of human readership. Redemption of the signed, single-use challenge is the application signal.

## Verify the rollout and make rollback boring

Test the transitions, not a provider mock's happy response. Before release, exercise a valid address, an intentionally rejected test address supported by the chosen mail system, duplicate queue delivery, a submission timeout after acceptance, a late transport event, an expired challenge, and an unavailable suppression store. Assert ledger rows and account state for every case. No guesswork.

Deploy with a canary domain or a small traffic slice only if it follows the production signing and event paths. Compare accepted submissions with terminal evidence by age bucket; a raw delivered percentage hides fresh messages that have not had time to settle. Also sample received headers to verify DMARC alignment after any DNS, return-path, or signing change.

Rollback should stop new worker consumption while leaving event ingestion alive. Reverting the application must not delete DNS keys, discard outstanding receipt IDs, or requeue every accepted job. Resume only jobs whose idempotency keys have no accepted receipt. For challenges near expiry, create a new generation through the normal signup policy and invalidate the old generation atomically.

The final readiness decision is straightforward: ship when the domain gate is reproducible, duplicate attempts converge on one message identity, suppression is consulted before submission, reconciliation is bounded, and the ledger can connect a signup to challenge redemption without treating acceptance or opens as delivery proof. Throughput comes later. Evidence comes first.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- Apple, Use Mail Privacy Protection on iPhone: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
