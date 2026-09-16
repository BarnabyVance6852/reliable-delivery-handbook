# Email Deliverability API Comparison (DKIM Rotation, Suppression, Events, and Compliance)

For an e-commerce report attachment, an email deliverability platform comparison should start at the API integration boundary: choose the option whose domain verification, DKIM rotation, suppression, and event flow fit behind one adapter and can prove that one logical report produces one delivery.

Short answer: choose the email platform whose domain verification, DKIM rotation, suppression, and event interface you can put behind one small adapter, then prove that adapter against duplicate requests and delayed events before comparing secondary features.

Integration effort is the deciding constraint here. A broad feature sheet does not help much if a team has to maintain separate state machines for domain setup, message submission, suppression checks, and webhook recovery. For an EU and US SaaS product, the evidence needed for a compliance review must also be collectable without turning the mail provider into the system of record.

I've been paged for missed jobs and duplicate deliveries. One representative production scenario starts with a scheduled job that renders a merchant's daily order report, attaches it to an email, and submits the message. The scheduler loses the acknowledgement and retries. Both attempts are valid from the transport's point of view, so the merchant receives the same report twice; in the inverse case, the job records "sent" immediately after submission, no terminal delivery event is correlated, and the on-call engineer cannot distinguish a delayed message from a workflow that stopped progressing. Finance may import the attachment twice. Support sees a green job while the merchant sees nothing. The retry intended to repair uncertainty creates another externally visible action. I used to treat the send call as the important boundary; production paging corrected that model. The durable boundary is the locally owned delivery attempt, identified before any network call and reconciled afterward.

One report. One attempt.

That gives us an invariant: one logical report has one stable idempotency key, while each submission and each event remains auditable. The application owns the report ID, recipient, template version, attachment digest, consent basis, submission state, and normalized delivery state. The email service transports the message and reports observations. Don't reverse those roles.

For bulk senders, mailbox policy belongs in this model too. Yahoo's sender guidance calls for authenticating mail, honoring unsubscribes, keeping complaint rates low, and avoiding unsolicited traffic. Those are not launch-day checkboxes. They are operating conditions that should be visible in dashboards and runbooks.

## Domain verification starts with a disposable rehearsal

Start with a scripted proof, not a procurement spreadsheet. Create a non-production sending domain, ask the platform for the required DNS records, publish them through the same controlled process used in production, and poll verification until it reaches a documented terminal state. Record every transition with the domain, environment, request ID, and timestamp. The useful comparison is how little provider-specific state leaks into your application and deployment process.

DKIM rotation deserves its own rehearsal. The system should allow old and new selectors to overlap while DNS propagates, expose enough state to distinguish "record not observed" from "ready," and let operators prove which selector signs current mail. Do not assume rotation is a one-click control-plane action. Your test is complete only after a received message can be inspected and the new signature can be associated with the planned change. The exact propagation window depends on DNS caching and the platform's procedure, so your mileage may vary; measure it in the environment you will operate.

## How should a SaaS team structure an email deliverability platform API comparison?

The adapter below shows the boundary I want. It is intentionally small: no vendor route appears in business logic, submission does not mark a report delivered, and a normalized event advances local state. The repository methods must be transactional in a real service, particularly `Reserve`, because two workers can race before either sends.

```go
package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var ErrAlreadyReserved = errors.New("delivery already reserved")

type Message struct {
	AttemptID       string
	Recipient       string
	Subject         string
	HTML            string
	AttachmentName  string
	AttachmentBytes []byte
}

type Receipt struct {
	ProviderMessageID string
}

type Event struct {
	ProviderMessageID string
	Kind              string
	OccurredAtUnix    int64
}

type Mailer interface {
	Submit(ctx context.Context, message Message) (Receipt, error)
}

type Attempts interface {
	Reserve(ctx context.Context, attemptID, attachmentDigest string) error
	MarkSubmitted(ctx context.Context, attemptID, providerMessageID string) error
	ApplyEvent(ctx context.Context, event Event) error
}

func DeliverReport(ctx context.Context, repo Attempts, mailer Mailer, message Message) error {
	digest := sha256.Sum256(message.AttachmentBytes)
	if err := repo.Reserve(ctx, message.AttemptID, hex.EncodeToString(digest[:])); err != nil {
		if errors.Is(err, ErrAlreadyReserved) {
			return nil
		}
		return err
	}

	receipt, err := mailer.Submit(ctx, message)
	if err != nil {
		return err
	}

	return repo.MarkSubmitted(ctx, message.AttemptID, receipt.ProviderMessageID)
}

func HandleDeliveryEvent(ctx context.Context, repo Attempts, event Event) error {
	return repo.ApplyEvent(ctx, event)
}
```

There is a sharp edge here — returning success for an existing reservation is correct only if another worker owns a recoverable attempt. A production repository therefore needs a lease or an outbox, plus a reconciler that finds reservations which never gained a provider message ID. The retry policy should resend only when local state proves that doing so cannot duplicate an accepted submission. If the provider offers an idempotency token, pass the same attempt ID through the adapter, but do not make that the only defense.

Event ingestion needs the same discipline. Authenticate incoming events using the platform's documented scheme, persist the raw payload before acknowledgement, deduplicate on a stable event identifier when one exists, and make state transitions monotonic. A late "processed" event must not move a message backward from "delivered." Polling can reconcile gaps, but it should call the adapter and produce the same normalized event shape as a webhook. Two ingestion paths, one state machine.

Acceptance is not delivery.

Suppression is a send-time gate and an event-driven update. Check the local suppression view before reserving a report, then update it from bounce, complaint, and unsubscribe observations. A provider-maintained suppression list may add protection, but the application still needs a portable record of why a recipient became ineligible and which policy can remove that status. Otherwise a migration can quietly reactivate addresses that should remain suppressed.

Template rendering belongs before reservation. A logic-less system such as Mustache makes the input contract inspectable, but it does not validate that required values are meaningful or that an attachment is the correct report. Render, validate required fields, compute the attachment digest, and store the template version before submission. Then an operator can reconstruct what was intended without keeping the mail platform as permanent evidence.

## Govern compliance evidence as production state

For EU and US SaaS compliance, compare evidence rather than labels. Ask where message content, recipient addresses, event payloads, and support-access logs are processed or retained; how deletion is performed; which contractual terms apply; and whether region choices cover every part of the path. I'm not sure any generic "EU-ready" claim answers a particular company's legal obligations. Counsel and the security owner have to map the documented data flow to the actual customers, contracts, and configured regions.

Keep that review separate from deliverability scoring. A platform can fit a data-handling requirement yet create too much integration work, or provide a tidy API while failing a contractual constraint. Both are disqualifying, but for different owners and with different evidence. Store the review date, approved regions, retention expectation, and evidence links alongside the integration's operational metadata so a later configuration change has an owner and a review path.

The platform categories create different integration work. None wins in every system.

| Operating model | Integration advantage | Cost you still own | Better fit when |
| --- | --- | --- | --- |
| Transactional email API | Message and event concepts are usually presented as one product boundary | Adapter semantics, suppression portability, and evidence export | A small team wants a narrow mail-specific control plane |
| General cloud email service | Identity and infrastructure controls may align with an existing cloud account | More assembly around templates, events, and operator tooling | The organization already operates that cloud deeply |
| Self-hosted mail transfer stack | Maximum control over processing and data location | Reputation, abuse handling, upgrades, observability, and on-call load | Mail operations are a deliberate core competency |

Use HTTP status classes only as transport signals. An accepted response means the submission was accepted under that API's contract; it does not prove inbox placement. A rate-limit response means the worker should follow the documented retry guidance without creating a fresh logical attempt. A validation response should stop automated retries and surface the rejected field. Keep the exact mapping inside the provider adapter because status details and event vocabulary are provider-specific.

Observability should answer three questions without opening a vendor console: Which report attempts are stuck in each state? Which sending domain or template version changed before the trend moved? Which recipients are suppressed, and why? Track queue age, time from reservation to submission, time from submission to terminal event, event-ingestion lag, duplicate-event count, and reconciliation backlog. Percentages need denominators and traffic segmentation; one aggregate delivery rate can hide a domain-specific problem.

## An acceptance drill measures recurring integration effort

Run the same acceptance fixture against every serious candidate. It should verify a domain in a disposable environment, rotate a DKIM selector under a documented change plan, send the same generated report fixture, ingest an event twice, suppress a recipient, attempt another send, and export the audit record. Score engineer-hours for initial work and recurring operations separately. A low-effort demo can conceal an expensive rotation or incident workflow.

Before launch, rehearse a lost submission acknowledgement, a duplicate event, a delayed event, a DNS change, and a suppression update racing with a scheduled report. Give the on-call engineer one documented action for each state. If the only instruction is "check the provider dashboard," the application has outsourced its diagnosis but retained the pager.

The catch is that the adapter-and-local-ledger approach is not suitable for every workload. Stick with a simpler direct integration when email is low consequence, duplicates are harmless, there is no attachment workflow, and the team can accept manual reconciliation. At the other extreme, choose a self-operated stack only when control over the full mail path justifies deliverability expertise and permanent operational ownership. For the usual e-commerce report, the middle path is calmer: a replaceable API adapter, a transactional local record, an outbox or lease, authenticated event ingestion, and a tested reconciliation job.

Make the final selection from the proof artifacts. The best fit is the option that meets the documented data-handling constraints and leaves the least provider-specific logic outside the adapter while preserving enough evidence to debug one missing or duplicated report. Feature breadth comes after that.

## Sources

- https://mustache.github.io/mustache.5.html
- https://senders.yahooinc.com/best-practices/
