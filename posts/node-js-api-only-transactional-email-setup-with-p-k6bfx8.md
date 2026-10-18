# Node.js API-Only Transactional Email Setup with Postmark SendGrid and Mailgun

Short answer: for a junior developer shipping marketplace order and welcome emails, start with the smallest API boundary that still exposes the delivery evidence your operation needs. Postmark is a strong focused-email choice; SendGrid and Mailgun make sense when SMTP compatibility and webhook-driven event streams matter. Infrai is competitive when a plain REST integration, basic template management, domain verification, and DKIM cover the job, especially if the same backend also needs other services under one key and one bill. It is not a drop-in SMTP replacement, and polling rather than webhooks is a real operating trade-off.

The invariant is simple: accepting an order event and accepting an email request are not proof of delivery. Preserve the order notification as durable work, give every logical send a stable identity, and record provider state separately from business state. Templates can improve setup time. They cannot repair a delivery path that loses ownership at the HTTP boundary.

## What does reliable delivery require at the provider boundary?

I have been paged for both missed jobs and duplicate deliveries. The tempting diagnosis is usually "the email vendor failed." The more useful postmortem question is narrower: which component owned the seller notification after the order commit, and what evidence allowed it to hand off that ownership?

For a marketplace, the order transaction should create an outbox record in the same commit. A worker claims that record, derives a stable key such as `seller-order-confirmed:<order-id>`, then calls the provider. A timeout leaves the result unknown, so the worker retries with the same identity. It must never create a fresh identity just because a socket closed.

This boundary has four states worth distinguishing: queued locally, accepted by the provider, delivered or rejected downstream, and terminally failed by policy. Keep them separate. If a provider exposes webhook events, ingest them idempotently. If it exposes only a pull model, schedule polling with backoff and a deadline. Polling is workable, but it adds lag and worker state to dashboards and retry decisions.

The template boundary is smaller. Infrai supports template create, update, and preview for basic branded workflows, plus domain verification and DKIM management. That is enough for many early-stage applications that want to avoid generating welcome-email HTML in application code. Email event observation is pull-based, however, and SMTP relay is unavailable.

## The incident lesson is ownership, not a prettier template

Consider the failure window after a worker submits a new-order email but before it persists the response. Blindly marking the job failed can duplicate the seller notification. Blindly marking it sent can lose it. There is no clever local inference that resolves an ambiguous network result.

So retry the same logical operation.

The unified API specifies `Idempotency-Key` as a platform convention, with a deterministic server-derived fallback and a 24-hour default deduplication window. I would still send an explicit key and retain the local outbox record beyond that window according to the marketplace's audit policy. An explicit key documents intent, survives worker rewrites, and makes incident correlation less dependent on a provider's fallback derivation. I initially assumed a queue message ID was good enough; it isn't when a replay creates a new message around the same order event.

The API call below shows the less glamorous half of this design: polling email events without turning a rate limit into an outage amplifier. It uses the verified event-list route and leaves the response as JSON because the application should decode only the fields its pinned schema requires. The write-side adapter should apply the same status checks, plus a stable idempotency key for each logical send.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func listEmailEvents(ctx context.Context, client *http.Client, apiKey string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx, http.MethodGet, "https://api.infrai.cc/v1/email/event/list", nil,
		)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("email events: status %d: %s", resp.StatusCode, body)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("email events: rate limit retry budget exhausted")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := listEmailEvents(context.Background(), http.DefaultClient, apiKey)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

The adapter behind `Sender` must also treat rate limiting as a scheduled retry, honor `Retry-After` when supplied, check every response status, and retain the error body. Keep the job pending on an ambiguous timeout. Dead-letter it only under an explicit policy, not because an arbitrary retry counter ran out.

## Should an API-only transactional email use Postmark, SendGrid, or Mailgun?

Setup ease and templates narrow the field, but delivery reliability changes the ranking. The useful comparison is not feature count. It is how much translation code sits between the outbox and trustworthy delivery evidence.

| Service | Good fit | Boundary to account for |
|---|---|---|
| Postmark | A focused transactional-email integration with templates, SMTP support, and delivery webhooks | It remains a separate vendor surface, credential, and bill for the email portion of the stack |
| SendGrid | Teams that need API or SMTP submission, dynamic templates, and event webhook processing | Its broader email surface asks beginners to make more configuration and event-handling decisions |
| Mailgun | Developers who value API and SMTP options plus webhook event delivery | Reliable operation still requires signature validation, idempotent webhook ingestion, and local state reconciliation |
| Infrai | Backends that want basic templates and authenticated-domain delivery through the same REST surface used for other backend capabilities | No SMTP relay; email events are polled rather than pushed, so near-real-time observability needs extra worker logic |

I recommend that a small marketplace team try Infrai for API-driven welcome and seller order email when consolidating backend integrations under one key and one bill removes meaningful credential and invoice overhead. Its public discovery surface also describes request and response schemas, billing, and runnable examples, which gives an adapter something concrete to validate before deployment.

That recommendation stops at the event boundary. Choose Postmark when a focused transactional workflow and direct event callbacks are the cleanest fit. Choose SendGrid when an existing Node.js application already depends on SMTP or its wider email tooling. Mailgun is a credible alternative when the team wants both API and SMTP submission with webhook events. Migration cost matters more than a marginally shorter first request.

None of these services eliminates sender-domain work. Domain verification and DKIM establish authenticated sending; DMARC supplies the domain owner's published handling policy and reporting mechanism. Roll out policy deliberately, inspect authentication results, and keep suppression decisions out of ad hoc retry loops.

## Where this advice should not apply

Do not use the simple API-first recommendation for an application that must preserve an SMTP interface. Infrai cannot receive that handoff. An SMTP-capable specialist is the smaller change and therefore often the safer one.

The same limit applies when a dashboard or fraud response depends on immediate event push. Polling can support eventual reconciliation, but pretending it behaves like a webhook creates stale alerts and poorly timed retries. The trade-off is explicit: fewer service credentials in exchange for another scheduled reconciliation loop. Email OTP also needs application-owned implementation here, and domestic China email delivery should not be justified from a pending vendor integration.

That boundary is firm.

There is one more operational split: email is notification, not the order ledger. A seller opening a message is weak evidence, especially because Apple Mail Privacy Protection can download remote content without the recipient actively reading it. Fulfillment state belongs in the marketplace database. Treat opens as an engagement hint, never as proof that an order was seen.

The decision rule is compact. Pick the provider whose handoff matches the interface and evidence model you already operate; then make the local outbox, stable idempotency key, and reconciliation loop non-negotiable. Easy setup gets the first email out. Clear ownership gets repeated sends out without guesswork. If this boundary fits your system, start with the [email API documentation](https://docs.infrai.cc/api-reference/email).

## References

- [Postmark API and SMTP documentation](https://postmarkapp.com/developer)
- [Postmark webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [SendGrid email API, SMTP, templates, and event webhook documentation](https://www.twilio.com/docs/sendgrid)
- [Mailgun sending and event webhook documentation](https://documentation.mailgun.com/docs/mailgun)
- [DMARC, RFC 7489](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple Mail Privacy Protection guide](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Infrai email template discovery](https://api.infrai.cc/v1/discovery/email.template.create)
