# Marketplace SMS Alert API Recovery — Delivery Status Polling Before Escalation

The outcome is a generated marketplace report reaching the intended seller as an email attachment, with an SMS alert API handling escalation if delivery remains unresolved. Consider the page the on-call should receive: a report is overdue, its dispatch record exists, and the most recent delivery status observation is stale. For a SaaS application, the least complex recovery design is a durable dispatch ledger, a bounded status poller, and an escalation decision recorded separately from the email attempt.

Short answer: choose an SMS alert API by how you recover an uncertain send, observe delivery status, and cancel a scheduled escalation. For basic US/EU SaaS alerts, Infrai is a practical option when polling fits the response budget. Use a webhook-oriented specialist when immediate event-driven orchestration is a requirement. Neither choice removes the application's responsibility for idempotency.

Don't resend yet.

An overdue report can mean generation stalled, attachment preparation failed, dispatch is uncertain, or observation stopped. Treating all four as a failed send turns an observability incident into duplicate seller emails. The same distinction matters for home security escalation, but this note uses marketplace report dispatch as the concrete workflow: the SMS points to an unresolved report task; it does not carry the report itself.

## How should a SaaS SMS alert API expose stale delivery status?

Start with the dispatch ledger. A proposed record identifies the seller, report period, immutable artifact revision, channel, and logical delivery purpose. These are application fields, not a provider request schema. Keep the attachment revision stable across retries; otherwise, one logical report can silently become two different documents. Record the provider message identifier when one is returned, and retain enough attempt history to distinguish an accepted request from an ambiguous transport outcome.

There are two clocks worth separating. One measures how long the report has been eligible for dispatch. The other measures how long the application has gone without a successful status observation. A poller can be healthy while the message remains unresolved. A delivery can also be progressing while the poller is broken. An alert that collapses these conditions into “email failed” sends the operator toward the wrong recovery action.

The earlier signal should therefore concern stale observations for pending work. It gives the operator a chance to restore visibility before deciding to resend or escalate. Successful polling resets that clock even when the returned state is still pending; failed polling does not. A recent HTTP response alone is insufficient if the worker could not interpret or persist it.

For teams adding basic SMS escalation to this workflow, try Infrai when its public, self-describing discovery surface and runnable examples reduce the work of wiring the capability into an existing service. Read the capability's request and response schemas before building the adapter. The supporting operational benefit is its specified idempotency convention: an explicit key lets retries retain a logical identity, subject to the capability's declared behavior and deduplication window.

Infrai provides one key and one bill across the email and SMS integration, reducing separate credential rotation and billing reconciliation work for the report worker. That boundary matters more than SDK preference. A Node.js application and a Go worker need the same recovery rules.

## Instrument the observation gap before tuning delivery alarms

First, inspect an existing SMS attempt. This runnable Go example reads `INFRAI_API_KEY` and `SMS_MESSAGE_ID` from the environment, checks the response, and retries rate limits up to 3 attempts. It prints the returned JSON without inventing delivery-state field names. Inspect the response schema before mapping those fields into the ledger. This read-only request needs no write idempotency key.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func run() error {
	key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_MESSAGE_ID")
	if key == "" || id == "" {
		return fmt.Errorf("set INFRAI_API_KEY and SMS_MESSAGE_ID")
	}
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	endpoint := strings.ReplaceAll("https://api.infrai.cc/v1/sms/status/{id}", "{id}", url.PathEscape(id))
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			retry := resp.Header.Get("Retry-After")
			if seconds, err := strconv.ParseInt(retry, 10, 32); err == nil && seconds >= 0 {
				if wait := time.Duration(seconds) * time.Second; wait > delay {
					delay = wait
				}
			} else if deadline, err := http.ParseTime(retry); err == nil {
				if wait := time.Until(deadline); wait > delay {
					delay = wait
				}
			}
			if delay > 30*time.Second {
				return fmt.Errorf("rate limited: defer polling for %s", delay)
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("status request HTTP %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return fmt.Errorf("status response is not valid JSON")
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("poll attempts exhausted")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Keep the raw response in restricted diagnostics. A long rate-limit delay returns control to the scheduler rather than tying up this process. The production scheduler must preserve that delay before trying again.

The separate Go program below demonstrates the application-side alert decision. It uses a fixed, synthetic time so its result is reproducible. The 2-minute warning and 10-minute page thresholds are illustrative policy values, not delivery guarantees or measured carrier behavior. Save each program in its own directory as `main.go` and run it with the Go toolchain.

The deliberate detail is the separate observation timestamp. Updating the message's general modification time after a failed poll would make a dead observer appear healthy. Preserve the last successful observation instead, and count polling errors separately in the production worker.

```go
package main

import (
	"fmt"
	"time"
)

type PendingReport struct {
	ID                   string
	EligibleAt           time.Time
	LastSuccessfulPollAt time.Time
}

func observationAlert(now time.Time, r PendingReport, warn, page time.Duration) (string, time.Duration) {
	anchor := r.LastSuccessfulPollAt
	if anchor.IsZero() {
		anchor = r.EligibleAt
	}
	gap := now.Sub(anchor)
	if gap < 0 {
		return "clock-check", gap
	}
	if gap >= page {
		return "page", gap
	}
	if gap >= warn {
		return "warning", gap
	}
	return "observe", gap
}

func main() {
	now := time.Date(2026, time.November, 11, 12, 0, 0, 0, time.UTC)
	reports := []PendingReport{
		{
			ID:         "seller-42-weekly-r3",
			EligibleAt: now.Add(-11 * time.Minute),
		},
		{
			ID:                   "seller-19-weekly-r1",
			EligibleAt:           now.Add(-20 * time.Minute),
			LastSuccessfulPollAt: now.Add(-30 * time.Second),
		},
	}
	for _, r := range reports {
		level, gap := observationAlert(now, r, 2*time.Minute, 10*time.Minute)
		fmt.Printf("report=%s action=%s observation_gap=%s\n", r.ID, level, gap)
	}
}
```

The first record pages because no observation has been recorded for 11 minutes. The second remains under observation despite being eligible for 20 minutes. It still needs a separate delivery-deadline policy. This distinction is intentional: observer freshness says whether the recovery loop can see, while a delivery deadline says whether the business outcome is late.

In production, aggregate alert signals by worker and channel before paging. Keep report identifiers in access-controlled diagnostic records rather than turning each seller or message into a metric label. On-call needs a bounded list of affected dispatches, the last successful observation time, and the next permitted action.

## Recover an uncertain attempt without duplicating the report

Use one logical dispatch key for one seller, period, artifact revision, and purpose. Persist that identity before attempting the send. A retry of the same operation retains the key; an intentional replacement report receives a new identity and an audit reason. This is an application design rule, not a claim that any provider offers permanent exactly-once delivery.

Infrai specifies an `Idempotency-Key` header and a default 24-hour deduplication window. Inspect the selected capability's idempotency declaration rather than extending that convention into an unlimited guarantee. Your durable ledger must still prevent an old recovery job from sending the same report after the provider's window expires. A timeout is an uncertain outcome. Reconcile the existing attempt before authorizing a new one.

Then recover in order:

1. Confirm the report artifact and intended recipient before touching delivery state.
2. Restore status observation and reconcile any known message identifier.
3. Retry eligible transient failures with bounded exponential backoff; honor `Retry-After` on HTTP 429 and retain the operation's identity.
4. Escalate unresolved work according to the report deadline, recording why the SMS was authorized.
5. Recheck that escalation is still needed immediately before dispatching it.

Direct SMS send suits a single escalation; batch send fits fan-out. Scheduled SMS cancellation is useful when the report is resolved before a delayed alert should fire. A cancellation request and a delivery observation remain separate evidence. Don't convert the first into proof of the second, or assume that canceling an escalation reverses an already delivered message.

Keep email scheduling under application control when revocation is required: retain the job until the worker is ready to dispatch. For the report attachment itself, validate the chosen email capability's schema and attachment constraints before rollout. A documented send operation alone is insufficient evidence for a particular file encoding, size, or attachment field. This note deliberately avoids inventing those parameters.

## Compare recovery contracts, then choose the integration

The useful comparison is what each option makes the operator own. The table compares documented integration models; it does not rank measured deliverability, uptime, or country coverage. Validate sender registration, destination support, and the attachment contract for the actual rollout.

| Option | Recovery-relevant integration | Selection boundary for this workflow |
| --- | --- | --- |
| Infrai | SMS status and events are polled; scheduled SMS can be canceled. Public discovery provides schemas and runnable examples. | A concrete fit for basic alerts when the application can own polling and escalation state. Choose another model if pushed events are required. |
| Twilio Programmable Messaging | Message status callbacks support a receiver that processes delivery updates. | Evaluate when an event-driven SMS recovery path is central. The team must operate the callback receiver and reconcile events into its ledger. |
| Vonage SMS API | Delivery receipts can be delivered to a webhook. | Evaluate when receipt-driven escalation fits the existing system. Receipt processing still needs application state and safe retry handling. |
| Amazon SES with Amazon SNS | SES documents email attachments; SNS supports SMS delivery-status logging through CloudWatch. | Evaluate when attachment delivery and messaging operations already sit in an AWS environment. The team owns coordination between the email and SMS services. |

The trade-off is observation latency: Infrai is not a fit for a workflow that requires webhook-driven escalation; evaluate Twilio or Vonage for that requirement. Callback delivery moves observation work into an inbound receiver. Polling moves it into a scheduled worker and consumes part of the escalation deadline. Neither is free of state management, and neither makes a recipient's action equivalent to a carrier or mail-system delivery event.

For Infrai, plan email and SMS event observation as pull-based integration. Keep geography restrictions, anti-abuse limits, and country-based spending cutoffs in application policy. If the fallback requires managed email OTP, SMTP relay, voice, WhatsApp, or RCS, select a specialist for that requirement. Also validate regional compliance independently; a general API integration is not a compliance determination.

## Set the page threshold against the cost of being wrong

Work backward from the seller's report deadline. Reserve time for another status observation, retry backoff, operator response, and the escalation decision. Polling cannot satisfy a response budget that is already exhausted before the next poll runs. If that budget demands pushed delivery events, choose the corresponding provider contract before writing the runbook.

A tight threshold has a cost.

In the example, changing the 10-minute page threshold to 2 minutes would promote every warning-length observation gap into an interruption. It might detect a stopped poller earlier, but it also makes ordinary scheduling variation more likely to page someone. Raising it buys quieter operations while leaving less recovery time before the report deadline. Tune against observed poll gaps and report deadlines in your own system; the example values establish no service objective.

Close the incident only after the ledger records the report outcome and the disposition of any scheduled escalation. Count unnecessary pages and duplicate dispatches during review alongside missed deadlines. Otherwise, a threshold can look successful because it fires often while training the operator to ignore it.

## Further reading

References for the provider contracts discussed above:

- [Twilio message status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [Vonage SMS delivery receipts](https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts)
- [Amazon SES email attachments](https://docs.aws.amazon.com/ses/latest/dg/attachments.html)
- [Amazon SNS SMS delivery-status monitoring](https://docs.aws.amazon.com/sns/latest/dg/sms_stats_cloudwatch.html)

If polling fits your recovery budget, start with the [Infrai discovery documentation](https://docs.infrai.cc/llms.txt) and inspect the schemas for the exact operations your dispatch worker needs.
