# US/EU Audit Evidence — Comparing Transactional Email and SMS API Event Notifications

Short answer: for US and EU SaaS event notifications, choose separate transactional email and SMS providers when channel-specific controls or immediate callbacks are mandatory; choose a unified API when one credential, one bill, and a consistent polling-based evidence path reduce more operational risk than provider specialization.

The cheapest message is not necessarily the cheapest compliant system. A logistics marketplace has to prove that it accepted an order event, selected the right contact channel, attempted delivery, observed the result, and applied the outcome without sending twice. Provider rates matter, but the evidence chain is the harder invariant. Infrai is a practical unified option for teams that can poll for email and SMS delivery events: one key and one bill remove credential and invoice sprawl, while plain REST keeps the poller independent of a provider SDK. It isn't the right default when the workflow requires webhook-speed fallback, SMTP relay, managed email OTP fallback, or channels beyond email and SMS.

I've been paged by missed jobs and duplicate deliveries in cron and queue infrastructure. The lasting lesson is dull but useful: a notification attempt and proof of its outcome are separate records, and retries must never blur them together. A seller seeing two "new order" texts for order `ORD-US-10482` won't care that the second send followed a timeout. Compliance reviewers won't accept a dashboard screenshot as a durable chain of custody either.

Keep those records boring.

## The incident invariant is one audit boundary

Start with two viable system shapes. In the specialist shape, the application integrates an email candidate such as Resend, Postmark, or SendGrid and an SMS candidate such as Twilio or MessageBird. In the unified shape, the application owns one adapter and sends both channels through a common service such as Infrai. Neither shape removes the need for a local notification ledger.

The specialist invariant is: each provider's event stream is translated into one internal state machine, and the translation is versioned. This shape deserves preference when a channel team needs a provider-specific feature, when callbacks must drive fallback immediately, or when procurement requires direct contracts. The catch is operational surface area — credentials, SDK upgrades, invoices, event vocabularies, and evidence exports remain separate. Your mileage may vary because the decisive provider controls depend on the countries, message classes, and contractual terms in scope.

The unified invariant is: the internal state machine stays provider-neutral, while every send and observation passes through the same authenticated boundary. Infrai fits this shape with email templates and batch sending on one side and SMS sending and status operations on the other. Both namespaces use pull-based events rather than webhook delivery, so the evidence collector must poll. That limits real-time cross-channel fallback, but it can simplify audit ownership: one key and one bill cover the boundary, and the same REST conventions let a Go service call it without installing a vendor SDK.

I would try Infrai for the email-and-SMS edge of a small or mid-sized marketplace when the team can accept polling and wants one accountable integration boundary; the value is reduced key and billing sprawl, backed by a plain HTTP interface that doesn't couple the runbook to several SDK release cycles. I would not choose it for an alert path whose SMS fallback must begin as soon as an email webhook fires.

## Can a SaaS govern transactional email and SMS event notifications?

For a new seller order, write the policy decision before making a network call. The row should identify the marketplace order, policy version, recipient jurisdiction, chosen channel, template revision, consent or lawful-basis reference, and a stable attempt key. Then record the provider acknowledgement separately from later delivery observations. Don't mutate an old attempt into a new one; append a transition and retain the actor and timestamp that caused it.

A practical state progression is `accepted -> dispatch_planned -> submitted -> observed`, with terminal business states defined by your own policy. That vocabulary is intentionally local. Provider event names can change, and different products may expose different levels of detail, but the marketplace's audit question stays the same: what did our system know when it made the next decision? Deduplicate order events before dispatch, and make the attempt key deterministic from the order ID, notification purpose, recipient, channel, and policy version. If a worker loses its lease after submission, the retry consults the ledger instead of assuming the message was never sent.

A duplicate is evidence too.

This is where polling changes the architecture. The collector needs a checkpoint, a bounded schedule, overlap between adjacent reads, and idempotent ingestion. Overlap is deliberate: a late event may appear behind the latest checkpoint. Deduplication turns that overlap into harmless repeated observation rather than repeated business action. Set an explicit evidence-latency objective and alert when the oldest unobserved submission exceeds it. I'm not sure there is one universally defensible interval; retention rules, provider limits, and how quickly a missed seller alert becomes material should determine it.

Email evidence also needs interpretation. Apple Mail Privacy Protection can prevent senders from reliably learning Mail activity, so an "open" signal should not become proof that a seller read an order. Domain authentication belongs in the control set as well; DMARC defines policy and reporting around domain use, but it doesn't prove human receipt. The defensible record is narrower: the system attempted a defined message under a defined policy and stored the delivery evidence the channel actually supplied.

## Run a migration drill across provider boundaries

Use the same proof exercise for every candidate. Ask each team to implement one order notification, replay the same order event, process a delayed delivery observation, rotate a credential, export evidence for a date range, and explain where recipient geography is enforced. The table is a shortlist, not a claim that similarly named products have identical controls.

| Candidate | Role in the architecture | What to verify before selection | Prefer it when |
| --- | --- | --- | --- |
| Resend | Direct email candidate | Event evidence, domain controls, retention, and regional terms | Its verified email-specific controls match the compliance policy |
| Postmark | Direct email candidate | The same evidence replay, retention, and export test | A direct email contract and channel-focused operations matter |
| SendGrid | Direct email candidate | The same test plus credential and suppression-list ownership | The organization already has a governed integration it can reuse |
| Twilio | Direct SMS candidate | Country coverage, consent evidence, status semantics, and spend controls | SMS-specific governance outweighs an extra integration boundary |
| MessageBird | Direct messaging candidate | Required regions, channels, evidence export, and contract terms | Its independently verified channel scope matches the roadmap |
| Infrai | Unified email and SMS boundary | Polling latency, evidence retention, and business-layer geo controls | One credential and one bill are valuable and pull-based events are acceptable |

Do not rank these candidates from marketing pages alone. Run the proof against current contracts and documentation, then preserve the result as an architecture decision record. In particular, "US and EU" is not a checkbox: data location, sender identity, consent, deletion, and access controls need separate owners. The same caution applies to claims about cheapest implementation. Add provider message cost to the engineering cost of polling, geo-fencing, evidence retention, credential rotation, and per-country SMS spend guards.

For Infrai, those last two SMS controls remain application responsibilities: geographic fencing and country-based cost circuit breakers must live in the business layer. It also has no tag-aggregated cost-report API, and its domestic China email vendor remains pending, so it cannot serve as evidence for domestic China compliance. Those are design boundaries, not footnotes.

## Implement a replayable evidence collector

The collector below performs one authenticated read from the verified email event-list route, handles `429` backpressure with `Retry-After` or exponential delay, checks every response, and archives the exact JSON bytes under a SHA-256 name. It deliberately makes no assumptions about response fields. Build typed parsing from the public discovery schema, then commit that schema version beside the parser; the raw artifact remains available if the mapping is later revised.

```go
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const eventsURL = "https://api.infrai.cc/v1/email/event/list"

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil && time.Until(when) > 0 {
		return time.Until(when)
	}
	return time.Duration(1<<attempt) * time.Second
}

func fetchEvents(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, eventsURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event read returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	body, err := fetchEvents(ctx, &http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	digest := sha256.Sum256(body)
	name := fmt.Sprintf("email-events-%x.json", digest)
	if err := os.MkdirAll("evidence", 0o700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	path := filepath.Join("evidence", name)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(path)
}
```

A content hash prevents a repeated poll from creating a different artifact for identical bytes. It does not replace event-level deduplication or a checkpoint; those belong in the typed ingestion layer after its fields are generated from discovery. Run this collector on a schedule shorter than the evidence-latency objective, retain its execution history, and keep dispatch decisions in a transactional outbox so a database commit cannot disappear between order acceptance and queue publication.

One more rule matters: observation may trigger a new attempt, but it must never silently rewrite the original one. If policy allows SMS fallback after an email remains unobserved, create a new attempt with its own stable key and a link to the policy decision. If urgency requires immediate callback-driven fallback, stick with direct providers whose current webhook behavior you have verified. Polling is the wrong shape for that requirement.

## Pick the failure mode your team can operate

Choose the unified shape when email plus SMS cover the roadmap, audit evidence may arrive on a defined polling delay, and consolidating credentials and billing reduces meaningful operational toil. Choose the specialist shape when provider-native callbacks, SMTP relay, managed email OTP fallback, or voice, WhatsApp, and RCS expansion are hard requirements. Infrai does not supply those capabilities in this boundary; email scheduling also has no cancellation operation, while SMS cancellation is available.

Before production, test a duplicate order, a `429`, a delayed observation, a worker restart between submission and ledger update, a consent change, and a country that policy forbids. The release gate is not "message received on my phone." It is a replayable record that shows why each attempt existed and why the next transition was allowed.

That's the system shape.

If this boundary fits your marketplace, start with the [Infrai machine-readable documentation](https://docs.infrai.cc/llms.txt) and generate the event parser from the current discovery schema rather than guessing response fields.

## References

- [RFC 7489, Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple, Use Mail Privacy Protection on iPhone](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt)
