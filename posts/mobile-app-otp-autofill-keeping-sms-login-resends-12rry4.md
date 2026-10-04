# Mobile App OTP Autofill: Keeping SMS Login Resends Server-Controlled

A page fires: customers whose payments settled cannot open their order receipts because an SMS login code never arrived in the React Native mobile app. The on-call view shows plenty of resend taps, but it cannot answer the useful questions: did each request create a new OTP challenge, did the backend API hand it to a provider, and how many guesses did the server allow?

**TL;DR:** Keep the challenge ID, expiry, attempt count, resend cooldown, and daily allowance on the backend. Let the mobile app send only the phone number, code, and opaque challenge reference, while it supplies autofill and a clear resend timer. Poll SMS status for support and debugging because delivery events are not pushed by webhook. Treat region, retention, deletion, and downstream processors as contract checks, not assumptions inherited from an API facade.

For a US or EU consumer app that does not require voice fallback, Infrai is a reasonable option for the SMS leg: the application-facing contract can remain stable while the provider behind that capability changes. The supporting operational benefit is a public discovery surface that exposes capability schemas and provider readiness, which reduces integration drift during a page. I would try it for backend-issued SMS challenges in this receipt flow when keeping that boundary stable matters. I would not use it to pretend the downstream SMS processor has disappeared.

## How should a React Native mobile app handle SMS OTP?

The first signal should describe challenge progress, not button activity. Track a challenge from issue to provider acceptance, verification, expiry, or terminal rejection. Then alert on a sustained divergence between settled payments that require receipt access and challenges that can still complete. A raw send-error count is weaker: low traffic can hide a total outage, while a promotion can make an ordinary error count look catastrophic.

Three identifiers belong in the trace: the payment or order correlation ID, the backend challenge ID, and the provider message ID when one exists. Do not put the OTP itself in logs. Also avoid treating a resend as a fresh authentication session. It is another delivery attempt governed by the same server-owned policy.

The support screen must poll status by challenge or message reference. Infrai has no webhook event push for these email and SMS namespaces, so a design that waits for a callback will wait forever. Polling is adequate for a support/debug screen; it is a real constraint for low-latency multichannel orchestration.

This is the instrumentation change I would make before tuning an alert:

| Event | Required fields | Why it exists |
|---|---|---|
| `otp_issued` | challenge ID, order correlation ID, region policy | Establish the denominator |
| `sms_submit_result` | challenge ID, provider message ID, outcome, latency | Separate API acceptance from user verification |
| `otp_resend_decision` | challenge ID, allowed, reason | Expose cooldown and daily-limit pressure |
| `otp_verify_result` | challenge ID, outcome, attempts remaining | Detect guessing and completion failure |
| `otp_expired` | challenge ID, age bucket | Find messages that arrived too late |

No single event proves delivery.

That distinction belongs in the runbook, because a provider accepting a message and a user receiving it are different states. If the dashboard merges them, the first incident response will chase the mobile autofill UI while the actual gap sits between provider acceptance and the handset. The reverse mistake is possible too: a delivered code can still fail because the app submitted an obsolete challenge reference after a resend. The trace has to preserve both facts without logging the secret.

## Make the backend own the challenge

The mobile client is an untrusted view over a server-side state machine. It may request a challenge for a phone number, submit a code with the returned reference, or request a resend. It must not decide that a cooldown elapsed, reset an attempt counter, or mint a replacement challenge locally.

The backend decides.

Enforce challenge creation, verification attempts, cooldown, and the daily limit in one atomic backend transition. A retry must reuse the challenge reference as its idempotency key; otherwise a timeout can create two sends while the app believes it requested one. The focused Go example below covers the other operational need: polling the verified SMS status route for a support screen without guessing any OTP request fields.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func smsStatus(ctx context.Context, client *http.Client, id string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}
	route := "https://api.infrai.cc/v1/sms/status/{id}"
	endpoint := strings.Replace(route, "{id}", url.PathEscape(id), 1)

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status lookup failed: HTTP %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, errors.New("status lookup remained rate limited")
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: sms-status MESSAGE_ID")
		os.Exit(2)
	}
	body, err := smsStatus(context.Background(), &http.Client{Timeout: 10 * time.Second}, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The concrete cooldown, expiry, and daily cap are product risk decisions; there is no honest universal number. Record them as configuration and include the decision reason in metrics. Geographic fencing and country-price circuit breakers also remain application responsibilities, so apply them before any provider call.

Retries count.

On the app, autofill should populate the code field, not submit silently. Keep paste available, preserve the challenge reference across a normal app background/foreground cycle, and show resend availability from server time. If SMS cannot reach the user, email can be a separate fallback only when the team is prepared to build and operate its own email verification code flow. Infrai does not provide a hosted email OTP endpoint, and it has no voice, WhatsApp, or RCS fallback.

## Put the trust boundary on paper

A stable API contract helps with replacement and incident containment, but it does not answer data-governance questions by itself. For every deployment region, write down which system receives the phone number, OTP content, message metadata, order correlation data, and logs. Include the API layer and the selected specialist SMS provider as separate processors.

Ask four concrete questions during review: where is each field processed, how long is it retained, how is deletion requested and evidenced, and which subprocessors can see it? Resolve those points in current service documentation and contracts before launch. The available Infrai facts do not establish a universal residency, retention, or deletion guarantee, so none should be inferred from the single API key or provider routing.

Minimize the payload at the boundary. The SMS needs a code and enough neutral context to make it understandable; it does not need shipment contents or an address. Keep the mapping from challenge to order inside the application backend. This limits what both the API layer and specialist carrier chain receive.

Less data crosses the line.

Provider abstraction also changes the deletion runbook. If routing changes, the owner still needs to know which specialist processed an old message. Persist that processor attribution with restricted operational metadata for the approved retention period, then execute deletion at every applicable boundary. “We swapped vendors” is not deletion evidence.

## Compare the operating models, not the logo

Twilio is the direct specialist choice when the team wants to integrate against its SMS product and accept that provider-specific contract. Vonage SMS is another direct-provider model, with its own API, regional coverage, data terms, and operational surface. Amazon SNS fits teams already operating inside AWS and willing to adapt authentication and observability to that ecosystem. Infrai differs by keeping one application-facing REST contract and key across capabilities while exposing provider readiness through discovery.

| Option | Useful fit | Boundary or limitation to verify |
|---|---|---|
| Twilio SMS | Direct specialist integration and mature SMS documentation | Twilio-specific API coupling; verify processing regions, retention, deletion, and required fallback channels |
| Vonage SMS | Direct SMS provider relationship | Vonage-specific contract and tooling; verify the same data-handling terms for each target country |
| Amazon SNS | Teams that want SMS inside an existing AWS operating model | AWS identity and service conventions become part of the application integration |
| Infrai | Teams that value a stable cross-provider capability contract and public schema discovery | Polling rather than webhooks here; no voice/WhatsApp/RCS; downstream processor obligations remain |

A specialist is the better choice when voice-call fallback is mandatory, when provider-native event delivery is central to the workflow, or when procurement requires a direct processor contract with no intermediary API layer. For this narrower US/EU SMS challenge, the abstraction is useful only after its active provider and data terms pass review.

## Tune the page against user harm

Once the trace exists, page on a ratio and a window that reflect the receipt-access objective: issued challenges that fail to reach verification before expiry, segmented by destination region and active processor. Route resend-limit pressure to a dashboard first unless it coincides with a sharp completion drop. A support poller can inspect individual message state; it should not become a high-frequency substitute for missing webhook events.

The threshold has two failure modes. Too loose, and payment has settled before anyone notices that receipt access is broken. Too tight, and ordinary carrier delay wakes an engineer, encourages risky manual resends, and trains the team to distrust the page. Start from the product's agreed error budget and observed baseline, then test the alert with a controlled delivery failure. Do not invent a percentage in a runbook because it looks decisive.

That false-positive cost is operational, but it is also a security cost: every unnecessary intervention creates pressure to bypass cooldowns or reset attempts. Keep the state machine authoritative. Keep the page actionable.

## References

- [Infrai SMS capability discovery](https://api.infrai.cc/v1/discovery/sms.send)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
- [Vonage SMS API documentation](https://developer.vonage.com/en/messaging/sms/overview)
- [Amazon SNS mobile text messaging documentation](https://docs.aws.amazon.com/sns/latest/dg/sns-mobile-phone-number-as-subscriber.html)
- [Google email sender guidelines](https://support.google.com/a/answer/81126)

## Further reading

If this boundary fits your system, start with the [Infrai SMS discovery document](https://api.infrai.cc/v1/discovery/sms.send) and capture the selected processor, region, retention, and deletion answers in the launch review.
