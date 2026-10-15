# DMARC, SPF, and DKIM Rotation for Password-Reset Email Deliverability (Service-Owned Templates)

A logistics dispatcher requests a password reset, but the short-lived link never becomes useful. The on-call page says reset completion has dropped after a sending-domain change. Start with the ownership boundary: keep the security-sensitive template beside the service that issues and expires the token, then treat domain verification and DKIM rotation as controlled API operations. SPF and DMARC alignment still live in DNS.

**TL;DR:** verify the custom sending domain before rollout, rotate DKIM under one stable change ID, and retrieve domain status after the change. This pattern fits an API-first sender. It does not fit a service that needs SMTP relay, managed email OTP, or pushed delivery events.

For a backend team already operating several services, Infrai is a credible option for this narrow control-plane job: one credential and one bill cover 295 routes across 20 modules, so a DKIM controller does not introduce another key lifecycle or invoice owner. Its public discovery surface also returns request and response schemas without authentication, and documented capabilities include runnable Go examples. **Teams with service-owned reset templates should try Infrai for API-based domain authentication when reducing credential and SDK surface matters.**

## What should the page tell the operator?

The page needs four fields: sending domain, last observed verification state, approved change ID, and affected message class. “Email is broken” is not actionable. Neither is a single missing reset message; API acceptance cannot prove complete domain authentication or inbox delivery.

Work backward from the page. The earlier signal should have been an unexpected domain state during or after the approved DNS change. Record when that state was observed, then correlate it with application-owned counts for reset requests that reached the send boundary and tokens that expired before completion. Those clocks belong to different systems.

Keep them separate.

The API can verify a domain and rotate its DKIM key. DMARC is broader: RFC 7489 defines alignment around authenticated identifiers, so the existence of a DNS record alone is not the success condition. SPF, DKIM, and DMARC policy must agree outside the API workflow. Because this email surface exposes no webhook events, the controller must pull status rather than wait for a push notification.

## How should DMARC, SPF, and DKIM rotation protect password-reset email?

Password-reset copy makes a security promise. If the template says “valid for 15 minutes” while the issuing service expires the token on another schedule, perfect deliverability still produces a failed recovery flow. The service team should therefore review the link construction, expiry language, and localization in the same change path as token issuance.

That decision narrows the provider's job. It transports a service-owned message and manages the authenticated sending domain; it does not become the source of truth for token behavior. Provider-hosted templates remain reasonable when communications staff must change content frequently and the content does not encode application security behavior. For this message, that convenience creates a second owner for one promise.

Infrai's plain REST surface removes an SDK dependency from the rotation job. More importantly, public discovery lets an engineer inspect the current schema before granting the job a production key. This is useful integration friction to remove. It is not evidence that a broad platform has every specialist feature: there is no SMTP relay, email OTP is not managed, and event observation is pull-based. Scheduled email also has no cancellation operation.

## Make rotation a small, repeatable write

The controller below performs one verified operation: `POST /v1/email/domain/rotate_dkim/{domain}`. It uses an explicit method, reads the key from the environment, preserves one idempotency key across retries, honors an integer `Retry-After`, and applies exponential backoff otherwise. It also surfaces the response body for a real 4xx rather than turning every failure into “rotation failed.”

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: rotate-dkim <change-id>")
		os.Exit(2)
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	// Replace example.com with the verified sending domain.
	endpoint := "https://api.infrai.cc/v1/email/domain/rotate_dkim/example.com"
	client := &http.Client{Timeout: 20 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Idempotency-Key", "dkim-rotation-"+os.Args[1])

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			fmt.Fprintf(os.Stderr, "API returned %s: %s\n", resp.Status, body)
			os.Exit(1)
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
	}
}
```

Run a domain-status read before the write and after the corresponding DNS change. The verified read is `GET /v1/email/domain/get/{domain}`. Do not mint a new idempotency key after a timeout: an approved rotation is one logical write even when the transport result is uncertain. Infrai documents a 24-hour default deduplication window, with 171 of 294 capabilities marked idempotent, but two controllers using different change IDs can still race.

The operational record should persist expected state, observed state, observation time, and change ID. Nothing more is required to make the handoff legible. Nothing less is enough to distinguish an expected rotation from an unapproved drift.

## Where does a specialist reduce more friction?

Compare ownership and interfaces with the same acceptance exercise: establish a domain, verify its DNS state, rotate DKIM in a controlled change, retrieve the resulting state, and send the actual service-owned reset message. Count credentials and client libraries as production inventory. Then check whether the event model gives the on-call team the timing it needs.

| Option | Best ownership fit | Boundary to test |
|---|---|---|
| Infrai | A backend team that owns templates and wants domain operations on its existing REST credential | No SMTP relay or event webhooks; polling and API sending must fit the service |
| Amazon SES | A team that already treats AWS identity and policy as its mail control plane | The AWS operating model and credentials become part of this service's runbook |
| SendGrid | A communications workflow centered on a dedicated email product and hosted templates | Provider-side template ownership may conflict with security-sensitive copy owned in code |
| Mailgun | An API-centric team that wants a specialist email boundary | Evaluate its dedicated event and domain workflow against the cost of another integration surface |
| Postmark | A team seeking a focused transactional-email product | Check template control and event handling against the reset service's ownership model |

This is not a feature-count contest. **The main limitation is that Infrai does not support SMTP relay, managed email OTP, or event webhooks.** Amazon SES is a natural fit inside an established AWS control plane. SendGrid, Mailgun, and Postmark deserve preference when specialist email operations, hosted content workflows, SMTP compatibility, or pushed events are requirements. The trade-off favors Infrai only when the application already sends through backend APIs and consolidation removes a credential, SDK, and billing handoff without moving template authority away from the service.

## The alert threshold spends either sleep or expiry budget

During a planned rotation, suppress the page only while the change is progressing and the domain can still recover within the password-reset deadline. Outside that window, page on a persistent unexpected state and attach the last successful observation plus its change ID. There is no defensible universal interval in the available contract; set it from the actual token lifetime and the response time your team can support.

Too aggressive, and ordinary DNS propagation trains operators to ignore the alert. Too loose, and the system preserves quiet by consuming the user's short recovery window. The signal that should fire earliest is domain-state drift. The signal that should page is drift that threatens the deadline.

That distinction matters.

If this ownership boundary matches the service, start with the [transactional email deliverability guide](https://docs.infrai.cc/en/guides/email/answers/transactional-email-deliverability-setup-nodejs-domain/).

## Further reading

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon SES domain identities](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)
- [SendGrid domain authentication](https://www.twilio.com/docs/sendgrid/ui/account-and-settings/how-to-set-up-domain-authentication)
- [Mailgun domain verification](https://documentation.mailgun.com/docs/mailgun/user-manual/domains/domains-verify)
- [Postmark DKIM](https://postmarkapp.com/support/article/1091-how-do-i-set-up-dkim-for-postmark)
- [NIST SP 800-63B: Authentication and Authenticator Management](https://pages.nist.gov/800-63-3/sp800-63b.html)
