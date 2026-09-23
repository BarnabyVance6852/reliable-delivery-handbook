# Password Reset Email Implementation: 5 API-First Steps Without an SMTP Relay

**Short answer:** put a small HTTP email adapter behind a durable job, and keep link issuance in the property platform. The application should own the opaque token, expiry, single-use transition, and generic user response; the delivery service should receive a message with an already-rendered URL. This is the simplest API-first boundary for signup verification and password recovery because changing a mail transport does not change account security.

Do not send inside the signup request. Commit the user and an outbox record together, return the same response shape for every address, then let a worker claim and deliver the message with a stable idempotency key. That extra queue boundary looks like more integration work on day one. It removes the harder work later: explaining a created account whose verification email vanished when an HTTP call timed out.

## 1. How should a password reset email API own the link?

A property-management signup often joins a person to a building, management company, or invitation. Email delivery cannot be allowed to decide that relationship. The account service should generate a cryptographically random, single-purpose token, store only its digest, attach an expiry, and consume it once in a database transaction. OWASP gives the same baseline for password-reset tokens: random, sufficiently long, linked to one user, invalidated after use, and stored securely.

The URL must use HTTPS and a trusted, configured origin. Do not build its host from the incoming `Host` header. A poisoned reset or verification link is a security failure even if the message was delivered perfectly.

Delivery cannot repair a bad link.

For a compact implementation, 32 random bytes provide a comfortable token space. A 15-minute lifetime below is a local policy choice, not a universal requirement; use a duration that reflects the threat model and the delays residents actually encounter. Signup verification may justify a different window from password recovery.

```go
package accountlink

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"time"
)

type PendingLink struct {
	UserID    string
	Purpose   string
	Digest    [32]byte
	ExpiresAt time.Time
	UsedAt    *time.Time
}

func NewToken(now time.Time) (string, PendingLink, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", PendingLink{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	return token, PendingLink{
		Purpose:   "signup_verification",
		Digest:    sha256.Sum256([]byte(token)),
		ExpiresAt: now.Add(15 * time.Minute),
	}, nil
}
```

Store `PendingLink` and the email job in the same commit. The clear token belongs only in the queued message payload long enough to render the URL; logs, metrics, and error strings should never contain it. Consumption should compare the digest, purpose, expiry, and unused state, then set `UsedAt` atomically. Two clicks must produce one state change.

## 2. Make one narrow delivery port

Integration effort stays bounded when business code knows one operation: deliver this message. It should not know provider-specific template identifiers, retry headers, or authentication shapes. Those belong in an adapter implementing a narrow interface.

The adapter can call an HTTPS email API without operating an SMTP relay. Keep the request generic and make the response classification explicit. A timeout is unknown, not proof that no message was accepted.

Timeouts lie.

```go
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Message struct {
	To             string `json:"to"`
	Subject        string `json:"subject"`
	Text           string `json:"text"`
	IdempotencyKey string `json:"idempotency_key"`
}

type Sender interface {
	Send(context.Context, Message) error
}

type HTTPSender struct {
	Endpoint string
	Token    string
	Client   *http.Client
}

func NewHTTPSender(endpoint, token string) *HTTPSender {
	return &HTTPSender{
		Endpoint: endpoint,
		Token:    token,
		Client:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (s *HTTPSender) Send(ctx context.Context, m Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.IdempotencyKey)

	resp, err := s.Client.Do(req)
	if err != nil {
		return fmt.Errorf("delivery outcome unknown: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("delivery rejected with status %d", resp.StatusCode)
	}
	return nil
}
```

This example intentionally stops at the transport boundary. Production code needs a typed result so the worker can distinguish permanent rejection from retryable throttling and an unknown outcome. Preserve that distinction in the adapter rather than spreading status-code logic through account handlers.

Also authenticate the sending domain. SPF publishes which systems may send for a domain, but RFC 7208 is clear about its scope: SPF validates an SMTP identity, not the visible message author by itself. Domain authentication and application-level link security solve different problems. Track both.

## 3. Treat the outbox as the schedule of record

The durable job is the operational center of the design. A row can carry `job_id`, `user_id`, `purpose`, `recipient`, `not_before`, `attempt`, and a state such as `pending`, `leased`, `sent`, or `dead`. Use the job ID as the idempotency key on every retry. Never generate a new key merely because a worker restarted. I have been paged by missed jobs and duplicate deliveries; the useful lesson is that a scheduler's acknowledgement and the mail service's acceptance are two separate facts, with a crash window between them. The runbook must preserve enough state to decide what happened without guessing from a resident's second click.

One job, one key.

The important sequence is short:

1. In one database transaction, create the pending account-link record and its outbox job.
2. Claim due jobs with a lease, so a crashed worker does not hold one forever.
3. Send with the stable job ID, recording the provider's non-secret message identifier when available.
4. Mark the job sent only after a successful response; otherwise schedule a bounded retry or move a permanent failure to review.

The trade-off is explicit. An outbox adds a table, a worker, lease recovery, and queue-age monitoring. It is a poor fit for a prototype where losing a test message has no consequence, and it may duplicate infrastructure when an existing durable workflow engine already provides transactional enqueue and stable activity identifiers. In those cases, keep the same `Sender` boundary and use the established scheduler. Direct send inside the web request is acceptable only when the team knowingly accepts lost messages, longer request latency, and ambiguous retries; that is usually the wrong bargain for a resident account flow.

Keep retries boring. For example, an installation might choose three attempts with exponential delay and jitter, capped before the link expires. Those values are a policy to test against the chosen delivery contract, not an Internet standard. A retry scheduled after expiry is useless load, so the worker should stop before then and issue a fresh link only through the normal user flow.

There is an unavoidable ambiguity if the remote service accepts the message and the response is lost. A stable idempotency key gives an HTTP provider a chance to deduplicate. If that contract is unavailable, assume duplicates can occur and make the email harmless: the same token, the same destination, and a one-time consumption transaction. Do not enqueue a new token for a transport retry.

One trap deserves a runbook note. A `sent` state means the remote system accepted a request; it does not mean the resident received or opened the email. Conflating those events produces misleading dashboards and bad support decisions.

## 4. Verify failure behavior before rollout

Test the boundary with a fake `Sender`, then test the real adapter in a non-production account using reserved test addresses or the delivery service's documented sandbox mechanism. The valuable cases are not a single happy-path screenshot. They are timeout after acceptance, throttling, permanent address rejection, worker death after send, simultaneous link clicks, an expired token, and a request for an address that does not exist.

OWASP recommends a consistent message and roughly uniform response timing for password-recovery requests so the endpoint does not reveal whether an account exists. Apply the same discipline anywhere signup behavior could expose tenancy or invitation membership. Rate-limit requests per account and per source, but do not lock the account as a side effect of repeated recovery attempts.

Observe the stages separately. Useful counters include jobs created, jobs claimed, accepted sends, retry classifications, expired-before-send jobs, and dead jobs. A histogram from enqueue time to acceptance reveals queue delay; delivery events, when the transport exposes them, belong to a later stage. Alert on sustained age of the oldest ready job and on dead-job growth, not on one transient HTTP failure.

Logs need correlation, not secrets. Record the job ID, purpose, attempt number, classification, and a stable internal user ID. Redact the recipient and token. Keep metric labels bounded; an email address or job ID must never become a label.

## 5. Deploy with a reversible boundary

Ship the adapter and worker behind configuration while the existing path remains available. Start with internal or designated test accounts, confirm that outbox age returns to baseline, and then increase the cohort. The rollback switch should stop new jobs from using the new adapter without deleting pending jobs or minting replacement tokens.

Before expanding traffic, the runbook should answer four questions: Can workers be paused without losing leases? How are dead jobs inspected without exposing tokens? Which response classes retry? Who can rotate the API credential? If any answer requires editing an account handler, the boundary is still too wide.

Rollback is a routing change. Pause claims, wait for active leases or let them expire, switch the sender configuration, and resume the same jobs with the same idempotency keys. Do not bulk replay every message marked sent. That turns a delivery migration into duplicate mail and creates no new evidence about receipt.

The final selection criterion is therefore integration behavior, not a feature checklist. Choose an HTTP delivery contract only after verifying documented authentication, explicit error classes, bounded timeouts, idempotent submission or a clear duplicate model, test facilities, and delivery-event authentication. The account service still owns the link lifecycle. That keeps signup verification and password recovery understandable during an incident, even when the transport changes.

## References

- RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- OWASP Unvalidated Redirects and Forwards Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Unvalidated_Redirects_and_Forwards_Cheat_Sheet.html
- Go `crypto/rand` package documentation: https://pkg.go.dev/crypto/rand
- Go `net/http` package documentation: https://pkg.go.dev/net/http
