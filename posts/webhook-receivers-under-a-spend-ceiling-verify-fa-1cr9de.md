# Webhook receivers under a spend ceiling: verify fast, enqueue, and refuse the rest

A webhook receiver has one job on the hot path: verify the signature over the exact bytes that arrived, put the raw body somewhere durable, and get out of the way. Use the unparsed body for the HMAC comparison, enqueue those same bytes, acknowledge with 204, and let a worker do everything else. Parsing, tenant lookup, fan-out, the write to your ledger — all of it belongs behind the queue, because every millisecond before the acknowledge is a millisecond the sender charges against its own timeout.

That part is settled engineering. The part almost nobody writes into the runbook is what the receiver should do when one tenant starts costing more than its contract is worth, and whether you would rather refuse its traffic or eat the bill.

## The page you get is the wrong one

Picture the alert that actually fires on a game platform that ingests purchase and anti-cheat events from studio backends. It isn't "receiver down". It's worker lag, or queue depth crossing a static threshold, and by the time it pages someone the intake has already been accepting happily for an hour.

On-call opens the dashboard and everything upstream is green. The receiver is returning 204 in single-digit milliseconds. Error rate is zero. No 5xx anywhere on the edge. The only thing moving is queue depth, and the autoscaler is doing exactly what it was told — adding workers, which costs money, which nobody is watching at 3 a.m.

Then someone looks at the per-tenant breakdown and it's one studio. Launch weekend. Their backend is replaying a purchase stream because their own retry logic treats a slow downstream as a failure, and each replay arrives with a fresh delivery id, so nothing dedupes.

The receiver behaved perfectly. That's the whole problem.

Acknowledging fast is a promise to absorb whatever arrives, and absorbing is not free. I've been paged for missed jobs and for duplicate deliveries, and the duplicate-delivery pages are always worse, because the system never looks broken — it looks busy. A receiver tuned only for latency will happily accept its way through your entire monthly budget and never emit a single error.

## Should the receiver verify the signature before it enqueues the raw body?

Yes, and the ordering is not negotiable: read the raw bytes, verify, then enqueue those same bytes untouched. Parse nothing before the comparison succeeds. If you deserialize first and re-serialize to build the signing string, key ordering and unicode escaping will eventually diverge from what the sender signed, and you will spend a day chasing a mismatch that only shows up for one tenant's payloads.

Two details do most of the work. Compare with a constant-time function — `hmac.Equal` in Go, `crypto.timingSafeEqual` in Node.js — so the comparison doesn't leak the expected digest through timing. And sign a timestamp alongside the body, then reject anything outside a narrow window, or a captured delivery stays replayable forever.

```go
const (
	maxBody  = 1 << 20 // 1 MiB; anything larger is a bug in the sender, not a payload
	maxSkew  = 5 * time.Minute
)

func (r *Receiver) Handle(w http.ResponseWriter, req *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxBody))
	if err != nil {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}

	keyID := req.Header.Get("Webhook-Key-Id")
	secret, ok := r.keys.Lookup(req.Context(), keyID) // scoped to exactly one tenant
	if !ok {
		http.Error(w, "unknown key", http.StatusUnauthorized)
		return
	}

	ts, err := time.Parse(time.RFC3339, req.Header.Get("Webhook-Timestamp"))
	if err != nil || time.Since(ts).Abs() > maxSkew {
		http.Error(w, "stale timestamp", http.StatusUnauthorized)
		return
	}

	mac := hmac.New(sha256.New, secret.Bytes)
	mac.Write([]byte(req.Header.Get("Webhook-Timestamp")))
	mac.Write([]byte{'.'})
	mac.Write(raw) // the exact bytes, never a re-encoded struct
	want, _ := hex.DecodeString(req.Header.Get("Webhook-Signature"))
	if !hmac.Equal(mac.Sum(nil), want) {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}

	// Idempotency lives here, not in the worker: the delivery id is the dedupe key.
	if err := r.queue.Enqueue(req.Context(), Job{
		Tenant:     secret.TenantID,
		DeliveryID: req.Header.Get("Webhook-Id"),
		Payload:    raw,
	}); err != nil {
		http.Error(w, "retry later", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

Note what the handler returns when the queue write doesn't complete: a status that asks the sender to retry, not a 204. Acknowledging a delivery you failed to durably store is how you build a silent data-loss machine.

If your intake is Node.js and Express, the one thing to get right is that the JSON body parser consumes the stream before your handler sees it. Mount a raw parser on the webhook path only:

```js
const express = require('express');
const app = express();

// Raw body for this route only; the rest of the app keeps express.json().
app.post('/hooks/tenant-events',
  express.raw({ type: 'application/json', limit: '1mb' }),
  (req, res) => {
    // req.body is a Buffer here — sign over it directly, parse after verifying.
    if (!verify(req.body, req.get('Webhook-Signature'), req.get('Webhook-Key-Id'))) {
      return res.status(401).end();
    }
    queue.enqueue({ deliveryId: req.get('Webhook-Id'), payload: req.body });
    res.status(204).end();
  });
```

Standard Webhooks and RFC 9421 both describe schemes in this shape. Pick one and write it down, because the failure mode of an undocumented signing string is a tenant who can't onboard and can't tell you why.

## Where the per-tenant key actually lives

Issuing a scoped key per tenant is the easy half: generate a random secret, store it under a key id, hand the id and secret to the studio once, never log either again. The OWASP secrets guidance on this is unglamorous and correct — a dedicated secret store, no secrets in environment dumps, rotation as a routine operation rather than an incident response.

Revocation is the half that bites.

The moment you cache secrets in the receiver — and you will, because a store lookup on every delivery is a latency and cost problem of its own — your revocation SLO becomes the cache TTL. Set a 15-minute TTL and you have told every tenant that a leaked key stays live for 15 minutes after you revoke it. Keep two active secrets per tenant so rotation is a rollover rather than a cutover, publish the revocation through the same channel that invalidates the cache, and measure the time from revoke to first rejected delivery. That number is the one worth putting on a dashboard, not cache hit rate.

The trade-off is real: a short TTL means faster revocation and more store traffic on every burst, which is exactly when you least want another dependency in the request path.

## The signal that should have fired an hour earlier

The instrumentation change is small and it is the whole point of this piece. Emit a counter at the moment of acknowledgement, labelled by tenant and key id, and alert on that counter against a per-tenant ceiling — not on queue depth, not on worker lag, not on receiver latency. Queue depth is a lagging, aggregated symptom. Accepted deliveries per tenant per minute is the leading, attributable cause.

Once the counter exists, the receiver can act on it in-line:

```go
func (r *Receiver) admit(ctx context.Context, tenant string) (retryAfter time.Duration, ok bool) {
	used, ceiling := r.budget.Window(ctx, tenant) // rolling 60s, per tenant
	r.metrics.Accepted.WithLabelValues(tenant).Inc()
	switch {
	case used < ceiling:
		return 0, true
	case used < ceiling*2:
		// Soft ceiling: still accept, but tell the sender to slow down.
		return 30 * time.Second, true
	default:
		// Hard ceiling: refuse. The sender's own retry queue becomes the buffer.
		return 5 * time.Minute, false
	}
}
```

Refusing above the hard ceiling returns 429 with `Retry-After`, and that response is the entire decision axis in one status code. Three options, three different bills:

| Response | What the sender does | What you pay | When it's right |
|---|---|---|---|
| 204, always | Nothing; delivery is done | Unbounded queue and worker spend | Tenants with a contractual volume floor you've already been paid for |
| 429 + `Retry-After` | Backs off, retries within its retention window | Bounded spend; some latency for that tenant | Burst-prone tenants on a metered plan |
| 401 after revocation | Gives up, raises a support ticket | Nothing, plus a support ticket | Compromised or non-paying tenants only |

Revoking the scoped key is the nuclear option and belongs in the runbook as such. It's the only one of the three that a tenant can't recover from without talking to a human, which is a feature when a key is compromised and a disaster when someone fat-fingers a tenant id at 3 a.m.

## Getting the threshold wrong in the other direction

Set the ceiling too low and you've built an outage with extra steps.

Most senders retry a 429 with backoff, which is the behaviour you're counting on. What they don't do is retry forever. Delivery retention windows are finite — typically hours, sometimes a day — and once a sender exhausts its schedule those events are gone, with no API to ask for them again. Refusing a legitimate launch spike for twenty minutes is recoverable. Refusing it for six hours means a studio's purchase events are permanently missing from your ledger, and you will be the one reconciling them by hand from their logs.

Refusals also make duplicates worse, not better. A sender that gets a 429 mid-batch usually replays the whole batch, so every refusal you issue is a promise to deduplicate later. If your worker isn't idempotent on the delivery id, a spend ceiling turns a cost problem into a correctness problem — double-granted in-game currency is a much more expensive page than an over-provisioned queue.

So the honest decision rule is narrower than it looks. A hard ceiling is the right call when the tenant is metered, the payload is replaceable from an upstream source of truth, and you have per-delivery idempotency proven in a test. If you need exactly-once-ish semantics on events that only exist in the webhook stream, stick with an elastic queue and a ceiling that only alerts, never refuses — buy the capacity and argue about the invoice on Monday.

I'm not sure there's a defensible universal number for the ceiling itself. The one heuristic that has held up for me is to set it against the tenant's own retry retention rather than against your queue's capacity: refuse for no longer than a fraction of the window in which the sender will still redeliver, and you can always accept your way out of a bad threshold. Your mileage will vary with how your senders implement backoff, which is a good reason to test that behaviour against a staging receiver before you trust it in a runbook.

Whatever you pick, write the number down next to the alert that fires, and write down who is allowed to revoke a key without a second pair of eyes.

## Further reading

- OWASP Secrets Management Cheat Sheet — https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- RFC 9421, HTTP Message Signatures — https://www.rfc-editor.org/rfc/rfc9421.html
- Standard Webhooks specification — https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md
- RFC 6585, Additional HTTP Status Codes (429 Too Many Requests) — https://www.rfc-editor.org/rfc/rfc6585.html
- Node.js crypto: `crypto.timingSafeEqual` — https://nodejs.org/api/crypto.html#cryptotimingsafeequala-b
- Express API reference: `express.raw` — https://expressjs.com/en/api.html#express.raw
