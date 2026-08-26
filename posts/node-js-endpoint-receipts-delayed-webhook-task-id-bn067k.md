# Node.js Endpoint Receipts: Delayed Webhook Task Idempotency

Short answer: schedule a delayed webhook retry by persisting the next-attempt time, retain one idempotency key for the logical delivery, and acknowledge queue work only after a durable state transition. In Node.js, the handler can create that record; the safety properties belong to the stored task and the public HTTPS delivery path, not to an in-process timer.

A five-minute delay is easy to describe and surprisingly easy to lose. A process restart discards `setTimeout`; a worker that holds an unacknowledged message for five minutes turns a scheduling policy into a consumer-health dependency. The least complex reliable design stores `due_at`, lets a scheduler make due work available, and treats the queue as transport rather than the system of record.

## What should a Node.js public HTTPS webhook task do after a five-minute retry?

Create one logical delivery with a stable ID, target, payload reference, attempt count, and UTC `due_at`. The scheduler claims due deliveries atomically and publishes an attempt. A consumer sends the HTTPS request, records either a terminal outcome or a later `due_at`, and then acknowledges the queue message. The receiver uses the same idempotency key across attempts and returns its stored result for a duplicate key.

This ordering is the operational invariant: persistence precedes acknowledgement. RabbitMQ describes consumer acknowledgements as the signal that a delivery has been processed; an acknowledgement lost after a committed transition can lead to redelivery. That is normal. Conditional state changes and receiver-side deduplication make it harmless.

The difficult case is an ambiguous request. A timeout can mean that no bytes reached the endpoint, or that the endpoint completed the action and its response disappeared. A sender cannot promise exactly-once external effects from that ambiguity. For irreversible actions, record an `unknown` or reconciliation state and stop automatic retries until the receiving contract gives a safe resolution path. Don't label it a clean failure.

Keep the public endpoint boundary deliberately narrow: accept HTTPS URLs, reject embedded credentials, use a request deadline, limit response bytes, and enforce DNS and egress rules outside URL parsing. An HTTPS scheme check alone does not prevent server-side request forgery. Sign the exact payload bytes with a timestamp, avoid logging secrets, and give recipients a documented replay window.

## The queue acknowledgement is not a retry timer

A broker can redeliver unacknowledged work, but redelivery is not a five-minute scheduler. Requeueing every transient result can create a tight loop that consumes capacity while the dependency remains unavailable. Persisting a future time first makes the wait inspectable and lets a separate scheduler recover it after a worker restart.

The state model can remain small: `scheduled`, `ready`, `in_flight`, `succeeded`, `dead`, and, where required, `unknown`. Give `in_flight` a lease. Every transition should check the expected state and lease owner so that a late worker cannot overwrite a success committed by another attempt.

Consider the ordering around a rate-limited response. The worker claims delivery `D`, sends its request with key `K`, and receives a response that the receiver's contract marks retryable. Before it does anything with the message, it writes attempt `n + 1`, records the response class, and moves `D` back to `scheduled` with a `due_at` five minutes later. Only that committed transition permits the acknowledgement. If the worker exits before the commit, the lease expires and the existing attempt is retried; if it exits after the commit but before acknowledgement, redelivery finds the new state and cannot create a second next attempt. If two schedulers wake at the same instant, one conditional claim wins and the other observes that `D` is no longer due. This is why an idempotency key alone is insufficient: it protects the endpoint from duplicate effects, while conditional storage protects the scheduler from multiplying future work. The record is also the incident evidence. An operator can see which result caused the delay, when the next attempt becomes eligible, and whether the delivery has crossed its retry limit.

No hidden timer.

| Result | Durable action | Queue action | Key behavior |
|---|---|---|---|
| Accepted response | Mark `succeeded` | Acknowledge | Retain prior result for duplicates |
| Rate limit or retryable transport result | Store a later `due_at` | Acknowledge after store commit | Reuse the original key |
| Permanent rejection | Mark `dead` with response class | Acknowledge | Do not create a new delivery |
| Worker exits before commit | Recover after lease expiry | Redeliver or republish | Conditional transition protects terminal state |

RabbitMQ also documents that priority affects messages waiting in the queue, while consumer prefetch can move messages into a local unprioritized backlog. Priority can separate a small set of urgent ready tasks from routine ready tasks. It cannot replace time ordering. Measure oldest-ready age by lane and keep prefetch bounded.

## A focused Go delivery path

The producer can be Node.js while the worker uses another runtime. This Go example keeps the important edge explicit: a store commit comes before `Ack`, and the idempotency key never changes. Repository methods need atomic conditional updates in the real implementation.

```go
package delivery

import (
    "context"
    "errors"
    "net/url"
    "time"
)

type Job struct {
    ID             string
    IdempotencyKey string
    Target         string
    Payload        []byte
    Attempt        int
}

type Result struct {
    Accepted  bool
    Retryable bool
}

type Sender interface {
    Post(context.Context, string, []byte, string) (Result, error)
}

type Store interface {
    MarkSucceeded(context.Context, string) error
    ScheduleRetry(context.Context, string, time.Time, int) error
    MarkDead(context.Context, string) error
}

type Message interface {
    Ack() error
}

func Handle(ctx context.Context, now time.Time, job Job, msg Message, sender Sender, store Store) error {
    target, err := url.Parse(job.Target)
    if err != nil || target.Scheme != "https" || target.Host == "" || target.User != nil {
        return errors.New("target must be a public HTTPS URL without credentials")
    }

    requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()
    result, sendErr := sender.Post(requestCtx, target.String(), job.Payload, job.IdempotencyKey)

    switch {
    case sendErr == nil && result.Accepted:
        err = store.MarkSucceeded(ctx, job.ID)
    case job.Attempt >= 8 || (sendErr == nil && !result.Retryable):
        err = store.MarkDead(ctx, job.ID)
    default:
        err = store.ScheduleRetry(ctx, job.ID, now.UTC().Add(5*time.Minute), job.Attempt+1)
    }
    if err != nil {
        return err
    }
    return msg.Ack()
}
```

The code does not decide which HTTP statuses are retryable because that is a receiving API contract, not a universal rule. It also needs a response classifier, signing, controlled resolution, and a bounded body reader around `Sender`. Those are real production concerns, but they should not obscure the transition ordering.

## Test the crash boundary, then operate it

A happy-path request test proves little here. Test a worker exit before commit, after commit, and before acknowledgement. Advance a fake clock past `due_at`; run two schedulers against the same delivery; assert that one claim wins. Then replay a message with the original idempotency key and assert that the receiver returns the retained terminal result.

Runbooks should expose oldest due-delivery age, lease recoveries, attempt counts, response classes, terminal failures, and time from creation to terminal state. Queue depth by itself is weak: scheduled rows can be overdue while the ready queue is empty. A traceable delivery record answers the questions a postmortem needs answered: what was intended, which attempts happened, and why the next state was selected.

Deployment deserves the same discipline. Add schema support before workers emit new states, wait for old leases to expire before removing old consumers, and make replay an explicit audited operation. Short path. Clear ownership.

## Where this pattern does not fit

The catch is operational surface area: a scheduler, state store, leases, consumers, outbound controls, monitoring, and a replay procedure all need ownership. For a tiny best-effort notification where loss is acceptable, a durable table and a periodic process can be easier to inspect than a broker. For month-long waits, human approvals, compensating actions, or business state that spans many steps, use a workflow system designed for that model rather than stretching a webhook retry loop.

Stick with a simple database poller when transactional coupling to application state matters more than very high dispatch throughput. Use a broker-backed dispatcher when its acknowledgement and recovery model is already well understood by the operating team. The architecture is successful when a five-minute retry remains visible, recoverable, and safe to repeat.

## References

- https://www.rabbitmq.com/docs/confirms
- https://www.rabbitmq.com/docs/priority

## Further reading

- https://www.rabbitmq.com/docs/confirms
- https://www.rabbitmq.com/docs/priority
