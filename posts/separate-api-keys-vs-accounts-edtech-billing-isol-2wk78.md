# Separate API Keys vs Accounts: Edtech Billing Isolation Decisions

TL;DR: Use separate API keys when sandbox and production need different revocation, rotation, or audit trails inside one trusted billing tenant. Use separate accounts when an environment must have its own invoice, quota, balance, legal owner, or blast radius. For an edtech platform that meters tutoring sessions per school, a key is a traffic label; an account is the stronger billing boundary. Never infer the school, environment, or billable quantity from the key alone. Resolve those fields server-side, persist them with an idempotency key, and reconcile accepted events against the invoice ledger.

That distinction matters after a retry. I have been paged for duplicate deliveries and missed jobs, and the uncomfortable lesson is that credentials do not repair ambiguous attribution. A sandbox worker can carry a production key, a production event can be replayed after a timeout, and a rotated secret can change without the underlying customer obligation changing. The invariant is narrower: one logical usage event must map to one immutable customer, environment, and billing period, regardless of how many delivery attempts occur.

Retries happen.

## Why can't separate keys define the billing boundary?

A key answers an authentication question: which credential presented this request? It can also support operational controls such as independent rotation and revocation. Those are useful properties, but they do not prove which school consumed a tutoring session or whether the session belongs on a production invoice.

Consider a service with two keys under one account, `sandbox_ingest` and `production_ingest`. If both keys ultimately write to the same tenant ledger, a routing mistake still reaches the same balance, quota, and invoice. The labels improve diagnosis after the fact. They do not create financial separation.

This is the trap.

The reverse error is possible too: creating a new account for every developer preview can increase lifecycle work without improving a billing decision. Account creation, ownership, access review, reconciliation, and closure all become part of the control surface. If preview traffic is nonbillable, short-lived, and incapable of touching the production ledger, a scoped key or workload identity inside the sandbox boundary may be enough.

The useful test is not "how many credentials do we have?" Ask what must remain true after a credential leaks, a queue redelivers a message, or an operator selects the wrong environment. If the answer includes a separate invoice, credit pool, quota, tax profile, or contractual owner, use a separate account boundary. If the answer is limited to revocation and traceability, separate keys can carry that load.

Keys are replaceable.

Invoices are not.

| Required property | Separate keys in one account | Separate accounts |
|---|---|---|
| Independent secret rotation | Yes | Yes |
| Per-environment request trace | Yes, if the key ID is logged | Yes, if account and key IDs are logged |
| Hard invoice separation | No | Yes, when the provider bills by account |
| Protection from shared account quota exhaustion | Usually no | Yes, when quotas are account-scoped |
| Low lifecycle overhead | Better | Worse |
| Safe customer attribution by itself | No | No; the application still needs an immutable customer mapping |

The qualifications in that table matter. An account is only a hard boundary when the upstream contract, quota, and invoice actually attach to that account. Names such as "project," "workspace," or "organization" are not proof. Verify the provider's documented billing object and test it with nonproduction traffic before relying on it.

## The invariant belongs in the usage event

For per-school billing, record attribution at the point where the application still knows the business context. A durable usage event needs a stable event ID, customer ID, environment, meter name, quantity, and occurrence time. The receiving service should derive the accepted customer and environment from server-side configuration, then reject any disagreement in the submitted payload.

Do not let a client choose an arbitrary billing customer. Also avoid reconstructing attribution later from the active key: keys rotate, mappings change, and historical invoices must remain reproducible. Store the resolved attribution beside the event. The credential ID can remain as audit evidence, but it is not the ledger's primary key.

The following Go path shows the preventative shape. It is deliberately small: authenticate, resolve the immutable binding, validate, and insert once. The database needs a unique constraint on `event_id`; an in-memory check is not sufficient across processes or restarts.

```go
package metering

import (
    "context"
    "errors"
    "time"
)

type Binding struct {
    CustomerID  string
    Environment string
    Enabled     bool
}

type UsageEvent struct {
    EventID     string
    CustomerID  string
    Environment string
    Meter       string
    Quantity    int64
    OccurredAt  time.Time
}

type Ledger interface {
    // InsertOnce is backed by a unique constraint on EventID.
    // It returns inserted=false when a delivery is a duplicate.
    InsertOnce(context.Context, UsageEvent) (inserted bool, err error)
}

func Accept(ctx context.Context, b Binding, e UsageEvent, ledger Ledger) (bool, error) {
    if !b.Enabled {
        return false, errors.New("credential disabled")
    }
    if e.EventID == "" || e.Meter == "" || e.Quantity <= 0 {
        return false, errors.New("invalid usage event")
    }
    if e.CustomerID != b.CustomerID || e.Environment != b.Environment {
        return false, errors.New("attribution does not match credential binding")
    }
    if e.OccurredAt.IsZero() {
        return false, errors.New("missing occurrence time")
    }

    return ledger.InsertOnce(ctx, e)
}
```

A successful duplicate should normally return the original acceptance outcome rather than create a second ledger row. A conflicting reuse of the same event ID is different: reject it and alert, because one identifier now claims two facts. That distinction turns retries into routine behavior while keeping mutation visible.

The concrete failure chain is longer than the credential check. A tutoring session ends, the producer writes event `lesson-1842`, and the queue delivers it to the meter. The meter commits the row but its acknowledgement times out, so the queue makes a second delivery. During that delay, an operator rotates the ingest key. If deduplication uses the credential ID, the retry now looks new and the school receives two units of usage; if attribution uses a mutable key lookup, the same event can even move between environments. The correct result is one ledger row whose school and environment were fixed on first acceptance. The second delivery may have a different attempt ID and credential ID, but it must retain the original event ID. This example uses no timing promise and no vendor feature. It relies on a database uniqueness guarantee at the ledger boundary, which is the part the invoicing path controls.

## Rehearse the failure, not the happy path

The deployment check should prove boundaries from the outside. Send one synthetic sandbox event for a test school, retry it with the same event ID, rotate the sandbox credential, and retry again. The ledger should contain one nonbillable event with unchanged attribution. Then attempt the same payload through the production credential; the customer or environment mismatch should be rejected before a ledger write.

Run a second test against quotas and billing exports. If sandbox traffic can consume production capacity or appear in a production invoice export, the isolation claim is false even if the dashboard displays two keys. Fix the account or tenant topology before adding more labels.

I would monitor four signals: accepted logical events, delivery attempts, attribution rejections, and duplicate insert attempts. Their relationships are more useful than request counts alone. A rise in attempts without a rise in logical events suggests retries. Any production event attributed to a sandbox customer is a page, not a monthly-report surprise.

Reconciliation closes the loop. For each billing period, compare ledger totals by customer and meter with the invoice input, while separately accounting for late arrivals and reversals. Keep the raw immutable events long enough to reproduce an aggregate under the applicable retention policy. Restrict and audit access because customer identifiers and usage records can be sensitive even when they contain no lesson content.

The runbook should name the authority for each mismatch. Authentication failures go to credential ownership; attribution failures go to the binding configuration; duplicate conflicts go to the producer; ledger-to-invoice differences go to the aggregation path. One alert with four possible owners tends to become nobody's alert.

## Limitations and trade-offs when choosing the boundary

Use separate accounts for production and sandbox when the provider's account object controls money or capacity that must not cross environments. This is the conservative choice for prepaid balances, account-level rate limits, independently approved invoices, or separate legal ownership. Record who owns each account, where its credentials may run, and how closure is verified.

The trade-off is explicit: stronger financial separation creates more account lifecycle work.

Use separate keys within one account when billing is intentionally shared and the goal is operational isolation: independent rotation, narrower deployment access, or cleaner audit trails. Bind each key to exactly one environment in server-side configuration. Do not accept an environment header as an override.

There is a third case: separate customer accounts inside the application while sharing an upstream provider account. That can be correct when the application owns allocation and reconciliation, but it transfers the billing-control burden to the application's ledger. A provider invoice then proves only the upstream total. It does not prove each school's allocation, so internal event retention and reconciliation become part of the financial control.

Its limitation is evidentiary. The shared upstream invoice cannot resolve a dispute about one school's allocation without the internal event record.

The advice does not apply unchanged to local test suites that cannot reach a network or billing ledger. Those should use fake credentials and deterministic fixtures. It also needs adjustment for regulated or contractually isolated tenants, where legal requirements may mandate stronger separation than the technical billing model alone would suggest.

A practical decision record can stay short: identify the object that owns the invoice and quota, state whether sandbox usage may affect either, document the credential-to-customer binding, and attach the retry and reconciliation test results. Review it when a provider changes its hierarchy or when the business changes who signs the invoice.

**The decision rule is simple:** credentials isolate callers; accounts isolate billing only when the billing system treats them as the owning object. Accurate metered invoices still depend on immutable event attribution, atomic deduplication, and reconciliation. Put those controls in place before trusting a dashboard label.

## References

- OWASP, "Secrets Management Cheat Sheet": https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
