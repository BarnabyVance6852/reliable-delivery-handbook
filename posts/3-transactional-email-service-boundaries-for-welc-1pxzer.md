# 3 Transactional Email Service Boundaries for Welcome Emails and Report Deliverability

A scheduled report is not delivered when the scheduler says it ran. TL;DR: for an ecommerce report attached to a transactional email, separate report generation, a durable send intent, and provider acceptance. Choose an API-first mail service only after checking domain authentication and retry semantics. The same authenticated sending domain can carry welcome mail, but the two jobs need different triggers and deduplication keys.

The trigger matters.

Consider a bounded production failure: a report job times out after submitting an attachment, and the worker retries without knowing whether the first request was accepted. The merchant gets two reports. Two sends. I would treat that as a delivery-state ambiguity, not proof that the cron expression was wrong. The invariant is narrower: one scheduled report period creates one durable intent, while any repeat submission must be identifiable as the same intent. Provider acceptance still does not prove inbox placement.

## Where does the duplicate actually enter?

It enters between successful remote submission and local acknowledgement. A database transaction cannot atomically commit alongside an unrelated email API. Record the report period and recipient as a unique intent before calling the API, store the generated artifact's stable identifier, and hand that intent to a worker. If the API supports a documented idempotency key, send the intent ID as that key and check its retention window. If it does not, a network timeout remains ambiguous: inspect provider-side events or reconcile by a stable message reference before attempting another submission. Do not label a timeout "not sent." An operator looking at a queue retry count has only evidence that the worker retried; they still need the remote reference or the provider's event record to determine whether the recipient could have seen a first copy. That is why the retry policy belongs in the runbook next to the reconciliation procedure, rather than being hidden in a generic HTTP client.

For the welcome message, the deduplication boundary is the account event, not a daily schedule. Do not share a key based only on the recipient address: that could suppress a later legitimate message or conflate welcome and report workflows. Nor should an attachment be reconstructed on every retry if its contents can change during the run; capture the report snapshot once, then reuse the artifact.

That snapshot matters. An inventory report generated before a stock correction and another generated afterward must not silently masquerade as the same attachment under one intent ID. Decide which version the scheduled run owns, persist its checksum, and make a later correction an explicit new delivery decision rather than an accidental side effect of a retry. This distinction is small in the send handler and large in an incident review.

## How should a transactional email service authenticate welcome emails for deliverability?

Domain verification in a provider dashboard is a setup checkpoint, not a deliverability result. Publish the provider's required DNS records, confirm they resolve publicly, and verify that the visible From domain aligns with an authenticated domain. SPF specifies authorization of sending hosts; DKIM signs selected message headers and body content; DMARC evaluates alignment with the visible From domain. A passing SPF lookup alone does not establish alignment. The relevant standards are RFC 7208, RFC 6376, and RFC 7489.

Keep attachment MIME construction at one boundary too. An API-first integration is useful when the application already owns the report bytes and metadata and the provider accepts those bytes through a documented API. It can require more integration work if the existing system already emits standards-compliant MIME through a managed SMTP path, or if the API imposes attachment limits that the report regularly exceeds. Check limits and encoding against the selected service's current documentation before committing to the path; there is no universal attachment ceiling. A link to an authenticated download may fit a large report better, but access control and expiry become part of the workflow.

No single protocol fixes this.

If operations already has a monitored SMTP relay and the application does not need provider event correlation, replacing that relay with an API can be the wrong trade-off: the team now owns new credentials, request retries, error classification, and attachment serialization. Conversely, an API may reduce integration effort when the application already prepares binary artifacts and the service exposes a documented send contract. Assess both against the same failure injection, not against a successful demonstration send.

## What should the worker record?

The worker should preserve intent ID, report period, artifact checksum, submission attempt ID, remote message reference when available, and the final observed delivery event. None of those fields should contain raw report contents. This focused Go branch assumes a store with an atomic acceptance update and a mail adapter whose repeated-key behavior is documented:

```go
type ReportIntent struct {
    ID, Recipient, ArtifactID, Checksum string
}

type Mailer interface {
    Submit(ctx context.Context, intent ReportIntent, key string) (messageID string, err error)
}

func sendReport(ctx context.Context, store IntentStore, mailer Mailer, id string) error {
    intent, err := store.Load(ctx, id)
    if err != nil { return err }
    if intent.MessageID != "" { return nil }

    messageID, err := mailer.Submit(ctx, intent.ReportIntent, intent.ID)
    if err != nil { return err } // A timeout is ambiguous; reconcile before retrying.
    return store.RecordAcceptance(ctx, intent.ID, messageID)
}
```

The early return protects a recorded acceptance. It cannot cover a crash after remote acceptance and before `RecordAcceptance`; that gap needs either a provider-enforced idempotency contract or reconciliation.

There is no free retry.

Test precisely that crash point, a repeated schedule firing for the same period, expired API credentials, a DNS misconfiguration, and a report larger than the configured attachment limit. Run a staged send to controlled mailboxes after DNS changes. Keep delivery, bounce, and complaint events correlated to the intent, but do not treat an open as reliable evidence of reading: Mail Privacy Protection can download remote content in the background.

If a report is regenerated on demand, duplicates are harmless, and a human can retry manually, a durable queue and reconciliation loop may cost more operational effort than they prevent. For scheduled financial or inventory reports, start with the unique intent and explicit ambiguous-outcome handling. Decide between API and SMTP by measuring adapter work, attachment handling, available idempotency or event correlation, and who owns the runbook. Price alone does not answer the integration question. Welcome mail also needs its own consent and purpose review where applicable; GDPR Article 7 sets conditions for consent, but a welcome message's legal basis cannot be inferred from its template name.

## References

- https://www.rfc-editor.org/rfc/rfc7208
- https://www.rfc-editor.org/rfc/rfc6376
- https://www.rfc-editor.org/rfc/rfc7489
- https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- https://gdpr-info.eu/art-7-gdpr/
