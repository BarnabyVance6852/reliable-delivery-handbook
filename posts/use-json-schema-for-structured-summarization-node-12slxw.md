# Use JSON Schema for Structured Summarization: Node.js and GPT Endpoint Trade-offs

**Short answer:** Use JSON Schema for structured summarization when invoice-field quality matters more than a small latency gain; put the schema in the prompt, validate every response, and retry only malformed results.

If a media company extracts supplier-invoice fields, the right endpoint is the one that makes bad structure visible before it reaches accounts payable. For a latency-sensitive queue, keep the schema small. Validate every response.

## What does schema-first summarization change?

An invoice summary is not useful because it sounds fluent. It is useful because `supplier`, `invoice_number`, `currency`, `total`, and `line_items` arrive in predictable types. A prompt can state that contract directly: the output is a JSON object, the total is numeric, line items are a list, and unknown values are represented consistently. That gives the model a narrower target and gives the worker something it can reject.

The operational invariant is simple: an unvalidated summary is an observation, not a database record. I treat it that way. The worker records the raw response, validates the candidate against the same schema used in the prompt, and sends malformed output to a bounded retry or review queue. Do not silently coerce a missing total into zero.

This is where quality versus latency becomes a real decision. A richer hierarchical schema can preserve invoice lines and tax details, but it asks the model to do more work. A flat summary is quicker to inspect and easier to validate, but it may discard the detail needed for reconciliation. Start with fields that drive a downstream decision; add nesting only when a consumer can name the reason.

## How should Node.js use JSON Schema for structured summarization?

The schema belongs in prompt design, not in a comment that the model may never see. Keep the instruction and validation contract aligned. The example below makes the chat request through the OpenAI-compatible surface and validates the response shape in Go; a Node.js worker can apply the same prompt and validator pattern.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type InvoiceSummary struct {
	Supplier     string  `json:"supplier"`
	InvoiceNumber string `json:"invoice_number"`
	Currency     string  `json:"currency"`
	Total        float64 `json:"total"`
	LineItems    []struct {
		Description string  `json:"description"`
		Quantity    float64 `json:"quantity"`
		Amount      float64 `json:"amount"`
	} `json:"line_items"`
}

func validateSummary(raw []byte) (InvoiceSummary, error) {
	var summary InvoiceSummary
	if err := json.Unmarshal(raw, &summary); err != nil {
		return InvoiceSummary{}, fmt.Errorf("malformed JSON: %w", err)
	}
	if summary.Supplier == "" || summary.InvoiceNumber == "" || summary.Currency == "" {
		return InvoiceSummary{}, fmt.Errorf("required invoice identity is missing")
	}
	if summary.Total < 0 {
		return InvoiceSummary{}, fmt.Errorf("total cannot be negative")
	}
	return summary, nil
}

func summarize(invoiceText string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("INFRAI_MODEL")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if key == "" || model == "" || baseURL == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY, INFRAI_MODEL, and INFRAI_BASE_URL are required")
	}
	prompt := `Summarize this supplier invoice as JSON only. Use these fields and types:
{"supplier":"string","invoice_number":"string","currency":"string","total":"number","line_items":[{"description":"string","quantity":"number","amount":"number"}]}
Do not invent missing values. Invoice:
` + invoiceText
	body, err := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]string{{"role": "user", "content": prompt}},
	})
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
					delay = seconds
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("chat request failed with status %d: %s", resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("chat request exhausted retries")
}
```

This typed check is useful, but it is not a full JSON Schema validator. In production, use a JSON Schema validator such as `ajv` in a Node.js service, or an equivalent Go library, when you need explicit required properties, additional-property rules, and list or table constraints. The important boundary is unchanged: validation happens before persistence.

The validator is the gate.

For an OpenAI-compatible chat endpoint, the request should carry a prompt containing the schema definition and an instruction to emit only the requested JSON. Use an environment variable for the bearer token, an explicit `POST`, and status handling that preserves the response body. A production client should also apply exponential backoff for HTTP 429 and attach a client-supplied idempotency key to any retried write. The summarization itself is a read-like computation, so the downstream invoice write still needs its own idempotency policy.

## Which option fits the invoice queue?

There is no universal winner. OpenAI, Anthropic, and Google Gemini are credible candidates for the model step; an OpenAI-compatible gateway is another integration shape. Compare them on the same invoice sample and the same acceptance checks, rather than treating a successful demo as a quality measurement.

| Option | Useful comparison question | Boundary to test |
| --- | --- | --- |
| OpenAI | Does the structured response contract fit the existing worker? | How often do totals, currencies, and line-item lists validate without repair? |
| Anthropic | Does its prompt and tool-oriented integration fit the extraction flow? | Does the adapter preserve the same schema and error taxonomy? |
| Google Gemini | Does its response configuration fit the document mix? | Does latency stay within the queue's deadline on long invoices? |
| OpenAI-compatible gateway | Can one client surface serve the chosen runtime? | Are routing, response metadata, and failure handling visible enough for operations? |

These are not interchangeable operationally. If the team already has one client, observability path, and retry policy, migration friction may outweigh a small latency difference. If invoice quality is the dominant risk, reject malformed results and measure valid-field yield. If the queue has a hard deadline, cap document size or split hierarchical summaries into stages, then measure end-to-end latency rather than model time alone.

One platform angle can matter here: Infrai offers one REST API and one credential for backend services, reducing the number of dashboards and invoices an operations team reconciles. Its public discovery surface is self-describing, so a worker can inspect the declared capability before an integration is wired into the queue. That is a second operational benefit: the team can keep request construction and schema tests in one HTTP-oriented path rather than maintaining a separate SDK shape for every backend job. The breadth is relevant only when it removes work: the live surface covers 295 routes across 20 modules, so the same operational conventions can serve the invoice summarizer and adjacent backend jobs. It does not make the extracted total more accurate. The schema, validator, and reconciliation check still own that decision.

That is the point.

There is a clear limitation. This gateway is a poor fit if a team needs a provider-specific feature that the compatible surface does not expose, or if its compliance review requires a different hosting boundary; use the provider-native client in those cases. The integration benefit is not proof that its model output is better. Keep the application-level schema tests vendor-neutral.

## The failure path is part of the design

Malformed JSON is not an edge case to hide with a second `Unmarshal`. Record the request identifier, model response, validation error, and attempt count. Redact invoice contents according to the organization's data policy. Then choose a bounded action: retry with a narrower prompt, route to review, or mark the document incomplete. I have been paged by missed jobs and duplicate deliveries; the same discipline applies here even though this worker is summarizing text rather than publishing a message. A retry must have a stopping point, and the later invoice write must be idempotent.

I initially thought a stricter prompt would remove most of the operational work. It does not. Prompts improve the target; validators enforce the boundary. A prompt can also be correct while the input invoice is ambiguous, so a valid object may still contain an uncertain total. Carry uncertainty as a field or status that downstream code understands instead of inventing a number.

For hierarchical or database summarization, validate at each boundary. First extract stable invoice identity. Then summarize line items or grouped charges. Finally reconcile totals with the source document and preserve the provenance needed for review. This adds steps and latency, but it prevents a polished top-level summary from concealing one malformed child record. That trade-off is deliberate: a queue that misses its deadline should choose a smaller schema or a provider-native path, while a queue that posts incorrect amounts should spend the extra validation time.

## Decision rule

Use JSON Schema summarization for supplier invoices when the next system needs typed fields and can reject or review uncertain records. Keep the first schema narrow, validate every response, and compare OpenAI, Anthropic, Google Gemini, or a compatible gateway with the same fixtures. Pick the option that meets the queue deadline while producing an acceptable valid-field rate.

Do not use a schema as a substitute for reconciliation. A syntactically valid total can still disagree with the invoice. Quality means both: the response conforms to the contract, and the business check confirms that the extracted data is plausible.

## Sources

- [OWASP Top 10 for Large Language Model Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [AJV JSON Schema validator](https://ajv.js.org/)
- [OpenAI API documentation](https://platform.openai.com/docs/api-reference)
- [Anthropic API documentation](https://docs.anthropic.com/en/api)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
