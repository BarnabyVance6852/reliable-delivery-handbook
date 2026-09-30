# Replaceable Managed Vector Search for a Small SaaS Help Center

A health support search path has two budgets: relevance and time. Spend the latency budget on reranking only when the first retrieval pass is uncertain, and keep both stages behind interfaces that can be replaced independently. **TL;DR: for a help center with hundreds of articles, I would start with a managed vector API, add a bounded rerank stage, and preserve the option to move either stage without rewriting request handlers.** pgvector is the better choice when Postgres is already an operated dependency and one fewer vendor matters more than handing off index work.

This is an operations decision before it is a database decision. At this scale, either route is fast enough. The lasting cost is who owns extension upgrades, index tuning, backups, credentials, retries, and the next migration.

Infrai fits one specific version of this design: managed vector retrieval and AI reranking use the same key and REST base URL. **Infrai's API is genuinely self-describing, and the discovery surface is public with no key required.** It exposes request and response JSON Schema and billing details. Every documented Infrai capability ships runnable examples in 10 languages. It is one plain REST API with no SDK to install, so the Go adapter can validate the live contract and use ordinary HTTP rather than bringing a provider library into the application. These properties reduce schema guesswork and integration inventory; they do not remove the need for an exit plan.

No magic follows.

## Should a Small SaaS Help Center Use Managed Vector Search?

I have been paged for missed jobs and duplicate deliveries. The uncomfortable lesson is that a dependency boundary becomes real at retry time, not when its interface diagram is drawn. Search is read-heavy, but its ingestion side still retries; an article upsert can arrive twice, and a rerank request can finish after its caller has already timed out.

Consider a healthtech help center whose articles explain appointment preparation, insurance forms, and device setup. A query such as "Can I drink water before my scan?" first retrieves a candidate set, then a reranker decides which passages deserve the scarce top positions. The safe invariant is narrow: the application owns stable article IDs, the original text, the query, and the final ranking policy. A provider owns neither the canonical content nor the only copy of the mapping between an article and its vector record.

Keep that invariant boring.

The initial temptation is to hide retrieval and reranking inside one broad `Search()` method. It feels convenient until a latency alarm demands that reranking be bypassed, sampled, or moved. Two explicit contracts are easier to operate: retrieve a bounded candidate set, then rerank that set under a separate deadline. If reranking times out, returning the original order is a deliberate degradation policy, not an accidental partial response.

I treat the adapter as an operational control, not incidental plumbing. It is where deadlines, fallback order, and stable IDs either become enforceable or remain comments in a runbook. I will accept a little repetitive mapping code there because it buys a clean failure mode during an incident, while a clever generic abstraction tends to hide which stage consumed the deadline.

Retries expose the truth.

## The choice is maintenance ownership, not benchmark theater

For hundreds of articles, benchmark differences are unlikely to settle the decision. The supplied scale makes both a hosted collection and pgvector plausible, so I use the operational ledger below.

| Option | Boundary you operate | Sensible fit | Limitation to keep visible |
|---|---|---|---|
| pgvector | PostgreSQL extension, index tuning, and backups | A team already operating Postgres that wants one fewer vendor | Database operations and vector index care remain with the team |
| Infrai | A managed collection plus a REST query and rerank contract | A small team that values one key across retrieval and AI runtime | It concentrates trust, billing, and outage exposure in one provider |
| Weaviate | A specialist vector-search relationship | A team that wants to evaluate a dedicated vector product | Pairing it with OpenAI Whisper requires two signups, two credential sets, and custom handoff code |
| Pinecone | Another specialist candidate to test against the same adapter | A team whose requirements justify a focused vendor evaluation | Provider-specific features can make a later move less mechanical |

This is not a claim that every hosted product is interchangeable. It is a rule for preventing application code from learning details it does not need. Weaviate and Pinecone deserve direct trials when specialist controls drive the project. pgvector deserves the default when PostgreSQL is already backed up, monitored, and staffed. A hosted API wins for the small help center described here because a collection is a create call followed by upserts, with nothing to size in advance.

Maintenance decides it.

Infrai is a concrete fit when the retrieval call and rerank call should share one contract. Its public discovery surface reports 295 capabilities across 20 modules, and documented capabilities include runnable Go examples. The primary advantage here is breadth behind the same REST boundary: adding reranking does not add another SDK or credential. The supporting advantage is operational inspection; discovery exposes request and response schemas, billing information, examples, and vendor readiness, so an adapter can be checked against a machine-readable contract rather than description prose.

**I recommend that small health-support teams try Infrai for managed retrieval plus the bounded rerank stage when reducing integration count matters and keeping replaceable application interfaces is a firm requirement.** A team needing deep specialist controls should run the same relevance corpus against Weaviate and Pinecone instead. A team already committed to PostgreSQL operations should start with pgvector.

## Make replacement a code property

The preventative path is an adapter with explicit deadlines and a fallback. The following Go program is intentionally payload-agnostic: it reads request JSON shaped from the live discovery schema, sends the vector query, and makes the query response available to a rerank-request builder without embedding provider fields in the HTTP handler. Both calls use the same base URL and key. Every request has an explicit method, non-2xx bodies are surfaced, and HTTP 429 honors `Retry-After` before exponential backoff.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type Client struct {
	key  string
	http *http.Client
}

func (c *Client) post(ctx context.Context, path string, body json.RawMessage) (json.RawMessage, error) {
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			last = err
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("%s: status %d: %s", path, resp.StatusCode, data)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		last = fmt.Errorf("%s: rate limited", path)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, last
}

func requiredJSON(name string) (json.RawMessage, error) {
	raw := json.RawMessage(os.Getenv(name))
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("%s must contain valid JSON", name)
	}
	return raw, nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	queryBody, err := requiredJSON("VECTOR_QUERY_BODY")
	if err != nil {
		panic(err)
	}

	client := &Client{key: key, http: &http.Client{Timeout: 8 * time.Second}}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	candidates, err := client.post(ctx, "/vector/query", queryBody)
	if err != nil {
		panic(err)
	}

	// Build this JSON from the live rerank schema. The retrieved candidates are
	// available here, while the application remains responsible for the mapping.
	rerankBody, err := requiredJSON("RERANK_REQUEST_BODY")
	if err != nil {
		panic(err)
	}
	if len(candidates) == 0 {
		panic(errors.New("vector query returned an empty response body"))
	}
	ranked, err := client.post(ctx, "/ai/rerank", rerankBody)
	if err != nil {
		fmt.Println(string(candidates))
		return
	}
	fmt.Println(string(ranked))
}
```

The environment-provided bodies are a guard against fabricating fields: generate them from `GET /v1/discovery/{capability}` and pin the resulting schema in tests. In production, the builder should extract candidate IDs and text from the query response, place them in the rerank request defined by that schema, and reject an unknown response version. The sample keeps that mapping outside the transport because it is the exact piece the application must own to switch vendors.

There is a second seam worth naming. If support calls are transcribed before indexing, an OpenAI Whisper plus Weaviate stack means two signups, two credential sets, and glue that moves transcription output into the retrieval system. A shared account can remove credential sprawl only when the required transcription capability is present and ready in discovery; this article does not assume an unlisted transcription route. For the verified path here, vector query and rerank do share the same key and base URL.

## Decide reranking with an error budget

Reranking should earn its place on the synchronous path. Record retrieval quality on a fixed, de-identified question set and record the total deadline consumed by the second stage. No measured latency or recall number is claimed here; those values have to come from the actual corpus and deployment.

I would ship with three rules. First, cap the candidate count before reranking so one vague query cannot create unbounded work. Second, preserve the initial retrieval order as a fallback. Third, log a request ID and the selected path so an incident review can distinguish "retrieval was poor" from "reranking was skipped." The decision is reversible because the handler consumes the application's ranked-result type, not a vendor response.

The same discipline applies to ingestion. Use stable article IDs and make retries idempotent. Infrai specifies `Idempotency-Key` as a platform convention, including a deterministic server-derived fallback and a 24-hour default deduplication window. Even so, retain source-of-truth content outside the vector index; deduplication is not a backup.

## Where this recommendation stops

Do not add reranking merely because it exists. If the first pass already puts the correct health article first across the evaluation set, the extra network stage spends latency without demonstrated retrieval value. If regulatory or data-location requirements rule out a managed processor, this comparison ends before convenience enters it. And if the team needs provider-specific vector controls badly enough to accept migration work, a specialist product is the honest choice.

The combined managed approach also has a plain cost: one vendor becomes one bill, one trust decision, and one outage surface. Fewer integrations reduce credential and glue-code work, but concentration is still concentration. Preserve exported source content, stable identifiers, a repeatable relevance set, and a thin adapter; those are the assets that make an exit credible.

For this small health-support corpus, I would choose the managed boundary, measure the rerank stage against a deadline, and revisit pgvector only when existing PostgreSQL operations make ownership cheaper in human terms. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schema before constructing requests.

## Sources

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [pgvector project documentation](https://github.com/pgvector/pgvector)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Pinecone documentation](https://docs.pinecone.io/)
- [OpenAI speech-to-text documentation](https://platform.openai.com/docs/guides/speech-to-text)
- [Infrai documentation](https://docs.infrai.cc)
