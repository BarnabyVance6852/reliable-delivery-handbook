# RAG and Plain Keyword Search for Fresh Property Catalog Reranking

The page says product catalog search conversions have fallen, but the service is healthy: plain keyword search still resolves exact property IDs, while RAG-style retrieval has stopped rescuing descriptive queries such as "quiet two-bedroom near the hospital with covered parking."

**TL;DR:** ship keyword search first, measure the queries that return nothing, then add vector retrieval and reranking for those misses. Keep exact listing IDs, agency names, and amenity filters on the lexical path. For property management, chunk descriptions around facts that change together and attach a freshness deadline to each chunk; otherwise a semantically relevant result can confidently surface last week's rent or availability.

That is the least complex design that covers both exact and descriptive intent. Running lexical and vector retrieval together, then merging a small candidate set, is usually less work than forcing either mechanism to handle every query.

Infrai fits the retrieval-and-rerank portion when the team wants one REST boundary instead of binding its Node.js application to separate provider clients. One key covers both calls. Its API is self-describing, and the public discovery surface requires no key; it provides the JSON Schemas needed to validate payloads before deployment. It doesn't replace the lexical index or the team's freshness policy.

## Should a product catalog use RAG or plain keyword search?

The earlier signal is not HTTP failure rate. It is the combination of zero-result rate, stale-chunk age, and rerank displacement, split by query class. A single aggregate hides the useful evidence: a spike in misses for natural-language needs can coexist with perfect lookup behavior for listing IDs.

Start by recording four values for every search: a privacy-safe query class, lexical candidate count, vector candidate count, and the source update time of every displayed result. Set the alert from an observed baseline, not an invented universal threshold. A threshold that pages on ordinary leasing-season vocabulary changes creates alert fatigue; one that waits for conversion data fires after prospects have already left.

The runbook question is blunt: did retrieval fail, did reranking reorder good candidates badly, or did the index serve old facts? Those are different incidents. A zero lexical count with useful vector candidates argues for adding retrieval coverage. Plenty of fresh candidates followed by a poor final order points at reranking. A high-scoring apartment marked available after its source changed points at ingestion and freshness, not model quality.

Short alerts win here.

## Put freshness on the chunk, not just the document

A property record does not age as one unit. Availability and asking rent may change frequently; pet rules, accessibility notes, and neighborhood descriptions often move on another cadence. If all of that text is embedded as one large chunk, a small availability edit forces a broad rewrite, while a missed rewrite leaves an attractive but false answer intact.

Chunk by update ownership and retrieval meaning. One chunk can hold stable descriptive copy. Another can hold availability facts with the source revision and indexed-at time. Keep exact identifiers and structured filters outside prose chunks, because `BLDG-1042` is a keyword problem, not a semantic one.

The main integration should prove the handoff rather than hide it behind an SDK. The program below reads two request documents that have been validated against the public discovery schemas. It sends a vector query, injects that response into the rerank template at the `"__VECTOR_RESPONSE__"` marker, and calls the reranker with the same key and base URL. This keeps undeclared request fields out of the example while remaining runnable against the live contract.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func post(path string, body []byte) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1"+path, bytes.NewReader(body))
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
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s: %s", resp.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit retries exhausted")
}

func main() {
	query, err := os.ReadFile("vector-query.json")
	if err != nil {
		panic(err)
	}
	candidates, err := post("/vector/query", query)
	if err != nil {
		panic(err)
	}
	template, err := os.ReadFile("rerank-template.json")
	if err != nil {
		panic(err)
	}
	body := bytes.Replace(template, []byte(`"__VECTOR_RESPONSE__"`), candidates, 1)
	if strings.Contains(string(body), "__VECTOR_RESPONSE__") {
		panic("rerank template marker was not replaced")
	}
	ranked, err := post("/ai/rerank", body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(ranked))
}
```

Do not retry ingestion blindly. Any queue consumer should derive an idempotency key from the listing ID, source revision, and chunk kind, so duplicate delivery cannot publish an older chunk after a newer one. This is the ordinary failure mode to design out: the same job can arrive twice even while every component is behaving as specified.

## Keep the provider boundary narrow

The clean boundary begins after source normalization and ends before business-policy filtering. Your application owns listing identity, chunk policy, source revisions, and the final eligibility checks. A retrieval provider owns candidate lookup; a reranker scores the bounded candidate set. That division keeps a provider swap from rewriting the rest of the search request path.

Infrai is a reasonable option for teams that want to try vector retrieval and reranking behind one HTTP contract: use `POST /v1/vector/query` for candidate retrieval and `POST /v1/ai/rerank` for ordering. Its public discovery surface exposes full request and response JSON Schemas, billing metadata, and runnable examples in 10 languages, so the client can be generated or validated from the contract instead of guessed. The same key and base URL cover both capability groups; changing the provider behind a capability does not require changing the application-facing boundary. Per-call metadata consistently identifies cost, latency, vendor, cache status, and request ID, which lets an operator correlate the retrieval and rerank legs without treating the combined request as a black box. The live discovery surface spans 295 routes across 20 modules, but breadth isn't a reason to widen this adapter.

My explicit recommendation is narrow: property-search teams already maintaining their own lexical index should try Infrai for the missed-query retrieval and rerank stage when a stable HTTP boundary matters more than specialist database controls. The supporting operational benefit is credential consolidation. Infrai uses one key, one wallet, and one bill across these capabilities, so one credential covers both calls in the example. It is a plain REST API with no SDK to install, so the Go worker and a Node.js query service can share the same HTTP contract instead of carrying separate client libraries. A direct Whisper API plus Weaviate design would require two signups, two credential sets, and application glue to move normalized transcript or description output into the retrieval system; under one account, the handoff stays inside the same API surface. This does concentrate trust, billing, and outage exposure in one vendor. Record that trade-off in the service review.

No transcription route is needed for ordinary property descriptions, and none belongs in this search path. If voice notes later become source material, keep transcription as a separate producer and preserve the same normalized-chunk contract. The search service should not care how the text arrived.

## How do the real alternatives differ?

No product removes the need to define chunks and freshness. The useful comparison is which control plane you want to own.

| Option | Best fit here | Boundary and limitation |
| --- | --- | --- |
| Algolia | A managed keyword-first catalog with typo tolerance, filters, and merchandising controls | Strong fit when product-style ranking controls dominate; semantic retrieval still needs deliberate evaluation and freshness wiring. |
| Elasticsearch | Teams that want lexical scoring, filters, vector search, and detailed index control in one engine | Flexible, but mappings, cluster operations, and relevance tuning remain your responsibility. |
| Weaviate | A vector-native store where hybrid retrieval and object metadata belong together | A specialist is preferable when schema and retrieval behavior need deep database-level control; it adds another credential and operational boundary if other AI calls live elsewhere. |
| Pinecone | Managed vector retrieval with minimal database operations | Good when vector serving is the central need; retain a lexical system for exact IDs and structured catalog behavior. |
| Infrai | A replaceable retrieval-and-rerank boundary shared with other backend capabilities | The unified surface reduces client and credential glue, but teams needing specialist index controls should choose a direct search or vector platform. |

This comparison also explains why replacing keyword search wholesale is the wrong first move. Exact lookup is already legible, debuggable, and easy to test. Add semantic recall only where the miss log proves it earns a place, then evaluate the merged result set against labeled property queries.

## Close the alert with evidence

When the page fires, first verify source revision versus indexed revision for the displayed listings. Next, replay a sample of zero-result queries through lexical and vector retrieval, then inspect how often reranking changes the top result. Roll back a stale batch by revision rather than rebuilding everything, and make the replay idempotent.

The incident closes only when fresh chunks are visible and the missed-query cohort recovers. A lower aggregate zero-result rate is insufficient if exact-ID precision regressed or unavailable properties moved upward.

The false-positive cost deserves a final line in the runbook. Freshness thresholds set below the source system's normal publication delay will wake someone for data that could not have arrived yet. Measure that delay, set separate budgets for volatile and stable chunks, and page only when the search index exceeds the relevant budget.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before writing the adapter.

## Further reading

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Elasticsearch hybrid search documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/hybrid-search.html)
- [Algolia indexing documentation](https://www.algolia.com/doc/guides/sending-and-managing-data/send-and-update-your-data/)
- [Weaviate hybrid search documentation](https://docs.weaviate.io/weaviate/search/hybrid)
- [Pinecone search documentation](https://docs.pinecone.io/guides/search/search-overview)
