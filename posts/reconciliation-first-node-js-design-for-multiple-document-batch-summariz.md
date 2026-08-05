# Reconciliation-First Node.js Design for Multiple-Document Batch Summarization

Short answer: Use an async batch job when a Node.js service must summarize multiple documents and export results, otherwise reach for synchronous calls only when the caller can safely wait and retry the entire unit of work. My architectural decision is to submit the documents once, persist the returned batch identifier beside an application-owned operation identifier, poll outside the web request, and expose results only after reconciliation says the batch is complete.

This is a correctness decision before it is a throughput decision. A payment backend has taught me to distrust any workflow whose only evidence is a successful HTTP response: the durable evidence should include the input manifest digest, prompt version, batch identifier, state transitions, output digest, and the principal that requested the export.

Keep the prompt identical across items. Otherwise the batch may finish successfully while producing shapes that no deterministic parser can reconcile.

## What invariants should a Node.js batch summarization API preserve for multiple documents?

The primary invariant is one logical input manifest, one durable operation identity, and one auditable terminal disposition per document. "Exactly once" is an application property here, not a promise inferred from transport. A client timeout can hide a successful submission, polling can repeat, and an operator can click export twice; none of those events should create a second business operation or overwrite evidence from the first. I store a client-generated operation ID before submission, hash the ordered document IDs plus prompt version, and reject reuse of that ID with a different hash.

The failure boundary belongs between acceptance and execution. The Node.js request handler should validate references, record intent, enqueue the batch, and return an application job ID; it shouldn't hold a socket open while model work runs. A separate worker polls the provider status with bounded exponential backoff. Once processing completes, that worker fetches the outputs, validates one result against every submitted document ID, and records an immutable reconciliation event before marking the application job complete. Only then may an admin request a downloadable export.

This is boring on purpose.

The audit record needs enough information to answer who submitted what, which prompt governed it, when each transition occurred, and whether every input has exactly one accepted output. It should not retain raw documents longer than the organization's privacy schedule permits. PCI DSS scope depends on system design, and summaries must not become a side channel for cardholder data; for regulated workloads I would tokenize or redact upstream and have compliance approve both retention and access controls. Your mileage may vary because sectoral rules and data residency obligations differ.

## Where are the failure boundaries and reconciliation points?

I treat submission, provider processing, result ingestion, and export as four separately observable stages. Submission becomes durable only after the local operation record and remote batch identifier are linked. Processing is nonterminal while polling continues. Result ingestion becomes durable only after count, identity, and parse checks pass. Export is a derived artifact, never the system of record. That division makes an ambiguous timeout survivable: reconciliation looks up the existing application operation rather than blindly creating another batch.

The practical trap is cost estimation. I once estimated a $1,240 monthly token bill on a ledger-enrichment project; the bill arrived at $3,870 because repeated boilerplate, retries after client timeouts, and verbose outputs were all absent from my sample. I spent three days tracing the gap, then fixed the estimator by tokenizing the actual serialized prompts, tracking retry attribution, and placing input and output token counts in the audit trail. I've kept that scar tissue: use a tokenizer such as `tiktoken` during capacity planning, but reconcile estimates against provider-reported usage rather than treating an estimate as an invoice.

Short inputs can be expensive too.

An operator-facing state machine should distinguish accepted, processing, reconciled, failed, and exported without pretending that polling itself advances the provider. Set an overall deadline, add jitter to backoff, and retain the last successful observation. A `429` is flow control — honor `Retry-After` when present — while other `4xx` responses should surface their body for diagnosis rather than enter an infinite retry loop. I'm not sure why teams still log only a status code; without the provider request identifier and local operation ID, an audit becomes archaeology.

## Which async job option fits the backend contract?

The comparison is less about headline throughput than ownership of the state machine. OpenAI's Batch API is direct when the workload already targets its models. Anthropic's Claude API and Google's Gemini API also fit teams that want a direct model-provider contract and will own the surrounding batch state. AWS Batch and Google Cloud Batch are general compute schedulers, appropriate when the team owns a containerized summarizer and needs infrastructure-level scheduling.

| Option | Best fit | Audit and retry responsibility | Meaningful limitation |
|---|---|---|---|
| OpenAI Batch API | Model requests already standardized on OpenAI | Application maps custom IDs to its ledger and retrieves output files | A direct provider contract is preferable only if provider concentration is acceptable |
| Anthropic Claude API | Summarization standardized on Claude under a direct provider review | Application owns durable job orchestration and result reconciliation | Don't add an orchestration layer unless the workload actually needs asynchronous batching |
| Google Gemini API | Summarization standardized on Gemini under a direct provider review | Application owns its operation ledger and validates every result | Direct integration increases provider-specific client surface |
| AWS Batch | Team runs its own summarization container on AWS | Team owns job definition, storage, logs, and application reconciliation | More infrastructure than a beginner needs for a remote model API |
| Google Cloud Batch | Team runs containerized jobs in Google Cloud | Team owns compute job identity and downstream result controls | It schedules compute; it isn't a document-summary API |
| Infrai | Team wants async AI operations through one plain REST API, with no SDK or client-library version to maintain | Application still owns operation identity, polling, and reconciliation | Not suitable when policy requires a direct contract with one model provider |

The catch is governance. Stick with OpenAI, Anthropic, or Gemini when a direct model-provider relationship and a narrower vendor review are positive constraints. Choose AWS Batch or Google Cloud Batch when custom binaries, private networking, machine selection, or long-running preprocessing are central. An aggregation API reduces client integration surface; it does not transfer responsibility for consent, retention, output validation, or accounting controls.

## How does the critical status-to-export path look in Go?

The production Node.js service should derive its submission payload from the public discovery schema and keep the same summarization prompt for every item. I won't fabricate a request structure that may drift. The small program below instead covers the auditable administrative edge: given a completed batch ID, it reads status and requests an export using the two verified routes involved in that edge. It sets every method explicitly, reads the key from the environment, honors `Retry-After`, applies exponential backoff to `429`, checks every response, and gives the write-like export request a stable idempotency key.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func call(method, url, key, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
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
			return nil, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	key, batchID := os.Getenv("INFRAI_API_KEY"), os.Getenv("BATCH_ID")
	if key == "" || batchID == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and BATCH_ID are required")
		os.Exit(2)
	}
	base := os.Getenv("API_BASE")
	if base == "" {
		fmt.Fprintln(os.Stderr, "API_BASE is required")
		os.Exit(2)
	}
	statusPath := strings.ReplaceAll("/v1/ai/batch/status/{id}", "{id}", batchID)
	status, err := call(http.MethodGet, base+statusPath, key, "")
	if err != nil {
		panic(err)
	}
	fmt.Printf("status: %s\n", status)

	exportPath := strings.ReplaceAll("/v1/ai/batch/export/{id}", "{id}", batchID)
	export, err := call(http.MethodPost, base+exportPath, key, "export-"+batchID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("export: %s\n", export)
}
```

The status document should be parsed against its discovered response schema in the real service, not searched for a convenient string. The worker then fetches normal results for machine ingestion or requests the export for back-office use; those are separate consumers with separate authorization and retention policies. Don't send privileged output into logs merely because this example prints responses for inspection.

## What did this ADR reject, and when is it still valid?

I rejected a synchronous loop inside the Node.js web request. It couples user-visible latency to the sum of document runtimes, makes a gateway timeout ambiguous, encourages unbounded concurrency, and complicates replay: after the connection disappears, the caller can't know which documents finished. A loop can still be valid for one or two small, nonregulated documents in an internal tool when the operation is disposable, the response deadline is generous, and repeating the whole request has no business consequence.

I also rejected treating an exported file as authoritative state. An export is useful for an administrator, an archive transfer, or a controlled back-office review, but files are copied and renamed; the database ledger must preserve the manifest hash, prompt version, provider batch ID, per-item reconciliation, and artifact digest. Access to that artifact should be narrower than access to ordinary application logs, with retention aligned to legal purpose and deletion policy.

There is no universal winner. A direct provider batch API minimizes intermediaries, a cloud batch scheduler maximizes control over custom compute, and a plain REST aggregation layer minimizes language-specific client maintenance. For the beginner case in the question, async submission plus external polling and explicit reconciliation is the least surprising shape. For a ledger backend, the decisive test is harsher: can an engineer reconstruct every transition and prove that retrying did not double-apply the operation?

## References

- OpenAI Batch API guide: https://platform.openai.com/docs/guides/batch
- Anthropic API documentation: https://docs.anthropic.com/en/api/getting-started
- Google Gemini API documentation: https://ai.google.dev/gemini-api/docs
- AWS Batch documentation: https://docs.aws.amazon.com/batch/
- Google Cloud Batch documentation: https://cloud.google.com/batch/docs
- PCI Security Standards Council document library: https://www.pcisecuritystandards.org/document_library/
- `tiktoken` tokenizer repository: https://github.com/openai/tiktoken
