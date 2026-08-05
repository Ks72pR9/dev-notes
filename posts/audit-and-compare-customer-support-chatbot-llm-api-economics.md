# Audit and Compare Customer Support Chatbot LLM API Economics

**Short answer:** The cheapest LLM API for a customer support chatbot is the candidate with the lowest measured cost per correctly resolved conversation under your own traffic, after retries, retrieval, escalation, and reconciliation; an OpenAI-compatible surface can reduce integration work, but it doesn't settle the economic question.

I build payment and ledger backends, so I don't accept a token price as a cost model any more than I accept a card authorization as settled cash. GPT, Claude, and Gemini belong in the candidate set if they satisfy the application's constraints, but the selection unit should be a replayable conversation, not a marketing input rate. The useful output is a signed-off evaluation ledger: prompt version, model identifier, input and output units, tool calls, retry count, latency, outcome, and an immutable request key.

That sounds fussy. It is also how the estimate survives contact with production.

## What constraint defines the cheapest LLM API for a customer support chatbot?

Start with the non-negotiable boundary: what counts as a correct support outcome? I use a compact rubric with mutually exclusive terminal states such as resolved correctly, safely escalated, incorrectly answered, and abandoned. A cheap response that invents a refund policy is a failed transaction. A costly response that should have been answered from a deterministic order-status endpoint is also a design failure, though of a different kind. Neither belongs in a provider victory lap.

The denominator matters. Cost per API call rewards terse failures and hides retries; cost per conversation still hides whether the conversation helped. I therefore calculate cost per correctly resolved conversation and report safe escalation separately. The numerator includes all model turns in the thread, retrieval queries, tool execution, retry traffic, and the engineering overhead that changes between protocols. I keep agent labor outside the primary number but beside it, because deflection and customer outcome are different accounts and shouldn't be netted together without an explicit business rule.

Before any model receives traffic, I also define data boundaries. Payment credentials, authentication secrets, and unnecessary customer attributes don't enter the prompt. Retention, regional processing, access logging, deletion, and human-review rules are release constraints, not columns to fill in after a benchmark. Your compliance limit may be stricter than mine, and I'm not sure a single universal rubric is possible; your mileage may vary with jurisdiction and product risk.

This immediately narrows the experiment. If a candidate can't meet the data-handling or audit requirements, its nominal price is irrelevant. If it can, it gets exactly the same frozen corpus, retrieval snapshot, tool contract, and grading procedure. Correctness first. Then economics.

## Build a conversation ledger before comparing candidates

I treat every evaluation run as an append-only financial event. Each conversation has a stable ID, each attempted completion has a distinct attempt ID, and the idempotency key is derived from the conversation, prompt revision, retrieval snapshot, model, and attempt policy. That doesn't make a remote inference exactly once; no ordinary HTTP boundary can promise that by itself. It gives the local system an exactly-once mindset: retries may happen, but duplicate intent is detectable, billable attempts remain visible, and a later reconciliation job can explain the difference between requested, observed, graded, and charged work.

One scar shaped this rule. I hit a data-shape mismatch after importing 18,742 evaluation rows under the assumption that every response carried a `usage.output_tokens` field; one adapter omitted it, while the error message said only `invalid record`, and I had to trace the first malformed envelope by replaying progressively smaller batches against the original captures. The failure wasn't a model-quality problem, and the successful inference calls gave me no clue about which accounting record had violated my schema. I compared the raw envelopes, found the absent field, checked the other optional members, and then rebuilt the import so absence stayed distinct from a measured zero. The missing raw envelope had made my first pass needlessly speculative. Since then, I preserve the provider-neutral normalized record and a hash of the source payload, validate optional fields explicitly, and reject incomplete cost records into a quarantine stream rather than silently converting absence to zero. I don't trust a cost total until I can walk backward from it to every accepted and rejected attempt.

Here is the narrow shape I want around an OpenAI-compatible adapter. The interface is intentionally boring — replacement should change transport code, not the evaluation ledger.

```go
package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Usage struct {
	InputUnits  int64
	OutputUnits int64
}

type Attempt struct {
	ConversationID string
	PromptRevision string
	Model          string
	AttemptNumber  int
	Usage          Usage
	Outcome        string
}

type Completer interface {
	Complete(ctx context.Context, idempotencyKey string, messages []string) (string, Usage, error)
}

func Key(conversationID, promptRevision, model string, attempt int) string {
	raw := fmt.Sprintf("%s\x00%s\x00%s\x00%d", conversationID, promptRevision, model, attempt)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
```

Persist an attempt before dispatch, then transition it through explicit states. Never overwrite the first observation with a retry. Short version: reconcile everything.

## How should a support chatbot compare API costs across compatible models?

Use a stratified replay, not a bag of convenient questions. I split the corpus by intent, conversation length, language, retrieval requirement, tool requirement, and risk class, then preserve the production mix when aggregating. Rare refund disputes deserve their own error budget even if password-reset questions dominate volume. The graders should be blind to candidate identity, and ambiguous cases should go to human review with written adjudication rules. Otherwise, small prompt-style preferences masquerade as model quality.

For GPT, Claude, Gemini, or any other candidate, I would record the same dimensions below. The table is a test plan, not a ranking; no public list price can supply these values for a particular application.

| Dimension | Measurement | Failure it exposes |
| --- | --- | --- |
| Outcome | Correct resolution and safe-escalation rates | Fluent but wrong policy answers |
| Full cost | All metered work across every attempt | Cheap first calls with expensive retries |
| Tail latency | End-to-end conversation turn latency | Acceptable averages with poor user experience |
| Retrieval | Documents fetched and actually cited in grading | Large contexts that add cost without evidence |
| Tool safety | Validated calls and authorization decisions | Correct prose paired with an unsafe action |
| Operations | Error class, retry reason, and duplicate detection | Hidden reliability and reconciliation work |

Normalize only after preserving raw observations. A common schema makes comparison possible, but it can erase differences if it supports merely the lowest common denominator. In particular, streaming events, tool-call arguments, finish reasons, usage timing, and error taxonomies may need adapter-specific capture beside the common record. OpenAI compatibility is useful at the request boundary; it isn't evidence that every behavioral or accounting field has identical semantics.

Price lists don't capture any of it.

Retrieval gets its own controlled experiment. A Postgres deployment can use pgvector for vector similarity search, but the model comparison should pin the database snapshot, embedding revision, filter policy, and top-k rule. Change any of those and I've changed the system under test. Run multiple seeds where nondeterminism matters, publish confidence intervals rather than a suspiciously precise winner, and retain failed attempts. Close calls should remain close calls.

## Streaming, retries, and tools change the bill

An in-app chatbot usually streams output because perceived latency matters. Server-Sent Events are a one-way server-to-client mechanism, and MDN documents the browser's `EventSource` interface plus the `text/event-stream` response type. That makes SSE a reasonable delivery choice for generated text, while tool approval, cancellation, and new user messages can travel through separate client-to-server requests. The ledger still closes only when the server records a terminal event; a disconnected browser isn't proof that remote generation stopped.

Retries require classification. A transport interruption before any durable response, a locally rejected tool argument, and a user cancellation are different events with different accounting consequences. Don't hide them behind one `retry_count`. Attach the attempt ID to logs and traces, retain timestamps for dispatch, first byte, last byte, and cancellation, and make the retry policy explicit enough that a replay uses the same rules. Fast retries can amplify cost and duplicate tool effects unless tool execution has its own idempotency key and authorization check.

This is where an OpenAI-compatible API can help: one internal message and streaming contract reduces adapter surface area. The catch is that compatibility isn't suitable as the only selection criterion when a required native feature, data-control term, event shape, or usage field falls outside that common contract. Keep a native adapter when it materially improves a required workflow; keep the common adapter when portability and operational simplicity carry more weight. Neither choice is free.

I also separate deterministic work from inference. Order lookup, account authorization, refund eligibility, and balance presentation should remain typed services with auditable rules; the model may classify intent or draft an explanation, but it shouldn't invent ledger state. This division cuts unnecessary context, limits unsafe actions, and produces evidence a reviewer can follow. It also gives the system a useful fallback: when generation is unavailable or confidence is below the approved threshold, the application can present verified facts or escalate instead of improvising.

## Roll out with reversible gates and reconciliation

Ship slowly.

Promote candidates through shadow replay, limited internal traffic, and a small production cohort with predetermined stop conditions. Version the prompt, policy text, retrieval snapshot, adapter, grader, and routing rule together. A release without that tuple can't be reproduced, and an unexplained routing change can corrupt the comparison faster than a model update.

Daily reconciliation should join request records, terminal stream events, usage observations, tool effects, and quality outcomes. Investigate unmatched entries; don't average them away. I want a report that answers which conversations were attempted, which were charged, which completed, which resolved correctly, which escalated safely, and which require manual adjudication. Audit trails are part of the product here, not paperwork.

There is no permanent winner. Traffic mix, prompts, retrieval data, policies, and model revisions move, so rerun the fixed suite on a schedule and before routing changes. Keep rollback mechanical: one versioned routing decision, bounded exposure, and no migration that makes the prior adapter unreadable. The final choice may differ across low-risk FAQ traffic and high-risk account actions. That's fine. A defensible split is better than a universal answer manufactured from a single price column.

The decision rule stays compact: choose the eligible candidate with the lowest reconciled cost per correct resolution, provided it clears the documented safety, latency, data, and operational gates. If two candidates are statistically indistinguishable, prefer the architecture the team can observe and reverse. That's the closest thing I trust to “cheapest.”

## References

- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [pgvector: Open-source vector similarity search for Postgres](https://github.com/pgvector/pgvector)
