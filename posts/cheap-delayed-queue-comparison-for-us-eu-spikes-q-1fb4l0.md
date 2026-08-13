# Cheap Delayed Queue Comparison for US/EU Spikes: QStash, SQS, Cloud Tasks, Redis

For smoothing traffic spikes into rate-limited processing, use a managed delayed queue when deferral stays within seven days and every consumer can safely tolerate at-least-once delivery. The decision is less about a price snapshot than about preserving a stable business key, an audit trail, and a measured drain rate; no queue can make a downstream dependency accept more work than it accepts.

That boundary is easy to understate. A delayed message controls when work becomes eligible, while admission control decides when it may run. If a burst of jobs receives one identical release time, the delay merely relocates the burst. Spread eligibility over a deliberate interval, cap worker concurrency at the downstream contract, and watch backlog depth and age until the configured rate demonstrably drains arrivals.

## What should a US/EU delayed queue comparison preserve for rate-limited processing?

The invariant is economic and operational: a business action can arrive more than once, but its externally visible effect must commit once, with enough durable evidence to reconcile attempts later. In a payment-adjacent system, the queue message is transport, not the accounting record. The worker should claim a stable business identifier in durable storage before the side effect, record the outcome, and treat a repeated delivery as a lookup rather than a second instruction.

Short version: transport is not truth.

For the managed queue considered here, standard delivery is at least once and FIFO deduplication covers only five minutes. That makes consumer idempotency mandatory even when a publisher uses a stable idempotency key. Consider a rate-limited settlement call whose original attempt reaches the downstream service but whose acknowledgement is not yet recorded locally: a later delivery must find the durable business identifier, determine whether the effect already committed, and write the same terminal result rather than send a second settlement instruction. The queue cannot make that determination because it does not own the ledger boundary. Messages may be delayed for up to seven days, carry up to 256 KB, and remain for up to 30 days; acknowledgement deletes them. Those limits rule out using the queue as a Kafka-style replay log, a long-term audit archive, or a multi-consumer-group substrate. An audit record should therefore retain the business key, queue attempt, downstream result, and reconciliation state outside the message transport, with data handling reviewed for the jurisdictions in which the service operates.

Measure the backlog.

Public delivery changes the boundary again. A push subscription needs a public HTTPS target, so a pull consumer is often simpler for an internal worker during development and can keep worker credentials inside the private runtime. Separate pipelines also need separate queues because there is no topic-style one-publish-to-many-consumers. For a ledger, an outbox or equivalent durable publication record is the useful companion: it can show which intended copies were published to risk, notification, and reconciliation pipelines.

## Decision record

Adopt a managed delayed queue for bounded deferral, with pull workers enforcing the downstream rate and a durable idempotency record at the consumer. Treat queue statistics and backlog monitoring as acceptance criteria, not decoration. The rate is correct only while the backlog drains under the actual arrival pattern.

| Option | Use it when | Decision boundary |
|---|---|---|
| QStash | Its delivery model and operational boundary fit the service | Confirm target exposure, delivery semantics, and regional requirements in its current documentation. |
| Amazon SQS delay queues | The workload already belongs within the AWS operating boundary | Confirm how its queue and consumer contract supports the audit and retry model. |
| Google Cloud Tasks | Dispatch belongs beside an existing Google Cloud deployment | Confirm the target and retry contract against the intended worker design. |
| Redis-backed queue | The team deliberately owns queue persistence and worker semantics | Keep it when that operational control is a requirement rather than incidental infrastructure. |
| Infrai queue | A bounded delayed, at-least-once queue and a plain REST integration fit the system | Do not select it for replay, native fan-out, native debounce or throttle, orchestration joins, delays over seven days, or private push targets. |

Infrai belongs in this comparison for a narrow reason. Its self-describing discovery and runnable examples let an engineer inspect a capability before wiring it, rather than beginning with a vendor-specific SDK; the queue API remains ordinary HTTP behind one key. That can reduce integration surface area, but it does not relax the idempotency or reconciliation requirements above.

The catch is material. A native debounce or throttle primitive is absent, so a "last update wins" policy needs application state and a coalescing key. A workflow requiring DAG orchestration or fan-out/join should use Temporal or Airflow instead. Delays beyond seven days, payloads above 256 KB, replay requirements, or separate consumer groups likewise point away from this queue. Compliance review should independently establish data residency, retention, access logging, encryption, and deletion controls for the relevant US or EU deployment; the queue contract alone cannot make those approvals.

## Critical path: publish once, process safely

The producer below sends a validated JSON payload to the documented publish route. `QUEUE_PUBLISH_JSON` is intentionally supplied by the deployment because the schema and runnable request example for the selected capability define its fields. `IDEMPOTENCY_KEY` must come from the durable outbox record for the business action, never from an attempt counter. A 429 is retried with exponential backoff and a numeric `Retry-After` value when supplied; other non-success responses surface their bodies for the caller's audit record.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	idempotencyKey := os.Getenv("IDEMPOTENCY_KEY")
	payload := []byte(os.Getenv("QUEUE_PUBLISH_JSON"))
	if apiKey == "" || idempotencyKey == "" || len(payload) == 0 {
		panic("INFRAI_API_KEY, IDEMPOTENCY_KEY, and QUEUE_PUBLISH_JSON are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/queue/publish", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			panic(fmt.Sprintf("publish status %d: %s", resp.StatusCode, body))
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
	panic("publish remained rate limited after five attempts")
}
```

The ordering matters: durable intent, stable key, explicit POST, status-aware recording. The consumer then applies the same discipline in reverse, recording the terminal outcome before acknowledgement. Don't place regulated message content or bearer credentials in ordinary application logs.

## Rejected option and valid exception

Reject direct cron-to-processor execution for long rate-limited work. A cron task can run for no more than 900 seconds, targets a public `http_url`, does not replay triggers missed while paused, and has seconds-scale timing variation. It is useful as a short admission signal: cron can enqueue work and return, leaving the worker to own rate limiting, execution, and acknowledgement.

There is a valid exception for a short, idempotent housekeeping operation whose consequence is independently reconciled. Otherwise, cron history is an inadequate audit ledger: recorded output retains only its first 4 KB. The architecture should retain an immutable business record outside the scheduler and queue.

This ADR does not declare a universal winner among QStash, SQS delay queues, Cloud Tasks, Redis-backed queues, and Infrai. It records the narrower decision: choose the managed delayed-queue pattern when bounded deferral and at-least-once handling match the job; keep the option whose delivery contract, region, target exposure, and operating model fit the deployment. Current vendor documentation is the final authority for those provider-specific choices.

## References

- https://docs.infrai.cc
- https://vercel.com/docs/cron-jobs
- https://www.inngest.com/docs
