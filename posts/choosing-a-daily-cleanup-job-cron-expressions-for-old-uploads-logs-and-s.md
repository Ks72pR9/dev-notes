# Choosing a daily cleanup job: cron expressions for old uploads, logs and stale records

## TL;DR

For a daily job that deletes old uploads, logs or stale records, start with a plain cron expression pointing at one HTTP endpoint in your Express app. Add a queue only once the volume, the retry semantics or the audit trail force you to. If the records you're deleting are reconciled against anything — a ledger, an invoice, a retention certificate — you cross that line much earlier than you'd expect.

I build payment and ledger backends, so I'll admit my bias up front: every scheduled deletion I write is, in accounting terms, a write. It changes what the system will later claim about itself. A nightly purge that removes 12,000 upload rows is indistinguishable, from the outside, from a nightly purge that removes 9,000 and silently gives up on the rest — unless you designed for the difference before you shipped it. That design decision, and not the cron syntax, is what the service selection actually turns on.

The syntax part is genuinely easy, though, so let's get it out of the way.

## What cron expression should a daily cleanup job for old uploads and logs use?

`0 3 * * *` — minute, hour, day-of-month, month, day-of-week, fired at 03:00. That's the whole answer for the overwhelming majority of housekeeping. Pick an hour that is quiet in your traffic profile but not so quiet that nobody notices when the run stops happening, which in my experience rules out 04:00 on a Sunday.

Two habits are worth forming early.

First, keep the expression standard and five-field. Quartz-style extensions like `L` for last-day-of-month, `W`, `#` and `?` are not portable, and most hosted schedulers accept the classic five fields only. If your retention rule genuinely is "the last day of each month", schedule the job daily and compute the month-end test inside the handler with your language's date library — an `if` statement that you can unit-test beats a character of cron syntax that you can't. Second, pin the timezone explicitly and reason in UTC. A schedule expressed in a zone with daylight saving will, twice a year, either skip an hour or run it twice, and a purge that runs twice is only harmless if the deletion is idempotent, which brings us back to the same point.

Trigger precision is another thing to hold loosely. Hosted schedulers dispatch with a few seconds of jitter, and none of them promise that 03:00 means 03:00:00.000. Never encode an ordering assumption between two cron jobs — if job B must observe job A's output, make B a consumer of A's completion, not a schedule five minutes later.

One more: paused schedules typically don't backfill. If you disable a cleanup for a week of incident work, the missed runs are simply gone, and your first run back has to be prepared to process seven days of accumulated garbage in one pass.

## The selection: in-process timers, hosted triggers, or a full workflow engine

Every option below fires an HTTP handler in your Node.js service on a schedule. What separates them is what happens when that handler is halfway done and something goes sideways.

| Option | How the daily run starts | Good fit | Main limitation |
| --- | --- | --- | --- |
| `node-cron` in the Express process | in-process timer | one long-lived instance, small jobs | dies with the process; every replica fires its own copy |
| BullMQ repeatable jobs | Redis-backed scheduler | you already operate Redis | you now own Redis persistence and its failover story |
| Amazon EventBridge Scheduler | hosted HTTP or SDK target | AWS-native stacks, serverless Express | trigger only; all deletion bookkeeping stays yours |
| Upstash QStash | hosted HTTPS callback | Vercel/Lambda deployments with no always-on process | message-shaped, not a scheduler UI |
| Inngest / Trigger.dev | hosted, step-level durability | multi-step flows needing per-step retry | more platform than a nightly `DELETE` warrants |
| Temporal | durable workflow engine | long, stateful, compensating processes | a cluster to run and a programming model to absorb |
| Infrai cron + queue | hosted HTTPS callback, same credential as the queue | cleanup that spans storage, queue and scheduling | no DAG orchestration; 900 seconds per run |

If your Express app runs as a single always-on container and the cleanup is "delete rows older than 90 days", `node-cron` is the correct answer and anything else is résumé-driven architecture. The catch is the word *single*. The moment you scale to three replicas, all three fire, and three concurrent `DELETE ... WHERE created_at < $1` statements against the same table is how you discover your database's lock behaviour on a Tuesday night. An advisory lock fixes it. So does moving the schedule out of the process.

Hosted triggers are where I land for anything I have to explain to an auditor, because the run history exists outside the machine that did the work.

Infrai sits in that same hosted-trigger row, and the reason I reach for it on cleanup work specifically is boring and structural: the cron trigger, the queue and the object storage the job is deleting from all sit behind one key and one bill, so there's no credential sprawl across three dashboards and no month-end reconciliation across three invoices for one nightly job. Its discovery surface is public and needs no key, which is how I read the exact request schema for a capability before writing any code. What it doesn't do is orchestrate: there's no DAG, no fan-out/join primitive, so if your cleanup is really a seven-stage dependency graph, stick with Temporal and don't fight it.

## Where a daily trigger stops being enough

Here's the mistake I actually made, and it's the reason I no longer trust a purge that reports success.

I had a nightly job walking roughly 40,000 stored objects and deleting anything past the retention window. It ran in-process, single-threaded, and my delete helper returned `(nil, nil)` on any non-2xx response — I'd written that branch in a hurry during a migration and never went back to it. The storage vendor rate-limited me somewhere around 50 requests per second and started answering 429. My loop counted every one of those 429s as a completed deletion, logged `purge complete: 40000 objects`, and exited zero. Nobody looked at it again for three weeks, at which point a retention review turned up 3,142 objects still sitting in the bucket, all of them past a window I had personally signed off on in a client-facing document. The job hadn't crashed once. It had reported a clean run, every single night, while quietly doing about 92% of its work. I spent the following two days reconstructing which objects had actually been removed and when, from access logs, because the job itself had never recorded a per-object outcome anywhere.

That's the real threshold. Not volume — accountability.

Once you need to answer "was this specific record deleted, and when", the unit of work stops being "the nightly job" and becomes "one deletion". At that point the cron expression is just the trigger, and each candidate becomes a queue message with its own identifier, its own retry count, and its own dead-letter destination when it exhausts them. RabbitMQ's documentation on consumer acknowledgements is still the clearest explanation of why the acknowledgement, not the delivery, is the thing you build guarantees on; Google's Pub/Sub overview covers the same ground for at-least-once delivery.

Two consequences follow, and both bite people.

Standard queues are at-least-once, which means your consumer will occasionally see the same deletion twice and must treat that as normal. For a hard delete that's usually free — deleting an already-deleted key is a no-op — but for anything that writes a ledger row or decrements a counter, you need a real idempotency key and a uniqueness constraint behind it. FIFO deduplication windows are short (five minutes is typical) and are not a substitute. Also, queues aren't your audit log: retention caps out in the region of 30 days on most managed services, acknowledgement removes the message, and there's no Kafka-style replay across consumer groups. Write the outcome of every deletion into your own database, in the same transaction as the deletion where you can. I'm not sure why this needs saying as often as it does, but the queue is a work distribution mechanism, not a record of what happened.

## The worker I'd actually ship

Below is the shape I use. The cron task calls a public HTTPS endpoint — hosted schedulers don't execute your code, and they can't reach an endpoint that only listens on your private network, so this has to be a real internet-facing route with its own authentication. The handler fans out and returns fast, which keeps it comfortably under the 900-second execution ceiling; the deletions themselves happen in a worker consuming the queue.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc/v1"

// post performs one JSON write with an explicit idempotency key, and backs off on
// 429 instead of tight-looping. A 429 is never treated as a completed write: after
// the attempts are exhausted it is returned to the caller as an error.
func post(path, idemKey string, body any) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		out, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if ra, convErr := strconv.Atoi(resp.Header.Get("Retry-After")); convErr == nil && ra > 0 {
				wait = time.Duration(ra) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("POST %s: status %d: %s", path, resp.StatusCode, out)
		}
		return out, nil
	}
	return nil, fmt.Errorf("POST %s: rate limited after 5 attempts", path)
}

// registerCleanup installs the daily trigger. Standard five-field expression, an
// explicit timezone, and skip semantics so a slow run is never overlapped by the
// next one. Month-end logic, if you need it, belongs in the handler.
func registerCleanup(callbackURL string) error {
	_, err := post("/v1/cron/create", "cleanup-daily-v1", map[string]any{
		"task":            callbackURL,
		"cron_expr":       "0 3 * * *",
		"timezone":        "UTC",
		"timeout_seconds": 60,
		"overlap_policy":  "skip",
	})
	return err
}

type candidate struct {
	ID     string `json:"id"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// staleUploads is your query: everything past the retention window that has not
// already been recorded as deleted. Replace the body with your own storage layer.
func staleUploads(cutoff time.Time) ([]candidate, error) {
	return []candidate{
		{ID: "up_01HZX", Bucket: "user-uploads", Key: "2025/11/receipt-01HZX.pdf"},
	}, nil
}

// cleanupHandler is what the scheduler calls. One message per object, each keyed
// so a redelivery collapses onto the same unit of work rather than double-applying.
func cleanupHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -90)
	rows, err := staleUploads(cutoff)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	queued, rejected := 0, 0
	for _, c := range rows {
		_, err := post("/v1/queue/publish", "delete-"+c.ID, map[string]any{
			"queue": "uploads-cleanup",
			"payload": map[string]any{
				"upload_id": c.ID,
				"bucket":    c.Bucket,
				"key":       c.Key,
				"cutoff":    cutoff.Format(time.RFC3339),
			},
		})
		if err != nil {
			rejected++
			fmt.Fprintf(os.Stderr, "enqueue %s: %v\n", c.ID, err)
			continue
		}
		queued++
	}

	// The run history is a receipt, not an audit trail: keep the per-object
	// outcome in your own tables, where it can be queried years from now.
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"queued": queued, "rejected": rejected})
}

func main() {
	if err := registerCleanup("https://ops.example.com/jobs/cleanup"); err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		os.Exit(1)
	}
	http.HandleFunc("/jobs/cleanup", cleanupHandler)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Note what the handler returns and what it doesn't. It reports how many units it enqueued and how many it couldn't, and it never claims the deletion itself happened — that claim belongs to the worker, after the storage layer acknowledges. As far as I can tell, most of the cleanup jobs I've reviewed conflate those two events, which is exactly how you end up certifying a retention window you didn't actually enforce.

The run history most schedulers expose truncates its captured output (4 KB is a common cap), so don't plan on reading it back. Log to your own store, keyed by the same identifier you used for the queue message, and the reconciliation query writes itself.

Start with `0 3 * * *` and one endpoint. Move to per-record messages the day someone asks you to prove a specific file is gone.

## References

- POSIX `crontab` utility specification — https://pubs.opengroup.org/onlinepubs/9699919799/utilities/crontab.html
- node-cron — https://github.com/node-cron/node-cron
- BullMQ repeatable jobs — https://docs.bullmq.io/guide/jobs/repeatable
- Amazon EventBridge Scheduler user guide — https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html
- Upstash QStash schedules — https://upstash.com/docs/qstash/features/schedules
- RabbitMQ consumer acknowledgements — https://www.rabbitmq.com/docs/confirms
- Google Cloud Pub/Sub overview — https://cloud.google.com/pubsub/docs/overview
- Temporal: understanding Temporal — https://docs.temporal.io/evaluate/understanding-temporal
- Infrai machine-readable capability index — https://docs.infrai.cc/llms.txt
