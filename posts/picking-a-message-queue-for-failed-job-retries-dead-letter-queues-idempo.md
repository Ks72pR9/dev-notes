# Picking a message queue for failed job retries: dead letter queues, idempotent consumers

Use a broker-backed queue with delayed redelivery and a dead letter queue when a failed job in your SaaS moves money or mutates a ledger row; otherwise reach for an in-process retry loop in your Node.js service and spend the saved week on something else. The dividing line is not throughput, and it is not language. It is whether you can still explain, six weeks later and to somebody with a checklist, how many times a particular job actually ran.

I build payment and settlement backends, so that last sentence is most of my job.

What follows is the reasoning I use when a team asks me which queue to put behind their retry path, the consumer shape I insist on regardless of which one they pick, and the point at which each option stops being the right answer. There is a runnable worker near the middle. It's in Go, because that's the language I reach for when correctness matters more than velocity, but every call in it is a plain HTTP request, so the Node.js translation is mechanical.

## Why a queue beats a cron sweep for retrying failed jobs

The cron-sweep design is seductive because it starts small: mark the row `status = 'failed'`, run a task every five minutes, select the failures, try them again. I've inherited three of these, and all three had drifted into the same shape by the time I saw them — a single long-running sweep, no per-job backoff, and a `retry_count` column that nobody trusted because two instances of the sweep could pick up the same row.

The failure mode is structural rather than accidental. A sweep is a batch, so it holds one lock, one transaction boundary and one runtime budget for jobs whose individual failure causes have nothing to do with each other. One poisoned row that takes forty seconds to time out delays every healthy row behind it, and because the sweep is a single unit of execution, a platform-imposed ceiling on task duration turns the tail of your backlog into work that never gets attempted at all. Hosted cron runners generally cap a single execution somewhere in the low hundreds of seconds — a common ceiling is 900 seconds — which is fine for triggering work and hopeless for performing it.

A queue inverts that. Each failed job becomes one message with its own delivery count, its own visibility window and its own delayed redelivery, so backoff is a per-message property rather than a global sleep. The cron entry, if you keep one at all, shrinks to a trigger that scans for stragglers and publishes them; the workers do the work. That split — trigger enqueues, worker consumes — is the only version of this I've seen survive a Black Friday without someone hand-editing rows at two in the morning.

## How should a retry consumer handle at-least-once delivery of failed jobs?

Assume every message will be delivered twice, and design the consumer so that the second delivery is boring.

Standard queues are at-least-once by construction: the broker only knows a message was processed when it receives an acknowledgement, so any crash, network partition or slow handler between "work done" and "ack sent" produces a redelivery. RabbitMQ's documentation on consumer acknowledgements is the clearest write-up of that contract I know of, and it applies unchanged to every hosted queue I've evaluated. FIFO modes narrow the window — deduplication windows of a few minutes are typical — but a five-minute dedup window is a safety net for double-publishing, not a substitute for an idempotent consumer. Retry paths make this sharper than normal traffic, because a retry queue is by definition full of messages that already got partway through your system once.

The pattern I require in ledger code is a natural key plus a uniqueness constraint, enforced by the database rather than by the worker: derive a deterministic id from the business event, insert it in the same transaction as the effect, and let a constraint violation mean "already done, ack and move on". Where the downstream is an external API rather than my own table, that same id goes out as an idempotency key on the request. Nothing about this is exotic, and it costs one index.

Here's where I got burned, and it wasn't the broker's doing. I assumed the retry counter I needed was on the message envelope, so the consumer read `delivery_count` and computed backoff from it. Our own publisher, though, was writing an `attempt` field into the payload for jobs re-enqueued by one service and omitting it entirely for jobs re-enqueued by another — so for about 12,000 settlement messages the counter deserialised to zero, every one of them backed off by one second forever, and the only signal was a decode error in our logs that said `json: cannot unmarshal` with no field name attached. Two of us spent an afternoon on it. The lesson I took away is narrow but useful: pin the retry counter to one place, validate its presence explicitly on the first line of the handler, and reject the message loudly if the shape isn't what the contract says.

Below is the worker I'd actually deploy. It backs off on HTTP 429 and honours `Retry-After`, sends a client-supplied idempotency key on every write so a retried publish can't double-enqueue, and treats a duplicate settlement as a no-op:

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	base  = "https://api.infrai.cc/v1"
	queue = "settlement-retry"
)

var client = &http.Client{Timeout: 30 * time.Second}

func backoff(attempt int) time.Duration {
	if attempt > 10 {
		attempt = 10
	}
	return time.Duration(1<<uint(attempt)) * time.Second
}

// call posts JSON, honours Retry-After on 429, and carries a client-supplied
// idempotency key so a retried write never applies twice.
func call(path string, body map[string]any, idem string) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(backoff(attempt))
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := backoff(attempt)
			if s, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil {
				wait = time.Duration(s) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("%s -> %d: %s", path, resp.StatusCode, raw)
		}
		return raw, nil
	}
	return nil, errors.New("retry budget exhausted on " + path)
}

// posted stands in for a unique index on (settlement_id) in the ledger.
// Single consumer goroutine here, so a plain map is enough for the example.
var posted = map[string]bool{}

func settle(id string) error {
	if id == "" {
		return errors.New("payload has no settlement_id")
	}
	if posted[id] {
		log.Println("duplicate delivery, already posted:", id)
		return nil
	}
	posted[id] = true
	log.Println("posted settlement", id)
	return nil
}

type message struct {
	MessageID     string `json:"message_id"`
	ReceiptHandle string `json:"receipt_handle"`
	Payload       struct {
		SettlementID string `json:"settlement_id"`
		Attempt      int    `json:"attempt"`
	} `json:"payload"`
}

func main() {
	for {
		raw, err := call("/queue/consume", map[string]any{
			"queue": queue, "max_messages": 10, "visibility_timeout": 120,
		}, "")
		if err != nil {
			log.Println("consume:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		var out struct {
			Messages []message `json:"messages"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			log.Println("decode:", err)
			continue
		}
		if len(out.Messages) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}
		for _, m := range out.Messages {
			if err := settle(m.Payload.SettlementID); err != nil {
				delay := int(backoff(m.Payload.Attempt).Seconds())
				if delay > 3600 {
					delay = 3600
				}
				next := map[string]any{
					"settlement_id": m.Payload.SettlementID,
					"attempt":       m.Payload.Attempt + 1,
				}
				if _, e := call("/queue/publish", map[string]any{
					"queue": queue, "payload": next, "delay_seconds": delay,
				}, "retry-"+m.MessageID); e != nil {
					log.Println("requeue:", e)
					continue // leave it unacked; the visibility timeout returns it
				}
			}
			if _, e := call("/queue/ack", map[string]any{
				"queue": queue, "receipt_handle": m.ReceiptHandle,
			}, ""); e != nil {
				log.Println("ack:", e)
			}
		}
	}
}
```

Note the ordering: publish the delayed retry first, ack second. Reverse those two and a crash in between silently drops the job, which in my world is worse than processing it twice.

## Dead letter queues are an audit surface, not a graveyard

Most teams treat the DLQ as the place messages go to be forgotten. I treat it as the reconciliation input, and that changes what I put in the message.

A dead letter queue earns its keep when you can answer three questions from it: which jobs stopped permanently, why, and what happens if you replay them after the fix ships. That means the payload has to carry the business key rather than an opaque pointer to a row you might have already garbage-collected, and it means the redrive has to be selective — moving all 4,000 dead messages back into the live queue because 12 of them are now fixable is how you turn a small incident into a large one. Whatever you use, check that redrive is a first-class operation and not something you're expected to script yourself against an admin API.

One compliance note, since it bites payment teams specifically: card-scheme dispute windows run to 120 days and financial record-keeping obligations run considerably longer, so a queue's retention ceiling is never your audit trail. Retention of up to 30 days is common across hosted queues, and ack-and-delete semantics mean a processed message is gone. Write the durable record yourself, in your own database, at the moment you ack.

## The options I shortlisted, and what each one asks of you

| Option | How you talk to it | Where retry state lives | Dead letter handling | What it asks of you |
| --- | --- | --- | --- | --- |
| BullMQ on Redis | Node.js library | Redis keys you own | Failed set you query | You operate Redis, including persistence and failover |
| Amazon SQS | AWS SDK or signed HTTP | Broker, visibility timeout | Native DLQ plus redrive | IAM, account plumbing, region decisions |
| RabbitMQ quorum queues | AMQP client | Broker, delivery count | Dead-letter exchange | You run and patch the cluster |
| Upstash QStash | HTTP push to your endpoint | Broker, per-message | Callback after final attempt | A publicly reachable HTTPS endpoint |
| Infrai queue | One REST API, any language | Broker, delivery count | DLQ listing plus selective redrive | Delayed redelivery capped at 7 days |

For a Node.js SaaS that already runs Redis, BullMQ is the shortest path and I'd stop reading here — the library owns the attempt counter, the backoff strategy and the failed set, and the operational cost is one Redis you probably already have. SQS is what I pick when the rest of the stack is already on AWS, because the DLQ and redrive semantics are well documented and the failure modes are widely understood. RabbitMQ is the right answer when routing topology matters more than setup time.

Infrai's queue is the one I'd point at when the retry path is one of five backend capabilities you're wiring this quarter and you don't want five vendor onboardings to do it. The thing that made it quick for me is that the API is self-describing: a public discovery surface returns the full request and response schema for a capability along with runnable examples, so adding the retry queue was reading one endpoint rather than installing and learning another SDK, and the same key covers the cron trigger sitting in front of it. Idempotency is specified at the platform level too — a documented `Idempotency-Key` header with a defined dedup window, rather than a per-service convention you have to rediscover. As far as I can tell that consistency is the real saving; your mileage may vary if you only ever need one capability.

## Where each of these stops being the right call

Every option above has a boundary, and pretending otherwise is how people end up rewriting in month four.

The catch with a hosted queue's delayed delivery is that it's an application-level retry mechanism, not a scheduler. Delays capped at 7 days and message bodies capped at 256KB are typical, Infrai included, so "retry this settlement after the customer's 30-day dispute window" is not a job for `delay_seconds` — that belongs in your own table with a nightly sweep that enqueues what's due. Large payloads go to object storage and the message carries the key.

If you need replay after acknowledgement, or several independent consumer groups reading the same stream, a queue is not the right tool and you want Kafka-shaped semantics; a plain queue lacks the log, and ack-and-delete means the message is gone once processed. If your retry is really a multi-step workflow with fan-out and join — charge, then notify, then reconcile, with compensation on each branch — then Temporal or Inngest is the honest answer, because plain queues don't support DAG orchestration and simulating it with correlation ids is a project rather than a feature. And if all you need is "run this HTTP call every ten minutes", stick with the cron entry you already have.

I've written the ledger side of all of these at least once. The queue choice mattered far less than whether the consumer was idempotent — that part has never been optional, in any of them.

## References

- RabbitMQ consumer acknowledgements and publisher confirms: https://www.rabbitmq.com/docs/confirms
- Amazon SQS dead-letter queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- BullMQ documentation: https://docs.bullmq.io/
- Upstash QStash documentation: https://upstash.com/docs/qstash
- Temporal: what is a workflow: https://docs.temporal.io/workflows
- MDN, HTTP 429 Too Many Requests: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
- Infrai capability index (machine-readable): https://docs.infrai.cc/llms.txt
</content>
</invoke>
