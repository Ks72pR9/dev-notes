# Shipment Webhook Task Processing: Cheap Queue Recovery for Rate Limits and Backoff

Short answer: select the task system by running an interruption-and-recovery drill, then keep the least complex option that can reconstruct every intended shipment webhook, enforce each subscriber's rate limit during replay, and preserve one logical delivery identity across retries and backoff. Cheap execution is useful, but an unaccounted delivery is operational debt rather than a saving.

For a media service sending one shipment update to many subscribers, the important question begins after a worker stops. Can the team identify the undelivered cohort, resume it without opening the rate-limit floodgates, and reconcile the final outcomes against the original fan-out set? A timer proves that code can start. It doesn't prove that interrupted work can finish.

## How should a Node.js SaaS recover queued webhook tasks after rate limits?

Run the selection exercise as a recovery drill, not a feature comparison. Insert a shipment event, derive the subscriber delivery set, interrupt workers at controlled boundaries, advance a fake clock, restart with a constrained destination, and account for every delivery. The surrounding application may use Node.js, while the proof harness and worker can use any language; the durable records, rather than a runtime-specific callback, define the contract.

The drill needs four checkpoints. First, stop immediately after delivery intent is committed but before any worker can lease it. Second, stop after a worker receives a lease but before it sends. Third, stop after sending but before it records an outcome. Fourth, stop while one subscriber is rate-limited and other subscribers remain ready. Each checkpoint asks a different question: whether work is discoverable, whether abandoned ownership expires, whether ambiguous remote effects reuse the same identity, and whether one destination's debt stays isolated.

The third checkpoint is the hard one. Consider an illustrative shipment `shp_8417` with deliveries for subscribers Alpha and Bravo. A worker leases Alpha's delivery, sends version 3 with the key `shipment:shp_8417:subscriber:alpha:v3`, and then loses its connection before committing the result. At that moment the local record can prove only that an attempt began; it cannot prove whether Alpha committed the remote effect. After the lease expires, a second worker may send again, but it must reuse the key rather than manufacture a new delivery identity. Meanwhile, Bravo's untouched delivery should continue through its own admission state instead of waiting for Alpha's ambiguity to be resolved. The ledger now shows two attempts for one logical Alpha delivery, and reconciliation can compare that delivery with the single source shipment version. No sender can infer from a lost connection whether a subscriber accepted a webhook. The system therefore needs an exactly-once mindset without making an exactly-once network claim: one logical delivery ID, one stable idempotency key, conditional state transitions, and an append-only record of attempts. A repeated request may still cross the network, but it represents the same intended effect. If the subscriber cannot deduplicate by that key, the ambiguity remains visible and must be reconciled instead of being disguised as success. This example is deliberately about evidence, not an assertion that the network delivered once.

Commit intent first.

An auditable delivery record should identify the shipment version, subscriber, policy version, current eligibility time, attempt count, and lease generation. Attempt records should capture the decision that followed a lease without copying authentication material or unnecessary customer data into logs. Retention, erasure, and operator access depend on contract and jurisdiction -- compliance limits are configuration inputs, not defaults a queue library can choose for the business.

There is no universal recovery-time target. I'm not sure what target is defensible without subscriber contracts, backlog distribution, and an agreed operational objective; a production-shaped drill supplies that evidence. What matters during selection is whether recovery time degrades predictably and whether the evidence survives a restart.

## Treat subscriber backlog as destination debt

A fan-out is not one queue-shaped problem. It is a set of destination debts created by one source event. If subscriber A has exhausted its allowance, subscriber B should not wait behind A merely because both deliveries were derived from the same shipment. Conversely, allowing all restarted workers to race for A can turn recovery into another rate-limit event.

Model three decisions separately. The queue decides when a delivery is eligible. A keyed admission control decides whether its subscriber may receive another attempt. The retry policy decides the next eligibility time after a classified outcome. This separation matters during replay: old work must pass through the current admission boundary, while its logical identity and historical attempts remain unchanged. A retry count alone can't explain why an attempt became eligible or which policy made that decision.

Backoff should be bounded, include jitter, and be recorded as a policy decision rather than buried in worker sleep. Sleeping workers hold capacity and lose the reason for the delay after a restart; a persisted `next_attempt_at` makes the delay inspectable. Delayed shipment tasks and immediate shipment tasks should enter the same admission path once eligible, otherwise the delayed path can bypass a subscriber limit that the immediate path respects.

Use an append-only attempt ledger beside a mutable delivery projection. The projection answers the dispatcher's fast question -- what is eligible now? -- while the ledger answers the operator's slower questions: what was attempted, under which lease and policy, and why is another attempt permitted? A lease expiry may return abandoned work to eligibility, but completion must compare the lease generation so that a late worker cannot overwrite a newer result.

One hot subscriber will otherwise hide inside aggregate metrics. Observe eligible age and ready depth by bounded destination cohorts, attempt classifications, lease expirations, and the age of the oldest unresolved shipment version. Avoid subscriber IDs, payloads, and authorization data in metric labels. The operational signal is progress and isolation, not a high-cardinality copy of the audit log.

## Make recovery a first-class state transition

The worker's useful abstraction is small: lease one eligible delivery, ask destination admission for a time, and record the resulting transition. The following Go sketch keeps those boundaries explicit and leaves storage and transport behind interfaces.

```go
package recovery

import (
	"context"
	"time"
)

type Delivery struct {
	ID             string
	SubscriberID   string
	IdempotencyKey string
	LeaseGeneration uint64
	PolicyVersion  string
}

type Transition struct {
	Kind       string
	EligibleAt time.Time
}

type Store interface {
	LeaseEligible(context.Context, time.Time) (Delivery, bool, error)
	CommitTransition(context.Context, Delivery, Transition) error
}

type Admission interface {
	NextAllowed(context.Context, string, time.Time) (time.Time, error)
}

type Transport interface {
	Deliver(context.Context, Delivery) (Transition, error)
}

func RecoverOne(
	ctx context.Context,
	now time.Time,
	store Store,
	admission Admission,
	transport Transport,
) error {
	delivery, ok, err := store.LeaseEligible(ctx, now)
	if err != nil || !ok {
		return err
	}

	allowedAt, err := admission.NextAllowed(ctx, delivery.SubscriberID, now)
	if err != nil {
		return err
	}
	if allowedAt.After(now) {
		return store.CommitTransition(ctx, delivery, Transition{
			Kind: "deferred_by_admission", EligibleAt: allowedAt,
		})
	}

	transition, err := transport.Deliver(ctx, delivery)
	if err != nil {
		return err
	}
	return store.CommitTransition(ctx, delivery, transition)
}
```

`CommitTransition` has the critical job: compare the lease generation, append an attempt fact, and update the projection in one transaction. A transport error does not by itself define a retry schedule; the lease recovery rule handles an interrupted worker, while a classified delivery result carries an explicit next eligibility time. Keep these cases distinct or an outage inside the worker process becomes indistinguishable from a subscriber response.

Test the state machine with a fake clock and deterministic policy inputs. Competing leases should yield one owner. An expired lease should become eligible without changing the delivery ID. A late completion should fail its generation check. A replay should retain its idempotency key. A rate-limited subscriber should move forward according to admission state without preventing a different subscriber from advancing.

Short tests are not enough here -- the acceptance suite should also kill the worker process at each drill checkpoint, restart against the same durable state, and reconcile intended deliveries with terminal outcomes. That is where an in-memory scheduler reveals its boundary.

## Compare systems by the evidence left after interruption

The cheapest simple option is the one whose operational obligations the team can actually carry. Evaluate categories against the same drill and include storage, retry amplification, observability, egress, retention, and on-call labor in the cost model. Price per task, examined alone, says little about the cost of reconstructing an ambiguous fan-out.

| System boundary | Suitable when | Recovery evidence required | Limitation that changes the choice |
| --- | --- | --- | --- |
| Database-backed worker | The team already operates transactional storage and fan-out volume is modest | Conditional leases, attempt rows, and a queryable delivery projection | Not suitable when the team cannot own pruning, admission control, and replay tooling |
| Managed task queue | Durable task ownership should sit outside the application process | Stable task identity, documented retention, and controlled redrive behavior | Application code may still need subscriber-keyed admission and business reconciliation |
| Durable workflow service | A shipment update expands into waits, branches, or compensating actions | Inspectable execution history and explicit versioning behavior | The workflow model and operational surface may be excessive for one webhook effect |
| Scheduled trigger plus durable scan | Coarse timing is acceptable and all work is rediscoverable from stored intent | Scan checkpoints, overlap rules, and a missed-run recovery test | Stick with a queue when each item needs isolated retry timing or prompt delivery |

Cloudflare documents Cron Triggers as scheduled invocations of a Worker, which makes that mechanism relevant to the scheduled-scan category. Inngest documents an event-driven execution model, which makes it relevant to the workflow category. These are examples of boundaries to verify, not a ranking and not substitutes for a recovery drill against the team's own state model.

The catch is ownership. A database-backed worker can be the smallest credible design, but only for a team prepared to own leases, destination admission, backoff policy, audit retention, and operator replay. Choose a managed queue when durable task operations should be delegated, and choose a workflow system when the business process genuinely has durable multi-step state. Use a scheduled scan as reconciliation or for coarse work; don't treat the schedule itself as the delivery ledger.

## Roll out by reconciling cohorts

Start by writing the same delivery intent to old and candidate paths while only the old path sends. Compare the derived subscriber set and logical delivery IDs for each shipment version. Then allow the candidate worker to send for a bounded subscriber cohort, with one control that stops new leases but leaves history intact.

Expand the cohort only after both paths agree on intended work and the candidate path closes every delivery as delivered, retry-eligible, or terminal under an explicit policy. Review oldest eligible age, lease expirations, duplicate-key observations, and terminal classifications during each expansion. Rollback should stop acquisition; deleting queued records would destroy the evidence needed to explain the rollback.

One final drill settles the selection: interrupt every transition, restore the workers, respect destination admission during the catch-up, and balance the shipment's intended subscriber set against its final outcomes. The winning implementation is the least complex one that leaves that equation provable.

## References

- https://developers.cloudflare.com/workers/configuration/cron-triggers/
- https://www.inngest.com/docs
