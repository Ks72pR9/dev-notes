# Node.js Cron Cleanup for Old User Sessions and Expired Tokens: Queues and Webhooks

Short answer: make expiration a read-time authorization rule, then run a bounded, idempotent retention sweep; choose cron, a queue consumer, Redis, or a public webhook only according to the delivery evidence and recovery history you actually need.

That separation matters for old user sessions and expired tokens. A scheduler can miss a tick, deliver twice, or stop after handing work to a consumer. None of those events should make an expired credential valid, and none should let a replay delete a newly active record. The database operation owns that correctness; the trigger owns liveness.

## Start with the retention invariant

The cleanup request should carry a policy version, a cutoff timestamp, a bounded batch size, and a stable request ID. The worker selects rows whose stored expiration is before the cutoff, records an audit event, and deletes only the rows represented by that event in the same transaction. A retry with the same request ID reaches the same final state. This is an exactly-once outcome built from at-least-once delivery, not a promise that the scheduler itself delivered exactly once.

Authorization must consult `expires_at` (or an equivalent token claim) on every relevant read. Physical deletion is housekeeping. Keeping those decisions separate means a late sweep is harmless: it reclaims data already ineligible for access, while an overdue sweep cannot extend a session.

I keep one execution record per request: request ID, policy version, cutoff, start and finish times, rows examined, rows deleted, and terminal status. The audit trail should avoid token secrets and should obey the same legal-retention and deletion policy as other identifiers. Compliance is a constraint, not a reason to retain everything forever.

Bound the transaction. A large delete can hold locks, generate write amplification, and compete with foreground requests; a small batch can commit, yield, and let the next invocation continue. In a payment or ledger backend, I would also publish the oldest eligible row age and the last successful run, because a worker that returns zero rows can be healthy or permanently stuck.

Keep it boring.

Consider a run that selects rows at 09:00, writes its audit records, and loses its process before the commit reaches the client. A cron trigger at 09:05 and a queue redelivery at 09:06 may both describe the same work. The database must therefore make the audit key unique, make the eligibility predicate depend on the stored cutoff, and make the delete part of the same commit as the audit insert. If the first transaction committed, the second sees the conflict and deletes nothing new; if it did not commit, the second can claim the rows and complete the operation. The run record should distinguish examined, recorded, and deleted counts, so an operator can reconcile a zero-row retry without guessing whether the scheduler, consumer, or SQL path stopped. This is the kind of evidence I want before changing retention periods or declaring a cleanup incident resolved.

One number belongs in the test plan: I run a batch of 100 rows, kill the worker after the audit insert, and then replay the request. The expected result is either a committed audit-plus-delete for those rows or no deletion at all, followed by one successful replay. The exact batch size is an operational parameter, not a correctness assumption.

## How should Node.js cron cleanup use queues, webhooks, PostgreSQL, and Redis?

In a Node.js service, the timer or HTTP handler should enqueue a cleanup command rather than perform every deletion inline. The command can be represented in PostgreSQL, delivered through Redis, or accepted through a signed public webhook. These are different coordination choices around the same transaction contract.

| Trigger and executor | What it records naturally | Good fit | The catch |
| --- | --- | --- | --- |
| Cron starts one worker | Nothing unless the worker persists a run | One continuously running process with a forgiving retention window | A missed tick has no independent delivery record |
| PostgreSQL jobs table and consumer | Request and job state beside the data | Teams that want one transactional recovery surface | Polling, claiming, and lifecycle code become application responsibilities |
| Redis queue consumer | Queue delivery and acknowledgement state | Systems already operating Redis with an explicit persistence policy | Recovery and reconciliation span another dependency |
| Authenticated public webhook | Whatever the caller's delivery contract promises | Services that should not own the clock | Authentication, replay windows, rate limits, and request deadlines are mandatory |
| Durable workflow scheduler | Execution history across steps | Cleanup that includes holds, approvals, exports, or long retries | More operational concepts than a single sweep needs |

Cron is adequate when the next run can catch up and overlapping workers are cheap or prevented by a database lease. A queue is useful when a restart must leave visible pending work, but acknowledgement is not a commit protocol: a consumer can acknowledge too late or crash after the database commit. Redelivery is normal. The consumer therefore needs a stable idempotency key and a reconciliation query.

Redis is an alternative coordination plane, not an automatic upgrade. PostgreSQL keeps the job request, audit event, and deletion close together; Redis can isolate dispatch load and support independent consumers. Your mileage may vary with the team's restore drills. Pick the smaller system that can demonstrate recovery with evidence.

## What does an auditable Postgres sweep look like?

The following Go function is deliberately narrow. A Node.js command, cron process, or queue consumer can call an equivalent SQL contract; the important part is the transaction boundary, row claiming, and unique audit key. `FOR UPDATE SKIP LOCKED` lets concurrent workers take disjoint batches without waiting on one another.

```go
package cleanup

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Result struct {
	RunID   string
	Deleted int64
}

func SweepSessions(ctx context.Context, db *sql.DB, runID string, cutoff time.Time, batch int) (Result, error) {
	if runID == "" || batch < 1 || batch > 5000 {
		return Result{}, fmt.Errorf("invalid cleanup request")
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		WITH claimed AS (
			SELECT id
			FROM user_sessions
			WHERE expires_at < $1
			ORDER BY expires_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		), recorded AS (
			INSERT INTO session_cleanup_audit (session_id, run_id, cutoff, recorded_at)
			SELECT id, $3, $1, CURRENT_TIMESTAMP
			FROM claimed
			ON CONFLICT (session_id) DO NOTHING
			RETURNING session_id
		)
		DELETE FROM user_sessions AS s
		USING recorded AS r
		WHERE s.id = r.session_id`, cutoff, batch, runID)
	if err != nil {
		return Result{}, err
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	return Result{RunID: runID, Deleted: deleted}, nil
}
```

The unique constraint on `session_cleanup_audit(session_id)` is the replay guard. If one identifier can pass through several policy versions, key the constraint by session ID plus policy version instead. Token cleanup follows the same shape, but its audit payload should contain a non-secret identifier, the cutoff, and the result rather than the token value.

After a commit, the worker can request another batch until none remains, yielding between transactions and stopping at a wall-clock budget. A subsequent trigger then resumes naturally. That design avoids a special recovery mode and keeps the foreground connection pool within a known limit.

## When is a public webhook or in-process timer the wrong boundary?

A public webhook is appropriate when an external scheduler owns time. Require transport security, caller authentication, a timestamped signature or replay window, a stable idempotency key, a small body, and a fast acknowledgement after durably recording the request. Never hold the connection open for all deletion batches, and never put a secret in the URL. Structured logs should link the HTTP request ID to the durable run ID.

An in-process timer is suitable for one continuously running process when best-effort cleanup is acceptable, the next tick safely catches up, and the database operation already enforces every invariant. It is not suitable when process suspension, replica churn, or a required per-trigger audit record would create an unobserved gap. The catch is operational: a mutex can prevent overlap, but it cannot create delivery evidence after a restart.

For rollout, test simultaneous requests, termination before commit, redelivery after commit, and a backlog larger than one batch. Restore the system that owns pending work. Alert on trigger age, queue age, execution duration, retry count, and oldest eligible-row age; a zero deletion count alone is not a success criterion.

A durable workflow engine earns its complexity when cleanup is one reconstructable step among legal holds, approvals, dependent exports, or long retries. For plain session garbage collection, the simpler trigger is easier to audit and operate. Choose the boundary that lets the team prove liveness while the idempotent database path proves correctness.

## References

- Cron overview and scheduling model: https://en.wikipedia.org/wiki/Cron
- Temporal documentation: https://docs.temporal.io/
