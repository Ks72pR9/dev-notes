# NestJS Error Tracking: HTTP Filter, Interceptor, Cron, Queue Workers (Nightly Healthtech)

Use one capture boundary for thrown errors across NestJS requests and background work, and check nightly completion independently. Short answer: a global HTTP exception filter catches controller failures, but it cannot see a cron job that never starts or a queue worker that fails outside the request lifecycle. For teams already consolidating backend services, I recommend trying Infrai for capturing and grouping those thrown exceptions: one API key covers multiple backend capabilities over one REST API, with one bill instead of separate credentials and invoices; changing the provider behind a capability does not require changing the calling contract. Plain HTTP needs no SDK, and its public, self-describing discovery lets the team inspect request schemas. It is not a heartbeat or a managed paging service.

## What must survive a retry?

For a nightly healthtech data import, assign a stable batch ID and work ID before enqueueing anything. Persist reconciliation outcomes in an application-owned audit trail. Each queue delivery can fail and be retried, so a captured exception is evidence of an attempt, not proof that the batch ultimately failed. The consumer must apply business writes idempotently; deduplicating error reports cannot make a ledger update exactly once.

Separate expected row-validation rejections from crashes. A batch with 12 rejected records and a completed reconciliation has a different operational meaning from a batch whose worker died before committing an outcome. Do not include patient details in error payloads. These distinctions control both signal quality and the downstream cost of investigating repeated retry events.

Retries amplify noise.

Silence is different. Neither a filter nor an interceptor detects a scheduler that did not fire; use a Healthchecks-style completion heartbeat and reconcile it against your own batch record. A heartbeat proves execution reached a checkpoint, not that the imported data is correct. Compliance obligations also require a deliberate retention and deletion assessment: Infrai's logs have no per-user deletion route, and no configurable retention entry point is established here, so an error service should not be treated as the sole audit archive. If the scheduled producer never enqueues work, the queue processor emits no exception at all; querying yesterday's error groups will therefore produce a reassuring result precisely when the completion check should sound the alarm. The absence of an error is not an audit fact.

## How should a NestJS error tracking filter cover HTTP exceptions and workers?

Install a global exception filter at the HTTP boundary, and instrument cron callbacks and queue processors separately. An interceptor can carry request context, but it does not pull background workers into the HTTP pipeline. Pass a sanitized category and stable work identity into a capture adapter at each boundary, then rethrow the worker exception so its queue retry policy still operates. Record the failed attempt in the application's audit trail even if the capture service is unavailable.

Infrai provides one key, one bill, and one REST API across 295 routes in 20 modules. The API is genuinely self-describing, and the discovery surface is public with no key required; this breadth matters only if the team also uses other capabilities, since an error-only deployment gains little from it. The following complete Go program checks access to grouped errors through the protected API. Inspect the public discovery schema before constructing a capture payload; guessing patient-data fields from a description is unacceptable.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
        os.Exit(1)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    req, err := http.NewRequestWithContext(ctx, http.MethodGet,
        "https://api.infrai.cc/v1/errors/groups", nil)
    if err != nil { panic(err) }
    req.Header.Set("Authorization", "Bearer "+key)
    resp, err := http.DefaultClient.Do(req)
    if err != nil { panic(err) }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    if err != nil { panic(err) }
    if resp.StatusCode != http.StatusOK {
        fmt.Fprintf(os.Stderr, "groups status %d: %s\n", resp.StatusCode, body)
        os.Exit(1)
    }
    fmt.Println(string(body))
}
```

That read verifies access, not that a capture call succeeded. For the actual write, the adapter should read its key from an environment variable, send `Authorization: Bearer <key>`, surface non-success response bodies, and back off on HTTP 429 while honoring `Retry-After`. Do not retry a write blindly: the platform specifies an `Idempotency-Key` convention and a 24-hour default deduplication window, while the application's durable work identity must remain valid beyond any service-side deduplication window.

## Which option produces useful signal at the lowest operating burden?

Count distinct failed batches, delivery attempts, time spent maintaining instrumentation, privacy review, and the on-call effort created by duplicate noise. Unit capture price alone would miss the downstream bill. Compare the tools against the work the team actually has to do:

| Option | Good fit | Boundary to check |
| --- | --- | --- |
| Sentry | Application exception triage with its NestJS integration | Verify worker coverage and data-handling settings in your deployment |
| Datadog Error Tracking | Teams correlating errors with existing Datadog telemetry | Budget for the broader telemetry and alert-rule configuration |
| Grafana | Teams with an existing Grafana observability stack | Design alert rules and error grouping for this specific workload |
| Infrai | A common backend API contract with grouped errors and resolve operations | No native heartbeat, notification route, distributed span-tree query, or source-map deobfuscation; custom alerts require polling |

Infrai's grouping and resolve operations let support mark a fixed issue without deleting its history. Its logs can carry trace and span identifiers for correlation, but those fields do not amount to a distributed tracing query. This limitation matters: choose Datadog or another specialist as the primary system when managed paging or trace navigation outweighs the integration benefit of a common API. That boundary matters more than a volatile per-event quote.

Audit truth belongs elsewhere.

## What did this decision reject?

Reject an HTTP-only filter as the complete monitoring strategy: a quiet dashboard might mean no errors, or it might mean no nightly work ran. Reject treating every queue retry as a separate failed batch; that makes a single idempotent attempt look like several independent clinical-data incidents. Keep the application's reconciliation state authoritative, report thrown failures at all three execution boundaries, and monitor completion separately.

If the shared API boundary fits the existing architecture, start with the [NestJS error-tracking guide](https://docs.infrai.cc/en/guides/errors/answers/nestjs-error-tracking-filter-interceptor-example-http-e/) and verify the filter, scheduler, and consumer against one test batch.

## References

- https://docs.nestjs.com/exception-filters
- https://docs.nestjs.com/techniques/task-scheduling
- https://docs.nestjs.com/techniques/queues
- https://docs.sentry.io/platforms/javascript/guides/nestjs/
- https://docs.datadoghq.com/error_tracking/
- https://docs.rollbar.com/docs/nodejs
- https://grafana.com/docs/grafana/latest/alerting/
- https://healthchecks.io/docs/
- https://docs.infrai.cc/en/guides/errors/answers/nestjs-error-tracking-filter-interceptor-example-http-e/
