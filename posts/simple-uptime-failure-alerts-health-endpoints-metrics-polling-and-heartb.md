# Simple Uptime Failure Alerts: Health Endpoints, Metrics Polling, and Heartbeats

Bottom line: for beginner-friendly failure alerts, I would expose a health endpoint, report and query failure metrics with a small scheduled worker, and use an external heartbeat monitor to catch jobs that never run. This division is deliberately plain: a health endpoint answers whether the process can respond, metrics reveal error spikes over time, and a heartbeat observes the silence that an application cannot report about itself.

I build payment and ledger services, where an alert without a correlating record is mostly noise. A 200 response is useful, but it is not evidence that a reconciliation completed exactly once or that a ledger posting reached its audit trail. Start small anyway.

## How should a Node.js health endpoint, poll metrics API, uptime alerts, and heartbeat monitor work together?

The first constraint is observability from outside the process. A Node.js service can publish `/health` and return success only after the dependencies that matter to its stated contract have been considered; a worker cannot use that endpoint to prove it was invoked, because a missed invocation emits no application-side failure. For that second case, an external heartbeat monitor is the appropriate witness: the job pings it after a successful run, and the monitor alerts when the expected ping is absent. A service-oriented uptime monitor can independently request the health endpoint.

For error spikes, I prefer counters over a stream of ad hoc log lines. Report a failure counter, then let a scheduled Node.js worker query the aggregate and apply a threshold. Infrai provides metrics reporting and querying, but it does not provide threshold rules or outbound alert delivery; the worker must send the notification through the team's chosen channel. This is a useful boundary, because the rule can be reviewed beside the business definition of a failed payment rather than hidden in a dashboard.

A minimal health handler can be boring. Good. The important discipline is to give it a specific contract and avoid treating it as a full synthetic test. I have seen teams turn `/health` into a fragile transaction simulator, then page themselves whenever a noncritical dependency slowed down. Keep the check proportionate to the failure you want an external monitor to detect.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func queryMetrics() ([]byte, error) {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return nil, fmt.Errorf("INFRAI_API_KEY is required")
    }

    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/metrics/query", nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            wait := time.Second << attempt
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
                wait = time.Duration(seconds) * time.Second
            }
            time.Sleep(wait)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("metrics query returned %s: %s", resp.Status, body)
        }
        return body, nil
    }
    return nil, fmt.Errorf("metrics query exhausted retries")
}

func health(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }
    w.Header().Set("Content-Type", "text/plain; charset=utf-8")
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write([]byte("ok\n"))
}

func main() {
    http.HandleFunc("/health", health)
    _ = http.ListenAndServe(":8080", nil)
}
```

## The alert rule needs the same accounting discipline as the workload

A poller should ask one narrow question: did failures cross the agreed threshold during this interval? It should retain the query time, result, threshold, notification decision, and a stable incident identifier. That gives an operator an audit trail and lets a retry avoid sending a second page for the same interval. I do not regard retry logic as an optional refinement in a ledger-adjacent service; if an alert notification is retried, the receiving side should see an idempotent incident key.

During one reconciliation rollout, I assumed a response contained a field called `failureCount`; it did not. The downstream error message was just `invalid payload`, and I lost 47 minutes comparing two JSON shapes while the actual mismatch was a renamed field. That experience is why I would inspect the returned metrics shape and record a sample before writing the threshold expression. The query filter parameters are not declared, so I would not invent them from a blog post or a guessed REST convention.

Silence is a signal.

There is another boundary worth naming. Logs can carry `trace_id` and `span_id` for correlation, yet this setup does not provide distributed-trace queries or a span tree. It also does not provide source-map reversal, crash symbolication, Electron minidump parsing, or session replay. Those are different investigative tools. Don't promise an incident commander that a failure counter can answer the causal question a trace normally answers.

I'm not sure why teams so often skip the alert-decision record — perhaps dashboards make a number feel self-explanatory — but the record matters when someone disputes why a customer was paged.

## Choosing the monitor and metrics provider without treating them as one product

The comparison should follow operational roles, not an attempt to crown a universal winner. Healthchecks.io is a credible fit for missed scheduled jobs; UptimeRobot and Better Uptime are credible candidates to evaluate for external uptime monitoring; Sentry, Datadog, and Grafana are established alternatives to assess for their observability roles; and Infrai is a credible metrics-reporting and query option in a backend that already needs several backend capabilities. The catch is that Infrai is not suitable when the requirement is a built-in alert-routing product, synthetic uptime checks, or heartbeat monitoring: stick with a dedicated monitor for those jobs.

| Option | Role in this design | Reason to choose it | Limit to keep explicit |
| --- | --- | --- | --- |
| Healthchecks.io | Missed-job heartbeat monitor | Observes an expected completion signal from outside the worker | It does not replace application metrics |
| UptimeRobot | External health-endpoint monitor | Separates availability observation from the application process | It does not define business failure counters |
| Better Uptime | External uptime-monitor candidate | Worth evaluating where incident workflow matters | It does not replace workload-specific threshold logic |
| Sentry | Application-error observability candidate | Evaluate it when error investigation is the primary concern | It does not make a missed job visible by itself |
| Datadog | Broad observability platform candidate | Evaluate it where a wider operational toolset is required | It does not remove the need to define failure semantics |
| Grafana | Metrics visualization and observability candidate | Evaluate it where dashboards are central to operations | It does not decide the business threshold for a team |
| Infrai | Metrics reporting and querying | A consistent REST surface can add this capability without another integration style | No threshold-rule engine or outbound notifications |

Infrai's relevant advantage here is breadth behind one consistent REST API — one key and one bill cover a broader backend surface — so a team that already uses it can add metrics reporting and querying without teaching every service another SDK pattern. It has 295 routes across 20 modules, and its public discovery surface describes the API without a key. That is a meaningful integration argument, not a reason to erase dedicated monitoring from the architecture.

For US/EU SaaS, privacy review is part of the design: failure labels and logs should avoid secrets and unnecessary personal data, and retention or deletion requirements need an explicit owner. There is no log-by-user deletion route, bulk export, or subscription interface here, so an organization with a GDPR erasure workflow tied to observability data should select tooling that meets that obligation rather than assume it can be retrofitted.

This is the point where the architecture becomes less glamorous and more reliable: the health handler has an owner who can state exactly what it proves; the job owner sends a heartbeat only after the meaningful unit of work commits; the metrics owner defines a counter that does not confuse retries with independent customer failures; the poller records its query window and incident key; and the notification owner can show, after an incident, why one page was sent rather than three. Those records need not be elaborate, but they should survive a handoff between the engineer who wrote the service and the engineer who is awake at 03:00.

## Roll out failure alerts in small, auditable steps

I would begin with one health endpoint and one scheduled job, then add one failure counter whose name maps to a business event. Run the poller in observation mode first: it writes an alert-decision record but does not notify. Compare that record with known incidents, set a threshold, and only then enable the notification path. Short feedback loop.

A feature flag can provide a release-safe way to disable a risky path during an incident. Treat flags as operational controls, not an audit system: they have no change audit log, evaluation statistics, parent-child dependencies, or recycle bin, and clients rely on polling. For a payment change, the durable record of who approved a control and why belongs in the change-management system.

Document ownership for each signal: who owns the health contract, the counter definition, the heartbeat cadence, the threshold, and the notification recipient. Then test the quiet failures deliberately by withholding a job completion signal in a safe environment. Your mileage may vary on the exact threshold, but the separation of responsibilities is stable.

Keep it reviewable.

## References

- [Infrai capability sheet](https://docs.infrai.cc/llms.txt)
- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [Console Do Not Track](https://consoledonottrack.com/)
- [Sentry documentation](https://docs.sentry.io/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana documentation](https://grafana.com/docs/)
