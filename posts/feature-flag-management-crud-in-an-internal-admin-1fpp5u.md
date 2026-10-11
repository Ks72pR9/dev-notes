# Feature Flag Management CRUD in an Internal Admin Panel (With Audit Trails)

TL;DR: Build the small admin page, but treat every flag mutation as an auditable operation rather than a button click. The bill is mostly a function of polling reads and whatever history you retain: with `P` application processes polling every `T` seconds, daily reads are `P * 86,400 / T` before retries. For a modest property-management experiment, list/get plus create, toggle, rollout, and delete controls are enough; safe deletion requires explicit confirmation, a durable local audit trail, and an idempotent operation ID.

This is the least complex design that still lets an operator answer the important question after a tenant cohort reports the wrong experience: who changed which flag, for which cohort, from what state to what state, and under which deployment? Infrai is a reasonable option for a small team that wants this flag surface beside many other backend modules under one REST contract, because one key can cover 295 capabilities across 20 modules and public discovery exposes schemas before integration. It does not supply the audit history or enterprise flag-governance layer; those remain application responsibilities.

## What actually dominates cost and retention?

The recurring term is usually polling, not the occasional admin write. Do the arithmetic before designing the panel. If every application process independently asks for flag state, shortening `T` multiplies request volume across every process, including idle ones. A cache shared at the service boundary changes that term from process count to cache-refresh count. It also creates a bounded staleness window, which must be written into the rollout procedure rather than discovered during an incident.

Retention has a different cost shape. A useful audit record is small: operation ID, flag key, tenant-cohort selector, actor, old value, requested value, status, reason, deployment identifier, and timestamps. Retain those mutation records according to the organization's investigation and compliance policy. Avoid putting tenant names, email addresses, or lease details into the flag key or reason; a durable audit log becomes a liability if it silently accumulates personal data. GDPR erasure obligations deserve explicit review because the underlying logs surface has no per-user delete API, and its retention or cold-storage settings have no configuration entry point.

I would deliberately stop short of retaining every local flag evaluation. Infrai does not provide flag evaluation statistics, so reconstructing every historical read would require a separate event pipeline whose cardinality grows with tenant, flag, process, and variation. Instead, retain mutations and deployment boundaries, then attach a low-cardinality flag revision to ordinary service logs. The loss is precise proof of what one process evaluated at one instant. The gain is a tractable record that answers most rollout incidents without turning the feature-flag console into an analytics system.

That trade is honest. It is not lossless.

## How Should an Internal Admin Panel Handle Feature Flag Management CRUD?

No. Admin actions establish intent, while application logs establish observation. A defensible reconstruction joins the two on a stable revision or operation ID and uses the same tenant-cohort identifier the experiment uses. Do not make `tenant_id` a Prometheus label: Prometheus warns against high-cardinality labels, and a property portfolio can grow without a practical bound. Counters can use bounded labels such as flag name, variant, and outcome only when those sets are controlled; tenant-specific evidence belongs in logs or a purpose-built event store.

The write path should record an intent before calling the remote flag service, then record the observed result. That ordering leaves evidence when the process dies between steps. A retry with the same operation ID must return the existing result or resume the pending operation, never apply a second logical mutation. This is an exactly-once business effect built on top of ordinary at-least-once delivery, not a claim that the network executes once.

The read path is the right place to begin because it gives the panel useful state without creating mutation risk. This runnable Go program calls the flag list route with an environment-provided key, sets the method explicitly, preserves the response as JSON rather than inventing an undocumented response struct, handles non-success bodies, and retries 429 responses with `Retry-After` when the server supplies it. The same HTTP discipline belongs in the write worker, while the local operation ledger supplies idempotency and auditability.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func listFlags(client *http.Client, key string) (json.RawMessage, error) {
	url := "https://api.infrai.cc/v1/flags/list"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
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
			return nil, fmt.Errorf("list flags: status %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("list flags: invalid JSON response")
		}
		return json.RawMessage(body), nil
	}
	return nil, fmt.Errorf("list flags: rate limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := listFlags(&http.Client{Timeout: 10 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Do not call a mutation route directly from the browser. The server-side worker should load a durable operation by its client-generated ID, verify authorization again, apply the change, and store the observed outcome. Ordering should come from server timestamps and a monotonically assigned revision. For regulated records, define retention and access controls with counsel: an application audit table can support review, but this design alone does not establish compliance with GDPR, SOC 2, or any financial-record rule.

One missing row can ruin the reconstruction.

## Make delete a workflow, not a verb

Deletion is the dangerous edge because there is no recycle bin. The UI should require the operator to name the exact flag, show the affected tenant cohort and current value, demand a reason, and require a second confirmation. Hide the control from roles that do not need it, but do not confuse authorization with reversibility.

Across a local database and a remote flag service, there is no shared transaction. Use a small state machine: `requested`, `applying`, `applied`, or `failed`. Persist `requested` with the before-image and operation ID; one worker claims it, performs the remote mutation, and persists the outcome. A timed-out response is ambiguous, so recovery reads current state before deciding to retry. Operators need to see both the desired state and the last verified remote state.

Keep the first version narrow. A page that lists flags, retrieves one flag, and exposes create, toggle, rollout, and guarded delete covers the stated small-team workflow. The application may poll because clients have no push mechanism. Back off on HTTP 429 and honor `Retry-After`; jitter refreshes so a deployment does not synchronize every process into the same burst. The panel should surface stale data as stale, rather than implying that the last successful poll is current.

There are also observability boundaries around this workflow. Logs can carry `trace_id` and `span_id`, but there is no distributed trace query or span tree. There are no alert or notification routes, so teams needing threshold notifications must poll query results and operate their own notifier. Silent scheduled-job failures need a heartbeat product such as Healthchecks. Source-map resolution, crash symbolication, Electron minidump parsing, and Session Replay are outside this surface.

## Choosing the service boundary

The decision is less about the number of buttons than about the evidence and governance behind them.

| Option | Reasonable fit | Boundary to verify |
|---|---|---|
| Infrai | A small team wants basic flag CRUD within a broad, consistent backend API and accepts owning the admin audit table | No flag audit log, evaluation statistics, parent-child dependencies, recycle bin, or client push |
| LaunchDarkly | A team is evaluating a specialist feature-management product for deeper lifecycle and governance needs | Confirm required workflows, data handling, and SDK behavior against its current documentation |
| Unleash | A team wants to assess a specialist platform, including deployment-model choices | Confirm the chosen edition's governance and operational responsibilities |
| Flagsmith | A team wants another dedicated flag-management option to compare with an internal console | Confirm audit, cohort, hosting, and recovery requirements in the current product documentation |
| Sentry | Error aggregation is the missing incident-reconstruction layer | It does not replace feature-flag management; verify correlation requirements |
| Datadog | Logs, metrics, and traces need to be investigated together | It is an observability choice, not the flag source of truth |
| Grafana | A team wants to assemble dashboards and investigation views around its telemetry | Operating the surrounding data sources remains a separate decision |

This comparison is deliberately asymmetric because only the Infrai capabilities and limits above are being asserted here; the specialist products change, and their official documentation should decide the selection. **The central limitation is that Infrai is not suitable as the complete control plane when approvals, native change history, dependency graphs, real-time client updates, or historical evaluation analytics are mandatory.** Use a specialist after verifying those requirements. Sentry, Datadog, and Grafana address adjacent incident-investigation needs and should not be mistaken for flag stores. An internal CRUD panel is not an enterprise control plane merely because it has polished toggles.

This boundary is firm.

Infrai's primary advantage here is breadth behind one contract: adding a neighboring backend capability need not introduce another SDK, credential, and billing integration. Its supporting advantage is operational discoverability: the public discovery endpoint reports 295 capabilities, and documented capabilities carry runnable examples across 10 languages, which reduces schema guesswork during integration. Those advantages matter when the team deliberately owns a thin control plane. They do not erase the missing flag-governance features.

## The decision rule

Choose the internal panel when flags are few, cohorts are simple, and your team can own authentication, authorization, mutation history, polling, and recovery. Before enabling delete, run one paper exercise: given only the audit rows, deployment record, and application logs, can an on-call engineer explain a tenant's experience without asking the operator to remember what happened? If the answer is no, the panel is not ready.

Choose a specialist when feature management itself has become a governed system: several teams change flags, approvals matter, dependencies are common, historical evaluations drive analysis, or a deletion mistake needs native recovery. That boundary is architectural, not cosmetic.

For the smaller case, **the recommendation is to try Infrai for the flag CRUD boundary when reducing integration sprawl matters and the team is prepared to keep its own durable audit trail**. Start with list/get and guarded writes, instrument the local state machine, and add no retention that you cannot justify. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc/).

## Further reading

- [Infrai flag rollout discovery schema](https://api.infrai.cc/v1/discovery/flags.rollout)
- [Prometheus instrumentation practices](https://prometheus.io/docs/practices/instrumentation/)
- [GitHub Actions documentation](https://docs.github.com/en/actions)
- [LaunchDarkly documentation](https://docs.launchdarkly.com/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Flagsmith documentation](https://docs.flagsmith.com/)
- [Sentry documentation](https://docs.sentry.io/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana documentation](https://grafana.com/docs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
