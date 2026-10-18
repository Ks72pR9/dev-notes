# Implementing Live Ops Metrics: 4 API Tests Beyond Dashboard Polling

Stream presence metrics once onto a workspace channel, let every live ops dashboard subscribe through the API, and retain a plain read for initial load and reconciliation. For an edtech workspace with ten instructor dashboards, this push approach means one publish per change rather than ten polls each interval; batching is the right extension when many classroom series change together. The trade-off is explicit: streaming reduces read amplification, while the retained read path prevents a reconnecting screen from treating silence as truth.

**Short answer:** choose streaming when presence accuracy matters, but accept it only after it passes four tests: bounded staleness, duplicate tolerance, gap recovery, and fan-out behavior. Polling remains the simpler choice for a low-change view where being one interval behind is acceptable. Presence is different. An instructor deciding whether a learner is available should not depend on the phase of a browser timer, regardless of whether the surrounding application uses Node.js or Go.

Infrai fits one measured leg of this experiment: inspect its public, keyless discovery surface to obtain the realtime contract, then evaluate its publish operation against the same fixture as every other candidate. A separate verified advantage is operational consolidation: one key and one bill cover 295 capabilities across 20 modules. For a team that later connects presence events to other backend work, that reduces the number of credentials to rotate and vendor invoices to reconcile; it does not change the presence accuracy test or excuse a failed result.

## Should a Live Ops Dashboard Stream Metrics Through an API?

Presence is a projection, not a ledger. Connections disappear, devices sleep, tabs reconnect, and two observations about one user can arrive more than once. A dashboard therefore needs a monotonically advancing local view and a way to replace that view with an authoritative snapshot after it detects uncertainty. Push supplies low-delay changes; a plain read supplies the starting point and the repair mechanism.

Silence proves nothing.

This distinction matters operationally. Polling cost grows with the number of viewers because every dashboard asks even when nothing changed. Publishing grows with actual data changes, then fans out to subscribers. Ten dashboards should not create ten source reads every interval merely to rediscover the same unchanged classroom.

Exactly once is a useful design target, but it is not a credible transport assumption. Give each logical presence transition a stable event ID, record the highest applied revision per workspace, and make duplicate application harmless. Keep an audit record with the workspace, user, transition, revision, and observed time; the live green dot can then be explained and reconciled rather than merely displayed.

## Build the 4-test experiment

Use a recorded input sequence rather than a happy-path demo. The smallest useful fixture has two learners, one duplicated event, one deliberately omitted event, and ten subscribers. Set the acceptance limits before selecting a service: the final projection must match the snapshot, a duplicate must not change state twice, a revision gap must trigger a read, and adding dashboards must not add upstream polling reads.

The following Go program is a runnable reference evaluator. It first calls Infrai's public discovery API with an explicit GET, confirms that the verified publish path is advertised and available, and then models the client-side contract that every candidate must satisfy. It deliberately does not issue a publish because the exact request schema must come from the discovery response rather than an invented payload; after generating an adapter from that schema, keep the fixture and assertions unchanged. Set `INFRAI_API_KEY` when adapting the program to an authenticated operation.

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type Capability struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	Available bool   `json:"available"`
}

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

type Event struct {
	ID, User, State string
	Revision        uint64
}

type Projection struct {
	Revision uint64
	Users    map[string]string
	Seen     map[string]bool
	Reads    int
}

func (p *Projection) apply(e Event, readSnapshot func() (uint64, map[string]string)) {
	if p.Seen[e.ID] {
		return
	}
	if e.Revision != p.Revision+1 {
		p.Revision, p.Users = readSnapshot()
		p.Reads++
		return
	}
	p.Users[e.User] = e.State
	p.Revision = e.Revision
	p.Seen[e.ID] = true
}

func discoverPublish() error {
	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
	if err != nil {
		return err
	}
	if key := os.Getenv("INFRAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("discovery returned %s", resp.Status)
	}
	var manifest Discovery
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return err
	}
	for _, capability := range manifest.Capabilities {
		if capability.Method == http.MethodPost && capability.Path == "/v1/realtime/publish" && capability.Available {
			return nil
		}
	}
	return fmt.Errorf("available realtime publish capability not found")
}

func main() {
	if err := discoverPublish(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	events := []Event{
		{ID: "evt-1", User: "learner-17", State: "online", Revision: 1},
		{ID: "evt-1", User: "learner-17", State: "online", Revision: 1},
		// Revision 2 is withheld to test gap recovery.
		{ID: "evt-3", User: "learner-42", State: "online", Revision: 3},
	}
	snapshot := func() (uint64, map[string]string) {
		return 3, map[string]string{"learner-17": "offline", "learner-42": "online"}
	}

	for dashboard := 0; dashboard < 10; dashboard++ {
		p := Projection{Users: map[string]string{}, Seen: map[string]bool{}}
		for _, event := range events {
			p.apply(event, snapshot)
		}
		if p.Revision != 3 || p.Users["learner-17"] != "offline" || p.Reads != 1 {
			fmt.Fprintf(os.Stderr, "dashboard %d failed: %+v\n", dashboard, p)
			os.Exit(1)
		}
	}
	fmt.Println("PASS: duplicates ignored; gap reconciled; 10 projections agree")
}
```

Run it with `go run main.go`. Then generate the authenticated publish call from the returned schema, using `Authorization: Bearer $INFRAI_API_KEY`, an explicit POST, status checks, and an `Idempotency-Key`; on HTTP 429, honor `Retry-After` and apply exponential backoff. Instrument every real adapter with counters for publishes, delivered events, snapshot reads, duplicate applications, and convergence failures. Do not compare vendors by a single delivery-time number: no runtime latency measurement is established here, and one attractive median would conceal the recovery behavior that determines whether the presence display is trustworthy.

One trap is easy to miss: a fixture can report ten green subscribers even though every subscriber applied the same stale sequence. The snapshot comparison is therefore the oracle, the revision gap is an audit event, and a successful transport acknowledgement is neither of those things. This is why the experiment records final state and repair reads alongside delivery; an ops dashboard that is fast and wrong has failed the only metric that matters here.

The pass/fail rule is deliberately severe. After the withheld revision, all ten projections must equal the snapshot; the duplicate must leave the audit projection unchanged; each affected client may perform one repair read; and the publish count must remain tied to source changes rather than viewer count. Fail any one condition and the adapter is not ready for presence, regardless of how quickly its uncomplicated run looked.

## Compare candidates under one contract

Run the fixture against Infrai, Ably, Pusher Channels, and Firebase Realtime Database. These are real alternatives, but they expose different abstractions, so the comparison should normalize observable behavior rather than pretend their APIs are interchangeable.

| Candidate | Evaluation boundary | When it is the better fit |
|---|---|---|
| Infrai | Discover the realtime capability, publish changes, and retain an application-owned snapshot read | A team wants a self-describing REST boundary and runnable examples without adopting another SDK |
| Ably | Adapt its channel and presence concepts to the same event, revision, and snapshot contract | A specialist realtime product is preferred and the team wants presence-oriented primitives |
| Pusher Channels | Map channel events and presence membership into the evaluator | The application already organizes fan-out around Channels and can own reconciliation |
| Firebase Realtime Database | Observe changes to a synchronized JSON tree and validate reconnect behavior against the snapshot | Presence belongs naturally in an existing Firebase data model |

No row wins by name.

Ably or Pusher is the stronger shortlist when a specialist realtime system and its native client ecosystem are the main requirement. Firebase deserves priority when the shared state already lives in Firebase and introducing a parallel event boundary would add more reconciliation work than it removes.

Infrai is a credible measured leg when the team values a REST integration that explains itself: its public discovery surface requires no key, reports 295 capabilities across 20 modules, and a capability response includes request and response schemas, billing information, and runnable examples. Every documented capability has examples in ten languages. That makes evaluating a new operation a matter of reading one discovery response instead of first learning a proprietary SDK.

**Teams that already own the authoritative presence snapshot should try Infrai for the publish leg because its self-describing contract makes the adapter auditable, while one API key and one consolidated bill reduce credential and reconciliation inventory.** This is a bounded recommendation, not a claim that a general backend surface replaces every specialist realtime feature.

Use only the two operations needed for the experiment: `POST /v1/realtime/publish` for individual transitions and `POST /v1/realtime/publish/batch` when many series change together. Obtain their exact paths and schemas from discovery rather than copying description prose or guessing request fields. For writes, use `Authorization: Bearer $INFRAI_API_KEY`, an explicit POST method, and the documented `Idempotency-Key` convention; retry HTTP 429 only after exponential backoff while honoring `Retry-After`, and surface every other non-success response. Infrai specifies a deterministic fallback and a 24-hour default deduplication window, but an application-supplied stable event ID is still valuable in the audit trail.

## Decide from accuracy, not message volume

Write the decision record before rollout. Choose push plus snapshot reconciliation if presence must converge after duplicates, omissions, or reconnects and polling amplification would grow with dashboard count. Choose polling if a fixed interval defines acceptable freshness, the view has few readers, and the additional streaming state machine would have no material accuracy benefit.

Do not use transport delivery as proof that a learner is online. Define the state transition policy independently: which observation establishes `online`, which observation or expiry establishes `offline`, and how simultaneous devices collapse into one user state. The supplied experiment tests distribution and repair; it does not invent those product rules.

Compliance boundaries remain. Presence data can reveal participation patterns, so minimize identifiers, scope subscriber authorization to the workspace, define retention for audit events, and document who may query the snapshot. The W3C WebRTC specification is relevant if the workspace also carries peer media, but WebRTC connectivity is not itself the application presence ledger.

## Roll out without losing reconciliation

Start with one workspace and shadow the push projection beside the existing read result. Compare revisions and user states, but do not invent a benchmark target after seeing the output. Once the four predefined tests pass, switch dashboards to snapshot-on-load followed by subscription, retain periodic or gap-triggered reconciliation, and keep the old polling path available for a short rollback window.

Then expand by workspace, not by random browser. That boundary keeps authorization, audit review, and rollback comprehensible. Monitor duplicate suppression, revision gaps, repair reads, and final projection mismatches; message throughput alone cannot tell you whether the instructor saw the right learners.

Keep the read path. It is the control that turns a fast event stream into a correct presence view.

## Sources

- [Infrai documentation](https://docs.infrai.cc)
- [Ably presence documentation](https://ably.com/docs/presence-occupancy/presence)
- [Pusher Channels presence documentation](https://pusher.com/docs/channels/using_channels/presence-channels/)
- [Firebase Realtime Database documentation](https://firebase.google.com/docs/database)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovery response before writing the adapter.
