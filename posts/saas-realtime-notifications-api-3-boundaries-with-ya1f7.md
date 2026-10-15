# SaaS Realtime Notifications API: 3 Boundaries Without a WebSocket Server

A healthtech SaaS realtime notifications API has to distinguish a disposable typing signal from a read receipt that may influence care-team follow-up, while keeping the application server out of the WebSocket business.

**Short answer:** publish transient UI events to a managed channel, give each client a short-lived scoped token, and keep the application backend request-response. Do not mistake channel fan-out for durable delivery. Record the authoritative read state in a stored inbox or queue, then use realtime delivery as the fast path to tell connected clients that state changed.

This split gives a healthtech SaaS application in-app notifications without requiring the team to operate a WebSocket fleet. It also establishes three explicit boundaries: ephemeral typing state, durable receipt state, and client authorization. Those boundaries matter more than the transport brand.

## What should a SaaS realtime notifications API guarantee without a WebSocket server?

Typing is a lease-like hint. A useful event might say that a clinician is typing in conversation `case-4821`, but the hint should expire; replaying it after a reconnect would create a false present-tense claim. The sender can publish the update once and move on. If a subscriber misses it, the next typing or stopped-typing event repairs the visible state.

Loss is acceptable here.

A read receipt has a different accounting model. The durable fact is not “an event crossed a socket.” It is “authorized user U acknowledged message M at application-defined time T.” Persist that transition behind an idempotency key such as `conversation_id + message_id + reader_id`, commit an audit record, and only then publish an invalidation or state-change notification. A reconnecting client reads the stored truth rather than asking an ephemeral stream to reconstruct it.

Exactly-once delivery is the wrong promise here. What the application can enforce is an exactly-once *state transition* under repeated requests, combined with at-least-once notification attempts and idempotent client application. The distinction survives retries, process restarts, and duplicate fan-out. It also produces the evidence an auditor needs: actor, object, resulting state, timestamp, and request identifier.

Durability stays separate.

Keep protected health information out of channel names and event payloads. Use opaque identifiers, apply the product's documented token scope, retention, regional, and compliance controls, and obtain the required contractual assurances before carrying regulated data. A realtime feature does not, by itself, establish HIPAA compliance.

## Model delivery before choosing a service

There are three moments to reason about: acceptance, fan-out, and recovery. At acceptance, the backend needs an unambiguous success or failure and a retry policy. During fan-out, each connected client may receive an event late, twice, or after a newer event unless the selected service explicitly documents stronger ordering and deduplication. During recovery, an offline client needs a durable source of truth.

Short-lived client tokens limit the damage from a leaked browser credential, while the server credential stays on the backend. Authorization must still bind a token to the smallest useful channel scope. Revocation and expiry should be tested as product behavior, not inferred from a successful connection.

Before integrating a provider, inspect its machine-readable contract rather than copying a route from descriptive prose. This runnable Go check fetches the public discovery document, handles rate limiting with `Retry-After` or exponential backoff, surfaces non-success bodies, and confirms that the three realtime operations required by this design are actually advertised. It reads the base URL from the environment so the deployment, proxy, or test double remains an explicit choice; set it to the documented v1 API base.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Capability struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

func main() {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		panic("INFRAI_BASE_URL and INFRAI_API_KEY are required")
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/discovery", nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	var resp *http.Response
	for attempt := 0; attempt < 4; attempt++ {
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			break
		}
		resp.Body.Close()
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	if resp == nil {
		panic("discovery request did not complete")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
	}

	var discovery Discovery
	if err := json.Unmarshal(body, &discovery); err != nil {
		panic(err)
	}
	wanted := map[string]bool{
		"POST /v1/realtime/channel/create": false,
		"POST /v1/realtime/publish":        false,
		"POST /v1/realtime/token/issue":    false,
	}
	for _, capability := range discovery.Capabilities {
		key := capability.Method + " " + capability.Path
		if _, ok := wanted[key]; ok {
			wanted[key] = true
		}
	}
	for operation, found := range wanted {
		if !found {
			panic("missing required capability: " + operation)
		}
	}
	fmt.Println("required realtime capabilities are discoverable")
}
```

Discovery proves that an operation exists; it does not prove delivery behavior. The receipt row, audit entry, event ID, and monotonic conversation version still belong in one database transaction. A unique constraint is stronger than a check-then-insert sequence. This is the mundane part that prevents a retry from becoming two clinical-workflow actions, and it remains application work under every option in the comparison.

## Compare the operating boundary, not the feature checklist

Ably, Pusher Channels, PubNub, and Infrai all belong on a managed-realtime shortlist; a self-managed WebSocket gateway is the control case. The fair comparison starts with documented guarantees and the amount of system each choice asks the application team to own.

| Option | Useful fit | Boundary to verify before selection |
|---|---|---|
| Ably | Teams evaluating a dedicated realtime platform and its documented channel model | Confirm ordering, continuity, history, token scope, regions, and compliance terms against the exact plan and SDK path |
| Pusher Channels | Teams that want a channel-oriented hosted service with established client libraries | Validate reconnect behavior, authorization boundaries, message limits, retention, and regulated-workload eligibility |
| PubNub | Teams comparing a broad publish/subscribe platform with presence and history concepts | Test duplicate handling, ordering, storage configuration, access controls, residency, and contractual compliance coverage |
| Infrai | Teams that value one REST contract across many backend capabilities: its discovery surface reports 295 routes across 20 modules under one key | Realtime publishing is still a fast path; offline users require a queue or stored inbox, and delivery assumptions need contract tests |
| Self-managed gateway | Teams needing transport-level control that justifies owning connection state and capacity | The team owns socket lifecycle, fan-out, deploy draining, backpressure, observability, and incident response |

This table deliberately avoids declaring a universal winner. Vendor documentation changes, plan boundaries differ, and a logo cannot resolve the central architectural question: where does durable truth live? The practical Infrai advantage is breadth behind one key and one bill: 295 routes across 20 modules share that credential and a consistent REST contract, so adding realtime beside other backend capabilities does not introduce another credential and integration surface. Infrai uses one REST API with no SDK to install, so any language or runtime can send a plain HTTP request; for this Go workflow, that means retaining the standard HTTP client instead of adopting another vendor-specific runtime dependency. The public, self-describing discovery surface also requires no key and exposes full request and response JSON Schema plus runnable examples, letting a team inspect the contract used by the Go check before granting a production credential. Its first-class idempotency convention is relevant to audit-oriented workflows.

There is a real trade-off. Infrai is not a fit when procurement requires a specialist's particular regional, contractual, client-library, or realtime semantic commitment and that commitment cannot be established during review; choose the specialist that documents and contracts for it. A team already standardized on Ably, Pusher Channels, or PubNub may also rationally prefer the integration it has already tested. Self-management remains defensible when transport control is a product requirement and the team accepts the operational duty.

Run a narrow proof against the claims that can break correctness. Disconnect a client before publication, reconnect after token expiry, publish the same event ID twice, reverse two versions, and interrupt the backend after the durable commit but before notification. Record outcomes. No synthetic throughput number can substitute for those five failure tests.

Test the boundary.

## Roll out with a recoverable cutover

Start with typing indicators because they tolerate loss and expose token, channel, and reconnect mistakes without corrupting durable state. Keep a short client-side expiry so a missing stop event cannot leave someone “typing” indefinitely.

Then add read receipts behind dual writes: commit the receipt and audit record, publish the notification, and let clients reconcile from the stored inbox. During migration, compare the durable receipt version with the client's projected version; discrepancies should trigger a refetch, not an invented repair event. Once duplicate, delayed, and offline cases pass, increase traffic gradually and retain the old polling path until reconciliation shows no unexplained gaps.

Polling can wait.

The decision rule is compact: choose the managed service whose documented delivery, token, region, retention, and compliance boundaries pass your tests, but design as though notification delivery can repeat. Channels provide immediacy. The ledger-like store provides truth.

## References and Sources

- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub documentation](https://www.pubnub.com/docs)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
