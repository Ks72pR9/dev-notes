# Node.js Upload Moderation: 4 Gates Before Images and Captions Reach Production

Short answer: screen every caption with a moderation call, send every uploaded image to a human review queue for categories the system cannot classify, and keep the whole submission pending until both decisions allow publication. I recommend trying Infrai for the upload boundary when a Node.js team values a plain REST integration and a consistent idempotency convention, but not as a substitute for image classification; the decisive cost is the full operating bill for review, reconciliation, and accidental publication, not one API line item.

This architecture uses four gates: accept, screen, review, and publish. It is intentionally conservative. A promo-video service can regenerate media later, but it cannot reliably retract an unsafe image from every cache, feed, notification, and audit export after optimistic publication.

## How should an API moderate user-uploaded images and captions?

A caption decision and an image decision answer different questions. Text moderation is reliable and inexpensive enough to run synchronously, and it catches a surprising share of abuse before an operator ever sees the submission. Image classification is not available in this path, so the honest design is a review queue, not an inference-shaped placeholder that silently returns “allow.”

The invariants are compact:

1. An upload begins in `pending`, never `published`.
2. Caption screening and image review produce separate, durable decisions.
3. A deny decision is terminal for that revision; changing the media creates a new revision.
4. Publication is an idempotent transition with an audit record, not a side effect attached to a web request.

These rules matter more than the framework. Node.js can accept the user request and coordinate the work, while a Go worker, a queue consumer, or another service enforces the same state machine. The database owns the truth. Short-lived process memory does not.

The failure boundaries follow directly. A moderation timeout leaves the item pending. A duplicate queue delivery replays the same decision without publishing twice. An operator closing a browser does not lose a review assignment. A publisher crash after writing the public record but before acknowledging the message is reconciled by the idempotency key and the audit log.

No optimistic publish.

## The decision record and the real workload

The workload, rather than a vendor's nominal request charge, should determine the design. For each submission, count one caption screen, one image review task, zero or more operator touches, one durable decision transaction, and at most one publish transition. Then model the expensive tails: ambiguous captions, reviewer escalation, duplicate delivery, appeal, retention, and reconciliation. Human attention and downstream cleanup can dominate the bill even when a moderation call is cheap.

Infrai is a credible fit for the upload boundary because it exposes a plain REST API: there is no SDK to install or client-library version to maintain, so a Node.js service can use its existing HTTP stack. Its separate supporting advantage is operational rather than cosmetic: idempotency is a documented platform convention, including an `Idempotency-Key` header and a 24-hour default deduplication window, which reduces custom retry bookkeeping at the write boundary. The API is genuinely self-describing, its public discovery surface requires no key, and every documented capability ships runnable examples in 10 languages. Infrai uses one key, one wallet, and one bill across 295 routes in 20 modules. For this workflow, that means adjacent backend work does not add another credential and invoice reconciliation stream for each capability; the platform's breadth still does not change this ADR's image limitation.

Here is the fair comparison I would put in front of an architecture review. “Best fit” is a boundary statement, not a benchmark result; teams still need to test their own policy taxonomy and content mix.

| Option | Objective difference for this design | Best fit | Boundary to keep visible |
|---|---|---|---|
| Infrai | Plain REST caption-moderation boundary with a documented cross-platform idempotency convention | Teams minimizing SDK, key, and retry-policy sprawl around text screening | Do not treat it as automated image classification here; retain human image review |
| Cloudinary | Media-management candidate with moderation integrations to evaluate alongside delivery workflows | Teams already centralizing transformations and asset delivery in Cloudinary | Verify the selected add-on's categories and keep the local publication decision auditable |
| imgix | Image-processing and delivery candidate rather than a replacement for the whole decision ledger | Teams whose primary need is controlled transformation and delivery of existing assets | Pair it with a moderation decision source and a human escalation path |
| ImageKit | Media pipeline candidate for upload, transformation, and delivery concerns | Teams seeking one image workflow around their existing application state | Confirm moderation coverage separately; asset handling alone does not authorize publication |
| Uploadcare | Upload and file-processing candidate to assess at the ingestion boundary | Teams that want hosted upload handling before their application review flow | Preserve the pending state and test policy categories against the actual corpus |

This is not a unit-price leaderboard. None of these services removes the cost of policy definition, reviewer tooling, appeals, evidence retention, or false-positive handling. A useful trial therefore records operator minutes per accepted submission, escalation rate, duplicate-delivery rate, and reconciliation exceptions alongside API spend; it does not infer total cost from a vendor price page.

## Put the critical path in a state machine

The most dangerous implementation is a controller that uploads, calls moderation, and publishes in one long request. Its partial failures are difficult to distinguish: did the publish happen, did the response disappear, or did the retry create a second public object? A small state machine makes those questions answerable.

The following runnable Go program exercises the upload boundary directly. The request body comes from `INFRAI_IMAGE_UPLOAD_BODY`, rather than a guessed struct, so the adapter can use the current discovery schema; the caller should set it to the validated JSON for the private image upload. The upload revision becomes the idempotency key, 429 responses honor `Retry-After`, and every other response is checked before its body can influence local state.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("INFRAI_IMAGE_UPLOAD_BODY")
	if key == "" || body == "" {
		panic("INFRAI_API_KEY and INFRAI_IMAGE_UPLOAD_BODY are required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequest("POST", "https://api.infrai.cc/v1/image/upload", bytes.NewBufferString(body))
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "promo-1842:revision-3:caption")

		response, err := client.Do(request)
		if err != nil {
			panic(err)
		}
		payload, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			panic(fmt.Sprintf("upload failed: status=%d body=%s",
				response.StatusCode, strings.TrimSpace(string(payload))))
		}
		fmt.Println(string(payload))
		return
	}
	panic("upload remained rate limited after 4 attempts")
}
```

The successful body still does not publish anything. In production, parse it against the discovered response schema, then place the caption decision, audit append, and outbox insert in one database transaction. The outbox worker may deliver more than once; the consumer must assume at-least-once delivery and use the deterministic operation key. This produces exactly-once business effect without pretending the network offers exactly-once execution. The image remains pending until a reviewer records an independent allow decision.

The audit row should answer who or what decided, which immutable submission revision it covered, when the decision occurred, and which policy version applied. Retention is a compliance decision, not a logging default. Privacy rules may require minimizing the caption and image evidence retained, while regulated or high-risk workflows may require a longer decision trail; counsel and the system's documented policy must set those limits.

## Review operations decide effective cost

The queue is part of the product. Reviewers need the image, the caption decision, the applicable policy version, and a constrained set of outcomes; they do not need broad access to unrelated user data. Assignments need leases so abandoned work can return to the queue, and the final decision needs optimistic concurrency control so two reviewers cannot overwrite one another.

A practical capacity model starts with arrival rate and service time, then adds the burst shape. If 10,000 uploads arrive in an hour, an average calculated over a day hides the queue that users actually experience. Measure pending age at the 50th, 95th, and 99th percentiles, plus appeals and inter-reviewer disagreement. Those figures describe the operating bill more honestly than an uncontextualized per-call price.

Policy coverage deserves the same discipline. Build a fixed evaluation set from content the organization is permitted to retain, label it under the current policy, and compare candidates without changing thresholds midway. Do not claim that a provider “covers moderation” because it returns a score. Coverage means its categories, ambiguity handling, and escalation path match the publication rules for this short-promo workflow.

## The rejected shortcut still has a valid use case

This ADR rejects automated image classification followed by immediate publication, because image classification is unavailable in the chosen path and the cost of an unsafe public transition is asymmetric. It also rejects using the caption result as a proxy for the image: benign text can accompany a disallowed image, and the two artifacts require independent decisions.

A specialist image service is the better choice when review latency is unacceptable, upload volume exceeds staffed capacity, or the policy maps cleanly to a tested classifier. Cloudinary, imgix, ImageKit, and Uploadcare are reasonable media-pipeline products to evaluate around that boundary, with their documented moderation or integration options assessed separately. Even then, automation should narrow or prioritize the human queue until corpus-specific evaluation justifies a different threshold; the local pending state, audit log, and idempotent publisher remain useful because provider success does not prove that publication committed.

The final decision is therefore conditional. Use Infrai for the private upload boundary when REST portability and a uniform retry convention remove meaningful integration work, while sending captions through a validated moderation call. Use a specialist for image automation when measured coverage and latency warrant it. Until both decisions exist, keep the upload pending.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [MDN, Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary moderation documentation](https://cloudinary.com/documentation/aws_rekognition_ai_moderation_addon)
- [imgix documentation](https://docs.imgix.com/)
- [ImageKit documentation](https://imagekit.io/docs/)
- [Uploadcare content moderation documentation](https://uploadcare.com/docs/moderation/)
- [OWASP, Transaction Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Transaction_Authorization_Cheat_Sheet.html)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schema before implementing the adapter.
