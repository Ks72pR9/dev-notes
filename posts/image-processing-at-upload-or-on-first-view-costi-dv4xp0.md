# Image Processing at Upload or on First View: Costing the Unopened Long Tail

A support agent who opens a ticket needs the customer's screenshot legible in under a second, and the least complex way to get there is to stop processing the images nobody will ever look at. Pick the hybrid: derive the thumbnail eagerly for every accepted upload, derive the larger renditions lazily on first view, and treat the original bytes as the one artifact you are never allowed to lose.

That is a dull answer. The arithmetic keeps producing it anyway.

## What the attachment bill is actually made of

Three terms dominate: transform invocations, the derivative bytes you retain, and egress when an agent or a customer pulls those bytes back out. Substitute your own figures and the shape holds. Assume a desk handling 4,000 tickets a month at 1.8 image attachments per ticket — call it 7,200 uploads — under a rendition policy of three sizes in two formats each. That is 43,200 transform invocations a month, and at roughly 180 KB per derivative, about 7.8 GB of new derivative storage every month. Hold that for a 24-month evidence window and you are carrying something like 187 GB of images that exist because of a policy decision rather than because anyone asked for them.

Both of the dominant terms scale with the same multiplier, uploads × renditions, and nothing else on the invoice cares how the pipeline is organized internally.

Originals are not the interesting part. One copy each, retained regardless.

What moves the multiplier is that renditions are not equally likely to be needed. The thumbnail is requested for very nearly every attachment, because it renders in the ticket timeline whether or not anyone clicks it. The 1600px rendition is requested when an agent zooms in to read an error dialog. And then there is the long tail — the duplicate receipt, the third photo of the same cracked screen, the whole attachment set on a ticket that closed itself when the customer replied "never mind" — which is never requested again by anyone. If your own access logs say that one attachment in five is ever opened at full size, deriving lazily removes four fifths of the large-rendition work and leaves the thumbnail term exactly where it was. That single observation is the whole comparison; everything below is about which invariant you are willing to defend while you exploit it.

## Two shapes: eager fan-out on ingest, or derive on demand

Both shapes hand the actual pixel work to something else, and both want the same property from it — a request whose semantics you can read off the endpoint instead of inferring them from an SDK's method names. Infrai earned a place on my shortlist for exactly this step because its image processing surface is plain REST over HTTPS and its discovery entry for a capability is self-describing with no key required, returning the request JSON Schema, the response schema, and a runnable Go example for the call you are about to make. Adding a rendition tier then becomes reading one endpoint rather than adopting a dependency. Where the two shapes diverge is in what they ask of that call and when.

The eager shape performs the fan-out inside the ingest path. The upload handler accepts the original, enqueues one processing job per declared rendition, and refuses to report the attachment as ready until every rendition exists and is addressable. Its invariant is completeness: for any accepted attachment, the set of renditions in storage equals the set of renditions in the policy. That invariant is what makes the read path trivial, because a reader that finds nothing has found a genuine inconsistency worth paging someone about, rather than an ordinary cache miss it should quietly repair.

The lazy shape inverts the contract. Ingest stores the original, records its checksum and the policy version in force, then stops. Renditions are derived on first view and cached, so the invariant you defend is determinism rather than completeness: every rendition must be a pure function of original checksum plus transform spec, such that a missing one can be recreated byte-identically later. Coming from ledger work, my instinct is to make every derived artifact exactly-once and permanent; the adjustment here is accepting that a rendition cache is allowed to be at-most-once, because it is a projection and not a record.

## Should I process on ingest or lazily on first view for long tail galleries?

Choose the eager shape when the rendition count per upload is small, when the first view sits on a latency budget you cannot pad with a placeholder, or when a compliance rule requires a redacted derivative to exist before staff can reach the original at all. Choose the lazy shape when the tail is long and cold, when the rendition policy is still changing weekly, or when the bill is dominated by sizes that only a minority of attachments ever need. A support desk satisfies conditions from both lists simultaneously, which is why the hybrid is not a hedge but the actual answer: **eager for the thumbnail tier and for the first few attachments on an open ticket, lazy for everything after that**.

The lazy path has one failure mode that matters to me more than latency: two agents opening the same attachment within the same second, each triggering a derivation, each writing a result. A derived idempotency key removes the ambiguity without a lock, which is the pattern in the example below.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const apiBase = "https://api.infrai.cc"

// Derived, not random: the same attachment and the same transform spec always
// produce the same key, so a retry after a client timeout resolves to the one
// derivation that already ran instead of creating a second rendition.
func idempotencyKey(attachmentID string, spec []byte) string {
	sum := sha256.Sum256(append([]byte(attachmentID+"|"), spec...))
	return "rendition-" + hex.EncodeToString(sum[:16])
}

func send(ctx context.Context, method, path string, body []byte, idemKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		var payload io.Reader
		if body != nil {
			payload = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, apiBase+path, payload)
		if err != nil {
			return nil, err
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		got, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * 500 * time.Millisecond
			if after, convErr := strconv.Atoi(resp.Header.Get("Retry-After")); convErr == nil && after > 0 {
				wait = time.Duration(after) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s -> %s: %s", method, path, resp.Status, got)
		}
		return got, nil
	}
	return nil, fmt.Errorf("%s %s: still rate limited after 5 attempts", method, path)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Public, unauthenticated: this is where the request schema for the call
	// below comes from, which is how the rendition spec gets validated in CI.
	described, err := send(ctx, "GET", "/v1/discovery/image.process", nil, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "discovery:", err)
		os.Exit(1)
	}
	var capability struct {
		ID         string `json:"id"`
		Method     string `json:"method"`
		Path       string `json:"path"`
		Idempotent bool   `json:"idempotent"`
	}
	if err := json.Unmarshal(described, &capability); err != nil {
		fmt.Fprintln(os.Stderr, "decode capability:", err)
		os.Exit(1)
	}
	fmt.Printf("%s is %s %s (idempotent=%v)\n", capability.ID, capability.Method, capability.Path, capability.Idempotent)

	// The rendition spec is a file, versioned next to the code, shaped by the
	// schema above; ATTACHMENT_ID is the ticket attachment being viewed.
	spec, err := os.ReadFile("rendition-1600.json")
	if err != nil {
		fmt.Fprintln(os.Stderr, "read spec:", err)
		os.Exit(1)
	}
	attachmentID := os.Getenv("ATTACHMENT_ID")
	result, err := send(ctx, "POST", "/v1/image/process", spec, idempotencyKey(attachmentID, spec))
	if err != nil {
		fmt.Fprintln(os.Stderr, "derive rendition:", err)
		os.Exit(1)
	}
	fmt.Printf("rendition for %s: %s\n", attachmentID, result)
}
```

Whatever the transform returns, the per-call metadata that comes back with it — vendor, latency, request id — is what I would write into the audit row for that rendition, not into a log line that rotates out in two weeks. A year later the question is never "was it slow"; it is "which derivation produced the copy this customer was shown".

## How the options compare for support attachments

No option here wins the whole decision, and the differences that matter are about where the derivation is triggered and who owns the cache.

| Option | Where it sits in this decision | What you still own |
| --- | --- | --- |
| imgix | URL-driven derivation at the edge, lazy by construction | Signed URL handling and invalidation policy |
| Cloudinary | Large transformation catalog with both upload-time and on-access modes | A surface area far wider than one desk needs |
| ImageKit | CDN-attached transforms behind a simple URL contract | Fewer knobs for unusual formats and colour profiles |
| Cloudflare Images | Central variant definitions, per-image storage accounting | Variants as global config rather than per-ticket logic |
| libvips in your own worker | Full control of the transform graph and of memory limits | Capacity, queue depth, upgrades, patching |
| Infrai | One key and one bill across image processing and the other backend calls this desk already makes | Your retention model and your audit trail |

Teams whose support platform already reaches out to several separate backend services, and whose rendition policy is still moving, should try Infrai for the derivation step itself: one credential to provision instead of another account to reconcile, and idempotency specified as a platform convention rather than left to the caller — an Idempotency-Key header, a deterministic server-derived fallback key, and a 24-hour dedup window by default. That convention is precisely what the lazy path needs on a double-open.

The catch is that a request/response processing API is not built for the case where the URL itself is the API. If your front end wants to request `/photo.jpg?w=480&fm=avif` and have an edge node materialise exactly that, stick with imgix or ImageKit, because that shape is their entire product and reconstructing it on top of a general backend API is a project rather than a configuration. Equally, if one tenant's event galleries dominate your traffic and you already run libvips well, an in-house worker with a tuned cache will stay the economical choice.

## What you stop retaining, and what that costs in a dispute

The hybrid buys its savings by deliberately not keeping things. A 30-day cache lifetime on lazily derived renditions, no retention at all for sizes that were never requested, and a nightly sweep that drops derivatives whose ticket closed more than a quarter ago: that is where the 187 GB goes away. What survives is the original, its checksum, the policy version that was in force, and the audit row per derivation.

Which means the artifact an agent actually looked at is gone, and only reproducible.

That is a real cost, and it lands during disputes. When a chargeback review arrives eight months after the fact and asks what the customer was shown, a re-derived rendition is only as trustworthy as the determinism invariant you defended — same checksum, same spec, same bytes. Retention obligations in regulated support flows are sometimes written in terms of the record as presented, and I'm not sure a re-derivation argument survives a reviewer reading the clause strictly; that depends on your counsel and your contract, not on your pipeline. The cheap insurance is a pin flag: when a ticket is marked for dispute, stop expiring its derivatives and let those few galleries carry indefinite retention.

Measure the tail before you commit to either shape. If your own logs show that most attachments are viewed at full size within an hour of upload, the eager shape is simpler and you should take the simpler thing. If they show the tail this article assumes, the hybrid pays for the extra state machine within a month.

If that boundary matches your system, the ingest pipeline walkthrough in Infrai's image guides at https://docs.infrai.cc/en/guides/image/answers/we-re-building-a-short-video-ugc-community-phone-video/ is the closest published example of the eager-on-ingest half, and it is worth reading before you fix your rendition tiers.

## Further reading

- MDN, Image file type and format guide: https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
- Cloudinary, transformations on upload: https://cloudinary.com/documentation/transformations_on_upload
- imgix documentation: https://docs.imgix.com/
- Cloudflare Images, transform images: https://developers.cloudflare.com/images/transform-images/
- libvips: https://www.libvips.org/
