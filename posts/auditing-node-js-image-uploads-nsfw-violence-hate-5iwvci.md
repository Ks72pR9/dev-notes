# Auditing Node.js Image Uploads: NSFW, Violence, Hate Symbols, and JSON Recovery

**Short answer:** Treat image upload moderation as an asynchronous, idempotent policy gate: quarantine the bytes, ask a multimodal classifier for schema-constrained NSFW, violence, and hate-symbol evidence, validate every field, and send any invalid or uncertain result to review rather than publishing it.

The important artifact is not the chat response. It is a durable decision tied to the exact image digest, policy version, classifier contract, and attempt history. A Node.js edge service can accept and hash the upload, but it should not make visibility depend on a free-form model sentence or on the lifetime of one HTTP request. Exactly-once delivery is not a realistic assumption at this boundary; idempotent effects and an append-only audit trail are.

This architecture decision record chooses a quarantined worker pipeline with a narrow JSON fallback. It rejects synchronous publish-on-parse for user-facing enforcement, while preserving that simpler option for low-risk prototypes with no automatic publication or denial.

## How should Node.js image upload moderation handle multimodal JSON fallback?

Separate observation from policy. The multimodal classifier reports category findings; a versioned policy function decides whether the upload is allowed, denied, or held for review. This distinction matters because the same visual material can have different treatment on a private archive, a public profile, a news surface, or an age-restricted product. A single `safe: true` field erases the evidence needed for a later appeal and prevents a policy change from being replayed against stored findings.

At ingress, the Node.js service should authenticate the caller, enforce byte and media-type limits, stream the object into quarantine, and compute a cryptographic digest during that stream. It then creates one moderation job keyed by `(tenant, digest, policy_version)`. A client retry may create another delivery, but it must not create another effective decision. The upload remains non-public until a terminal transition commits.

The classifier contract should require one finding for each policy category: `nsfw`, `violence`, and `hate_symbols`. Each finding needs a bounded score or an explicit categorical judgment, while the overall document needs `needs_review` and a contract version. Do not let the model invent category names. Do not infer a missing category as safe.

The preferred response path is JSON constrained by the runtime's supported schema mechanism. The fallback is smaller than many implementations make it: accept exactly one JSON object, reject unknown fields and trailing values, apply the same semantic validator, and retain the raw response under restricted access. It isn't a prose-repair system. If fenced output, commentary, duplicated objects, missing findings, or out-of-range values cannot be reduced without interpretation, review wins.

There is genuine uncertainty around context-heavy hate-symbol classification. Historical documentation and endorsement can contain the same pixels while requiring different policy outcomes. A score alone cannot resolve that distinction, so the contract should permit review and the policy must preserve it. I'm not sure a universal threshold can be defended across products; a labeled evaluation set, reviewer adjudication, and documented risk tolerance are what would resolve the threshold for a particular deployment.

## Invariants and failure boundaries

Four invariants govern the design. No unreviewed object becomes public. One idempotency key has one effective terminal decision. Every state transition records the prior state, next state, actor, reason, and time. A missing or malformed classification can never become an implicit allow.

No silent allows.

The failure boundary begins after quarantine and ends at publication. Invalid file types stop before classification. Transport timeouts, rate limiting, invalid JSON, schema mismatch, and exhausted retries remain inside the worker boundary and converge on `review`; they do not leak into a public visibility state. Retries must reuse the same job identity and record an attempt number, because retrying an unkeyed effect can create conflicting evidence even when each individual call succeeds.

Use a compare-and-set transition or an equivalent database constraint when committing the terminal state. Consider the awkward sequence, because it is where a superficially correct worker usually becomes unauditable: worker A acquires attempt 1 and sends the quarantined object for classification; its lease expires after the classifier has accepted the request but before the result returns; worker B then acquires attempt 2 under the same moderation key and obtains its own valid evidence; B commits `review`, perhaps because the category score falls inside a policy uncertainty band; A wakes later with evidence that would produce `allow`. Both classifier calls completed correctly. The database must still permit only B's guarded transition from `classifying` to the effective terminal state, while A records a superseded attempt and leaves visibility unchanged. Reversing that order must yield the same single-decision property. A unique row alone is insufficient if an unconditional update can replace its contents, so the commit contract needs the expected prior state, policy version, and decision key in its predicate. Publication then consumes the committed decision through another idempotent effect and records the decision version it applied. If that consumer crashes after changing visibility but before acknowledging the queue message, replay observes the recorded version and does nothing. This is the exactly-once mindset applied honestly: at-least-once work at multiple boundaries, idempotent state changes at each boundary, and reconciliation for anything left pending. The sequence is longer than a synchronous handler, but every duplicate has a name, every discarded result has a reason, and no race can quietly turn ambiguity into public content.

The audit record should include the content digest, media metadata, object reference, policy version, classifier contract version, prompt-template digest, adapter configuration identifier, normalized findings, raw-response reference, attempt history, and final reason code. Do not put the image bytes or a broadly usable signed URL into ordinary logs. Access to raw evidence should be narrower than access to the final decision because uploads can contain sexual, violent, identifying, or otherwise sensitive material.

Retention is a compliance decision, not a logging default. Privacy, safety, and legal owners need to approve how long original images, raw model output, reviewer notes, and appeal records remain available, and the answer may differ by jurisdiction and user age. Your mileage may vary. The system therefore needs independently configurable retention for evidence and operational telemetry, plus deletion events that are themselves auditable without retaining the deleted content.

Observe the state machine rather than counting successful classifier calls. Useful signals include oldest quarantined job age, transition latency, schema-invalid rate, review rate by category, duplicate suppression, retry exhaustion, reviewer overrides, and appeal reversals. A trace helps diagnose one execution, but sampling and expiry make it a poor decision ledger. Reconciliation should periodically find jobs whose lease expired or whose state has remained non-terminal beyond the operational deadline, then requeue them under the original idempotency key.

## Contract and deployment choices

The options differ less in model capability than in where ambiguity is allowed to survive.

| Option | Enforcement boundary | Failure behavior | Suitable use | Principal limitation |
|---|---|---|---|---|
| Schema-constrained evidence plus policy | Worker validates before commit | Invalid evidence goes to review | Public or regulated upload gates | Schema support and subsets can vary by runtime |
| Strict single-object JSON fallback | Same validator as the primary path | Rejects commentary, extra objects, and unknown fields | Controlled compatibility path | Cannot safely repair semantic omissions |
| Free-form multimodal chat | Human interprets prose | No automated terminal decision | Prompt exploration and analyst review | Unsuitable for automatic allow or deny |
| Human-only review | Reviewer queue | Escalation remains manual | Rare, severe, or highly contextual material | Latency, consistency, reviewer safety, and staffing |

The first two rows belong to one production design, not two competing policies. The schema-constrained response is primary; the strict object decoder is a compatibility fallback; both feed the same semantic checks and policy function. If their normalized meanings could differ, the fallback is too permissive.

Deployment should start in shadow mode against a licensed and governed evaluation set, then compare category-specific false positives and false negatives across the slices that matter to the product. Aggregate accuracy hides asymmetric harm. A false allow can expose users to prohibited material, while a false deny can suppress lawful speech; hate-symbol context and borderline sexual content often need separate adjudication guidance. Thresholds, prompts, and policy versions should be immutable once used for a decision, with new versions introduced as new records so historical outcomes remain reproducible.

Cost belongs in capacity planning, but it is not the architectural argument. Estimate classifier calls after digest-based duplicate suppression, quarantine storage, review volume, evidence retention, and reconciliation traffic. A cheaper call that produces more ambiguous cases can increase reviewer load; an expensive call is not automatically more accurate. Measure the complete path.

## Critical path in Go

The Node.js ingress only needs to produce the immutable job contract described above. The following Go worker core shows the state transition and strict fallback boundary without coupling policy code to a commercial endpoint. `Classifier` is responsible for requesting schema-constrained multimodal evidence; `Store.Commit` must atomically enforce the expected prior state and unique idempotency key.

```go
package moderation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type Job struct {
	TenantID      string
	Digest        string
	ObjectRef     string
	PolicyVersion string
}

type Finding struct {
	Category string  `json:"category"`
	Score    float64 `json:"score"`
}

type Evidence struct {
	ContractVersion string    `json:"contract_version"`
	Findings        []Finding `json:"findings"`
	NeedsReview     bool      `json:"needs_review"`
}

type Classifier interface {
	ClassifyWithSchema(ctx context.Context, objectRef string) ([]byte, error)
}

type Store interface {
	Existing(ctx context.Context, key string) (decision string, found bool, err error)
	Commit(ctx context.Context, key, expectedState, decision string, evidence []byte) error
}

func Moderate(ctx context.Context, job Job, classifier Classifier, store Store) (string, error) {
	key := job.TenantID + ":" + job.Digest + ":" + job.PolicyVersion
	if decision, found, err := store.Existing(ctx, key); err != nil {
		return "", err
	} else if found {
		return decision, nil
	}

	raw, err := classifier.ClassifyWithSchema(ctx, job.ObjectRef)
	if err != nil {
		return commitReview(ctx, store, key, "classifier_unavailable")
	}

	evidence, err := decodeEvidence(raw)
	if err != nil {
		return commitReview(ctx, store, key, "invalid_evidence")
	}

	decision := applyPolicy(evidence)
	if err := store.Commit(ctx, key, "classifying", decision, raw); err != nil {
		return "", err
	}
	return decision, nil
}

func commitReview(ctx context.Context, store Store, key, reason string) (string, error) {
	evidence, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		return "", err
	}
	if err := store.Commit(ctx, key, "classifying", "review", evidence); err != nil {
		return "", err
	}
	return "review", nil
}

func decodeEvidence(raw []byte) (Evidence, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	var evidence Evidence
	if err := decoder.Decode(&evidence); err != nil {
		return Evidence{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Evidence{}, errors.New("expected exactly one JSON object")
	}
	if evidence.ContractVersion == "" {
		return Evidence{}, errors.New("missing contract version")
	}

	required := map[string]bool{
		"nsfw":         false,
		"violence":     false,
		"hate_symbols": false,
	}
	for _, finding := range evidence.Findings {
		seen, allowed := required[finding.Category]
		if !allowed || seen || finding.Score < 0 || finding.Score > 1 {
			return Evidence{}, fmt.Errorf("invalid finding for %q", finding.Category)
		}
		required[finding.Category] = true
	}
	for category, seen := range required {
		if !seen {
			return Evidence{}, fmt.Errorf("missing finding for %q", category)
		}
	}
	return evidence, nil
}

func applyPolicy(evidence Evidence) string {
	if evidence.NeedsReview {
		return "review"
	}
	// Category thresholds belong to a tested, versioned policy package.
	return "allow"
}
```

The sketch deliberately omits thresholds and transport details. Universal cutoffs would be invented policy, while a concrete commercial route would turn an architecture record into a vendor tutorial. Test the decoder with unknown keys, duplicate categories, missing findings, trailing JSON, boundary scores, and arbitrary prose. Test the state machine with duplicate deliveries, lease expiry, concurrent commits, policy-version changes, and replayed jobs. Then test publication separately: only a committed `allow` may change object visibility.

The example returns `review` when classification or validation fails, but production code should distinguish retryable attempts from terminal review. A bounded retry policy may reschedule rate-limited or timed-out calls while retaining quarantine; once its deadline or attempt budget is exhausted, one guarded transition records review. The audit log should make that sequence legible without requiring access to raw user content.

## Rejected shortcut and its valid use case

Calling multimodal chat inside the upload request and publishing whenever a response contains parseable JSON is rejected for an enforcement gate. It couples user latency to classifier latency, loses durable retry state, encourages permissive parsing, and leaves reconciliation with no stable job identity. Even worse, a process can return success before the visibility change commits, or lose the response after commit and invite the client to repeat an unkeyed operation.

The catch is operational weight. Quarantine storage, a durable queue, evidence controls, review tooling, and reconciliation all add latency and ownership cost. This design is not suitable for a private prototype that stores no user content, makes no automatic enforcement decision, and can tolerate manual inspection. In that narrow case, a synchronous free-form call can be a useful prompt experiment. Keep it out of the publication path.

Text tokenization and vector similarity can support adjacent workflows such as prompt budgeting or retrieval over reviewed policy material, but neither substitutes for image evidence validation or a policy state machine. Don't expand the critical path merely because those tools are available. The moderation gate needs fewer moving parts, stronger contracts, and an audit record that can explain every consequential transition.

## References

- https://github.com/openai/tiktoken
- https://github.com/pgvector/pgvector
