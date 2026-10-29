# Image Generation API Comparison for SaaS MVP Catalogs — Go Portability

Short answer: for a SaaS MVP that turns messy product descriptions into catalog images, keep image generation behind a small Go port, store the normalized request and output manifest, and make provider selection a data decision rather than a business rule. The important design choice is not which model looks best in a demo; it is whether the same request can be retried, reconciled, audited, and replayed after a provider changes.

A product description such as `linen shirt, blue, relaxed fit, studio background` is not a reliable image specification. It can contain missing attributes, contradictory adjectives, supplier markup, or tenant-specific policy. The catalog pipeline therefore needs a boundary between description cleanup, image generation, and publication. That boundary is where portability becomes useful.

## What must remain true when the provider changes?

I would record four invariants before comparing APIs. First, a catalog item has one stable generation intent, identified by tenant, item revision, prompt policy version, and an idempotency key. Second, a provider response is an artifact, not the source of truth: the system stores its URI, content hash, provider name, model identifier, and safety status in an append-only audit record. Third, a retry cannot publish a second image for the same intent merely because the first request timed out. Fourth, an image is not visible in the storefront until a separate publication step has accepted the artifact.

Those rules draw the failure boundary clearly. A provider timeout is an unknown outcome, not proof that generation did not happen. A malformed description is a validation failure, not a reason to retry. A successful generation with an unusable aspect ratio is an artifact rejection, not a payment-like reversal. The distinction matters because a queue worker can safely retry transport errors only when the intent and the resulting artifact are reconciled afterward.

The exactly-once mindset is useful here even though an external image API rarely gives an exactly-once guarantee. We can make our state transition exactly once. We cannot make an arbitrary remote side effect exactly once without a shared transaction or a provider-side idempotency contract. That limit belongs in the architecture record, not in a hopeful comment beside a retry loop.

The ledger is the product boundary.

That is the portability test.

## Which portability boundary fits a catalog pipeline?

There are three practical boundaries. A direct SDK call in the catalog service is the fastest prototype, but it couples prompt shaping, authentication, response parsing, and retry policy to one provider. A common internal service can centralize those concerns, although it adds an operational hop. A Go package with a narrow interface is usually the best MVP boundary when the catalog service already owns the job lifecycle: the package isolates provider adapters while the application owns durable state and publication.

| Boundary | Useful property | Failure it leaves exposed | Appropriate use |
| --- | --- | --- | --- |
| Direct provider call | Minimal code path | SDK types leak into catalog state | A disposable spike |
| Internal generation service | One policy and queue boundary | More deployment and tracing work | Several product teams share generation |
| Go port with adapters | Small replacement surface | The application still owns reconciliation | One service needs provider choice |

The port should describe business intent, not vendor vocabulary. `Generate` can accept a normalized prompt, dimensions, a tenant policy, and an idempotency key. It should return a provider-neutral artifact reference plus enough metadata to audit the decision. Do not expose a provider-specific field such as `guidance_scale` in the core interface unless the business actually needs to preserve that control across providers. Otherwise a migration becomes a schema migration disguised as configuration.

Here is the critical path, deliberately leaving persistence outside the adapter. The adapter may make one remote call, but the worker must save intent before it makes that call and must save the result before publication.

```go
type ImageRequest struct {
	TenantID       string
	ItemRevision   string
	Prompt         string
	Width          int
	Height         int
	PolicyVersion  string
	IdempotencyKey string
}

type Artifact struct {
	Provider  string
	Model     string
	URI       string
	SHA256    string
	MediaType string
}

// Provider is intentionally smaller than any provider SDK.
type Provider interface {
	Generate(context.Context, ImageRequest) (Artifact, error)
}

func (w *Worker) Run(ctx context.Context, req ImageRequest) error {
	if err := w.ledger.RecordIntent(ctx, req); err != nil {
		return err
	}

	if artifact, ok := w.ledger.ArtifactFor(ctx, req.IdempotencyKey); ok {
		return w.ledger.PublishIfApproved(ctx, req, artifact)
	}

	artifact, err := w.provider.Generate(ctx, req)
	if err != nil {
		return w.ledger.RecordUnknownOrFailure(ctx, req, err)
	}
	if err := w.ledger.RecordArtifact(ctx, req, artifact); err != nil {
		return err
	}
	return w.ledger.PublishIfApproved(ctx, req, artifact)
}
```

The `RecordUnknownOrFailure` name is intentional. A deadline exceeded after the remote server accepted the request cannot be classified from the client alone. The ledger should preserve that uncertainty and a reconciler should inspect provider-side records, callback data, or an artifact locator when the provider offers one. A generic retry with a new random key is the dangerous path: it converts an ambiguous result into duplicate work.

## How should a SaaS MVP compare image generation APIs?

The prompt is not the product. A preprocessing step should extract a constrained product record: category, material, color, silhouette, allowed scene, exclusions, and requested dimensions. Keep the original description and the normalized record. If a buyer disputes an image, the team needs to know whether the problem came from extraction, prompt policy, provider output, or publication.

For a SaaS MVP, compare providers against the same normalized fixtures and the same acceptance rules. Measure the properties that affect this workflow: output dimensions, supported input and output formats, asynchronous behavior, moderation response shape, request limits, regional availability, data handling terms, and the availability of a stable model identifier. Visual preference can be evaluated by people, but transport and state behavior should be tested automatically.

OpenAI's DALL-E is commonly exposed as a hosted image-generation API, which reduces infrastructure owned by the application but makes the application dependent on that API's request and response contract. Stability AI's Stable Diffusion is a model family with deployable and hosted variants, so the portability question includes GPU operations, model licensing, and image-serving controls when the team runs it itself. Black Forest Labs' FLUX is another model family available through hosted and self-managed routes; the relevant boundary is again the selected distribution, license, runtime, and API contract rather than the model name alone. These are different operational shapes, not a simple quality ranking.

A useful decision record can therefore say: use a hosted adapter while request volume and latency requirements are uncertain; consider self-hosting only when control over weights, data path, or predictable capacity justifies GPU operations; preserve the same ledger and publication contract in both cases. The recommendation is about the boundary, not a brand.

The trade-off is explicit: portability adds an adapter and reconciliation work, while a direct integration buys speed at the cost of a larger migration surface. There is no universal winner.

One trap deserves special attention. A provider may return a URL that expires or is not suitable for long-term catalog delivery. Fetch the artifact into controlled object storage, verify its media type and dimensions, calculate a content hash, and keep the provider locator only as audit metadata. Publication should reference the controlled object, never an opaque temporary response URL.

## What does the rejected option teach us?

The rejected option is a single provider-specific job table whose columns mirror one SDK: prompt text, response URL, and status. It looks tidy until the first migration. The table cannot distinguish an unchanged retry from a new catalog revision, and its URL column may silently become invalid. More importantly, a provider switch now changes the meaning of historical rows. That is an audit defect. The adapter boundary is a poor fit for a one-off design tool with no durable catalog, where its extra state and reconciliation work would be needless overhead; a direct call is reasonable there.

This design still has a valid use case: a throwaway internal experiment where no customer-facing artifact is published and the data can be deleted. It is not an adequate boundary for a multi-tenant catalog. Once an image influences a listing, the system needs provenance and a durable state machine.

I would test the state machine with a small matrix: duplicate delivery of the same queue message, worker death after the remote call, provider timeout, malformed image bytes, policy rejection, and a later catalog revision. Each case should assert ledger rows and publication visibility, not only the returned Go error. A test that checks `err == nil` misses the expensive part of the workflow.

Observability should follow the same logic. Emit a trace and counters for intent creation, remote submission, reconciliation, artifact validation, and publication. Hash prompts only when they contain sensitive tenant data, and keep access to the original prompt governed by the product's retention policy. Logs should include the idempotency key and item revision, never a raw secret or an unbounded prompt blob.

The MVP can stay small: one queue, one durable intent table, one artifact table, one adapter per supported route, and a reconciler. The complexity belongs in explicit state transitions. It does not belong in a growing collection of provider-specific flags.

## References

- OpenAI, “Images and vision,” https://platform.openai.com/docs/guides/images
- Stability AI, “API documentation,” https://platform.stability.ai/docs/getting-started
- Black Forest Labs, “API documentation,” https://docs.bfl.ml/
- IETF, RFC 9110: HTTP Semantics, https://www.rfc-editor.org/rfc/rfc9110
- IETF, RFC 8785: JSON Canonicalization Scheme, https://www.rfc-editor.org/rfc/rfc8785
- OpenAI, “Batch API guide,” https://platform.openai.com/docs/guides/batch
- Cohere, “Rerank overview,” https://docs.cohere.com/docs/rerank-overview
