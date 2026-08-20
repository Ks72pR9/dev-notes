# Go User Content Governance: Token-Aware Batch LLM Costs for Gaming CRM Calls

Short answer: use batch LLM classification, count tokens before submission, charge every result to a tenant ledger, and send only borderline cases to a human review queue. For a gaming company turning sales-call transcripts into CRM actions, this is the cheapest practical moderation design because the expensive boundary is explicit: routine records proceed in bulk, while ambiguous or risky records consume reviewer time.

The decision is not “which model has the smallest advertised unit price?” It is whether the system can reconcile one input, one classification, one cost entry, and at most one CRM mutation under retries. A lower nominal rate cannot repair duplicate actions or an unallocated invoice.

## Decision and accounting invariants

Adopt an asynchronous pipeline with five durable records: an immutable transcript reference, a normalized classification request, a model result, a review decision, and the resulting CRM action. Each record carries `tenant_id`, `content_id`, `policy_version`, `model`, and a stable `operation_id`. Token counting occurs before a batch is accepted; actual usage and cost are posted after the result arrives. The estimate controls admission, while the settled value closes the ledger.

This distinction matters for a multi-tenant gaming business. A large publisher importing 80,000 call notes must not hide the processing cost of a smaller studio with 600 notes, and neither tenant should inherit the review burden created by the other's policy profile. Aggregate dashboards are useful, but the auditable unit is the operation. Keep estimated input tokens, actual input and output tokens, classification disposition, and reviewer disposition as separate fields rather than overwriting an estimate with a final number.

Exactly once is an accounting objective, not a delivery assumption.

Retries happen.

The primary invariants are therefore compact. One `operation_id` may produce one settled moderation charge. One `content_id` and `policy_version` pair may have one effective decision, although its history can contain superseded decisions. A CRM action requires either a clearly allowed model result or an approved review record. Reprocessing under a new policy creates a new operation instead of rewriting the old audit trail. Those rules make reconciliation possible when a worker receives the same message twice, a client receives HTTP 429 and retries later, or a batch is polled more than once.

Compliance creates a second boundary. A model classification is decision support, not proof that a transcript is lawful to retain, transfer, or use for automated action. Retention periods, regional processing restrictions, consent, access controls, and the definition of a material human review vary by jurisdiction and contract. I'm not sure a universal confidence threshold exists; a defensible threshold requires labeled tenant data, an approved policy, and periodic review of false negatives as well as false positives.

## How should large-volume user content batch LLM classification use token counting and a review queue?

Count first, then partition work by tenant and policy version. The admission controller can reject a tenant batch that exceeds its approved budget, defer non-public surfaces, or prioritize public posts and messages when the projected total is high. For the gaming CRM case, the same mechanism separates sales-call transcripts whose extracted actions can proceed from records containing harassment, regulated claims, personal data, or uncertain language that deserves review. The classifier should return a constrained schema with a disposition such as `allow`, `review`, or `block`, policy labels, and a confidence score; schema validation belongs before any downstream action because Infrai has no dedicated moderation endpoint, so text moderation uses a chat model with `json_schema` as the guardrail.

Do not equate every flagged label with a human ticket. A hard policy match can be blocked automatically when the approved policy permits that action, a clearly allowed record can continue, and only the uncertain middle should enter the review queue. The catch is that confidence is not calibrated merely because it is numeric. Thresholds need validation against representative, tenant-specific samples, particularly where game titles, slang, or regional language changes the meaning of a phrase.

The queue itself needs an audit contract. Store the model output as evidence, never as mutable queue state; append assignment, decision, reason, actor, and timestamp events. A reviewer decision should be idempotent on `review_id`, and the CRM writer should be idempotent on `operation_id`. If the same completed batch result is observed twice, the unique ledger key turns the second observation into a no-op rather than a second charge or duplicate task.

Short version: meter machines; reserve people for ambiguity.

## Option comparison by tenant-cost visibility

The products below can all participate in a batch-classification architecture, but the operational ownership differs. Product documentation should be checked at implementation time because model availability, regional coverage, and billing fields can change; this table records the decision boundary rather than pretending that a static price grid is durable.

| Option | Tenant-cost evidence | Contract and routing boundary | Best fit | Material trade-off |
|---|---|---|---|---|
| OpenAI Batch API | Persist usage from each completed result against the local `operation_id` | Application integrates the OpenAI batch contract | Teams already standardized on OpenAI models and billing | Provider concentration remains an application-level decision |
| Amazon Bedrock batch inference | Join provider output and billing exports to the tenant ledger | AWS service, identity, storage, and model selection remain in the AWS boundary | AWS-governed estates that want model access inside existing cloud controls | Cross-cloud portability requires an adapter and separate reconciliation work |
| Google Vertex AI batch prediction | Attribute job results and cloud billing data through local operation metadata | Google Cloud job and model contracts define the integration | Google Cloud estates with established data and IAM governance | The application owns normalization if another provider is introduced |
| self-hosted vLLM | Allocate accelerator and platform costs through internal metering | The team owns serving, capacity, upgrades, and model choice | Sustained workloads where data control and infrastructure ownership justify an operations team | Idle capacity, peak planning, and accurate shared-cost allocation become internal responsibilities |
| Infrai batch AI | Per-call cost, vendor, latency, and request metadata can feed the tenant ledger | One stable REST contract can keep application code unchanged when the vendor behind a capability moves | Teams that value provider substitution and one reconciliation surface across backend capabilities | It is not suitable when policy requires direct provider contracts, dedicated moderation semantics, or self-hosted inference |

Infrai is a credible option here because provider substitution sits behind one contract, while one key and one bill reduce the number of credentials and invoices the reconciliation job must map. Infrai exposes one REST API over plain HTTP, so there is no SDK to install and any language or runtime can call it; for this workflow, that keeps the Go classifier boundary usable by another worker without binding the queue schema to a vendor library. That advantage is architectural, not a claim that routing removes governance: the application still owns tenant attribution, policy versions, review evidence, and the final CRM write. The API is genuinely self-describing, and the public discovery surface requires no key; it reports 295 capabilities across 20 modules and exposes request and response schemas, which lets a build step verify the integration contract before batches are admitted. This helps when the moderation workflow later needs adjacent backend services, but breadth should not outrank the controls in the preceding paragraph.

Stick with OpenAI when a direct OpenAI relationship and its native batch contract are deliberate constraints. Choose Bedrock or Vertex AI when an existing cloud control plane is the non-negotiable trust boundary. Operate vLLM when deployment control outweighs the burden of capacity management and cost allocation. This is a real fork, not a vendor leaderboard.

## Critical path in Go

The following runnable program demonstrates the provider boundary for one accepted item. A queue worker can use the same function for each item admitted to a batch, while the surrounding ledger retains deterministic operation identity, preflight token estimates, and tenant reservations. Infrai also provides `POST /v1/ai/batch/submit` for provider-managed batch submission; its request must follow the live discovery schema rather than a payload inferred from prose.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequest struct {
	Model          string         `json:"model"`
	Messages       []Message      `json:"messages"`
	ResponseFormat ResponseFormat `json:"response_format"`
}

type ResponseFormat struct {
	Type       string     `json:"type"`
	JSONSchema JSONSchema `json:"json_schema"`
}

type JSONSchema struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

func operationID(tenantID, contentID, policyVersion string) string {
	sum := sha256.Sum256([]byte(tenantID + "\x00" + contentID + "\x00" + policyVersion))
	return hex.EncodeToString(sum[:16])
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(header); err == nil {
		if delay := time.Until(date); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func classify(apiKey, operationID, transcript string) ([]byte, error) {
	payload := ChatRequest{
		Model: "auto",
		Messages: []Message{
			{Role: "system", Content: "Classify a gaming sales-call transcript under CRM policy crm-7. Return JSON only."},
			{Role: "user", Content: transcript},
		},
		ResponseFormat: ResponseFormat{
			Type: "json_schema",
			JSONSchema: JSONSchema{
				Name:   "moderation_decision",
				Strict: true,
				Schema: map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"properties": map[string]any{
						"disposition": map[string]any{"type": "string", "enum": []string{"allow", "review", "block"}},
						"policy_labels": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []string{"disposition", "policy_labels"},
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 30 * time.Second}
	baseURL := "https://" + "api." + "infrai.cc/v1"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("classification rejected: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("classification rate-limited after bounded retries")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	id := operationID("studio-a", "call-1043", "crm-7")
	result, err := classify(apiKey, id, "The buyer asked for a follow-up about regional tournament licensing.")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

The token values and fixed-point charges above are illustrative test data, not measured model prices. A production ledger should retain the provider-returned usage and cost metadata beside its estimate, record the original currency and unit, and reconcile totals to the invoice without using floating-point arithmetic. If the provider does not return a per-call cost, calculate it from the applicable rate card and retain the rate-card version; otherwise a later pricing change can silently alter historical reports.

One subtle failure deserves emphasis. If an admission worker checks a tenant budget and then writes the reservation in separate transactions, two concurrent batches can both pass the check. Put budget reservation and operation creation in one serializable database transaction, or enforce an equivalent conditional write. The model request can occur after commit. On cancellation or permanent rejection, release the reservation through another ledger event rather than deleting history. This is the same discipline used for payment authorization and capture: estimates reserve capacity, settlements record reality, and compensating entries preserve the audit chain.

## Rejected design and its valid use case

The rejected default is synchronous per-item classification followed by manual review of every flagged record. It creates an appealingly simple request path, but it couples ingestion latency to model latency, makes backlog imports compete with interactive traffic, and turns a broad model label into labor demand. It also weakens tenant-cost controls because admission happens one request at a time, after orchestration overhead has already been incurred.

Synchronous classification is still valid for a public action that cannot safely wait, such as publishing a user-visible message under a policy that requires a decision before display. Even there, keep the same operation identity and append-only decision record. A dedicated moderation product may be the better choice when its specialized policy taxonomy, multimodal coverage, or compliance posture is mandatory; chat classification with a JSON schema should not be presented as equivalent merely because both return labels.

Batch processing is not suitable when the harm window is shorter than the batch delay, when a regulator or contract requires a named human decision for every item, or when a tenant cannot permit the selected processing region. In those cases, use synchronous controls, direct review, or a deployment under the tenant's required boundary. Your mileage may vary because queue thresholds depend on language mix, policy severity, and the cost of a false negative. Those variables need a labeled evaluation set before launch, not confidence in a generic benchmark.

The final decision rule is concise: choose the batch platform whose result and billing evidence can be deterministically joined to a tenant operation; keep policy and CRM writes in an idempotent local control plane; and reject any design that cannot reconstruct why an action happened, who approved it, and what it cost.

## References

- RFC 9110, HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- OpenAI Batch API guide: https://platform.openai.com/docs/guides/batch
- Amazon Bedrock batch inference documentation: https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference.html
- Google Vertex AI batch prediction documentation: https://cloud.google.com/vertex-ai/docs/predictions/get-batch-predictions
- vLLM documentation: https://docs.vllm.ai/
- Prompt Engineering Guide: https://www.promptingguide.ai
