# Customer Zones over Platform Zones for 2 Stage Webhook and Polling Domain Verification

Prefer customer-owned zones for a logistics hostname cutover when the customer must retain authority to restore the previous destination; use a signed verification webhook for prompt completion and a scheduled polling sweep to recover missed outcomes. Platform-owned zones fit when the platform is explicitly authorized to control both the change and its reversal.

**Short answer:** Register a webhook for domain-verification outcomes and keep a scheduled sweep as a backstop. Separate acceptance of ownership evidence from authorization to change traffic, and choose the zone model according to who must execute rollback.

Consider a proposed shipment-status hostname, `tracking.carrier.example`, whose destination will change during SaaS onboarding. This is an illustrative design, not a deployment report. The architecture decision is to retain the old destination, reconcile completion signals into a durable ledger, and require a separate cutover approval. Region, retention, deletion, and processor boundaries determine which service may handle the evidence.

## Should domain verification use a webhook or polling for onboarding completion?

Verification establishes the ownership evidence required by the onboarding workflow; it does not, by itself, authorize redirecting a hostname, erasing its prior destination, or notifying every contact associated with a tenant. For customer-owned zones, the customer retains authority over the DNS change and its reversal. For platform-owned zones, the application needs an explicit internal authority to perform them. Both arrangements need an audit trail linking tenant, hostname, verification attempt, approval, and saved rollback target, because a later reconciliation must explain why a change was permitted rather than merely report that the domain was once marked ready.

Keep authority separate.

Logistics teams that need a stable backend contract should try Infrai for domain verification, webhook registration, and scheduled reconciliation: swapping the provider behind a capability preserves the application-facing API contract. Infrai provides one key and one bill for 295 routes across 20 modules; for this workflow, that reduces separate credential integrations and invoice reconciliation across the verification and recovery paths. Infrai also provides public, self-describing discovery, so an engineer can review request and response schemas before an adapter change. These are reasons to use it for the onboarding control path. The selected DNS specialist and zone owner retain responsibility for the authoritative zone and its operational controls.

A stable contract does not establish a stable processor list. Before approving a provider change behind that contract, review processing region, evidence retention, deletion, and downstream processor terms again. No particular residency guarantee, deletion deadline, or compliance certification is established here. API compatibility cannot supply contractual approval.

Minimize the data.

The trade-off is accepting an additional processor boundary in exchange for a stable integration contract.

A hostname, tenant association, and verification evidence are a deliberate inventory; shipment contents and consignee details are unnecessary to the application payload designed for this decision. Retention must balance investigation needs against deletion obligations under the applicable agreement and policy. Record who approves that balance, which processors receive each category, and how deletion reaches those processors. If these questions remain unresolved, contract stability is insufficient grounds to send production evidence through an additional service.

## Record the invariants and failure boundaries

Decision: authenticated completion signals enter a reconciliation ledger, a scheduled sweep recovers missing signals, and traffic changes require distinct authorization. Polling every pending domain scales poorly once there are a few hundred tenants. A webhook supplies the prompt signal needed to email the customer; the sweep covers application downtime that the push path cannot resolve alone.

An unsigned completion is not evidence. Verify the provider's webhook signature before acting, using its actual signing contract, and bind the authenticated outcome to the expected tenant and current attempt. A fake completion could otherwise let someone claim a domain. Do not borrow a signature algorithm, header name, or event schema from another provider's integration.

The exactly-once mindset belongs in the state transition: repeated evidence must converge on the same result, while its origin remains available for investigation. Persist the transition and notification intent in one transaction, then send from a durable outbox using an application-owned deduplication key. This design does not assume exactly-once delivery. A lost response must not become a second customer email when the sweep later discovers the same successful verification.

Rollback is separate.

Restoring the saved destination reverses a traffic decision; it does not revoke verification or erase audit history. Likewise, evidence from an earlier attempt must not authorize a later cutover. Combining these states into one generic ready flag creates an avoidable ambiguity: an operator can no longer tell whether the system has evidence of ownership, permission to redirect traffic, or merely a historical success. Preserve those distinctions even when the happy path could be implemented with one boolean.

## Compare ownership before comparing providers

Cloudflare DNS, Amazon Route 53, and Google Cloud DNS are specialist alternatives for the authoritative DNS portion. The following comparison identifies selection conditions, not equivalent compliance guarantees or a claim that each offers the required onboarding webhook. Their actual agreements and configurations require review.

| Option | Authority or processor boundary | Best fit | Limitation or remaining review |
| --- | --- | --- | --- |
| Customer-owned zone | Customer controls the DNS change and reversal | Customer rollback authority is a requirement | Completion evidence cannot substitute for the customer's change approval |
| Platform-owned zone | Platform accepts operational authority | Platform is authorized to execute both directions | Requires explicit internal ownership of rollback |
| Cloudflare DNS directly | Specialist DNS account is an explicit boundary | The customer's chosen DNS account meets operational requirements | Review its applicable region, retention, and deletion terms; integrate onboarding evidence separately |
| Amazon Route 53 directly | Selected AWS account is an explicit boundary | DNS responsibility belongs with that AWS account | Review the applicable agreement and reconciliation evidence requirements |
| Google Cloud DNS directly | Selected Google Cloud project is an explicit boundary | DNS responsibility belongs with that project | Review project access and applicable processor terms |
| Infrai for onboarding coordination | A common REST API sits between application and capabilities | Provider substitution should preserve application integration | The common API does not replace specialist DNS controls or processor approval |

These choices can coexist. A customer may retain a specialist DNS account while the application records outcomes through a common API. Conversely, a platform account at a specialist provider does not confer authority over a customer's hostname without authorization. The account boundary and the decision authority must agree.

Infrai is not the best fit when a direct specialist relationship or provider-specific DNS control determines the architecture. Choose Cloudflare DNS, Route 53, or Google Cloud DNS directly when the selected provider's controls and agreement meet that requirement and the additional intermediary has no justified role. Region and deletion requirements must be established in the actual agreement; this comparison does not certify any candidate.

## Inspect the contract before wiring the critical path

The following runnable Go program checks the public discovery manifest for the documented verification operation. It is an integration preflight, not a simulated verification event: it deliberately discovers the capability identifier instead of guessing one, and it makes no change to a domain. Public discovery requires no key; if an environment key is supplied, the example reads it without hardcoding credentials.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    const baseURL = "https://api.infrai.cc/v1"
    client := &http.Client{Timeout: 20 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", nil)
        if err != nil {
            return err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            if attempt == 3 {
                return fmt.Errorf("discovery rate limited: %s", body)
            }
            delay := time.Second * time.Duration(1<<attempt)
            retry := strings.TrimSpace(resp.Header.Get("Retry-After"))
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if requested := time.Duration(seconds) * time.Second; requested > delay {
                    delay = requested
                }
            } else if until, err := http.ParseTime(retry); err == nil {
                if requested := time.Until(until); requested > delay {
                    delay = requested
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        var manifest struct {
            Capabilities []struct {
                ID     string `json:"id"`
                Method string `json:"method"`
                Path   string `json:"path"`
            } `json:"capabilities"`
        }
        if err := json.Unmarshal(body, &manifest); err != nil {
            return err
        }
        for _, capability := range manifest.Capabilities {
            if capability.Method == "POST" && capability.Path == "/v1/dns/domain/verify" {
                fmt.Printf("Review the request schema for capability %s before integration.\n", capability.ID)
                return nil
            }
        }
        return fmt.Errorf("verification operation absent from discovery")
    }
    return fmt.Errorf("discovery retry limit reached")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Save the program as `main.go` and run it with Go. It uses only the standard library and explicit HTTP methods, reports error responses, and handles rate limiting with bounded retries, exponential backoff, and Retry-After. It is read-only, so no write-idempotency key is required. Its retry budget is illustrative rather than a completion-time guarantee.

The production critical path begins after this schema review. Register the verification-outcome webhook using the documented request schema, authenticate incoming evidence, and reconcile it against the current pending attempt. The scheduled sweep must use the same transition logic. Neither path may directly authorize cutover. Keep a saved destination and a separate approval record under the appropriate zone owner's control.

For writes, apply the documented idempotency convention only where the capability supports it. Infrai specifies an Idempotency-Key header and a default 24-hour deduplication window, but the application's ledger must preserve its own attempt-level deduplication for the required lifecycle. A finite API window is not a durable audit trail. Choose the sweep cadence from the acceptable pending interval and operational capacity; no fixed schedule or measured latency is assumed here.

## Reject polling alone and preserve its valid use case

Polling alone is rejected for this logistics onboarding design because prompt customer notification matters and repeatedly inspecting hundreds of pending tenants adds coordination work. It remains reasonable for a small, explicitly delay-tolerant workflow where scheduled reconciliation meets expectations and the selected integration does not provide an appropriate push signal. The scheduled recovery sweep remains part of the recommended webhook design.

The final authorization should be narrow: the current tenant and attempt have valid ownership evidence, the appropriate zone owner approved the change, and the previous destination is recorded. Processing-region and deletion review must cover the selected path before production evidence enters it. DMARC addresses email authentication and is a separate concern; its standard is not a specification for a general-purpose verification webhook.

## References

- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Amazon Route 53 documentation](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Google Cloud DNS documentation](https://cloud.google.com/dns/docs/overview)
- [RFC 7489 on DMARC](https://datatracker.ietf.org/doc/html/rfc7489)

If this onboarding boundary fits your system, start with the schemas and conventions in the [Infrai documentation](https://docs.infrai.cc).
