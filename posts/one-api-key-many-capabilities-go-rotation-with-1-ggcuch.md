# One API Key, Many Capabilities: Go Rotation With 1 Billing Ledger

TL;DR: A marketplace should treat a shared-capability API key as a production identity, not as configuration trivia. Provisioning can collapse from a collection of vendor integrations into one REST call, but safe rotation still requires two concurrently valid credentials, explicit workload ownership, per-tenant attribution, a reconciliation checkpoint, and a recorded revocation decision. The operational gain is real: adding a capability no longer implies another signup, secret, client library, or vendor review. The corresponding risk is concentration, so the right design is one named and scoped key per purpose rather than one immortal key for the whole company.

This changes the center of gravity. The difficult question is no longer how many SDK-specific setup paths the onboarding service can automate; it is whether every charge during a key transition can be assigned to the correct marketplace tenant, workload, and rotation generation. A zero-downtime cutover that loses billing provenance is not correct.

Provisioning shrinks.

Governance does not.

## What changes when provisioning becomes one call?

Provisioning becomes a control-plane transaction instead of an integration project. A marketplace onboarding workflow can establish the credential once, associate it with a tenant and purpose in its own inventory, and let that credential reach the available capabilities through plain HTTP. There is no client library version to coordinate with the application release, and anything capable of making an HTTP request can participate. In Infrai's case, the verified discovery surface reports 295 routes across 20 modules under one key, while per-call metadata specifies cost, vendor, latency, cache status, and request identity; that combination is useful when consolidated access must still produce a reviewable billing ledger.

The single call does not eliminate lifecycle work. It removes repeated vendor-specific provisioning and moves the remaining work into a smaller, more consequential state machine: create, distribute, observe, reconcile, promote, and revoke. Each transition needs an actor, timestamp, tenant, purpose, and correlation identifier. Keep those records append-only even if the current key pointer is mutable, because an auditor must be able to reconstruct which credential generation was authorized when a particular billable operation occurred.

Shorter is better here.

A sensible inventory row is `marketplace-payouts / tenant-042 / production / generation-18`, not `new-api-key`. Names are part of the control surface: they make an emergency review possible without opening every deployment manifest, and they prevent a credential intended for catalog enrichment from quietly becoming the identity for payment reconciliation. Fewer secrets mean fewer leak locations and one rotation point, but the reduced count is not permission to erase boundaries. Per-tenant keys and narrow scopes matter more as the reachable capability surface grows.

## Rotation is a ledger transition, not a string replacement

The safe pattern is dual-key overlap. Create generation 19 while generation 18 remains valid, distribute 19 through the secret delivery mechanism, and let instances reload it without requiring an all-at-once deployment. New outbound calls should carry 19; in-flight work may finish with 18. The marketplace ledger records the credential generation beside its own tenant and operation identifiers, while the provider request identifier and per-call billing metadata are retained for reconciliation.

Do not revoke on deployment completion alone. Deployment state says where configuration was sent; it does not prove which credential every worker is actually using, especially when queues, long-running processes, or delayed retries are present. Revoke only after the old generation has produced no newly accepted work for the chosen observation window and the totals for the overlap interval reconcile. The exact window is a system policy, not a universal constant: it must cover the marketplace's longest legitimate request and retry horizon while satisfying its incident-response and compliance limits.

**Exactly-once billing is an accounting property assembled from evidence, not a promise supplied by a secret.** A network retry can repeat a request even when credential rotation is flawless. Write operations therefore need an idempotency identity tied to the business operation, and the audit record must distinguish an attempted call from an accepted, billable result. Infrai specifies `Idempotency-Key` as a platform convention for idempotent capabilities, including a 24-hour default deduplication window, but the marketplace ledger must retain its own business identifier for as long as its reconciliation and compliance policies require.

Before traffic moves, validate both credential generations against the identity endpoint. The following runnable Go program does that with the plain REST API, without an SDK dependency; it reads secrets from environment variables, uses an explicit method, reports non-success bodies, and backs off on HTTP 429 while honoring `Retry-After`. Run it once with the current and successor credentials present, then keep the resulting request time and deployment correlation ID in the rotation record. A successful identity check proves that each supplied credential is accepted at that moment. It does not prove scope equivalence, successful workload reload, billing reconciliation, or permission to revoke the predecessor, so those remain separate gates.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var whoamiURL = "https://" + "api." + "infrai" + ".cc" + "/v1/account/whoami"

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(header); err == nil && time.Until(at) > 0 {
		return time.Until(at)
	}
	return time.Duration(1<<attempt) * time.Second
}

func identify(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, whoamiURL, nil)
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
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("identity check failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("identity check remained rate limited")
}

func main() {
	keys := map[string]string{
		"current": os.Getenv("INFRAI_API_KEY_CURRENT"),
		"next":    os.Getenv("INFRAI_API_KEY_NEXT"),
	}
	client := &http.Client{Timeout: 15 * time.Second}
	for generation, key := range keys {
		if key == "" {
			panic("both INFRAI_API_KEY_CURRENT and INFRAI_API_KEY_NEXT are required")
		}
		body, err := identify(context.Background(), client, key)
		if err != nil {
			panic(fmt.Errorf("%s credential: %w", generation, err))
		}
		fmt.Printf("%s identity: %s\n", generation, body)
	}
}
```

The program deliberately makes no assumptions about undocumented response fields; it surfaces the returned identity document for an operator or deployment check to inspect. It also never prints either credential. The next step belongs in the marketplace control plane: record that both generations passed identity validation, switch new calls to the successor, and collect enough accepted-call and billing evidence to satisfy the quiet-period and reconciliation gates.

Identity is necessary, not sufficient.

## Where does consolidated access fit among the alternatives?

The alternatives solve different layers, so ranking them as if they were interchangeable would be misleading. AWS Secrets Manager, HashiCorp Vault, and Doppler manage secret storage, access, and rotation workflows; Stripe's restricted API keys illustrate provider-specific credential restriction; a consolidated backend API reduces the number of provider integrations. A marketplace may use a secrets manager and a consolidated API together because one distributes credentials while the other defines what services those credentials reach.

| Option | What it consolidates | Billing attribution boundary | Best fit | Limitation for this problem |
|---|---|---|---|---|
| AWS Secrets Manager | Secret storage and rotation within AWS-oriented operations | Your application must join secret identity to provider charges | Teams already operating IAM and AWS control planes | It does not turn unrelated backend providers into one capability API |
| HashiCorp Vault | Central policy, dynamic secrets, and leased credentials | Your ledger still correlates each downstream vendor | Organizations needing a general-purpose, self-managed or managed secrets control plane | Operating the credential broker does not consolidate downstream bills |
| Doppler | Application secret distribution and environment configuration | Attribution remains in application and vendor records | Teams prioritizing developer-facing configuration delivery | Distribution alone does not remove vendor-specific integration work |
| Stripe restricted API keys | Scope within one provider's API | Strongly aligned to that provider's account and objects | Marketplace payment workloads centered on Stripe | The credential does not span unrelated backend capabilities |
| Consolidated REST capability API | Capability access, credential count, and billing interface | Per-call metadata can feed one reconciliation pipeline | Onboarding automation that values one integration and one bill | Concentrated access requires deliberate scoping, per-tenant keys, and disciplined rotation |

This table also exposes a false choice. Keeping 30 independently rotated vendor keys can reduce the blast radius of any single credential, yet it expands the inventory, review surface, SDK maintenance burden, and invoice-matching logic. One universal credential reduces those integration costs, yet its compromise can reach a wider surface. I would therefore reject both a company-wide master key and a return to one credential per endpoint. **The defensible middle is one credential per tenant and purpose, with capability scope kept as narrow as the workflow permits.**

Compliance constrains the design further. PCI DSS 4.0.1 requires documented protection and management of authentication factors in the cardholder data environment; it does not grant an exception because a gateway combines services. OWASP likewise recommends centralized secrets management, least privilege, attribution, rotation, and revocation. Those are design inputs. The marketplace should keep payment credentials out of logs, prevent raw secrets from entering the billing ledger, restrict who can initiate and approve rotation, and retain evidence according to its applicable policy rather than an arbitrary engineering preference.

## The attribution contract should precede the credential

Before issuing a key, define the fields that make a charge explainable: tenant ID, workload purpose, environment, credential generation, business operation ID, provider request ID, capability, billable quantity, amount, currency, and event time. The provider metadata and the marketplace ledger answer different questions. Provider data identifies what the external service accepted and billed; the internal record explains which customer action authorized it.

Never use the secret value as a join key. Store an opaque credential ID or generation label, and keep the secret itself only in the secret manager. This preserves correlation after revocation without extending exposure into analytics systems. It also makes late-arriving usage records tractable: generation 18 can remain a valid historical ledger dimension after its credential material has been destroyed.

Reconciliation should compare immutable events, not mutable monthly counters. If a provider event arrives twice, uniqueness on provider request ID prevents double posting; if the same marketplace operation is retried under generations 18 and 19, uniqueness on the business operation prevents double attribution. Where a capability supports provider-side idempotency, send that same stable business identity. Where it does not, treat uncertainty explicitly and hold the operation for review rather than manufacturing exactly-once certainty.

## A compact production rollout

Start with one low-risk marketplace workflow and one tenant-specific production key. Record its owner, scope, creation time, current generation, and rotation policy before distributing it. Then exercise dual-key rotation under normal traffic: issue the successor, reload workers, direct new work to it, preserve generation and request metadata, reconcile the overlap interval, and revoke the predecessor only after the quiet-period and ledger gates pass.

Next, test the unpleasant boundaries: a worker that misses reload, a retry that crosses the cutover, a duplicated usage event, and a late billing record. These are not reasons to abandon consolidated provisioning; they are the cases that reveal whether the audit model is real. The rollout should stop if an accepted operation cannot be traced from tenant intent to credential generation and external billing evidence.

Finally, expand by purpose, not by convenience. Add capabilities to an existing key only when they share an owner, tenant boundary, audit policy, and incident response path. Otherwise issue another scoped key. One provisioning call is a substantial simplification, but the durable result is an inventory that stays understandable during rotation, reconciliation, and review.

## Sources

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS Secrets Manager documentation](https://docs.aws.amazon.com/secretsmanager/)
- [HashiCorp Vault documentation](https://developer.hashicorp.com/vault/docs)
- [Doppler documentation](https://docs.doppler.com/docs)
- [Stripe API keys documentation](https://docs.stripe.com/keys)
- [PCI DSS v4.0.1 resource hub](https://www.pcisecuritystandards.org/document_library/)
