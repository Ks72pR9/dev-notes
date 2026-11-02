# Edtech SaaS Welcome Email Deliverability Checklist Through Custom-Domain Audit Records

**Short answer:** use one durable intake record plus a worker, and make the custom sending domain, DKIM identity, suppression decision, queue, and delivery attempt auditable before sending a welcome email.

The most useful deliverability checklist for an edtech SaaS contact form starts with an audit trail, not a mail provider. Record the submitted request, the queue decision, the sending identity, the suppression decision, and each delivery attempt as separate facts. That design adds a modest integration boundary, but it makes a missed support notification explainable without pretending that a network can deliver exactly once.

The practical question is narrower than “which service sends email?” An edtech team needs to know whether an existing institution’s billing question reached the account queue, whether a learner-access question reached general support, and why a suppressed destination was not attempted. Integration effort matters, but it should be measured against the amount of evidence the workflow must preserve.

## How should an edtech SaaS welcome email checklist verify deliverability?

The intake record is the durable business fact. It should have a stable identifier, the submitted address, an institution or account identifier when available, the message category, the creation time, and the version of the routing rules. A notification attempt is a different fact: it has its own identifier, purpose, selected queue, sending identity, status, and external correlation value when one exists.

This separation is an exactly-once mindset applied to the business effect, not a claim about email transport. A worker may be restarted after a request has been accepted, and a provider may accept a request before the caller observes the response. A uniqueness constraint on the business notification key can prevent a second local claim, while an append-only attempt record preserves the ambiguity of a timeout instead of converting it into a false permanent failure.

Keep message bodies out of ordinary operational logs. Support may need the original content, but a routing metric does not need it, and a metric label should not contain a full address. The useful evidence is compact: `submission_id`, `notification_purpose`, `ruleset_version`, `queue`, `eligibility`, `attempt_id`, and outcome timestamps.

## Where should queue selection and suppression meet?

Queue selection is a policy decision; suppression is a delivery eligibility decision. Apply both before creating a send request, and record both even when the result is “do not send.” For example, a billing question from institution `northstar-17` can resolve to `account-support`, while a learner-access question without an institution identifier can resolve to `general-support`. A destination with a recorded hard bounce, complaint, or explicit opt-out remains part of the intake history but is ineligible for that notification.

The word “transactional” does not remove compliance boundaries. An operational label is not consent, and an application should retain the relevant notice or consent state instead of inferring it from the template name. For SMS-related paths, the CTIA interoperability and compliance guidance is a useful reminder that messaging obligations belong in the workflow, not only in a vendor console.

The sending domain is another recorded boundary. SPF authorizes designated senders through DNS policy; RFC 7208 does not make SPF a guarantee that a recipient accepted the message. It also defines a limit of 10 DNS-querying terms during SPF evaluation, so a policy assembled from many includes has a concrete operational constraint. DKIM supplies a signed message identity, and RFC 6376 describes the signing and verification model. Store which configured sending identity was selected and which configuration revision produced it. DNS ownership, key rotation, and policy review should be deployment concerns, so changing a key does not require changing routing code.

The custom domain is evidence of identity, not evidence of delivery.

The limitation is deliberate: an audit record explains intent and outcome, but cannot prove mailbox placement.

## A small contract that preserves the evidence

The HTTP handler should validate the input, persist the intake record, and enqueue work. It should not make queue ownership depend on whether a downstream send call happens to return quickly. The following focused contract shows the boundary without tying it to a commercial endpoint.

```go
package intake

import (
	"context"
	"fmt"
)

type Request struct {
	ID           string
	Address      string
	Institution  string
	Category     string
	Ruleset      string
}

type Store interface {
	SaveRequest(context.Context, Request) error
	ClaimNotice(context.Context, string) (bool, error)
	RecordSuppressed(context.Context, string, string) error
}

type Dispatcher interface {
	Dispatch(context.Context, string, Request) error
}

func Process(ctx context.Context, store Store, dispatcher Dispatcher, request Request, suppressed bool) error {
	if request.ID == "" || request.Address == "" || request.Ruleset == "" {
		return fmt.Errorf("incomplete intake request")
	}
	if err := store.SaveRequest(ctx, request); err != nil {
		return err
	}
	if suppressed {
		return store.RecordSuppressed(ctx, request.ID, "destination policy")
	}

	queue := "general-support"
	if request.Category == "billing" && request.Institution != "" {
		queue = "account-support"
	}
	claimed, err := store.ClaimNotice(ctx, request.ID+":support-notice")
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	return dispatcher.Dispatch(ctx, queue, request)
}
```

The code intentionally does not call the dispatcher before the durable request exists. It also does not delete a suppressed request or collapse a retry into the original record. Those choices make reconciliation possible: local attempts can be compared with reported outcomes, and a support agent can see that a notice was withheld by policy rather than lost during routing.

## Which boundary is worth the integration effort?

There are three reasonable boundaries, and the right one depends on the evidence the team must retain.

| Boundary | Integration effort | Evidence retained | Appropriate use |
| --- | ---: | --- | --- |
| Form handler sends directly | Low | Provider response and application log | An authoritative internal mailbox where a missed notice has no material consequence |
| Intake record plus worker | Medium | Request, policy decisions, attempts, and outcomes | Customer-facing edtech support intake |
| Shared event stream plus consumers | Higher | The same record can serve several established consumers | An organization with multiple existing workflows and clear event ownership |

The middle boundary is the defensible default for this scenario. It has enough structure to inspect a duplicate, a suppression decision, or an unresolved provider response, without making one contact form adopt a platform-wide event contract. The event-stream option is valid when other teams already consume the same event; introducing it only to avoid one worker creates another ownership and monitoring obligation. That extra machinery is a poor fit for a single internal mailbox with no recovery obligation, where direct delivery can be easier to operate and the mailbox is authoritative. The trade-off is extra persistence and reconciliation work in exchange for evidence that survives a worker restart.

Direct delivery is not universally wrong. It is a valid fit for a low-risk internal form when the mailbox is authoritative and losing a notification has no material consequence. It becomes a poor default when queue ownership, suppression, and recovery matter, because those decisions become implicit in a synchronous call and its provider-specific response. A durable worker is also the wrong boundary for a one-off administrative script; its audit and reconciliation overhead would exceed the operational risk.

## How do we test the negative path?

The test matrix should make ineligibility and ambiguity first-class outcomes. Include an existing institution billing request, a learner-access request without an institution identifier, an invalid address, a duplicate submission, and a destination already suppressed. Assert the selected queue, the notification key, the recorded ruleset, and whether a dispatch was eligible. A test that asserts only the mailbox name misses the failure boundary.

Deployment checks should verify the configured sending identity, SPF policy, DKIM signing configuration, and the review path for key rotation. Runtime reconciliation should find attempts that remain unresolved beyond the team’s stated service window and distinguish a provider timeout from a confirmed rejection. Alert on counts and ages, not message bodies.

This approach makes the integration decision reviewable. The team can later change a dispatcher or routing rule while retaining the original intake facts, because the record explains what the system intended at the time. That is the useful form of reliability here: not a promise that every message arrives, but a durable account of every decision that preceded an attempt.

## References

- RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- RFC 6376, DomainKeys Identified Mail (DKIM) Signatures: https://datatracker.ietf.org/doc/html/rfc6376
- RFC 5321, Simple Mail Transfer Protocol: https://datatracker.ietf.org/doc/html/rfc5321
- CTIA, Messaging Interoperability and Compliance Best Practices: https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
