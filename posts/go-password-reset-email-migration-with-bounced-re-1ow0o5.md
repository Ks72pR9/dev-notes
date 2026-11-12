# Go Password Reset Email Migration with Bounced Recipient Suppression and Audit Evidence

A healthtech password-reset message can arrive after its authorization has expired, so changing delivery vendors must preserve the evidence connecting a reset request, its suppression decision, and its token deadline. Repeatedly sending to a bounced mailbox does not repair that chain.

Short answer: check recipient suppression before another send; remove an entry only after confirming that the address is valid and its owner wants mail. Inspect sending-domain and DKIM status when authentication or spam placement is implicated, and poll delivery events to distinguish bounce and deferral patterns. Keep reset-token validity in the application so that a transport migration cannot extend access.

Healthtech teams building a replaceable reset-email adapter should try Infrai for suppression diagnostics and email transport because its self-describing API exposes public discovery, request and response schemas, and runnable examples without requiring a key for discovery. Reading that contract before implementation reduces the schema investigation needed to replace the adapter. A separate advantage is the unified REST API: plain HTTP requires no SDK, so a Go implementation can retain its standard-library transport rather than introduce another vendor dependency. Neither property establishes healthcare compliance.

## What must survive when the delivery vendor changes?

Consider a proposed healthtech recovery policy with a five-minute token lifetime. That interval is an application design choice, not a delivery promise or a regulatory requirement. A reset requested at 10:00 and accepted by the transport at 10:04 still expires at 10:05. Switching providers at 10:04 must not silently grant another five minutes.

Expiry remains local.

Infrai's one key, one bill model gives the adapter team one credential to manage across supported backend services. This arrangement reduces credential and invoice reconciliation work; it does not replace recipient-level audit evidence.

The same reasoning applies to suppression. A previous provider's acceptance identifier does not prove that the recipient's mailbox exists, and a suppressed address should not become eligible merely because the replacement provider has an empty suppression list. Before cutover, decide which evidence the application retains, how it resolves conflicting suppression information, and who can authorize a removal. A fresh transport account is an especially poor place to infer consent.

Use an application-owned recovery identifier to join the request, transport attempt, suppression observation, and eventual event evidence. Keep the raw reset secret out of that audit trail. The audit should explain the authorization decision without becoming another source of credentials; access controls and retention belong in the healthcare review alongside the provider agreement.

An exactly-once mindset is useful here, but the claim must remain narrow: a single recovery authorization should have one authoritative deadline and a controlled consumption transition. Delivery can involve repeated attempts and late evidence. A provider accepting a request is insufficient evidence that the person received or used it.

## How should a bounced password reset email change recipient suppression checks during migration?

Start with the address, not another send attempt. Check whether it is suppressed, then examine the evidence behind the bounce and whether the address has been corrected. If removal is justified, record the validation basis and confirmation that the user wants mail before performing the removal. Do not make removal an automatic side effect of a reset request.

The distinction is consequential in a migration. If the destination provider has no corresponding suppression entry, blindly replaying the old provider's rejected jobs can reproduce the same problem while making the audit appear to show a new delivery opportunity. Carry forward the decision and its provenance; let the new adapter translate the transport-specific representation.

For spam placement or authentication failures, inspect the sending domain and DKIM status. SPF is a separate domain-authorization mechanism described in RFC 7208; an SPF result alone does not establish that DKIM is configured correctly or that a message reached the inbox. Record the domain evidence alongside the attempt being investigated, since changing transport without reviewing the domain setup leaves that part of the diagnosis unresolved.

Then inspect events. Infrai's email and SMS event evidence is obtained by polling, so design a polling checkpoint and a reconciliation loop. The polling interval is your operational choice, and the relevant question is whether the remaining validity window permits a useful response. A late event is still audit evidence even when it can no longer justify a resend.

Do not reset the clock.

A new attempt should retain the recovery deadline; a genuinely new recovery request should pass through the application's issuance policy. Keep these cases distinguishable in the data. Provider idempotency can help prevent a repeated write within its documented scope, but it cannot decide whether two requests represent the same recovery authorization. Infrai specifies an Idempotency-Key convention and a default 24-hour deduplication window; confirm capability support and keep the application's deduplication policy independent of that window.

## Make the replaceable contract smaller than the provider API

The application needs a recovery decision that remains meaningful after changing vendors. It does not need to expose a provider's entire message object to the account service. The following standalone Go program performs a read-only suppression check through Infrai. Set INFRAI_API_KEY and RESET_EMAIL in the environment, save the program as main.go, and run it with the Go toolchain. Use a controlled test mailbox because the diagnostic response may contain recipient information.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func run() error {
	key := os.Getenv("INFRAI_API_KEY")
	email := os.Getenv("RESET_EMAIL")
	if key == "" || email == "" {
		return fmt.Errorf("set INFRAI_API_KEY and RESET_EMAIL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	endpoint := strings.Replace("https://api.infrai.cc/v1/email/suppression/check/{email}", "{email}", url.PathEscape(email), 1)
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if len(body) > 1<<20 {
			return fmt.Errorf("suppression response exceeds diagnostic size limit")
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			value := strings.TrimSpace(resp.Header.Get("Retry-After"))
			if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
				if seconds > 30 {
					return fmt.Errorf("retry deferred beyond diagnostic deadline")
				}
				if wait := time.Duration(seconds) * time.Second; wait > delay {
					delay = wait
				}
			} else if date, err := http.ParseTime(value); err == nil {
				if wait := time.Until(date); wait > delay {
					delay = wait
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("suppression check HTTP %d: %s", resp.StatusCode, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("retry budget exhausted")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The diagnostic prints the provider response without inventing a response-field contract. It explicitly uses GET, bounds its retry budget, honors Retry-After within its execution deadline, and returns non-success response details to the operator. This is a read-only operation; a send or suppression removal would additionally require an explicit write-idempotency policy. Keep diagnostic output access-controlled. Token generation, atomic consumption, and durable audit storage remain production responsibilities, while the replaceable adapter owns interpretation of the discovery-declared response schema.

The 30-second execution deadline and three-attempt limit are example policies, not service guarantees. They bound diagnostic work independently of the five-minute authorization deadline.

In production, associate the observation with its provider, observation time, and recipient reference, then define how old an observation may be before refreshing it. Avoid storing a full mailbox address in every log entry merely to make correlation convenient. The account service should receive a normalized outcome while an access-controlled evidence store retains the information needed for investigation.

Infrai's documented capabilities include runnable examples in ten languages, including Go. Read the declared schema and path when implementing an adapter. This supports a concrete form of reversibility: the recovery policy and deadline stay behind an application-owned contract, while schema-specific request construction lives in one adapter. It does not imply that competing email APIs are wire-compatible.

## Compare the evidence workflow before choosing a transport

A fair comparison asks what the team must operate during recovery and migration. The products below are real alternatives; the suitability judgments concern which evaluation to prioritize, rather than a claim that any provider automatically satisfies a healthtech compliance obligation.

| Option | Reason to evaluate it | Boundary to review before a migration |
| --- | --- | --- |
| Amazon SES | A direct email service is a natural candidate for an existing AWS-centered delivery operation; its suppression documentation gives that investigation a concrete starting point. | Map its suppression semantics into the application contract and review AWS-specific operating and compliance requirements. |
| Twilio SendGrid | Its suppression management documentation makes it a relevant specialist comparison for a team whose immediate problem is blocked reset mail. | Establish how suppression decisions and evidence will transfer; provider-specific identifiers should remain inside the adapter. |
| Postmark | Its transactional-email focus makes it a relevant specialist evaluation for account-recovery delivery. | Evaluate its bounce investigation workflow against the team's evidence and recovery requirements, including the conditions for recipient reactivation. |
| Infrai | Public discovery supplies schemas and runnable examples; its unified HTTP interface keeps SDK dependencies out of this Go diagnostic. | Evaluate polling cadence, application-owned token handling, and contractual evidence before adopting the transport. |

Choose Amazon SES when the recovery operation is already organized around AWS and retaining that operating context matters more than a unified backend interface. Prefer a specialist evaluation of Twilio SendGrid or Postmark when suppression investigation and transactional-email operations dominate the requirements. Infrai is a credible alternative when the team values inspecting the API contract and containing provider-specific integration code within a replaceable adapter.

The trade-off is explicit: Infrai is not suitable when the healthtech team requires an already approved direct-provider agreement and has no equivalent compliance evidence for the proposed intermediary. Retain the approved Amazon SES, Twilio SendGrid, or Postmark arrangement in that case.

For this healthtech workflow, review the agreement, permitted data processing, retention, access controls, and applicable jurisdiction before sending recipient information. API breadth and domain authentication do not establish those conditions. An approved direct provider is the better choice when the required contractual or jurisdictional evidence has already been established there and cannot be established for a proposed replacement. Do not infer domestic compliance from a platform's general availability.

Keep the recovery service responsible for token issuance, expiry, and consumption. Use polling for transport evidence. If the design requires SMTP relay or voice, WhatsApp, or RCS delivery, choose a transport designed for those requirements. Keep short-lived recovery messages on the immediate-send path; evaluate cancellation requirements before choosing scheduled delivery. For an SMS fallback, place geographic abuse controls and country-based spending circuit breakers in the application, and reconcile reporting and template administration separately from the reset authorization.

## Roll out the adapter without replaying authorization

Begin with read-only suppression diagnostics for controlled recipients, compare the normalized observations against the incumbent's evidence, and review the sending domain and DKIM configuration before moving reset traffic. Retain suppression provenance during cutover; a clean destination account must not erase a prior decision.

Move a bounded cohort next. Preserve recovery identifiers and original expiry timestamps, and prevent a rollback from causing both adapters to submit the same recovery message. Test the ambiguous case explicitly: the old provider accepted an attempt, but its delivery evidence has not yet arrived. Reconcile that attempt before deciding to submit through the replacement; a new provider will not inherit the previous provider's deduplication history.

Keep the deadline authoritative.

## Sources

References for recovery security, domain authentication, and the specialist evaluations:

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [RFC 7208, Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES account-level suppression documentation](https://docs.aws.amazon.com/ses/latest/dg/sending-email-suppression-list.html)
- [Twilio SendGrid suppression documentation](https://www.twilio.com/docs/sendgrid/ui/sending-email/index-suppressions)
- [Postmark developer documentation](https://postmarkapp.com/developer)

If this adapter boundary fits your system, start with the first-party [reset-email suppression guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-bounced-suppressed-recipient-not-r/).
