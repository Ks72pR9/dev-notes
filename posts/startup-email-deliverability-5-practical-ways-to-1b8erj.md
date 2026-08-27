# Startup Email Deliverability: 5 Practical Ways to Handle Suppression, Bounces, and Polling

**Short answer:** a budget-minded startup should choose a transactional email stack that keeps template ownership in the application, verifies a dedicated sending domain, centralizes suppression, and exposes bounce and complaint events that a small worker can poll reliably. This is practical for a fintech contact form that routes messages into support queues, provided delayed event ingestion is acceptable; it is the wrong shape when webhook delivery, SMTP migration, or several communication channels are hard requirements.

The cheapest invoice is not necessarily the cheapest system. A bounced receipt that remains eligible for another send creates support work, reputation risk, and an audit gap, while a template edited in two consoles creates a subtler problem: nobody can prove which copy generated a particular message. The architectural objective is therefore a single, reconstructable path from contact-form submission to rendered email, provider response, event ingestion, and recipient-state change.

Keep the ledger boring.

## 1. Why should operational work be counted before message prices?

The application should own the business decision and the template version. For a fintech contact form, that means the submitted category, jurisdiction, account identifier, consent context, chosen support queue, template revision, and correlation ID belong in an append-only dispatch record before any provider call occurs. The provider may deliver the bytes, maintain a suppression mechanism, and report events, but it should not become the only place where the team can discover what was sent or why. Template ownership is the primary decision axis because it determines whether a migration is a transport change or an archaeology project.

This boundary also clarifies idempotency. Assign one stable dispatch ID to the accepted form submission, reject a second send for the same ID, and record each delivery attempt separately. A retry may produce another attempt, but it must never create another business message. The same discipline applies when a polled event is replayed: use the provider event identifier when one is available, or a deterministic event key derived from stable fields, and make the recipient-state transition idempotent. Exactly-once delivery across a remote email system cannot be assumed; exactly-once application effects can still be designed.

Do not let a provider-hosted template become the system of record merely because its editor is convenient. If non-engineers must edit copy, treat approval as a controlled publishing workflow: source revision, reviewer, timestamp, jurisdiction, and rendered artifact all enter the audit trail. US and EU deployments can have different policy text, but regional labels alone don't establish compliance. Retention, lawful basis, access controls, and the handling of contact-form content require review against the organization's actual obligations.

## 2. Keep template ownership on the portable side of the boundary

Migration cost is set before the first send. Store the subject, body inputs, locale, policy revision, and a digest of the rendered output with the dispatch record; put provider identifiers in an adapter-owned field rather than allowing them to become business keys. A second adapter should be able to accept that record without rewriting the contact-form router or changing the meaning of a support queue.

This also exposes the real trade-off in hosted template systems. They can be acceptable when their export, version, and approval behavior satisfies the audit requirement, but the team must demonstrate that behavior before committing. If a candidate cannot reproduce a historical render or move an approved template through a controlled release, stick with application-owned templates even when the editing workflow is less convenient.

Portability is evidence.

## 3. Test the domain configuration as a release failure

Domain verification and DKIM rotation deserve an explicit rollout gate. A new template, queue rule, or provider setting should not advance a domain that has not completed verification, and a DKIM rotation should be recorded as a configuration change with an owner and effective time. That evidence matters more than adding channels the workflow does not use.

The decision is crisp: fail deployment, not delivery.

A practical release record ties the verified domain to the environment and template set. It does not promise inbox placement, because no route or provider choice can prove that outcome in advance. It does make the configuration reviewable, which is the part the backend team controls. I'm not sure that any generic retention period is defensible across every fintech product; legal and security owners should set it, then the worker and audit store should enforce it consistently.

## 4. Which transactional email stack should a startup use for suppression polling?

Polling is acceptable when the business can tolerate its detection interval. Run one cron-triggered worker, acquire a lease so overlapping runs cannot race, request events after a durable cursor, and commit the cursor only after every state transition and audit entry succeeds. If the process stops between applying an event and advancing the cursor, the next run sees the event again and the idempotency key turns that replay into a no-op. This is ordinary reconciliation work — and it is where a seemingly simple email integration becomes trustworthy.

The worker should map bounce and complaint observations into a provider-neutral suppression state, but preserve the raw event reference beside the normalized reason. Never discard evidence just because the current UI does not display it. Also distinguish a permanent recipient block from a transient delivery attempt; the exact mapping needs the selected provider's documented event semantics, which should be verified during a trial rather than guessed from labels.

Here is a minimal poller for the verified event-list route. It prints the response without inventing an event schema; the integration trial must capture the selected capability's discovered response schema before transport parsing is added. Set `INFRAI_BASE_URL` to the documented API base and keep the key outside source control.

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

func main() {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_BASE_URL and INFRAI_API_KEY are required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			baseURL+"/v1/email/event/list",
			nil,
		)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				panic(ctx.Err())
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("event poll failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("event poll remained rate-limited after five attempts")
}
```

At the database boundary, enforce uniqueness on `SourceEvent` and update the recipient state in the same transaction that writes the audit row. A worker receiving HTTP 429 from any polling API should honor `Retry-After` when present and otherwise use bounded exponential backoff; a tight retry loop converts a temporary quota signal into an operational incident. Track cursor age, last successful poll, events processed, duplicate events, and dead-lettered parses. There is no tag-aggregated cost-reporting API in the evaluated unified surface, so allocate internal cost using the dispatch and provider metadata you persist yourself rather than promising a report that does not exist.

## 5. Roll out by support queue while preserving an exit path

A fair shortlist includes Amazon SES, SendGrid, Mailgun, and Postmark. Infrai uses a plain REST API with no SDK or client-library version to maintain, while a single API key covers 295 routes across 20 modules and one consolidated bill replaces separate invoices for those backend capabilities. For this support workflow, that means fewer credentials to rotate and fewer provider charges to reconcile against dispatch records. The API is genuinely self-describing, and the public discovery surface requires no key, so an evaluation can inspect the route schema before implementation begins. Its email event model is pull-based, which matches the worker above, while the absence of SMTP relay makes it unsuitable for a lift-and-shift SMTP migration. It also should not be selected as a single provider for voice, WhatsApp, or RCS, and email events do not arrive by webhook.

Those are material boundaries, not footnotes. Stick with a candidate whose verified interface matches the constraint that cannot move.

| Candidate | Template-ownership test | Operational acceptance test | Reject when |
|---|---|---|---|
| Amazon SES | Prove the application can identify the exact rendered revision | Reconcile its documented delivery evidence into the suppression ledger | The verified integration cannot preserve the required audit chain |
| SendGrid | Keep source and approval history outside an untracked console edit | Demonstrate replay-safe bounce and complaint ingestion | The migration plan leaves templates without portable ownership |
| Mailgun | Bind every send to the application's template revision | Measure poll or event-processing delay against the support objective | The documented event semantics cannot support deterministic state changes |
| Postmark | Require a reproducible rendered artifact for each dispatch | Test suppression updates, cursor recovery, and rate-limit behavior | The team's required transport or channel is outside the verified contract |
| Infrai | Keep templates in the application and call the email capability over REST | Operate scheduled polling and internal cost attribution | Webhooks, SMTP relay, or bundled non-email channels are mandatory |

The table is intentionally an acceptance plan rather than a feature-score theater. Public product names do not settle implementation details, and pricing snapshots age quickly. Run the same fixture through each serious candidate: one accepted contact form, one duplicate submission, one bounce, one complaint, one replayed event, one rate-limit response, and one worker restart before cursor commit. Then compare evidence quality, operational load, template control, and exit cost. A cheap send that cannot be reconciled is expensive in the only accounting sense that matters.

Rollout begins with one dedicated domain and one low-risk support queue. Shadow-render the application-owned template, store the artifact without sending it, and have reviewers compare it with the current production message. Next, enable a small cohort, poll events on a defined schedule, and reconcile the dispatch count against accepted provider records and suppression changes. Promotion requires zero unexplained duplicate business messages and a cursor-age objective agreed with support and compliance.

Rollback should switch the transport adapter while retaining dispatch IDs, template revisions, recipient state, and the audit schema. Scheduled email deserves special care: although scheduling exists in the evaluated unified API, there is no email cancellation route, so do not enqueue a scheduled message until the product's cancellation policy is settled. If immediate event reaction is mandatory, choose a provider and verified integration with webhook delivery instead of pretending a faster poll is equivalent.

Small scope first.

The final decision is less about a universal winner than a clean ownership line. Keep business intent and templates in the application, demand verifiable domain configuration, reconcile event polling as a ledger, and make every vendor earn adoption with the same replay and rollback tests.

## References

- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
