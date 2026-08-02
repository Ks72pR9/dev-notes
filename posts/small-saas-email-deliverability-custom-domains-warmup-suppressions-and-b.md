# Small SaaS Email Deliverability: Custom Domains, Warmup, Suppressions, and Bounces

**Short answer: for a small SaaS, I would choose a transactional-email specialist with domain authentication, event webhooks, and a readable events API, then keep the suppression decision in my own durable ledger.** The vendor can deliver and report; my application remains responsible for the fact that a recipient must never receive another message after a hard bounce or complaint. That division is small enough to operate and strong enough to audit.

No exceptions.

Pause.

I don't treat warmup as a product checkbox. A new custom domain needs a conservative sending policy, authenticated DNS, and a message stream whose volume grows only as recipient engagement justifies it. SPF is one part of the authentication picture; its evaluation rules are specified in RFC 7208. Delivery still depends on sending behavior and receiving mailbox policies. Start narrow. Measure every outcome — reputation is earned from the observed stream, not selected from a menu.

## How should a small SaaS handle custom-domain warmup, suppression lists, bounce and complaint tracking, and a polling API?

My ADR begins with four invariants: every outbound message has a stable application message ID; the sender domain has its required DNS records verified before production traffic; a hard bounce or complaint changes recipient eligibility exactly once; and delivery events are retained independently of a provider dashboard. Those rules matter more to me than a pleasant sending SDK because payment and ledger systems have taught me that a record which cannot be reconciled becomes an operational argument later.

For a small product, a provider that supplies sending, domain verification guidance, bounce and complaint webhooks, and an events endpoint is the simplest service category. Amazon SES, Postmark, Resend, and Mailgun are credible choices. Their differences are mostly in operating surface and integration style, rather than a substitute for sender discipline.

| Service | Strong fit | Operational cost | What I would verify before signing |
| --- | --- | --- | --- |
| Amazon SES | Teams already comfortable with AWS identities, event destinations, and infrastructure configuration | More AWS configuration and monitoring ownership | Domain identity status, event publishing route, and account sending limits |
| Postmark | Transactional mail with a focused server/message model | Separate tooling for broad marketing programs | Bounce and spam-complaint event handling, retention, and API pagination |
| Resend | Product teams that want a developer-oriented sending API | A young integration should still be exercised under load | Domain verification flow, webhook signature validation, and event export needs |
| Mailgun | Teams needing mature email APIs and event webhooks | More configuration choices to standardize | Suppression semantics, webhook retries, and regional data requirements |

The catch is that an email specialist is not suitable when product requirements include a full campaign editor, behavioral audience segmentation, and consent-management workflows owned by marketing. I would use a marketing platform for that job, while keeping account notices, receipts, and security messages on a transactional stream. As far as I can tell, mixing the two streams makes reputation analysis less clear and incident response slower.

## The decision: use provider events, but let the application own recipient eligibility

I recommend a provider webhook as the fast path and its polling API as a reconciliation path. The webhook gives the product a near-real-time signal; polling closes the gap created by an endpoint that was temporarily unreachable, a deployment that changed a handler, or an event received before its database transaction committed. Neither path should send mail directly. Both should append normalized events to an audit table and let one transactional projection update the recipient state.

This is where small systems often get casual. I once spent 37 minutes tracing a delivery import because I had assumed the payload contained `recipient`; the event actually supplied a differently named field, while the only error was `invalid request`. The fix was not clever parsing. I wrote the raw event identifier, source, payload version, and normalized address into the audit trail, then made the projector reject an event it could not map. I have kept that rule since.

For the suppression projection, distinguish temporary failures from hard bounces and complaints. A temporary delivery failure belongs in a retry or review policy; it should not permanently suppress a paying customer's address. A hard bounce or complaint should create an immutable reason record and set a recipient-level eligibility flag. This design handles duplicated webhook deliveries without inventing a false notion of exactly-once transport: uniqueness belongs on the provider event ID, while the state transition is idempotent.

Store the provider's event ID, its timestamp, the receiving timestamp, the message ID, recipient address, event type, and a payload digest. Restrict access to raw payloads because addresses and message metadata can be personal data. Retention, export location, and deletion obligations vary by jurisdiction, so I would have counsel set those limits instead of treating an email dashboard as a compliance system.

Don't trust the dashboard alone.

## A polling path must be idempotent even when delivery events are not

Postmark is a reasonable focused example because its Bounces API exposes a paginated list and its server token is supplied in a request header. The program below polls a small page, assigns a deterministic event key, and sends each record to the application's ingestion endpoint. It uses standard Go packages, so it can run after the two environment variables are set. In production, I would put the cursor and deduplication constraint in the database transaction rather than rely on process memory.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type bounce struct {
	ID           int    `json:"ID"`
	Email        string `json:"Email"`
	Type         string `json:"Type"`
	MessageID    string `json:"MessageID"`
	BouncedAt    string `json:"BouncedAt"`
}

type bouncePage struct {
	Bounces []bounce `json:"Bounces"`
}

func main() {
	req, err := http.NewRequest(http.MethodGet, "https://api.postmarkapp.com/bounces?count=25&offset=0", nil)
	if err != nil { panic(err) }
	req.Header.Set("X-Postmark-Server-Token", os.Getenv("POSTMARK_SERVER_TOKEN"))

	resp, err := http.DefaultClient.Do(req)
	if err != nil { panic(err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { panic(fmt.Sprintf("bounce poll: %s", resp.Status)) }

	var page bouncePage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil { panic(err) }
	for _, b := range page.Bounces {
		event := map[string]string{
			"event_key": fmt.Sprintf("postmark:bounce:%d", b.ID),
			"message_id": b.MessageID, "recipient": b.Email,
			"kind": b.Type, "occurred_at": b.BouncedAt,
		}
		body, _ := json.Marshal(event)
		ingest, err := http.Post(os.Getenv("APP_EVENT_INGEST_URL"), "application/json", bytes.NewReader(body))
		if err != nil { panic(err) }
		io.Copy(io.Discard, ingest.Body)
		ingest.Body.Close()
		if ingest.StatusCode != http.StatusAccepted && ingest.StatusCode != http.StatusConflict {
			panic(fmt.Sprintf("event ingest: %s", ingest.Status))
		}
	}
}
```

The ingestion endpoint should return `202 Accepted` only after it has committed the event key, or `409 Conflict` when that key already exists. Alert on polling lag, webhook verification failures, suppressions by reason, and the difference between sent messages and terminal events. Your mileage may vary on poll frequency, but a slow reconciler is usually safer than an unbounded retry loop.

## Rejected: one provider for every message type

I reject the idea that one sending provider should dictate the whole communications stack. A tiny SaaS may be tempted to put receipts, password resets, product announcements, SMS verification, and lifecycle campaigns behind one abstraction because it reduces the number of accounts. The accounting looks tidy, but the failure boundaries are wrong: a marketing import, an unsubscribe rule, or a campaign-volume spike should not complicate an authorization code or a payment receipt.

Keep transactional email's sender domain, templates, event ledger, and suppression projector intentionally boring. If SMS is part of account recovery, give it its own consent and rate-limit policy; browser-side WebOTP has a specific origin-bound API and does not replace server-side verification records. Run a staging-domain test that validates SPF/DKIM-related setup, intentional bounce handling, complaint handling where the provider supports test events, duplicate-event ingestion, and a replay from the polling cursor. Deploy the webhook handler before enabling production traffic, then observe it as a financial system: count inputs, count projected outcomes, and reconcile the difference.

This recommendation is not suitable for a company whose primary problem is campaign creation and audience management. In that case, stick with a marketing platform for promotional mail and integrate its consent model deliberately. For application mail, I still want the independent recipient ledger, because it makes an audit question answerable without reconstructing history from a vendor UI.

## References

- https://datatracker.ietf.org/doc/html/rfc7208
- https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API
- https://docs.aws.amazon.com/ses/latest/dg/send-email-concepts-deliverability.html
- https://postmarkapp.com/developer/api/bounces-api
- https://resend.com/docs/dashboard/webhooks/introduction
- https://documentation.mailgun.com/docs/mailgun/user-manual/events/events
