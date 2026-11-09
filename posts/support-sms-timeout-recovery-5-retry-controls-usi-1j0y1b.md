# Support SMS Timeout Recovery: 5 Retry Controls Using Status Polling

A media contact form can contain a subscriber's account problem, a correction request, or a sensitive complaint; routing it to a support queue creates an evidence obligation before it creates a messaging task. A timeout must not silently authorize duplicate SMS notifications.

Short answer: persist the routing decision and notification intent before sending, reuse a stable idempotency key for timeout recovery, and reconcile the recorded message identifier through status polling before authorizing another send. Choose the integration whose records let an auditor reconstruct that sequence. An exactly-once mindset should describe application invariants, with delivery treated as an observed outcome.

## 1. What evidence should a contact-form alert preserve?

Consider an illustrative submission, `contact-2048`, assigned to subscriber support under policy revision `7`. These are application identifiers. The evidence chain should connect that submission to its queue decision, recipient eligibility check, notification intent, transport attempts, and subsequent observations, while keeping the contact-form narrative out of the SMS itself.

Keep payloads small.

The notification record should contain the submission identifier, selected queue, policy revision, communication purpose, recipient reference, stable deduplication key, and timestamps for decisions and observations. Store the provider's message identifier when available. Distinguish an operator-approved resend from an automatic transport retry; otherwise, two notifications can look legitimate in separate systems even though both arose from one ambiguous request. Retain the request fingerprint with the key so that recovery cannot silently change the recipient or message content while claiming to be the same operation.

This is a design requirement, not a compliance certification. Account identity does not establish consent, and a delivery record does not establish that the support workflow satisfies retention, access-control, or jurisdictional obligations. Those decisions require the organization's applicable policy and legal review. The record must explain why the selected queue was entitled to learn that a submission existed.

The ledger analogy is useful: an attempt is an entry, an observed outcome is another entry, and reconciliation connects them without rewriting uncertainty into success.

## 2. How should SMS event notifications retry after a timeout?

Create the routing record and notification intent in the same application transaction, using an outbox worker for communication. Enforce a uniqueness constraint over submission, queue, channel, and notification purpose. Include a business revision when queue reassignment legitimately requires another alert; a worker restart does not create that authorization. A Node.js Express handler can commit this intent and return control to the contact-form workflow while a worker owns SMS dispatch and reconciliation. The language of that handler does not change the transaction boundary.

A timeout proves uncertainty.

After an ambiguous send, preserve the original intent and idempotency key. If a provider identifier was recorded, poll status and events to reconcile the existing message before considering a resend. If the response was lost before its identifier could be stored, replay only under a verified idempotency contract, using the same request and key within its deduplication window. Otherwise, escalate the unresolved attempt. The concrete trap is replacing the key after a timeout: that converts recovery into a second logical notification.

Status polling belongs to the worker, with a bounded schedule and a persisted next-check time. Map documented provider outcomes into internal application states; do not guess that acceptance means delivery. On HTTP 429, honor Retry-After when present and apply exponential backoff. Preserve uncertainty when the observation budget expires. A polling deadline is an application decision, not evidence that the provider rejected the original send.

Cancellation can help queued or scheduled SMS when support has already handled the submission, but cancellation and delivery can race. Record both the cancellation request and the subsequently observed outcome.

The trade-off favors explainability over immediacy because the support policy requires a reconstructable authorization trail. A delayed alert with a traceable unresolved state is preferable to another alert whose authorization cannot be reconstructed.

## 3. Make the recovery decision explicit

The following runnable Go program reads the status of an already-recorded SMS message. Configure `INFRAI_BASE_URL` with the documented HTTPS API base ending in `/v1`, `INFRAI_API_KEY` with the credential, and `SMS_MESSAGE_ID` with the identifier persisted by the sender. It performs a read, never a resend; the raw response remains available for mapping through the discovered response schema rather than assuming undocumented fields.

```go
package main

import (
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
	base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_MESSAGE_ID")
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" ||
		u.Path != "/v1" || u.RawQuery != "" || u.Fragment != "" ||
		u.User != nil || key == "" || id == "" {
		return fmt.Errorf("configure the HTTPS versioned base, key, and message ID")
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet,
			base+"/sms/status/"+url.PathEscape(id), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("status observation unresolved: %w", err)
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			value := resp.Header.Get("Retry-After")
			if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds > 0 {
				if wait := time.Duration(seconds) * time.Second; wait > delay {
					delay = wait
				}
			} else if deadline, err := http.ParseTime(value); err == nil {
				if wait := time.Until(deadline); wait > delay {
					delay = wait
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, data)
		}
		fmt.Println(string(data))
		return nil
	}
	return fmt.Errorf("observation budget exhausted")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The 10-second request timeout and three-attempt limit are example policy choices, not measured service behavior. This program backs off on rate limits, including Retry-After expressed as seconds or an HTTP date, and surfaces unsuccessful responses. Use its printed response only in a controlled test environment; production workers should store the necessary observation with access controls and a timestamp.

Persist before dispatch.

Production dispatch also needs an atomic claim or equivalent concurrency control, durable audit trails, and a verified mapping from transport responses to internal observations. Evaluate the replay deadline again immediately before a write, using the original attempt time; never refresh it merely because another worker acquired the job. Keep observation separate from dispatch so that a contact-form retry and an SMS transport retry cannot accidentally share authority to create a new notification. This program does not implement the database transaction or send an SMS.

## 4. Compare integration boundaries against the evidence requirement

Infrai offers a self-describing REST API, public discovery with runnable examples, and one key, one wallet, one bill across 295 routes in 20 modules. Inspecting a capability's request and response schemas gives the engineer an integration contract without learning another SDK. The unified API is also a separate operational advantage: email and SMS for the contact-form workflow share one API key and a single bill, reducing credential provisioning and invoice reconciliation across those communication steps. Its platform idempotency convention specifies an Idempotency-Key header and a default 24-hour deduplication window; verify applicability to the particular write capability before treating a replay as safe. Credential consolidation also concentrates access, so scope application access and protect the key accordingly.

That integration fits a workflow whose delivery evidence can arrive through scheduled status and event polling. Keep recipient eligibility, geographic abuse rules, country-based spending circuit breakers, and internal notification accounting in the application. Technical discovery is evidence of an API contract; it cannot establish jurisdictional compliance.

The specialist alternative separates email and SMS transport. These products are complementary in that architecture, rather than equivalent implementations of every backend capability:

| Product | Role in this workflow | Evidence question and selection boundary |
| --- | --- | --- |
| Twilio | SMS transport, with a documented status-callback workflow | Favor it when callback-based delivery updates are required; persist and deduplicate callback observations before they affect the audit record. |
| Resend | Email transport for the contact-form handoff | Evaluate its documented idempotency semantics against the replay horizon; email evidence must still link back to the support intent. |
| SendGrid | Alternative email transport | Evaluate its mail and event workflow against the support policy; an email integration does not replace the SMS adapter or authorize SMS fallback. |

An email timeout must not itself authorize SMS fallback. Otherwise, an accepted email with a lost response can generate a second channel notification whose business justification is absent from the record. A policy can authorize both channels, but the ledger should then contain two explicit intents and their eligibility decisions.

The shared API approach reduces integration boundaries when polling meets the support team's observation deadline; the specialist stack earns its additional adapters when independent channel selection or callback timing is a material requirement. Compare the same cases across candidates: an ambiguous send, a recipient who becomes ineligible, and a deliberately authorized second attempt. Do not infer equivalent suppression semantics or compliance coverage from a product category.

## 5. Roll out with reconciliation evidence

Start with one support queue and a controlled recipient set. Exercise response loss, worker restart, duplicate contact-form submission, rate limiting, and a cancellation racing with delivery. These are proposed validation cases, not reported test results. Verify that each path preserves the original intent, that a delivery observation never rewrites its authorization history, and that unresolved attempts remain visible to operators.

Keep one invariant: **a timeout cannot create a new notification intent**.

Expand only after an operator can reconstruct the routing policy, recipient decision, stable key, attempts, and observations from the stored record. Set retention and access rules before collecting production evidence; a complete audit trail can itself contain sensitive data. For an email branch, RFC 8058 addresses one-click unsubscribe in its applicable context, rather than certifying this support workflow or establishing permission to send SMS.

## References

These references support the transport evaluation and the narrow unsubscribe boundary. They do not certify the application's compliance.

- Twilio, Message resource and status callbacks: https://www.twilio.com/docs/messaging/api/message-resource
- Resend, idempotency keys: https://resend.com/docs/dashboard/emails/idempotency-keys
- SendGrid, Mail Send API: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send

## Sources

- RFC 8058, One-Click Unsubscribe: https://datatracker.ietf.org/doc/html/rfc8058
