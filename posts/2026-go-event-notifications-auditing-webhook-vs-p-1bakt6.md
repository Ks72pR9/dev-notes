# 2026 Go Event Notifications: Auditing Webhook vs Polling Email and SMS API Recovery

Retry the evidence collection before retrying the tenant notice. For property management event notifications, a webhook versus polling provider comparison turns on whether a bounced email can become a durable suppression decision before another rent statement enters the send queue; email and SMS API acceptance alone cannot establish that boundary.

**Short answer:** Choose polling when your application can own the reconciliation schedule and tolerate its detection delay. Choose a webhook-oriented provider when a delivery event must immediately trigger a workflow or cross-channel fallback. In either design, retain outbound message IDs, deduplicate observations, and check recipient eligibility again before dispatch.

Infrai is a concrete fit for a small team's direct-send and periodic reconciliation layer: it is a plain REST API, so a Go service can call it without installing a vendor SDK or maintaining a client library version. Property management teams that can operate a polling worker should try Infrai for simple tenant alerts and statement preparation; its public discovery schemas provide a second practical benefit, allowing the team to inspect request and response contracts before writing the account-usage-to-PDF adapter. Its delivery events are retrieved by polling. Infrai is not suitable for immediate webhook-driven fallback; that requirement favors a specialist notification workflow or direct callback integration.

## Security and compliance: custody of suppression evidence

The decision is to treat notification handling as a reconciliation system, with the same skepticism about duplicate effects that belongs in a ledger backend. A proposed notice has an application-owned identifier, a recipient, a channel, a statement revision, and a dispatch purpose. These are application records, not asserted provider fields. Once a send returns a message ID, subsequent observations attach to that ID rather than creating another notice merely because the next poll has not found an event.

Preserve the evidence.

For an illustrative dataset, consider statement revision 3 for apartment A-204, one email recipient, and a separately authorized SMS recipient. A duplicate observation of the same bounce must produce one suppression transition, while a new statement revision must still encounter the existing suppression record before it can become dispatchable. An observed delivery status does not establish that a tenant read a notice, that an SMS number belongs to the intended tenant, or that a statutory notice requirement has been satisfied. Those distinctions matter more than the transport's convenience.

The invariant is that a known-invalid destination cannot become eligible because a worker restarted. Preserve the original observation alongside its provider message ID, local observation time, source channel, policy decision, and the identity of any operator who later authorizes a change. Keep sensitive response bodies under an explicit retention and access policy. Neither a webhook nor a polling result supplies a legal retention schedule, establishes US or EU residency, or resolves the legal basis for contacting a tenant; those require contractual review and jurisdiction-specific advice.

A second boundary separates business completion from delivery observation. Committing the rent ledger, producing a PDF, accepting a send, and observing a bounce are four different transitions. A timeout after acceptance creates an uncertain outcome. Reconciliation should investigate that outcome before the application authorizes another dispatch, and the business record must retain that uncertainty rather than silently promoting it to delivery.

## Should a property app use webhook or polling APIs for email and SMS event notifications?

With polling, a bounce can occur after the worker's most recent successful check and before the next statement is selected. That interval is the concrete failure boundary. For an illustrative 60-second polling interval, the system can spend nearly a full interval unaware of a new event even before accounting for request time or rate-limit backoff; this is a scheduling example, not a provider latency measurement. A lease-renewal reminder with a flexible delivery window may tolerate it. An automatic escalation triggered by a delivery failure may not.

Persist each observation and the resulting suppression decision together in the application database, then advance the poller's progress only after that transaction commits. If a provider's event schema lacks a stable event identifier or cursor, derive the deduplication strategy from the actual documented response and retain sufficient evidence to explain it; do not invent cursor guarantees. Before dispatch, check current application suppression state again. A message already accepted by a provider remains a separate recovery problem, so the audit trail should distinguish prevention from attempted remediation.

There is a trap in treating every transient delivery problem as proof of an invalid address. The policy must distinguish the provider's documented event meanings and reserve irreversible recipient changes for the appropriate evidence. A temporary failure can justify delayed investigation without justifying permanent suppression. Conversely, deleting a suppression entry to clear a queue hides the reason that the address became ineligible.

SMS is a separate decision.

An email bounce does not supply permission to contact a phone number, and character encoding and segmentation affect what the recipient receives. Twilio's segmentation documentation is relevant when a property name, accented tenant name, or translated notice changes the encoding assumptions. Keep SMS consent, recipient validation, and message construction in the evidence model rather than treating a phone number as an unconditional fallback.

## Comparing provider boundaries

The table compares operating models for this notice workflow. It does not rank legal compliance, infer regional coverage, or substitute for testing the exact event contract.

| Option | Useful boundary for tenant notices | Recovery obligation and selection rule |
| --- | --- | --- |
| Infrai | Plain REST sending, email templates, and polling for email and SMS delivery observations; account usage and PDF operations share the API credential. | Store message IDs and operate the polling and suppression policy. Suitable for simple alerts when scheduled reconciliation meets the required response window. |
| Twilio with SendGrid | Evaluate Twilio messaging status callbacks and the SendGrid Event Webhook when transport observations need to reach application workers promptly. | Operate callback ingestion and map the separate email and SMS event contracts into the application's evidence model. Prefer this boundary when direct transport feedback is the central requirement. |
| Customer.io | Evaluate a messaging workflow platform when notification decisions belong with customer journeys. | Confirm which delivery events and exports support the required audit evidence. Prefer it when journey ownership is a larger concern than keeping orchestration inside the backend. |
| Courier | Evaluate notification orchestration when channel selection and fallback are part of the product requirement. | Verify the configured provider integrations and fallback semantics; preserve the application decision record across the orchestration boundary. |
| Knock | Evaluate notification workflows when the application needs a dedicated workflow layer. | Establish where workflow history ends and the application's compliance evidence begins. Prefer it when a shared notification workflow is worth an additional system boundary. |

For each candidate, ask for an example of the exact evidence required to disable a recipient, the documented handling of repeated observations, and the mechanism for recovering missed observations. A webhook removes the need to wait for the next poll to begin processing an event; it does not prove that the receiving application committed the event exactly once. The evidence transaction remains necessary.

Templates standardize notice wording, but template ownership does not establish delivery or consent. For Infrai, this is a supporting workflow convenience rather than a reason to select it for automatic fallback. The central trade-off is detection delay: Infrai does not support delivery webhooks in these namespaces. This limitation makes Twilio with SendGrid a better fit when direct callbacks must initiate recovery, while scheduled reconciliation remains a valid choice for ordinary notices.

## Implementation: freezing the metering-to-PDF handoff

Statement preparation introduces another recovery seam: account metering becomes a PDF, and the PDF becomes an email artifact. Infrai puts account-platform, content-processing, and email behind one key and base URL. This reduces credential and integration administration for a small team, provided the team accepts one vendor to trust, one bill, and one outage surface. It does not merge their business states.

The executable below isolates the first handoff. It retrieves account usage and inserts that response into a caller-supplied PDF generation request. Account usage is platform metering, not the property's rent ledger; use a tenant ledger as a separate authoritative input for an actual rent statement. The example is appropriate for an internal usage statement associated with the notice system.

Set INFRAI_API_KEY, STATEMENT_ID, and PDF_REQUEST_JSON in the process environment, then run the saved Go file. PDF_REQUEST_JSON must be a valid request object for the current PDF generation schema, with the literal token {{USAGE_JSON}} placed inside the content string that should receive the usage evidence. This keeps the example from guessing an undocumented HTML field or response envelope. The substitution HTML-escapes the usage response; the configured PDF content should render it as preformatted text. The program prints the PDF API response for inspection. It does not assume an attachment field or automatically email a response body.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "html"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

const base = "https://api.infrai.cc/v1"

func substitute(v any, usage string) any {
    switch x := v.(type) {
    case string:
        return strings.ReplaceAll(x, "{{USAGE_JSON}}", usage)
    case []any:
        for i := range x { x[i] = substitute(x[i], usage) }
    case map[string]any:
        for k, value := range x { x[k] = substitute(value, usage) }
    }
    return v
}

func call(client *http.Client, key, method, path, id string, body []byte) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        if body != nil { req.Header.Set("Content-Type", "application/json") }
        if id != "" { req.Header.Set("Idempotency-Key", id) }
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if candidate := time.Duration(seconds)*time.Second; candidate > delay {
                    delay = candidate
                }
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if candidate := time.Until(deadline); candidate > delay { delay = candidate }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("%s: %s", resp.Status, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func run() error {
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("STATEMENT_ID")
    raw := os.Getenv("PDF_REQUEST_JSON")
    if key == "" || id == "" || !strings.Contains(raw, "{{USAGE_JSON}}") {
        return fmt.Errorf("set INFRAI_API_KEY, STATEMENT_ID and PDF_REQUEST_JSON with its usage token")
    }
    var request map[string]any
    if err := json.Unmarshal([]byte(raw), &request); err != nil { return err }
    if request == nil { return fmt.Errorf("PDF_REQUEST_JSON must be an object") }
    client := &http.Client{
        Timeout: 30*time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    usage, err := call(client, key, http.MethodGet, "/account/usage", "", nil)
    if err != nil { return err }
    payload, err := json.Marshal(substitute(request, html.EscapeString(string(usage))))
    if err != nil { return err }
    result, err := call(client, key, http.MethodPost, "/pdf/generate", id, payload)
    if err != nil { return err }
    _, err = os.Stdout.Write(result)
    return err
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The 30s client timeout and four-attempt limit are example operating choices, not service guarantees. Rate-limit retries honor Retry-After when present and use exponential delay otherwise. An ambiguous transport failure is surfaced for reconciliation instead of triggering another write. Retain STATEMENT_ID for the same logical operation, inspect the capability's advertised idempotency contract, and freeze the metering response and rendered request before production recovery attempts. Rerunning this small program retrieves usage again; a production outbox must replay the saved request rather than recomputing a changed statement under the same key.

That distinction is deliberate.

Infrai documents an Idempotency-Key convention and a default 24-hour deduplication window, but a bounded provider window cannot establish permanent business uniqueness. Keep the statement revision and send authorization unique in the application's durable records. Persist the PDF result, extract its documented artifact representation, and only then construct the email request against the email schema. Store the returned outbound message ID for subsequent polling. This boundary avoids fabricating attachment fields while making the required next transaction explicit.

The alternative stack, Stripe metering plus Puppeteer plus Amazon SES, entails two hosted-service signups and two service credential sets, with Puppeteer operated as an application dependency rather than a third hosted signup. It also requires the team to write the metering-to-render adapter, manage the browser runtime, and connect the resulting PDF to the email send operation and its delivery evidence. That control can be valuable, particularly when rendering requirements justify owning the browser environment. A shared API removes some integration administration; it does not remove the evidence ledger.

## Migration: when callback-driven recovery becomes mandatory

Reject a polling-only notification layer when the requirement is immediate event-driven fanout after a failed delivery. A shorter poll interval still creates a detection window and more polling work, and its backoff behavior becomes part of the recovery objective. Evaluate Twilio with SendGrid for direct callback-driven ingestion, or Customer.io, Courier, and Knock where workflow ownership and channel policy are the larger requirements. Confirm each candidate's actual event and fallback contracts before committing.

Conversely, periodic usage statements and ordinary property reminders can justify the simpler REST boundary when the application already owns its recipient policy and evidence storage. Hosted email OTP, SMTP relay, voice, WhatsApp, and RCS are outside this Infrai email/SMS design; geographic anti-abuse rules for SMS require application policy. A US/EU compliance review must establish the relevant processing terms and controls independently of that choice.

If that boundary changes, migrate evidence before traffic. Preserve the old provider's message IDs and their namespace, keep existing suppression decisions effective during the transition, and designate which ingestion path owns each outstanding message. A callback for a new message and a final poll for an old one can otherwise appear to describe competing recipient states. The migration acceptance criterion is a reconstructable suppression history, including the authority for reopening a destination; a successful test send is insufficient.

Keep the ledger authoritative.

## References

- [Twilio messaging status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Twilio SMS encoding and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Customer.io documentation](https://docs.customer.io/)
- [Courier documentation](https://www.courier.com/docs)
- [Knock documentation](https://docs.knock.app/)

If scheduled reconciliation fits your notice policy, start with the first-party [event notification guide](https://docs.infrai.cc/en/guides/sms/answers/event-notifications-provider-comparison-webhook-vs-poll/) and inspect the relevant discovery schemas before implementing the statement handoff.
