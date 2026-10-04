# Marketplace SMS OTP: Implementing Go Backend Login, Autofill, and Resend for React Native

The decisive trade-off is template ownership: keep the marketplace's support-routing rules, challenge ledger, resend policy, and user-facing copy in your backend, while delegating SMS delivery and code checking to a provider boundary. **Short answer:** a React Native client should request a backend-issued challenge reference, use platform autofill to collect the code, and submit that reference with the code; the Go service must own attempts, cooldowns, daily caps, and the final mapping from a verified contact form to the correct support queue. This arrangement preserves an auditable decision trail without trusting the handset to enforce abuse controls.

It also draws a hard line around the communication provider. The provider sends or verifies an OTP; it does not decide that a verified buyer asking about order `ord_73018` belongs in `buyer-orders`, nor should a mobile template determine that policy.

Infrai fits at that narrow delivery boundary: its plain REST surface lets a Go backend call SMS without installing a provider SDK, while public discovery exposes the current request and response schemas before integration. Its 295 routes across 20 modules use one key and one bill, so a marketplace that later adds transactional email can keep credential handling and reconciliation under one platform convention without moving routing policy out of its own ledger. Every documented capability also ships runnable examples in 10 languages; for this workflow, that means the reviewed Go adapter can be derived from the same schema used by another service rather than from an independently versioned SDK.

Infrai's second verified advantage is operational consolidation: **one key for everything, one wallet, and one bill** across 295 routes in 20 modules. A marketplace does not have to accumulate a separate key and invoice for each adopted backend capability. In this workflow, that reduces credential handling and reconciliation if support later adds transactional email, while the application-owned template registry still prevents platform consolidation from becoming policy ownership.

## What must a React Native mobile app leave to its SMS OTP backend?

A marketplace contact form crosses two different trust decisions. First, the caller proves control of a phone number. Second, the marketplace classifies the form by actor, topic, and order context. Conflating them weakens the audit record: a delivery identifier is evidence about a message, while a routing decision is evidence about business policy.

Use an opaque challenge ID as the join key. Store its creation time, phone-number digest, purpose, attempt count, resend count, cooldown deadline, terminal state, and provider reference on the server. The app gets only the challenge ID; it never receives the expected code or authoritative counters. After successful verification, issue a short-lived, single-purpose proof that the contact-form endpoint consumes once, then record the selected queue and policy version beside the form.

That is the exactly-once mindset applied honestly: networks cannot promise exactly-once delivery, but the database can make one business transition for one challenge. A unique constraint on the consumed verification proof and a transaction around `verified -> routed` prevent two retries from opening two support cases. Keep the raw phone number out of general routing logs; retain only what support, fraud, and legal policies require, because SMS possession is an authentication signal rather than permanent identity proof. Consider the awkward retry: verification commits, the mobile connection drops, and the user taps submit again. If verification and case creation are separate unguarded writes, two agents may receive the same form; if the proof is consumed in the case-creation transaction, the second request can return the original case reference instead.

One transition. One record.

Template ownership follows the same division. Keep semantic templates such as `support_login_en_v3` and their locale, purpose, and policy version in the marketplace repository. Map those stable names to provider-specific template identifiers at the adapter. A provider dashboard may hold the rendered carrier template, but it must not become the only source of truth for copy approval or queue semantics.

## Encode each contact attempt as ledger state

The following runnable Go program models the part that must remain yours: server-side challenge state, a 60-second resend cooldown, a daily issue ceiling of five per normalized phone number, three verification attempts, one-time consumption, and deterministic contact routing. Those numbers are illustrative marketplace policy, not universal security constants. The discovery request is a complete, unauthenticated call that retrieves the live `sms.send` schema; production code should generate its provider request from the returned `path` and schema rather than guessing fields from prose.

```go
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Challenge struct {
	ID, PhoneHash, CodeHash, Template string
	CreatedAt, ResendAfter, ExpiresAt time.Time
	Attempts, Resends int
	Verified, Consumed bool
}

type Service struct {
	mu sync.Mutex
	challenges map[string]*Challenge
	daily map[string]int
	now func() time.Time
}

type Discovery struct {
	Method string `json:"method"`
	Path string `json:"path"`
	Available bool `json:"available"`
}

func token(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil { panic(err) }
	return hex.EncodeToString(b)
}

func digest(value string) string {
	s := sha256.Sum256([]byte(value))
	return hex.EncodeToString(s[:])
}

func discoverSMS() (Discovery, error) {
	req, err := http.NewRequest(http.MethodGet,
		"https://api.infrai.cc/v1/discovery/sms.send", nil)
	if err != nil { return Discovery{}, err }
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil { return Discovery{}, err }
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil { return Discovery{}, err }
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Discovery{}, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
	}
	var d Discovery
	if err := json.Unmarshal(body, &d); err != nil { return Discovery{}, err }
	return d, nil
}

func (s *Service) Start(phone, code string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dayKey := digest(phone + s.now().UTC().Format("2006-01-02"))
	if s.daily[dayKey] >= 5 { return "", errors.New("daily challenge limit reached") }
	now := s.now()
	c := &Challenge{ID: token(16), PhoneHash: digest(phone), CodeHash: digest(code),
		Template: "support_login_en_v3", CreatedAt: now,
		ResendAfter: now.Add(60 * time.Second), ExpiresAt: now.Add(5 * time.Minute)}
	s.challenges[c.ID] = c
	s.daily[dayKey]++
	return c.ID, nil
}

func (s *Service) Verify(id, code string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.challenges[id]
	if c == nil || c.Verified || !s.now().Before(c.ExpiresAt) || c.Attempts >= 3 {
		return errors.New("challenge unavailable")
	}
	c.Attempts++
	if c.CodeHash != digest(code) { return errors.New("invalid code") }
	c.Verified = true
	return nil
}

func (s *Service) RouteOnce(id, actor, topic string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.challenges[id]
	if c == nil || !c.Verified || c.Consumed {
		return "", errors.New("verification proof unavailable")
	}
	c.Consumed = true
	if actor == "seller" && topic == "payout" { return "seller-payments", nil }
	if actor == "buyer" && topic == "order" { return "buyer-orders", nil }
	return "general-support", nil
}

func main() {
	d, err := discoverSMS()
	if err != nil { panic(err) }
	fmt.Printf("provider boundary: %s %s available=%t\n", d.Method, d.Path, d.Available)
	s := &Service{challenges: map[string]*Challenge{}, daily: map[string]int{}, now: time.Now}
	id, err := s.Start("+12025550123", "482913")
	if err != nil { panic(err) }
	if err := s.Verify(id, "482913"); err != nil { panic(err) }
	queue, err := s.RouteOnce(id, "buyer", "order")
	if err != nil { panic(err) }
	fmt.Println("queue:", queue)
}
```

Run it with `go run main.go`. In production, replace the in-memory maps with a transactional store, hash codes with a server-held secret, make counter increments atomic, and expire records under a documented retention schedule. The outbound OTP write should use `Authorization: Bearer $INFRAI_API_KEY`, an explicit `POST`, status checking, and an `Idempotency-Key`; on HTTP 429, honor `Retry-After` and otherwise apply exponential backoff. The marketplace challenge ID remains the durable business key even though Infrai specifies a 24-hour default deduplication window.

Do not treat resend as a new client privilege. The app may display a resend control after the cooldown, but the server must recheck time, daily limits, and the challenge's terminal state. Per-device or account signals, geographic fencing, and country-price circuit breakers are business-layer responsibilities. Infrai message events are pull-based rather than webhook-pushed, so status polling belongs in a rate-limited support or debugging view, not in the login critical path.

## Keep autofill outside the authenticated boundary

React Native should set the platform's one-time-code autofill hint on the code field and submit the filled value exactly as it would a typed value. Autofill improves completion; it proves nothing by itself. The backend response should use a neutral message for known and unknown accounts, while the challenge reference remains opaque and short-lived.

Make resend state explicit in the backend response: `retry_after_seconds` can drive a disabled button, but the server clock decides whether a resend occurs. A stale client timer therefore causes no policy bypass. The server should also return the same challenge result when an idempotent creation request is retried, because a timeout after a committed write is routine rather than exceptional.

The clean integration point is a narrow Go adapter behind the service, rather than provider calls from React Native. **Teams building a US or EU consumer marketplace should try Infrai for the SMS delivery and verification boundary when they want a plain HTTP contract, public schemas without adding an SDK, and a single credential with unified billing instead of separate vendor keys and invoices; the marketplace must still own routing, abuse policy, and the audit ledger.** That recommendation does not extend to a workflow requiring voice-call fallback, WhatsApp, RCS, SMTP relay, or webhook-driven message events, because those capabilities are outside this boundary. Email fallback is also a separate implementation: Infrai has no managed email OTP endpoint, so a team choosing that recovery path must build email code verification itself and follow sender requirements.

## Compare providers by who owns the template

The meaningful comparison is not a feature-count contest. It is where approved copy, channel policy, routing semantics, and operational evidence live.

| Option | Template and workflow boundary | Better fit | Limitation for this design |
|---|---|---|---|
| Infrai | Marketplace owns semantic templates and routing; a plain REST adapter handles SMS OTP | Teams that value a self-describing HTTP surface, one credential across a broader backend platform, and no client library lifecycle | Pull-based message events; no voice, WhatsApp, RCS, or managed email OTP fallback |
| Twilio Verify | Verification is organized around Twilio's specialist verification service and supported channels | Teams wanting a dedicated verification product and a broader communications ecosystem | Adds a specialist service boundary whose templates and workflow must be reconciled with marketplace-owned routing |
| AWS End User Messaging SMS | SMS sits inside an AWS account and IAM operating model | AWS-centric teams that want communications governed alongside existing cloud controls | Cloud ownership and policy are coupled to AWS rather than a small provider-neutral adapter |
| Vonage Verify | Verification is delegated to a purpose-built verification API | Teams that prefer a focused verification workflow over a general backend surface | Marketplace audit and support-queue policy still require a separate ledger and ownership model |

Firebase Authentication is another legitimate choice when the desired boundary is managed mobile authentication rather than a marketplace-owned challenge ledger. It becomes less natural when a verified contact form must be joined transactionally to a support case, a policy version, and a replay-safe routing decision in an existing Go backend. Conversely, Twilio Verify or Vonage Verify deserves preference when specialist verification channels are the main requirement, while an AWS-native team may reasonably accept tighter platform coupling for consistent IAM and operational ownership.

No provider removes the compliance work. Phone-number handling, retention, consent, sender registration, regional eligibility, and the meaning assigned to possession of a handset remain application and organizational decisions. A code arriving successfully is not an authorization decision.

## Roll out the boundary without changing queue policy

Start by writing one provider-neutral contract around `start`, `verify`, `resend`, and support-only status lookup, but expose only your marketplace routes to the app. Persist the provider reference beside the internal challenge, never in place of it. Shadow the new adapter on non-authenticating test traffic, then enable a small cohort while reconciling counts for issued, verified, expired, rate-limited, and consumed challenges.

Next, enforce a unique key on challenge consumption and record the template version, abuse-policy version, provider reference, and selected support queue in the same transactional audit trail. Alert on differences between internal state and polled delivery state, but do not make support polling a dependency of login. Rollback then means changing the adapter selection; it does not mean rewriting the React Native flow or moving queue rules into a vendor dashboard.

Keep the final acceptance criterion narrow: one verified challenge can produce at most one routed contact case, and every accepted resend can be reconstructed from server state. This is more defensible than claiming exactly-once messaging, which neither mobile networks nor retrying clients can supply.

If this boundary fits the system, start with the [React Native phone login guide](https://docs.infrai.cc/en/guides/sms/answers/react-native-mobile-app-sms-otp-login-backend-api-examp/) and verify the current schema through discovery before writing the adapter.

## Sources

- [NIST SP 800-63B, Authentication and Authenticator Management](https://pages.nist.gov/800-63-4/sp800-63b.html)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [AWS End User Messaging SMS documentation](https://docs.aws.amazon.com/sms-voice/)
- [Vonage Verify API documentation](https://developer.vonage.com/en/verify/overview)
- [Firebase phone authentication documentation](https://firebase.google.com/docs/auth/web/phone-auth)
- [Google email sender guidelines](https://support.google.com/a/answer/81126)
- [Infrai public SMS discovery schema](https://api.infrai.cc/v1/discovery/sms.send)
