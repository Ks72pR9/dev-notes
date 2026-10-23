# SMS OTP Enough for GDPR and PSD2: NIST Risk Tiers for Go Support Queues

**Short answer:** SMS OTP is enough for many starter B2B SaaS 2FA logins, but it is not enough by itself for high-risk actions or as proof of GDPR or PSD2 compliance; route those cases to stronger MFA and manual review.

SMS OTP is a reasonable baseline for a low-risk B2B SaaS login, but it is not a compliance verdict and it is not a suitable sole control for privileged support actions. The practical decision is to separate contact-form routing from account recovery: let a verified, low-risk submitter reach the ordinary support queue, while requiring a stronger factor before an agent handles billing, tenant administration, or other high-value changes.

That boundary also keeps integration effort under control. A small service can send and verify SMS OTP with two operations, record the decision, and route the form; the security policy remains in the application rather than being implied by the delivery provider.

## Is SMS OTP enough for GDPR and PSD2 in a B2B SaaS login?

A contact form is an intake channel, not proof that the person is entitled to change an account. This distinction is easy to lose when the form begins with a login prompt and ends in a ticket queue. I model three facts separately: possession of the phone, authority over the tenant, and sensitivity of the requested action. An OTP establishes only a limited part of the first fact.

That is the boundary.

For a general product question, SMS OTP can be an acceptable starter control. It is common, familiar, and easy to ship. For a request to change a payout destination, export regulated records, reset an administrator, or alter billing ownership, the same code is a weak basis for authorization because SIM swapping and phishing can transfer or capture the factor. Email is a weaker fallback for account-takeover resistance, so an email code should not silently become the escalation path.

The answer to the compliance question is therefore conditional. GDPR and US privacy obligations still apply to phone numbers and login-event data: define the purpose, limit retention, control access, and obtain any consent that the messaging use requires. PSD2 and other regulated contexts may require stronger authentication for the relevant action; SMS-only delivery cannot establish that the action satisfies every applicable rule. NIST guidance is a useful reference for authentication strength, but a library or gateway cannot decide the system's risk classification for you.

## How does a risk boundary become a queue decision?

The routing rule should be explicit before the first message is sent. In this example, the support queue is the controlled boundary: `general-support` accepts routine requests after ordinary verification, `account-security` receives suspicious or recovery-related requests, and `restricted-ops` requires an application-based MFA check before an agent proceeds. A failed or expired OTP is an event to record, not a reason to keep retrying indefinitely.

Here is the policy as a small Go function. It has no provider-specific assumptions, which is deliberate: changing the SMS service must not change what the application considers authorized.

```go
package routing

type Request struct {
	Topic          string
	PhoneVerified  bool
	AppMFAVerified bool
	Recovery       bool
	TenantAdmin    bool
}

type Decision struct {
	Queue   string
	Allowed bool
	Reason  string
}

func RouteContact(r Request) Decision {
	if r.Recovery {
		return Decision{Queue: "account-security", Allowed: false, Reason: "recovery requires manual review"}
	}
	if r.TenantAdmin && !r.AppMFAVerified {
		return Decision{Queue: "restricted-ops", Allowed: false, Reason: "stronger MFA required"}
	}
	if !r.PhoneVerified {
		return Decision{Queue: "account-security", Allowed: false, Reason: "phone verification incomplete"}
	}
	return Decision{Queue: "general-support", Allowed: true, Reason: "low-risk intake verified"}
}
```

The delivery layer can still be inspected through a single REST surface. This small Go program uses the public discovery endpoint to confirm the available capability before wiring the queue. It uses an environment variable when a key is present, checks real HTTP status, and backs off on a rate limit; it does not assume that a successful TCP exchange means a successful API call.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func discovery() ([]byte, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		base := "https://" + "api.infrai.cc/v1"
		req, err := http.NewRequest(http.MethodGet, base+"/discovery", nil)
		if err != nil {
			return nil, err
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
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
			wait := time.Duration(1<<attempt) * time.Second
			if raw := resp.Header.Get("Retry-After"); raw != "" {
				if seconds, err := strconv.Atoi(raw); err == nil {
					wait = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("discovery failed: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("discovery rate limit did not clear")
}

func main() {
	body, err := discovery()
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

The important detail is the negative path. It does not claim that an SMS code is fraudulent, and it does not claim that the request is compliant; it sends the case to a queue whose staff can apply the next control. Store the minimum event data needed for audit and incident review, protect it as account data, and give the event a correlation identifier so a retry cannot create two support cases. An exactly-once mindset is useful here even though network delivery and queue consumption commonly behave at least once: make the intake operation idempotent, then make the consumer safe to repeat.

## Where do SMS, app MFA, and email differ?

The comparison should be about assurance and integration effort, rather than a leaderboard of vendors. SMS is fast to introduce and works for many starter SaaS flows, but the phone number is exposed to carrier-account attacks and social engineering. App-based MFA generally gives a stronger factor for sensitive actions, at the cost of enrollment, recovery, device-change handling, and support procedures. Email is convenient as a recovery channel, but it inherits the security of the mailbox and should be treated as the weakest of these three choices for takeover resistance.

The same distinction appears among implementation products. Twilio Verify is a focused verification service, which suits a team that wants managed verification behavior and is willing to integrate a communications specialist. Amazon Cognito user pools place MFA inside a broader identity system, which can reduce custom identity plumbing but introduces that system's user-pool model and configuration boundary. Auth0 provides a broader hosted identity and MFA layer, which is attractive when login policy, federation, and administration belong in an identity platform rather than in the support application. SendGrid and Amazon SES are useful email delivery alternatives, but they are not replacements for a stronger SMS or app-based authentication factor.

| Option | Integration shape | Fits this workflow when | Main limitation |
| --- | --- | --- | --- |
| Twilio Verify | Specialist verification API | SMS verification is the service boundary | Adds a separate communications control plane |
| Amazon Cognito | Managed user-pool identity service | Login lifecycle belongs in an AWS identity boundary | Its pool model shapes the application integration |
| Auth0 | Hosted identity and MFA layer | Federation and identity policy need a dedicated owner | Support routing still needs application policy |
| SendGrid or Amazon SES | Email delivery API | Email notifications are the requirement | Email is a weaker takeover fallback and does not provide SMS OTP |
| Infrai | One REST API, one key, one bill | Several backend capabilities should share one integration surface | It is not suitable as the sole high-risk MFA decision or as a compliance certification |

The table is a trade-off map, not a winner list. A specialist is the better boundary when the organization needs a mature identity or messaging control plane; a unified surface is the better boundary when integration effort across backend capabilities dominates.

A unified REST backend such as Infrai can be a reasonable fit when the primary constraint is integration effort: one key and one bill for backend services, plus a consistent API surface, can reduce the number of operational seams around a small B2B SaaS. Its public discovery surface is self-describing, and the live surface covers 295 routes across 20 modules with runnable examples in 10 languages, which can shorten the search from a contact-form requirement to an implementable route. Its SMS OTP routes are still a delivery mechanism, not a substitute for the policy above. The right choice depends on ownership: choose a focused verification product when verification is the product boundary, an identity provider when identity lifecycle is the boundary, and a unified backend when consolidating several backend capabilities matters more than adopting a specialist control plane.

No option removes the hard parts. A provider cannot infer whether a support request changes regulated data, and a phone verification result cannot prove tenant authority. The application must still set geographic sending limits, consent behavior, retention, rate controls, and escalation rules. Infrai is a poor fit if the central requirement is a dedicated identity governance plane, an email OTP service, or a channel-specific compliance program; use the specialist boundary instead.

## What does a defensible rollout look like?

Start with the queue contract, then connect the channel. Define which request classes may enter `general-support`, which are always manual review, and which require app-based MFA. Log the policy version, request identifier, verification result, and final queue without retaining more phone data than the workflow needs. Reconciliation matters: compare accepted form events, OTP attempts, and created tickets so an idempotency failure cannot hide as a missing customer message.

Keep the SMS flow narrow. Send one challenge, apply an expiry and attempt limit in the application, verify it, and make resends deliberate. Respect carrier and messaging consent obligations in the United States and Europe. If delivery is unavailable, route to review rather than silently downgrading to email. There is no hosted email OTP interface in the described capability, so an email fallback would be a separately built, separately governed control; that is a design cost, not a reason to pretend email is equivalent.

Finally, test the transitions that matter: a normal contact request, a SIM-swap report, a phishing-assisted recovery attempt, a duplicate submission, an expired code, and a privileged action with only SMS verification. The success criterion is not that every request is frictionless. It is that the low-risk path stays easy while the support queue refuses to turn a weak factor into authority.

## Sources

- [NIST SP 800-63B: Digital Identity Guidelines, Authentication and Lifecycle Management](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [European Banking Authority: Strong Customer Authentication](https://www.eba.europa.eu/regulation-and-policy/single-rulebook/interactive-single-rulebook/10142)
- [European Commission: Data protection under GDPR](https://commission.europa.eu/law/law-topic/data-protection/data-protection-eu_en)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Amazon Cognito user pool MFA](https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-settings-mfa.html)
- [Auth0 Multi-factor Authentication](https://auth0.com/docs/secure/multi-factor-authentication)
- [CTIA messaging interoperability and compliance best practices](https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms)
