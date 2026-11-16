# Media Auth API: Preserving Email Login Sessions While Bounding Phone Code Retention

Give readers of an existing media app phone one-time-code login while keeping the application responsible for its server-side sessions. The least complex option is to preserve the email signup and login boundary, add a separately verified phone credential, and avoid making every returning reader request another message. The operating bill contains message attempts, retained authentication evidence, and investigation work; which term dominates depends on actual traffic and retention policy.

Consider a planning example, not a measured workload: 10,000 login challenges with one initial send and 0.3 additional sends per challenge produce 13,000 message attempts. Additional sends account for 3,000 attempts. If that term dominates the measured bill, changing the resend policy moves it directly; trimming a small audit record does not. Monetary dominance still requires the team's delivery invoices and storage measurements, rather than an assumed unit price.

TL;DR: For email signup and login in an existing Node.js backend without a client SDK, select an authentication API by its verification, retry, and session boundaries. Keep a server-side session behind a protected browser cookie, make duplicate verification requests converge on one session issuance, and introduce phone login without silently merging accounts. Set evidence retention before launch. A successful phone challenge establishes possession of that credential; it does not establish entitlement to a paid subscription.

## What actually makes readers request another code?

Separate a reader's deliberate resend from a transport retry and from an attacker initiating fresh challenges. All three can increase message attempts, but treating them as the same event encourages a dangerous optimization: allowing unlimited retries because the interface calls them resends. OWASP's Authentication Cheat Sheet calls for controls against automated attacks and discusses the limitations of SMS authentication. Those limitations matter when deciding which media accounts may use a phone code as their only login factor.

For this design, give a challenge an opaque identifier and bind its purpose to phone login. Associate a resend with that challenge, enforce a server-side sending allowance, and make the interface show when another request is available. A browser timer is feedback; the server remains the enforcement point. Do not promise a particular cooldown or attempt budget without examining delivery latency, abuse, and the recovery path. A number copied from another application can produce fewer sends while also locking out legitimate readers.

Measure the denominator carefully.

The example's 0.3 additional sends is an assumption to replace with observations. Record initial sends, explicit resends, delivery retries, challenge completions, and sessions issued as distinct event types, with a correlation identifier that does not contain the phone number. A ratio of sends to completed sessions can reveal friction, but it can also rise during abuse; retain enough aggregate context to distinguish those explanations before changing the policy. Do not interpret fewer messages as proof of safer login.

The change worth evaluating is preserving an acceptable existing session so that opening another article does not require another challenge. This reduces opportunities for interruption, but extending session lifetime also extends the exposure window for a stolen session. Choose idle and absolute expiry deliberately, and require fresh authentication for sensitive changes such as replacing the phone credential or changing account recovery settings. Convenience needs a boundary.

## How should an auth API connect email signup and phone login?

Adding phone login to email signup creates an identity-linking problem before it creates a session problem. A phone number supplied during an unauthenticated request must not become a shortcut into an existing reader account merely because another record appears to match. Require an authenticated linking flow and verification of the new credential; specify recovery separately, including the case where a reader no longer controls the old phone. OWASP's guidance on changing sensitive account information and reauthentication provides the relevant basis. There are two ledgers here in the architectural sense: the history of authentication decisions and the record of subscription entitlements. They answer different questions. The former explains why a session was issued; the latter determines whether a reader can access a particular paid article. Keep the entitlement check attached to the application's authorization decision, including after login succeeds. Linking credentials must not manufacture a second subscription or transfer one through an incidental identifier match. This separation also helps answer the original API selection question. An API usable through ordinary server-side requests can fit a backend without a browser SDK, but that integration property says little about recovery, revocation, or auditability. Ask whether failed verification consumes attempts, what simultaneous successful verifications mean, how expiration is enforced, and how the application learns that a credential was replaced.

Obtain explicit contract answers. A successful demonstration of email signup cannot establish the behavior of recovery or concurrent verification.

For the browser session, OWASP's Session Management Cheat Sheet describes cookie protections including Secure, HttpOnly, and SameSite, as well as session identifier renewal after authentication. Apply these protections with HTTPS, choose SameSite behavior for the application's navigation needs, and address CSRF for state-changing operations. Keep the session identifier out of URLs and logs. A phone challenge changes how the reader authenticates; it does not eliminate session management.

## Make verification retries converge on a durable decision

An exactly-once mindset is useful here as a local invariant: for a particular accepted challenge, the application should commit one session-issuance decision. It is not a claim that the network delivers a request exactly once. A response can disappear after the transaction commits, and two verification requests can arrive together. Both cases need defined outcomes.

The following Go example expresses the application boundary; it is not a complete verifier or a commercial API integration. The implementation behind this interface must enforce challenge expiry, attempt limits, and code verification, and atomically commit challenge consumption, session issuance, and an audit event. Its replay behavior must also bind the request to the originating login attempt. Possession of a challenge identifier alone must never authorize retrieval of a session.

```go
package authentication

import "context"

type VerifyInput struct {
	ChallengeID string
	AttemptKey  string
	Code        string
}

type SessionIssue struct {
	SessionID string
	Replayed  bool
}

type ChallengeCommitter interface {
	// Commit verifies the challenge and atomically records its outcome.
	// A valid retry of the same bound attempt returns its prior issuance.
	// Codes and session identifiers must never enter audit logs.
	Commit(context.Context, VerifyInput) (SessionIssue, error)
}

func CompleteLogin(
	ctx context.Context,
	committer ChallengeCommitter,
	in VerifyInput,
) (SessionIssue, error) {
	return committer.Commit(ctx, in)
}
```

The small interface leaves the difficult work visible in the contract. An attempt key is an idempotency input, not proof of identity. The durable implementation needs a uniqueness constraint or equivalent serialization rule, a way to distinguish an authorized replay from a conflicting attempt, and an explicit policy for what happens after the replay window expires. Merely caching the first HTTP response leaves unresolved the interval between a committed database write and a failed response write. Consider the failure sequence as a design exercise: verification succeeds, the database commits, and the connection closes before the browser receives its cookie. A retry must recover the already committed decision under the same verified binding, without issuing another independent session. Rejecting every retry prevents that recovery; accepting every challenge identifier permits unauthorized replay. The trade-off is additional short-lived replay state in exchange for a recoverable login attempt. Test both sides of that boundary, including a request that reuses the identifier with a different attempt binding.

Do not audit secrets.

Instead, record the event identifier, challenge correlation, internal account reference where appropriate, decision category, policy revision, and timestamps needed for reconstruction. Keep the audit event in the same transaction as session issuance, or use a transactional outbox when the audit destination is separate. That is a proposed design requirement: its purpose is to prevent a session from existing without durable evidence of the decision that created it. Delivery of the resulting event may still be repeated, so consumers need deduplication too.

## Test the friction budget before changing retention

The acceptance suite should exercise a lost success response followed by a retry, concurrent submissions of the same valid code, expired challenges, exhausted attempts, and a phone-link request against an existing email account. Check the resulting session and audit records, not just status codes. A test that observes two successful responses can still conceal two separately issued sessions.

Deploy with separate controls for initiating challenges, accepting phone login, and linking credentials. During a staged rollout, compare completion rates and resend rates with the application's own baseline, and inspect whether support contacts are shifting toward recovery problems. Avoid sensitive identifiers as metric labels. The point is to learn whether session policy is imposing unnecessary challenges without building a second identity database in the monitoring system.

There is a team cost as well: ownership of the transactional boundary, recovery decisions, and incident interpretation remains with the application team even if delivery and verification are external. A smaller API surface can reduce integration work, but an undocumented retry contract transfers complexity into support and reconciliation. Require an executable account of that contract before broad deployment.

## Retain decision evidence and deliberately discard credential material

Define separate retention policies for active challenge state, idempotency replay state, session records, security events, and aggregate operational metrics. They serve different purposes. Challenge state should become unusable at expiry; deleting it physically is a separate lifecycle decision. Replay state must cover the documented retry window, while session revocation must remain effective for the relevant session lifetime. An audit record need not preserve the code, the cookie value, or the complete request body to explain a decision.

Compliance limits cannot be reduced to a universal number of days. Applicable jurisdiction, contractual duties, incident response requirements, and data minimization obligations determine which records the organization may retain and for how long. Have the responsible legal and security owners approve the schedule. OWASP guidance supports secure authentication and session handling; following it alone does not certify regulatory compliance.

For this media application, the defensible endpoint is an explicit loss of detail: stop keeping raw verification codes, session credentials in logs, and unrestricted copies of authentication request bodies. After the approved investigation window, delete or de-identify linkable event detail under a documented policy, including relevant backup lifecycle rules. Preserve only the aggregates and decision evidence that the policy justifies.

That choice has a cost. Once detailed delivery and attempt evidence is gone, a support investigation may establish that a challenge was rejected without reconstructing precisely why a particular reader never completed it. Accept that limit explicitly. The session-security-versus-friction decision is credible only when the team can explain both the evidence it retains and the questions it has chosen to stop answering.

## References

The authentication guidance supports the discussion of reauthentication, account changes, automated attacks, and SMS limitations. The session guidance supports the cookie, expiry, renewal, and logging boundaries. The workload arithmetic and transactional design above are illustrative engineering proposals, not reported measurements or guarantees from these sources.

- OWASP Authentication Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- OWASP Session Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html

## Further reading

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html
