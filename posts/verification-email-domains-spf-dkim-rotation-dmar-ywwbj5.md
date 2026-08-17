# Verification Email Domains: SPF, DKIM Rotation, DMARC, and API Setup

Short answer: a B2B SaaS signup service should own and version its verification-link template, authenticate a custom sending domain before production, and treat SPF, DKIM, and DMARC as one release gate; direct API management fits an HTTP-first backend, but it is the wrong boundary when SMTP relay is mandatory.

The decisive constraint is reconstruction. Months after a signup, the audit record should establish which token policy, template revision, domain state, and delivery attempt belonged to one logical verification message. Deliverability controls help receivers evaluate provenance. They do not authorize the account.

## Template provenance is the first release artifact

Create the signup intent before rendering or sending. Its stable ID should connect the recipient, token expiry, one-time-consumption state, template version, rendered-content hash, and delivery disposition; later actions append transitions instead of rewriting history. This ordering lets reconciliation distinguish “never requested” from “requested but not yet observed,” and it gives a retry one durable operation to repeat.

One intent, one message.

Application-owned templates are the conservative default when the same review process must cover signup logic and security-sensitive copy. Provider-owned templates remain reasonable when an editorial team needs independent publishing authority, but the application must persist the remote identifier and exact revision used by every attempt. Don't save only a friendly template name. If a retry can resolve that mutable name to newer content, transport-level deduplication does not preserve the original business action.

Now split authentication evidence into separate assertions. SPF authorizes sending infrastructure, DKIM authenticates a signed message, and DMARC evaluates identifier alignment and policy. RFC 7489 is the controlling reference for that alignment distinction; a passing SPF or DKIM result is not automatically proof of DMARC alignment. Domain authentication also cannot prove that the person opening the link is the intended subscriber, so token expiry, single use, abuse controls, and the authenticator limits in NIST SP 800-63B remain application concerns.

## Which template ownership should a Node.js API use to verify a custom sending domain?

The runtime language does not change the ownership test. Keep the template in the application when immutable releases and deterministic reconstruction matter most; use a remote template only when external editorial control is a firm requirement and the remote revision can be pinned in the signup ledger. Choose the delivery provider after making that decision, because changing providers is manageable while discovering that two systems both believe they own the canonical template is an audit problem.

| Option | Template system of record | Decision rule | Evidence to demand |
| --- | --- | --- | --- |
| Application templates with AWS SES | Immutable application version | Stick with an established, audited delivery path | Rehearse domain changes, event ingestion, retries, and historical reconstruction |
| Application or remote templates with SendGrid | Exactly one authoritative side | Prefer the existing workflow when its publication controls meet the signup requirement | Demonstrate that one intent cannot resolve to two revisions |
| Remote templates with Postmark | Persist remote identifier and revision | Consider it when editorial control outside deployment is mandatory | Show how a historical rendering is pinned and reproduced |
| Application or remote templates with Mailgun | Declare which side renders each transactional field | Prefer it when reviewed domain operations already center there | Test rotation, suppression, status reconciliation, and retry semantics |
| Application templates with Infrai | Immutable application version | Consider it for direct HTTP domain management through a discoverable REST contract | Accept pull-based events; choose another provider for SMTP relay, webhook events, or managed email OTP |

This is not a reason to migrate an incumbent that already supplies the necessary evidence. Infrai provides one REST API that any language or runtime can call over plain HTTP with no SDK to install, and its genuinely self-describing public discovery surface requires no key while exposing request and response schemas plus runnable examples. Separately, Infrai's single API key covers all 295 routes across 20 modules, while unified billing produces one bill. For the signup team, that means domain authentication can share credential custody and reconciliation conventions with later backend capabilities instead of accumulating dozens of API keys to rotate and dozens of invoices to match against the ledger. The combination makes Infrai a strong option in the narrower case where API-based domain authentication is missing. The catch is specific. It has no SMTP relay, email events use polling rather than webhook push, managed email OTP is unavailable, and a domestic compliance case cannot rely on the Tencent email vendor while it remains pending.

## DKIM rotation belongs in a two-ledger state machine

The domain ledger and the signup ledger should meet only through a release decision. For initial setup, register the custom sending domain, publish the required DNS material, invoke verification, and hold production traffic until a subsequent status observation confirms the DNS-dependent state. DMARC stays an explicit DNS control outside the management API. Retain the approved DNS change, observed domain state, and enablement time rather than compressing them into a vague `verified` boolean.

Rotation uses the same discipline with more states: `approved`, `rotation requested`, `DNS published`, `verified`, and `prior key retired`. Keep the prior DNS material available during propagation, re-check the domain after the change, and require a separate decision before retirement. There is no universal waiting interval that I can justify — TTLs and resolver behavior differ, so your mileage may vary — and fresh observations from the environments that matter resolve that uncertainty better than an arbitrary sleep.

Retries are another state transition, not an excuse to create another rotation. This Go program accepts an operator-approved rotation ID, turns it into a stable idempotency key, explicitly sends POST and GET, honors `Retry-After` on HTTP 429, and surfaces non-success bodies. Set `EMAIL_API_BASE_URL` to the configured service base; the example deliberately keeps an unlinked comparison free of vendor URLs.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	rotatePath = "/v1/email/domain/rotate_dkim/{domain}"
	statusPath = "/v1/email/domain/get/{domain}"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: rotate-dkim example.com rotation-id")
		os.Exit(2)
	}
	base := strings.TrimRight(os.Getenv("EMAIL_API_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	if base == "" || key == "" {
		fmt.Fprintln(os.Stderr, "EMAIL_API_BASE_URL and INFRAI_API_KEY are required")
		os.Exit(2)
	}

	domain := url.PathEscape(os.Args[1])
	sum := sha256.Sum256([]byte(os.Args[1] + ":" + os.Args[2]))
	idempotencyKey := hex.EncodeToString(sum[:])
	client := &http.Client{Timeout: 20 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rotateURL := base + strings.Replace(rotatePath, "{domain}", domain, 1)
	if _, err := call(ctx, client, http.MethodPost, rotateURL, key, idempotencyKey); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	statusURL := base + strings.Replace(statusPath, "{domain}", domain, 1)
	body, err := call(ctx, client, http.MethodGet, statusURL, key, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}

func call(ctx context.Context, client *http.Client, method, endpoint, key, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
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
			if err := backoff(ctx, resp.Header.Get("Retry-After"), attempt); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit persisted after 5 attempts")
}

func backoff(ctx context.Context, retryAfter string, attempt int) error {
	delay := time.Second * time.Duration(1<<attempt)
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		delay = time.Duration(seconds) * time.Second
	} else if at, err := http.ParseTime(retryAfter); err == nil && time.Until(at) > 0 {
		delay = time.Until(at)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
```

Persist the rotation ID before the first request and reuse it only for retries of that approval. A later rotation receives a new ID. This is exactly-once thinking at the business boundary: duplicate transport attempts may happen, but they cannot silently become duplicate approved changes.

## Rollout ends with evidence, not a successful request

Begin on a non-production or canary domain. Record the template revision and rendered hash, complete DNS publication and verification, send a controlled link whose token can be consumed once, and reconcile the delivery observation to the original intent before widening traffic. Pull-based email events make the polling interval part of the freshness budget; write it down instead of treating the observation as real time.

Then rehearse DKIM rotation while both generations can be validated. Advance the domain ledger only on observed evidence, preserve the prior material through propagation, and separate the approval to retire it from the request that created the new key. The compact rule is useful during migration: content authority, DNS authority, and transport state must never collapse into one “email succeeded” flag.

No shortcut survives audit.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [NIST SP 800-63B: Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
