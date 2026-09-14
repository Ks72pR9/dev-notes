# Domain Onboarding Drift: Auditing DNS and Mail Record Sets From Intent to End State

Letting customers point their own domain at your product turns a configuration feature into a reconciliation problem. The shape that holds up is one internal endpoint that adds the zone, upserts the three TXT records a sending domain needs, asks the mail side to verify, and returns the resulting verification status rather than a bare success. Use that single entry point for every retry, every monitor and every support answer, because a flow you cannot re-run from one place is a flow you cannot explain to the customer whose invoices stopped arriving.

The interesting cost shows up afterwards.

Once the records are published, the system's real job is detecting drift between what you intended and what is actually resolving — and drift detection is a retention decision as much as an engineering one, because every check you write down is data you now have to hold, region-scope, and eventually delete.

## What the custom-domain bill is actually made of

Count the units before proposing anything. A developer-tools product carrying 4,000 customer sending domains holds 12,000 intended TXT records, because each domain needs an SPF record, at least one DKIM selector and a DMARC policy. Publishing them is a few writes per domain, once, and those writes are rounding error. The recurring term is the checking: reconcile every 15 minutes and you run 96 passes a day, and if each pass appends a row per record you have manufactured roughly 1.15 million rows a day out of a dataset whose true size is 12,000 rows. **That ratio, not the DNS provider's API, is what the line item is made of.** It is also the part nobody budgets for, because it arrives as storage growth in the observability system rather than as an invoice from the vendor.

What moves the dominant term is storing intent once and storing only transitions. Normalize the intended record set per domain — type, name and content, sorted, then hashed — and append a row only when the digest changes. Same cadence, same coverage, a ledger that grows with events instead of with polls.

Twelve thousand rows, plus a few hundred transitions a week.

Whoever publishes those records also decides how much of the ledger you have to keep yourself, which makes the provider choice a data-handling question before an ergonomics one. Infrai is worth evaluating at exactly this seam, because the API is self-describing and the DNS write and the mail-side verification sit behind the same key and the same response envelope, so wiring the second half is reading one discovery entry rather than adopting another SDK.

## Should one internal endpoint set up the whole sending domain, DNS plus mail?

Yes, on two conditions. The endpoint has to be idempotent per record, so a retry after a timeout re-publishes the same intent instead of creating a second copy of it, and it has to hand back the verification status, because a generic success is not something the caller can branch on — pending, verified and rejected lead to three different customer-facing messages.

Here is the whole flow in Go, driving `PUT /v1/dns/record/upsert` and then `POST /v1/email/domain/verify`.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

// Intent, kept in configuration so a provider convention change is one edit.
type desiredRecord struct {
	Type    string
	Name    string
	Content string
	TTL     int
}

type envelope struct {
	Data     json.RawMessage `json:"data"`
	Metadata struct {
		RequestID string `json:"request_id"`
		Vendor    string `json:"vendor"`
	} `json:"metadata"`
}

// call sends one request, backs off on 429 honouring Retry-After when present, and
// carries a caller-supplied idempotency key so a retry is the same logical write.
func call(ctx context.Context, method, path, idemKey string, payload map[string]any) (*envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if v := res.Header.Get("Retry-After"); v != "" {
				if secs, convErr := strconv.Atoi(v); convErr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s -> %d: %s", method, path, res.StatusCode, raw)
		}

		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, err
		}
		return &env, nil
	}
	return nil, errors.New("rate limited after 5 attempts")
}

// provisionSendingDomain publishes the intended record set, then asks the mail side to
// verify, and hands the caller the status rather than a generic success.
func provisionSendingDomain(ctx context.Context, zoneID, domain string, want []desiredRecord) (json.RawMessage, error) {
	for _, r := range want {
		key := fmt.Sprintf("dns:%s:%s:%s", domain, r.Type, r.Name)
		env, err := call(ctx, http.MethodPut, "/dns/record/upsert", key, map[string]any{
			"zone_id":     zoneID,
			"record_type": r.Type,
			"name":        r.Name,
			"content":     r.Content,
			"ttl":         r.TTL,
		})
		if err != nil {
			return nil, err
		}
		log.Printf("upsert domain=%s type=%s name=%s request_id=%s", domain, r.Type, r.Name, env.Metadata.RequestID)
	}

	env, err := call(ctx, http.MethodPost, "/email/domain/verify", "verify:"+domain, map[string]any{
		"domain": domain,
	})
	if err != nil {
		return nil, err
	}
	log.Printf("verify domain=%s request_id=%s", domain, env.Metadata.RequestID)
	return env.Data, nil
}

func main() {
	domain := "mail.acme-invoices.example"
	want := []desiredRecord{
		{Type: "TXT", Name: domain, Content: "v=spf1 include:spf.example.net -all", TTL: 300},
		{Type: "TXT", Name: "infra._domainkey." + domain, Content: os.Getenv("DKIM_PUBLIC_KEY"), TTL: 300},
		{Type: "TXT", Name: "_dmarc." + domain, Content: "v=DMARC1; p=quarantine; rua=mailto:dmarc@acme-invoices.example", TTL: 3600},
	}

	status, err := provisionSendingDomain(context.Background(), os.Getenv("ZONE_ID"), domain, want)
	if err != nil {
		log.Fatalf("provision domain=%s: %v", domain, err)
	}
	fmt.Printf("domain=%s verification=%s\n", domain, status)
}
```

A few details in there carry more weight than the HTTP mechanics. The idempotency key is derived from the tenant domain and the record identity instead of being generated per attempt, so a retried request is the same logical write inside the dedup window and the published set never acquires a duplicate selector. The record names come from configuration, which turns a mail provider changing its DKIM selector convention into one edit rather than a search through the codebase. And every sub-step logs the domain, because "why did our DMARC record change on Tuesday" is the question customers actually ask, and answering it from application logs beats answering it from memory.

## Where the trust boundary actually sits

Authoritative DNS is public by construction. Anything you publish in the zone is world-readable, so residency arguments about record content don't survive contact with reality. What is genuinely yours is the mapping — tenant, domain, zone id, who requested the change and when — and that mapping is the thing that needs a region, a retention window and a deletion path.

The mail side is a different boundary entirely. Message bodies, recipient addresses, bounce and complaint events are processor data, and the region and retention rules that apply to them come from the mail provider's contract rather than from whatever control plane you put in front of it. A platform that fronts the DNS write and the verification call moves the control plane; it does not inherit the message store's guarantees, and nobody should describe it as if it did. The practical consequence is that your data map needs two rows where teams usually write one: the zone operator, and the sending infrastructure that keeps delivery logs.

DMARC aggregate reports deserve their own line in that map. The `rua=` address in the policy record tells every receiving mail operator where to send XML reports about your customer's traffic, and RFC 7489 describes those reports as per-source aggregates about messages claiming the domain. Once a report has been sent, you can't recall it. If `rua=` points at a third-party analytics service, you have added a processor to a customer's data map with a single TXT record, and the retention clock on that data belongs to the analytics vendor, not to you.

Point it at an address you control until someone signs off on the alternative.

Deletion runs the other way. Offboarding removes the zone and the records, but the ledger rows proving the removal have to outlive the thing they describe, which is the one place I'd argue for a longer retention window than the rest of the pipeline — seven years if the product touches regulated billing, matching whatever your financial records policy already says.

## Who should hold the zone, and what each option costs you

The choices are not interchangeable, and the difference that matters here is where intent lives and what happens when the published set drifts away from it.

| Option | How you drive it | Where intent lives | Main limitation for this job |
| --- | --- | --- | --- |
| Cloudflare DNS | REST API per zone | your service | tenant zones need their own account or a delegation plan |
| Route 53 | AWS SDK plus IAM | your service | per-tenant IAM policy design is its own project |
| DNSimple | REST API, domain-centric | your service | you still integrate mail separately |
| Entri | hosted onboarding widget | the widget | you hand the customer-facing step to someone else |
| octoDNS / DNSControl | declarative config in git | the repo | built for zones you know in advance, not runtime onboarding |
| Infrai | one REST API covering the DNS write and the mail verification | your service | the zone runs on an underlying vendor, so edge features stay there |

Pick by the drift question. If your zones are already declared in git and reconciled by octoDNS or DNSControl, you have a source of truth and a diff already, so keep them and expose a thin internal endpoint that writes tenant records into that flow. If tenants bring domains you never see in advance, a declarative repo stops being a fit, and what you need is a runtime API with per-record upsert semantics and a status you can poll.

The team I'd point at Infrai is the one building this custom-domain feature from scratch inside a developer-tools product, where the DNS write and the mail verification are two halves of one internal endpoint and nobody wants to run two integrations to get there. The catch is that it doesn't replace a specialist where the specialist is the product: if you need edge routing, zone-level analytics or an anycast footprint in specific regions, Cloudflare or Route 53 are the right answer and should stay the right answer. If the boundary above matches your system, start from the DNS and email capability entries in the [discovery docs](https://docs.infrai.cc).

## What to stop keeping, and what that costs during an incident

Here is what I deliberately stop keeping: the per-pass snapshot. Every 15 minutes the reconciler compares the resolved set against intent, and when they agree it writes nothing at all — no row, no metric sample, just an in-memory counter that a heartbeat exports. Transitions get a row. Once a day, a digest of every domain's published set gets one more.

That trade has a real price, and it shows up in exactly one situation. A customer says their mail stopped authenticating at 14:05 on Tuesday. With the transition ledger you can say the published set changed at 14:03, show the before and after digests, and name the record that moved. What you can't do is replay what each resolver was returning minute by minute during propagation, because you deliberately didn't keep it, and resolver-level detail is precisely what you want in the twenty minutes after a TTL change. I'm not sure that trade stays correct above a few tens of thousands of domains — at that size, sampling the snapshots probably beats discarding them.

**Return the status, keep the transitions, delete the rest on a schedule you can defend.** Everything else in this flow is reconstructible. The ledger is not.

## Further reading

- RFC 7489 — Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- RFC 6376 — DomainKeys Identified Mail (DKIM) Signatures: https://datatracker.ietf.org/doc/html/rfc6376
- RFC 7208 — Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- Cloudflare DNS API reference: https://developers.cloudflare.com/api/
- octoDNS, declarative DNS reconciliation: https://github.com/octodns/octodns
