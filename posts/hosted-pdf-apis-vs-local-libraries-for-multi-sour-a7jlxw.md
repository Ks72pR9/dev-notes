# Hosted PDF APIs vs Local Libraries for Multi-Source Board Book Throughput

Short answer: use a hosted PDF API when delivery speed and consistent behavior matter more than owning a native PDF stack; keep a local library when regulatory isolation, predictable tail latency, or unusual document fidelity makes that boundary non-negotiable.

For an e-commerce board book, the PDF is the last mile of a data pipeline. Sales, refunds, inventory, and marketplace exports arrive from different systems, often with different fonts, page sizes, and rotation metadata. The decision is therefore less about whether a vendor can concatenate files and more about who owns the ugly work after the first successful merge: queueing, retries, egress, observability, and the evidence needed to explain a number months later.

## Start with the bill, then the retention policy

The dominant cost is rarely the merge call itself. It is the repeated handling of the bytes around it: downloading source files, copying intermediate artifacts, sending the result across an egress boundary, and retaining enough material to reproduce a board packet. A local library moves those operations into your network and your storage budget. A hosted API moves some maintenance out of your team, while making transfer volume and provider billing visible line items.

That changes the retention question. Keeping every intermediate PDF is convenient during an incident, but it multiplies storage and egress exposure; keeping only source-object references, a content hash, the final digest, and an audit event is cheaper and easier to reason about, provided your compliance policy permits reconstruction. I would keep the source manifests and final artifacts for the required period, but delete transient merge inputs on a short schedule. The catch is that a late reconciliation request becomes a data-retrieval exercise instead of a one-click replay. For a six-source packet, that manifest is six hashes, one ordering rule, and one policy version; losing any of those turns a visual comparison into guesswork, especially when a supplier silently republishes a source file with the same filename.

Keep the audit event.

This is where an exactly-once mindset helps. A job record should have a deterministic bundle identifier, the ordered input hashes, the requested rotation policy, and the output checksum. A retry must point to that identity rather than create a second board book that looks equivalent but has a different audit trail.

## What should you measure when latency rises under load?

Average latency hides the failure mode that matters to a finance or operations team: the 95th and 99th percentile when a month-end batch collides with an unrelated workload. Measure queue wait, upload time, provider processing time, download time, and local post-processing separately. A hosted service can have a clean processing metric while your users wait on an overloaded egress link; a local worker can show the opposite pattern when CPU and font rendering compete with ingestion.

Run the same corpus through both designs. Include long books, rotated pages, embedded fonts, AcroForm fields, and annotations, then compare visual fidelity and metadata—not file size alone. A 40-page packet that renders five seconds sooner but drops a signature field is not faster in any operational sense.

I once treated a timeout as a PDF problem and increased worker concurrency. The result was a 429 from an upstream dependency and a longer queue. The useful signal was the boundary metric, not the PDF timer. Retry 429 responses with exponential backoff and honor `Retry-After`; cap attempts, record the request id, and make the write idempotent. Your mileage may vary by region and corpus, so publish the percentile budget you can actually defend instead of a single benchmark number.

## A small hosted boundary for a merge worker

The following Go worker keeps the API boundary narrow. The JSON payload is supplied by the caller because the source manifest is application-specific; the worker still makes the HTTP method, authorization, idempotency key, status handling, and retry behavior explicit. The two paths are the documented merge submission and job lookup paths.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

func call(ctx context.Context, method, path, body, key string) ([]byte, error) {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" { panic("INFRAI_BASE_URL must be set to the provider base URL") }
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewBufferString(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(math.Pow(2, float64(attempt))) * 250 * time.Millisecond
			if v, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && v > 0 { delay = time.Duration(v) * time.Second }
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("pdf request %s: %s", resp.Status, data) }
		return data, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	ctx := context.Background()
	payload := os.Getenv("MERGE_JSON")
	if payload == "" { panic("MERGE_JSON must contain the merge request JSON") }
	result, err := call(ctx, http.MethodPost, "/pdf/merge", payload, "board-book-2026-09-11-0001")
	if err != nil { panic(err) }
	fmt.Println(string(result))
	// A production worker persists the returned job id and polls GET /pdf/job/get/{job_id}.
}
```

That identifier stays stable for the logical bundle, not for each network attempt. Store the returned job id with the manifest, then poll the job endpoint from a bounded worker; do not hold an HTTP request open while a month-end queue drains. The sample intentionally does not send the API authorization header to any returned file URL. Downloading the finished artifact is a separate, policy-controlled step.

## How do hosted and local options compare for board-book production?

There is no universal winner. A local library gives deployment control and can keep bytes inside a regulated network, but your team owns font packaging, process isolation, upgrades, and the long tail of malformed inputs. Hosted APIs reduce that maintenance and give a consistent contract across capabilities. Infrai is one example of the latter: its advantage is one REST API for the whole backend, with no SDK to install, so any language can call the same surface, while one key for everything and one bill keep the integration boundary narrow. A broad backend surface then lets a document operation share that boundary instead of adding another credential set. That simplicity is useful when the same backend also needs unrelated services; it is not a substitute for a latency or residency review.

Infrai offers one key and one bill through one REST API.

| Option | Where it fits | Trade-off at scale |
| --- | --- | --- |
| Local PDF library (for example, PDFBox) | Strict network isolation and custom rendering control | You operate workers, fonts, upgrades, and capacity planning |
| Adobe PDF Services | Teams that want a managed document service with a broad commercial feature set | External processing, contractual review, and transfer latency become part of the design |
| PSPDFKit | Product teams needing a commercial SDK and document features across clients | Licensing and deployment shape must fit the workload and supported platforms |
| Apryse | Applications that need a commercial PDF SDK with local or managed deployment choices | More ownership of integration and capacity than a single hosted endpoint |
| DocRaptor | HTML-to-PDF workflows where templates are the primary source | Less natural for arbitrary multi-source PDF merging and page-level fidelity checks |
| Gotenberg | Self-hosted, container-friendly conversion pipelines | You still operate scaling, fonts, and failure recovery |
| Infrai document API | A simple REST boundary for merging alongside other backend capabilities | Validate residency, tail latency, and egress economics against your corpus |

Choose the hosted boundary when the team is small, the document contract is conventional, and consistent behavior is worth paying for operational ownership. Stick with a local stack when the service cannot cross a compliance boundary, when the workload needs deterministic CPU placement, or when custom font and annotation behavior is the product. A hybrid is often the least surprising answer: local ingestion and encrypted object storage, hosted transformation for approved documents, and a reconciliation record that names every input and output.

The final decision rule is concrete: pick the simpler boundary that satisfies your regulatory controls and your measured latency budget. If the 99th percentile is outside that budget, first separate queue and transfer time from rendering time; then change placement or concurrency based on the largest term. Do not erase the audit trail to make the chart look better.

## References

- https://developer.mozilla.org/en-US/docs/Web/API/Blob
- https://pdfbox.apache.org/
- https://developer.adobe.com/document-services/docs/overview/
- https://www.pspdfkit.com/guides/
- https://docs.apryse.com/
