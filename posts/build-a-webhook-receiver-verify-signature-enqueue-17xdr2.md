# Build a Webhook Receiver — Verify Signature, Enqueue Raw Bytes Fast

Rotate a production webhook key by accepting a bounded pair of secrets, verifying the signature over the untouched request bytes, durably enqueueing the verified envelope, and acknowledging only after queue admission succeeds. The deciding constraint for a gaming backend is the spend ceiling versus refused traffic: overlap prevents avoidable rejection during deployment, while a hard queue budget prevents a traffic burst from becoming an open-ended liability.

**Short answer:** treat verification, durable admission, and acknowledgment as one ordered intake transaction. Keep gameplay side effects out of that transaction.

This architecture decision record applies to an Express deployment even though the critical-path example below is Go. In Express, capture the raw body before any JSON body parser transforms it, then pass those exact bytes to both the verifier and queue envelope.

## How should a webhook receiver verify a signature before it can enqueue?

The first invariant is byte identity. Signature verification consumes the exact bytes received, not a parsed object serialized again. Whitespace, number formatting, and key order are representation details that parsing can change; verification therefore belongs before semantic decoding.

The second invariant is bounded secret selection. During rotation, the verifier may try the active key and one retiring key, each identified by an internal key version. It must not scan an unbounded key history. OWASP's secrets-management guidance recommends defined rotation processes, controlled access, auditing, and attention to caching behavior; those concerns become concrete here because every extra accepted secret enlarges both the credential exposure window and the audit surface.

The third invariant is durable-before-success. A successful response means the authenticated event crossed the durable queue boundary. It does not mean the game state, entitlement, or player ledger already changed. A worker can retry downstream effects using the sender's event identifier as an idempotency key, while the intake record preserves the received timestamp, digest, key version, verification result, and queue admission result as an audit trail.

Keep the failure boundary narrow. An invalid signature is refused. A valid event that cannot fit inside the configured admission budget is also refused with a retryable response. Only a verified, durably accepted envelope earns success.

No ambiguity.

Capacity wins.

## Decision: bounded overlap with durable admission

The rotation state is small: `active` and optional `retiring`. Install the new secret as active while retaining the old one, switch the sender to sign with the new secret, observe which key version validates incoming events, and remove the retiring secret after old-key traffic has ended according to the rotation policy. Secret values never enter logs or queue metadata.

| Option | Refused traffic during rollout | Spend-ceiling behavior | Audit quality | Decision |
|---|---|---|---|---|
| Abrupt replacement | Old-key requests fail as replicas converge | Predictable, but rejection can be noisy | Clear but incomplete across the cutover | Reject for rolling changes |
| Bounded two-key overlap | Either approved key validates during the controlled window | Work and accepted-key count stay bounded | Records the exact key version used | Adopt |
| Accept first, verify later | Intake can acknowledge unauthenticated bytes | Queue consumption occurs before authenticity is established | Success no longer proves authenticated admission | Reject for this boundary |

The spend ceiling is enforced at admission, not estimated afterward. Set an explicit maximum body size, concurrency, and queue capacity outside the handler, then let admission failure propagate to the sender as refusal. This chooses finite resource exposure over unconditional acceptance. It may produce retry traffic when the queue is full, but it keeps a burst from silently exceeding the operating envelope.

The trade-off is explicit. A 1 MiB body ceiling and two-key validation window are concrete limits in this example, not universal defaults; a game that legitimately emits larger payloads must raise the bound deliberately and account for the resulting memory exposure. Likewise, a receiver facing expensive asymmetric verification or many independent senders may need a different isolation model, because this design's short synchronous authentication step can consume handler capacity before queue admission. The design is unsuitable when the sender cannot retry refused traffic and losing an event is worse than temporarily storing unverified bytes. In that narrower case, use a separately budgeted quarantine store, label acknowledgment as receipt rather than authentication, and keep quarantine data outside the trusted event stream.

## Critical path: verify bytes, enqueue, then acknowledge

The handler makes the ordering visible. It uses a generic durable queue interface and HMAC-SHA-256 as the concrete signature mechanism; sender and receiver must agree on the exact scheme and encoding. It deliberately does not parse JSON on the intake path.

```go
package webhook

import (
    "context"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "io"
    "net/http"
    "time"
)

const maxBodyBytes = 1 << 20

type Key struct {
    Version string
    Secret  []byte
}

type Keyring struct {
    Active   Key
    Retiring *Key
}

type Envelope struct {
    ReceivedAt time.Time
    Body       []byte
    BodySHA256 [32]byte
    KeyVersion string
}

type DurableQueue interface {
    Enqueue(context.Context, Envelope) error
}

type Handler struct {
    Keys  Keyring
    Queue DurableQueue
    Now   func() time.Time
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
    if err != nil {
        http.Error(w, "request body rejected", http.StatusRequestEntityTooLarge)
        return
    }

    keyVersion, err := verify(body, r.Header.Get("X-Webhook-Signature"), h.Keys)
    if err != nil {
        http.Error(w, "signature rejected", http.StatusUnauthorized)
        return
    }

    envelope := Envelope{
        ReceivedAt: h.Now().UTC(),
        Body:       body,
        BodySHA256: sha256.Sum256(body),
        KeyVersion: keyVersion,
    }
    if err := h.Queue.Enqueue(r.Context(), envelope); err != nil {
        http.Error(w, "admission unavailable", http.StatusServiceUnavailable)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

func verify(body []byte, encoded string, keys Keyring) (string, error) {
    supplied, err := hex.DecodeString(encoded)
    if err != nil {
        return "", errors.New("invalid signature encoding")
    }
    candidates := []Key{keys.Active}
    if keys.Retiring != nil {
        candidates = append(candidates, *keys.Retiring)
    }
    for _, key := range candidates {
        mac := hmac.New(sha256.New, key.Secret)
        _, _ = mac.Write(body)
        if hmac.Equal(mac.Sum(nil), supplied) {
            return key.Version, nil
        }
    }
    return "", errors.New("signature mismatch")
}
```

Production wiring must inject a real clock and a queue whose successful return means durable admission, rather than placement in process memory. Emit low-cardinality counters for verified, rejected, admitted, and capacity-refused outcomes. Key version is useful audit metadata; the secret and full signature are not. Alerting should distinguish authentication failure from saturation because their remedies differ.

The consumer performs semantic validation after dequeue, claims the external event identifier in an idempotency store, applies the gameplay or account mutation, and records the result. Exactly-once delivery is not assumed. The design pursues exactly-once effects: duplicate deliveries may exist, but a durable idempotency decision prevents the same event from granting an entitlement or posting an account movement twice. Reconciliation compares accepted event identifiers with completed effects and makes gaps inspectable.

Testing should preserve the ordering. Use fixtures containing original bytes and expected signatures; include a one-byte mutation, malformed signature encoding, both key versions, an oversized body, a full queue, a duplicate event, and a worker crash after claiming the idempotency key. A rotation rehearsal is complete only when metrics show validation moving from the retiring version to the active version and removal of the old secret causes no unexplained authentication failures.

## Why reject accept-first processing?

The rejected design acknowledges immediately, stores raw input, and verifies later. Its valid use case is an isolated capture system where the storage tier is intentionally allowed to contain hostile input, strict quotas bound that storage, and the response contract explicitly means only "bytes received," not "authenticated event accepted." This limitation matters more than response speed.

That contract is wrong for this gaming webhook boundary. It lets unauthenticated traffic consume the same durable capacity reserved for legitimate events, weakens the meaning of a success response, and postpones the audit decision that matters most. Under a fixed spend ceiling, early verification is also the cleaner admission control: cryptographic work remains bounded by body size and two candidate keys, while durable capacity is reserved for authenticated envelopes.

The resulting rule is compact: **during rotation, accept two controlled credentials but only one authenticated queue contract**. Refuse unverifiable input, refuse work beyond declared capacity, and acknowledge promptly after durable admission. That policy keeps the service live without converting availability into unlimited acceptance, and it leaves enough evidence to reconcile every accepted event with its eventual gameplay effect.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
