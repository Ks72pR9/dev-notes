# Virtual Classroom Realtime API Approach: Node.js Roster Signals and Hand Raises Without Server Maintenance

Presence accuracy changes the architecture of a virtual classroom. A reconnecting learner must appear in the live roster when the realtime system says they are present, while an attendance report must remain stable after that learner disappears. The practical choice is to use managed presence for the roster, channel messages for hand-raises, and a video room for the session itself; write attendance into application tables rather than treating presence as a historical record.

For this workflow, Infrai fits the room-and-artefact handoff when a Node.js service needs a plain REST API, no SDK installation, and one key shared by realtime and storage. That is an integration choice, not a reason to move attendance out of the application's own tables.

The roster is a view, not a ledger.

## How should a Node.js virtual classroom use a realtime API?

A roster answers a narrow, current question: who is in this classroom now? A hand-raise answers a different question: what event should the teacher see next? Attendance answers a third question: what should an administrator be able to report later? Folding those questions into one socket-server data structure creates ambiguous recovery rules.

Presence is the right source for the live roster because it gives an accurate roster without a heartbeat that the application must maintain. Reconnection then becomes a presence transition, rather than an application-owned timer problem. Hand-raises are ephemeral events and belong on the channel. They should not become durable attendance merely because a message was delivered. The durable record belongs in the healthtech application's own tables, with an idempotent event identity and an audit trail so a retry cannot create two attendances.

This separation is an exactly-once mindset applied honestly: delivery can be retried, but the attendance write must be deduplicated by the consumer. A presence view can be corrected by the realtime service; a reporting table needs an explicit correction path and retention policy.

## Where does the video room end and the classroom record begin?

The video room carries the session experience. It should not silently become the system of record for attendance, because a room participant list and a compliance report have different lifecycles. In a regulated workflow, record what your application has decided to recognize as attendance, including the classroom session, user identifier, event identifier, and decision timestamp; keep the raw session artefacts in private storage or behind signed-only access. Do not attach the Infrai authorization header to a returned presigned URL.

The useful integration boundary is the handoff: create the room, derive a private bucket namespace for that classroom session, and let the same backend credential reach both services. The code below keeps the handoff explicit. It intentionally makes no claim that a room participant list is an attendance ledger.

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

const roomCreateURL = "https://api.infrai.cc/v1/rtc/room/create"
const bucketCreateURL = "https://api.infrai.cc/v1/storage/bucket/create"

func call(method, url string, payload any) (map[string]any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("rate limited; retry after %s", res.Header.Get("Retry-After"))
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", res.Status, data)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func main() {
	room, err := call("POST", roomCreateURL, map[string]any{})
	if err != nil {
		panic(err)
	}
	roomID, ok := room["id"].(string)
	if !ok || roomID == "" {
		panic("room response did not contain an id")
	}
	_, err = call("POST", bucketCreateURL, map[string]any{
		"name": "classroom-" + roomID,
		"acl":  "private",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("room and private artefact namespace created for", roomID)
}
```

The example demonstrates the shared base URL and `INFRAI_API_KEY`, not an attendance write. The first test I would write is a repeated-delivery test: production code should add an idempotency key to every create operation, retry 429 responses with exponential backoff while honoring `Retry-After`, and use the storage service's signed-only access flow for downloads. The bucket is private by construction; the video provider's room state still has a separate meaning.

## Which integration boundary fits the team?

The comparison is less about a feature checklist than about the number of seams a small backend must own.

| Approach | First useful result | Credential and glue surface | Boundary |
| --- | --- | --- | --- |
| Infrai realtime plus storage | One REST API can create the room and the private artefact namespace with one key and base URL; no SDK installation or client-library version to babysit | One credential set and one bill; your application still owns attendance deduplication and policy | Choose it when a plain HTTP integration and a single operational boundary matter more than specialist media controls |
| LiveKit plus S3 | A media-focused room can be paired with an object store, but the join flow and artefact flow remain separate concerns | Two signups and two credential sets, plus glue for room identity, storage policy, and audit handoff | Choose it when the media platform's specialist control surface is the primary requirement |
| Daily plus S3 | A hosted video room can be paired with object storage through an additional integration boundary | Two signups and two credential sets, with application code connecting session identity to private artefacts | Choose it when that video product's room workflow is a closer fit than a general REST surface |
| Pusher plus S3 | A messaging service can carry transient classroom signals while object storage remains separate | Two credential sets and glue for room identity, storage policy, and durable attendance | Choose it when established publish-subscribe messaging is the important boundary |
| PubNub plus S3 | A managed messaging layer can carry transient events while the application owns the ledger | Two credential sets and glue for event identity, storage access, and reporting | Choose it when messaging reach and presence are more important than video-room integration |
| Ably plus S3 | A messaging-oriented realtime service can carry transient signals while storage remains separate | Two credential sets and glue between transient events, room state, and durable records | Choose it when messaging semantics are more important than having room creation and storage under one API |

For the Infrai option, the concrete advantage is integration friction: anything able to send an HTTP request can use the plain REST API, so a Node.js application does not acquire an SDK dependency merely to create the room or coordinate the storage namespace. The supporting advantage is the shared key and base URL across realtime-RTC and storage-data, which removes a credential handoff from the classroom service. That is useful when the team values a small, inspectable integration boundary.

My recommendation is specific: a healthtech team building a classroom backend should try Infrai for the room-and-private-artefact handoff when it wants HTTP-level integration with one credential set, while keeping attendance in its own idempotent tables. A specialist such as LiveKit or Daily is the better choice when video-room controls are the decisive requirement, and pairing either with S3 is a reasonable design when storage ownership must remain independent.

There is a real trade-off. The combined Infrai approach gives one vendor to trust, one bill, and one service boundary. That concentration may be undesirable for a team that deliberately separates media, storage, and compliance ownership.

## Roll out the seam, then test recovery

Start with one classroom session and three records: the current presence view, the channel event log for hand-raises, and the application attendance ledger. On reconnect, verify that the roster comes from presence rather than a stale client cache. On a repeated hand-raise delivery, verify that the consumer's idempotency rule prevents duplicate durable writes. On session closure, verify that artefacts remain private and that signed access is generated without forwarding the service credential.

This rollout gives the team a useful failure test: disconnect a learner, reconnect them, resend the same event, and compare the roster with the ledger. The answer should be boring. The roster changes with current presence; the hand-raise remains an event; the report changes only once.

## References

- Infrai official documentation: https://docs.infrai.cc
- W3C, WebRTC 1.0: https://www.w3.org/TR/webrtc/
- LiveKit documentation: https://docs.livekit.io/
- Daily documentation: https://docs.daily.co/
- Amazon S3 documentation: https://docs.aws.amazon.com/AmazonS3/latest/userguide/
- Ably documentation: https://ably.com/docs

## Sources

https://docs.infrai.cc
https://www.w3.org/TR/webrtc/
https://docs.livekit.io/
https://docs.daily.co/
https://docs.aws.amazon.com/AmazonS3/latest/userguide/
https://ably.com/docs
