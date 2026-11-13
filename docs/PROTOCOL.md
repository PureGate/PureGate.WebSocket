# PureGate.WebSocket wire protocol

Version 1. This is what a browser client and an ASP.NET Core server send each other over one WebSocket. The .NET package `PureGate.WebSocket` and the npm package `@puregate/web-socket` both implement it, and both test suites check the same byte-level test vectors (`test-vectors/frames.json`).

## Transport

- A WebSocket (RFC 6455) over HTTP/1.1, or over HTTP/2 (RFC 8441) where both ends support it.
- The client offers the subprotocol `puregate.v1`, and the server must select it. A server without a matching version refuses the upgrade with HTTP 400. A client whose offer comes back unselected treats that as fatal and does not reconnect.
- Everything travels in binary messages. A text message is answered by closing with 1003.
- A receiver processes a message only once it has received all of it, so a dropped connection never leaves half a message processed.
- Each side has a maximum message size. The server announces its own in Welcome (`maxMessage`) and the client in Hello. A sender never sends a larger message:
  - Starting something that would not fit fails locally with `payload_too_large`: a Request, a StreamOpen, an upload's StreamItem, or an Event from the client.
  - A reply that would not fit is replaced by a failure with `payload_too_large`: a failed Response instead of the Response, a failed StreamEnd instead of a download's StreamItem.
  - An Event from the server that would not fit is dropped for that connection and reported in the server's logs and metrics.

## Encoding primitives

There are no fixed-width integers; every number is a varint.

| Name | Encoding |
|---|---|
| `u8` | One byte. |
| `varint` | Unsigned LEB128: 7 bits per byte, least significant group first, high bit set on every byte but the last. Values are below 2^32, so at most 5 bytes. Encoders write the shortest form. Decoders reject a varint longer than 5 bytes or a value of 2^32 or more. |
| `str` | Byte length as a `varint`, then that many bytes of UTF-8. |

## Messages and frames

```
message = frame+
frame   = length:varint  kind:u8  body        length counts kind and body
```

A sender may put any number of frames in one message, up to the receiver's limit, and should coalesce frames that are ready at the same time. Receivers process frames in order.

### Frame kinds

| Kind | Name | Sent by | Layout after `kind` | Sequenced |
|---|---|---|---|---|
| 1 | Hello | client | JSON | no |
| 2 | Welcome | server | JSON | no |
| 3 | Request | both | `flags id name [timeout] [trace] payload` | yes |
| 4 | Response | both | `flags id [error] payload` | yes |
| 5 | Event | both | `flags name [trace] payload` | yes |
| 6 | StreamOpen | client | `flags id name credit:varint [trace] payload` | yes |
| 7 | StreamItem | the stream's producer | `flags id payload` | yes |
| 8 | StreamEnd | the stream's producer | `flags id [error] payload` | yes |
| 9 | Credit | both | `id:varint amount:varint` | yes |
| 10 | Cancel | both | `id:varint` | yes |
| 11 | Ack | both | `count:varint` | no |
| 12 | Ping | both | nothing | no |
| 13 | Pong | both | nothing | no |
| 14 | Authenticate | client | JSON | no |
| 15 | Authenticated | server | JSON | no |
| 16 | GoAway | server | nothing | no |

Kinds 0 and 17–255 are reserved. Receiving a reserved kind, or a kind from the wrong side, is a protocol error. A stream's producer is the server for a download and the client for an upload (see [Streams](#streams)). "Sequenced" frames are counted for acknowledgement and resume (see [Acknowledgement and resume](#acknowledgement-and-resume)).

### Flags

| Bit | Name | Meaning | Valid on |
|---|---|---|---|
| 0 | Binary | The payload is raw bytes, not JSON. | Request, Response, Event, StreamOpen, StreamItem, StreamEnd |
| 1 | HasTimeout | A `timeout:varint` field follows the name: the milliseconds the caller still waits, measured when the frame is sent. | Request |
| 2 | HasError | An `error` field follows the id. The exchange failed. | Response, StreamEnd |
| 3 | HasTrace | A `trace:str` field follows: a W3C `traceparent`. | Request, Event, StreamOpen |
| 4 | Upload | The client produces this stream's items (an upload) instead of reading them. | StreamOpen |
| 5–7 | | Reserved, must be 0. | |

A set flag that is reserved, or not valid on the frame's kind, is a protocol error. Fields appear in the order of the layout column.

### Payload

The payload is whatever is left of the frame after its fields; it has no length of its own.

- Binary flag clear: a UTF-8 JSON value without a byte order mark. An empty payload means no value (`void` in C#, `undefined` in TypeScript), which is different from the JSON `null`.
- Binary flag set: raw bytes. An empty payload is a zero-length byte array.
- On a failed Response or StreamEnd, the payload carries optional error details as JSON, or nothing.

### Ids

The side that starts an exchange picks its id: Requests and StreamOpens share one counter per direction, from 1 to 2^32 − 1. After wrapping, ids still in use are skipped. The two directions number independently, so each frame says whose exchange it means:

| Frame | Its id names an exchange started by |
|---|---|
| Request, StreamOpen, Cancel | the sender |
| Response | the receiver |
| StreamItem, StreamEnd, Credit for a stream | the client, which opens every stream |

Every exchange has a definite end, even after a Cancel, and its id is free for reuse only after that end:

- A Request ends with its Response.
- A download ends with the server's StreamEnd.
- An upload ends once the server has sent its Response and the client has sent StreamEnd or Cancel.

A Credit with id 0 is not about a stream; it returns event credit (see [Flow control](#flow-control)).

### Names

Operations and events are named with dot-separated segments, such as `orders.get`. Each segment matches `[A-Za-z_][A-Za-z0-9_]*`, and a name is 1–256 bytes. Names are compared exactly.

```
name = code:varint  [str]
```

| `code` | Meaning |
|---|---|
| `0` | Literal: the name follows. |
| odd, `2n + 1` | Definition: name number `n` is the name that follows. |
| even, `2n`, `n ≥ 1` | Reference to name number `n`; nothing follows. |

Each direction has its own table. A sender numbers definitions 1, 2, 3, … in the order it sends them, and sends names literally once it has defined 1,024. A receiver rejects a definition out of order or past 1,024, and a reference to a number not yet defined. Tables belong to the session, so they survive a resume.

### Errors

```
error = code:varint  [custom:str]  message:str
```

`code` 0 means a custom code follows as text: 1–64 bytes of `[a-z0-9_]`, chosen by the application, such as `not_found`. Otherwise it selects one of these:

| Code | Name | Meaning |
|---|---|---|
| 1 | `timeout` | The deadline passed before the handler finished. |
| 2 | `cancelled` | The caller cancelled, or the handler was cancelled. |
| 3 | `unauthorized` | The connection is not authenticated, or its authentication expired. |
| 4 | `forbidden` | Authenticated, but the operation's authorization policy refused it. |
| 5 | `unknown_operation` | No handler is registered for the name. |
| 6 | `invalid_payload` | The payload could not be read as the operation's request type. |
| 7 | `payload_too_large` | The payload exceeds a limit. |
| 8 | `overloaded` | The receiver refused the work: rate limit, memory budget or queue. |
| 9 | `handler_failed` | The handler threw. The message is generic unless the server includes exception details. |
| 10 | `connection_closed` | The connection closed before the exchange finished. Mostly produced locally. |
| 11 | `protocol_error` | The peer broke this protocol. Produced locally. |

`message` is human-readable text and may be empty.

## Handshake

1. The client opens the WebSocket with the subprotocol `puregate.v1`. The server checks the `Origin` header and any authentication on the upgrade request (cookies, for instance) before accepting. A failure here is an ordinary HTTP 401 or 403, and no WebSocket is opened.
2. The client's first frame is Hello. Right after Hello, in the same message or later, it may send Requests, Events and StreamOpens without waiting for Welcome, within the initial windows below. A client that is resuming waits for Welcome instead.
3. The server answers with Welcome before any other frame, then processes the frames that followed Hello.

A Hello that fails closes the connection with the matching close code (see [Closing](#closing)), and frames sent after it are discarded.

### Hello (client → server), JSON

| Field | Type | Meaning |
|---|---|---|
| `token` | string, optional | A credential for the server's token authenticator, for apps that cannot use cookies. |
| `contract` | string, optional | The client's contract hash (see the server plan). |
| `resume` | `{ session: string, received: number }`, optional | Resume this session. `received` is the number of sequenced frames the client has processed. |
| `calls` | number | How many server → client Requests the client accepts at once. At least 1. |
| `maxMessage` | number | The largest message the client accepts, in bytes. |

### Welcome (server → client), JSON

| Field | Type | Meaning |
|---|---|---|
| `connection` | string | This connection's id, for logs and support. |
| `session` | string | The session id, an opaque secret of at least 128 random bits. |
| `resumed` | boolean | Whether the session in Hello's `resume` was resumed. |
| `received` | number | When resumed: the sequenced frames the server has processed. |
| `calls` | number | How many client → server Requests may be outstanding. At least 4. |
| `streams` | number | How many streams may be open at once. At least 1. |
| `events` | number | The client → server event window. At least 16. |
| `maxMessage` | number | The largest message the server accepts, in bytes. |
| `heartbeat` | number | Milliseconds (see [Liveness](#liveness)). |
| `resumeWindow` | number | Milliseconds a dropped session stays resumable. 0 means resume is off. |
| `contract` | `"current"` or `"stale"`, optional | Present when Hello carried a contract hash: whether it matches the server's. |
| `expires` | number, optional | Milliseconds until the connection's authentication expires (see [Re-authentication](#re-authentication)). Absent when it doesn't expire. |
| `route` | string, optional | This server instance's routing id, for resuming behind a load balancer (see [Acknowledgement and resume](#acknowledgement-and-resume)). Not a secret. |

Before Welcome arrives, the client may use 4 calls, 1 stream and 16 events, the minimums every server advertises.

## Re-authentication

A connection's authentication can expire, for example when it was made with a short-lived token. Welcome's `expires` says when.

- The client refreshes before then by sending Authenticate, JSON `{ "token": "…" }`. The server validates the token as it validated Hello's and answers with Authenticated, JSON `{ "expires": … }`, where `expires` is again optional. Other frames keep flowing and use the current identity until the refresh succeeds.
- The refreshed token must be for the same user. A token for another user, or one that fails validation, closes the connection with 4001.
- At expiry without a refresh, the server closes with 4001. The session stays resumable, so a client that can present a fresh credential reconnects and resumes without losing anything.
- Authenticate and Authenticated are not sequenced. A resume re-authenticates through Hello instead of replaying them.

## Streams

The client opens every stream with StreamOpen, so a stream's id is always the client's.

- **Download** (Upload flag clear): the server produces the items.
  - StreamOpen carries the request payload and the client's initial credit.
  - The server sends StreamItems within credit and ends with exactly one StreamEnd. The client grants more credit as the application reads.
  - After a Cancel from the client, the server ends the stream with a StreamEnd that fails with `cancelled`.
- **Upload** (Upload flag set): the client produces the items.
  - StreamOpen carries the request payload, and its `credit` field must be 0. The server grants credit as its handler reads, starting when the handler begins.
  - The client sends StreamItems within credit, then ends with StreamEnd (a failed one if its source failed), or gives up with Cancel.
  - The server answers with exactly one Response, the handler's result. If the handler finishes before the client's StreamEnd, the Response comes first: the client then stops and sends StreamEnd, and the server discards items already on the way.

## Flow control

Every window is a promise the receiver makes and the sender keeps. Going past one is a protocol error.

- **Calls.** At most `calls` Requests outstanding, each freed by its Response. This applies in both directions: the server's `calls` from Welcome, the client's from Hello. No Credit frames are needed.
- **Streams.** At most `streams` streams open at once, downloads and uploads together.
- **Client events.** At most `events` Events sent and not yet credited. The server returns credit with a Credit frame (id 0, amount = count) as its handlers pick events up, about every quarter of the window.
- **Stream items.** A producer never sends more StreamItems than it has credit for, and Credit frames (id = stream id) grant more.
  - A download's StreamOpen carries the client's initial credit, in items. The client grants more as the application takes items, about every half of its buffer.
  - An upload starts with no credit. The server grants it as its handler reads.
- **Server events** have no window. The browser WebSocket API cannot slow the network down: it queues messages for the page as fast as they arrive. So the server watches acknowledgements instead (see below) and applies its slow-consumer policy.

## Acknowledgement and resume

**Counting.** Each side counts the sequenced frames it sends and the ones it processes, per session. The client counts from the first sequenced frame after Hello, and the server from the first after Welcome. A receiver adds a message's frames to its count only once it has processed the whole message.

**Ack.** A receiver with unacknowledged frames sends Ack with its count (modulo 2^32). It does so no later than 64 frames, or one second, after the first frame it has not acknowledged. The client acknowledges frames only after it has dispatched them to the page. That makes the server's unacknowledged count a measure of how far behind the page is. The server closes a consumer that falls too far behind with 4003, according to its policy.

**Resume.** When Welcome's `resumeWindow` is above 0, both sides keep every sequenced frame they sent until it is acknowledged. After the connection drops, the client may reconnect within `resumeWindow` and send Hello with `resume`. If the session is still there and the client authenticates as the same user, the server answers with Welcome `resumed: true` and its own `received` count. Then each side resends, in order, every sequenced frame after the other side's count, and carries on. Ids, name tables, windows, credits and outstanding exchanges carry over unchanged.

If the session expired, or a frame needed for the replay was already discarded, the server answers with `resumed: false` and a new session. The client then fails its outstanding exchanges with `connection_closed`, and the server ends the old session.

A session has at most one connection. A resume while an older connection of the session is still open closes the old one with 4005.

**While disconnected.** A resumable session keeps running on the server while it has no connection. Handlers finish, and everything the session sends goes into the replay buffer, to go out when the client resumes: Responses, stream items within credit, events. When the buffer is full, the session can no longer resume, and the server ends it.

Frames the client creates while disconnected are new to the server. They wait in the client's queue and go out after the next Welcome, whether or not the session resumed.

**Several server instances.** A session lives in one server instance, so a resume must reach that instance. There are two ways:

- the load balancer keeps each browser on one instance with cookie affinity, which works because browsers send cookies with the upgrade;
- the server sends `route` in Welcome, the client adds it to the reconnect URL as the query parameter `route`, and the load balancer routes on it.

A resume that reaches another instance fails safely: that instance doesn't know the session and answers `resumed: false`.

## Liveness

Welcome's `heartbeat` (15 seconds by default) sets the pace. The client sends Ping when it has received nothing for `heartbeat`. If still nothing arrives within another `heartbeat`, it closes the connection and reconnects. The server answers every Ping with Pong. On its own side, the server also sends WebSocket ping frames, which browsers answer automatically even while the page is busy.

## Shutting down

A server that is stopping, for example during a deploy, sends GoAway on every connection. After GoAway:

- **The server** starts nothing new on the connection: no events, no server → browser calls. It still handles the frames that arrive and finishes the exchanges in progress. Then it closes with 1001, at the latest when its drain timeout passes.
- **The client** starts nothing new on the connection. It opens a new connection right away and sends new calls and events there, while the old connection finishes its exchanges.

Sessions are held in the server's memory and end with it, so the new connection starts a new session (`resumed: false`).

## Closing

Either side ends the connection with a WebSocket close frame. Outstanding exchanges that don't resume fail with `connection_closed`.

| Code | Meaning | Client reconnects |
|---|---|---|
| 1000 | Normal close. | no |
| 1001 | Going away: a server shutting down after GoAway, or the page leaving. | yes |
| 1003 | A text message was received. | no |
| 1006 | Abnormal close, no close frame (network loss). | yes |
| 1009 | A message exceeded the receiver's limit. | no |
| 1011 | Unexpected server error. | yes |
| 4000 | `protocol_error`: malformed frame, broken window, reserved kind or flag. | no |
| 4001 | `unauthorized`: authentication failed, or expired without a refresh. After an expiry the session stays resumable. | once, resuming: with a fresh token if the client has a token provider, otherwise with the browser's current cookies |
| 4002 | `handshake_timeout`: no Hello in time. | yes |
| 4003 | `slow_consumer`: the client fell too far behind. | yes, resuming if it can |
| 4004 | `contract_rejected`: the server refuses stale contracts. | no |
| 4005 | `session_replaced`: another connection resumed this session. | no |

## Example

The client calls `orders.get` with `42`, sending Hello and the Request in one message:

```
20 01 {"contract":"9f3a1c0b7d2e4a65"}                 Hello, length 32
11 03 00 01 03 0A "orders.get" "42"                   Request: length 17, flags 0, id 1, defines name 1
```

The server coalesces Welcome and the Response:

```
LL 02 {"connection":"…","session":"…",…}             Welcome
18 04 00 01 {"id":42,"total":9.5}                     Response: length 24, flags 0, id 1
```

The second call to `orders.get` refers to name 1, and a third waits at most 5,000 ms:

```
05 03 00 02 02 "7"                                    Request: id 2, name reference 1
07 03 02 03 02 88 27 "7"                              Request: HasTimeout, id 3, timeout 5000
```

The handler fails with an application code:

```
1E 04 04 03 00 09 "not_found" 0F "Order 7 missing"    Response: HasError, id 3, custom code
```
