# Reactor foundations

The epoll/eventfd details below describe the original Linux backend. Windows
uses the same Reactor lifecycle through Poller/Wakeup; see [platform.md](platform.md).
Ready events now carry registration tokens. Both read and write callbacks can
run for one ready event, with registration rechecked between callbacks.

The first layer keeps descriptor ownership, event registration, and thread
handoff separate:

```text
MainReactor
  EventLoop
    listener Channel -> listen fd
  accept4 loop
    -> UniqueFd
    -> round-robin SubReactor selection
    -> SubReactor::AdoptConnection

SubReactor
  pending queue (mutex protected)
  eventfd (EPOLLIN | EPOLLET)
  EventLoop
    client Channels -> client fds

complete Frame
  -> FrameMessage (owned payload)
  -> moodycamel MPMC queue
  -> ThreadPool workers

worker response
  -> ConnectionHandle
  -> SubReactor response queue
  -> per-connection sequence reorder buffer
  -> codec encode
  -> output buffer
  -> send until EAGAIN
```

## Ownership

`UniqueFd` owns a descriptor. A `Channel` is a non-owning registration view:
the descriptor owner must outlive the Channel, and the EventLoop must outlive
both. Reactors arrange their members in that order so Channels are removed
before descriptors close.

MainReactor owns the listening descriptor and its Channel, but keeps non-owning
pointers to SubReactors. The server orchestrator must keep every SubReactor
alive until MainReactor has stopped. A successful `accept4` creates a
`UniqueFd`; MainReactor uses round-robin selection and moves that descriptor
into the selected SubReactor. If the selected queue is full, MainReactor tries
the remaining SubReactors. When every queue is full, the descriptor closes at
the end of the accept iteration.

## Thread handoff

The MainReactor thread appends an accepted descriptor to the SubReactor queue
under a mutex and writes one `uint64_t` to the SubReactor eventfd. That eventfd
is registered in the SubReactor EventLoop as `EPOLLIN | EPOLLET`. The kernel
wakes that EventLoop, whose Channel callback reads the eventfd until `EAGAIN`,
swaps out the whole pending queue, and registers every connection in the
SubReactor epoll instance. The eventfd is only a doorbell; descriptor ownership
moves through the queue.

The queue is bounded. `AdoptConnection` moves ownership only on success. On
failure MainReactor keeps ownership and tries another SubReactor; if all are
full, it closes the connection. This is the current explicit overload policy.

The same eventfd also wakes the SubReactor for worker responses. Its wake
callback drains the eventfd to `EAGAIN`, drains pending connections, then drains
the MPMC response queue. One doorbell handles both control directions while
the payloads remain in their independent bounded queues.

## Event processing

All Channels are edge triggered. Readable callbacks drain accept or `recv`
until `EAGAIN`. `EPOLLRDHUP` is included in the readable path so existing bytes
are read before `recv` returns zero. EventLoop dispatches both readable and writable readiness when requested, rechecking the registration token between callbacks. Callback copies remain alive even if a callback removes its Channel.

## Framing

Each connection owns a `ProtocolCodec` instance, one reusable input buffer, one
output buffer, and one stable connection ID. The default codec wire format is:

```text
+--------------------------+-------------------+
| 4-byte big-endian length | length-byte payload |
+--------------------------+-------------------+
```

The length excludes the four-byte prefix. A payload may be empty, but its
length cannot exceed `kMaxFramePayloadSize`, currently 16 MiB.

`ProtocolCodec::Decode` returns `NeedMore`, `FrameReady`, or `ProtocolError`.
For a complete frame it returns consumed bytes, payload offset, and payload
size. SubReactor consumes only the declared bytes and immediately decodes any
remaining coalesced frame. A custom HTTP codec can therefore own its own
request parsing state.

`MaxDecodeBufferSize()` bounds incomplete input assembly. `Encode()` enforces
the codec's own response-payload limit. Output backlog uses the separate
`SubReactorOptions::max_pending_output_bytes` policy because multiple encoded
responses may be waiting for a slow client.

`DecodeResult::required_bytes` lets a stateful codec communicate the next known
read target without exposing its internal state to Connection. Zero means the
target is not yet knowable.

Length-prefixed framing uses two states:

```text
read 4-byte length -> required_bytes = 4
read payload       -> required_bytes = 4 + payload_length
```

An HTTP/1.1 codec can use these states:

```text
scan for CRLFCRLF  -> required_bytes = 0
parse Content-Length
read body          -> required_bytes = header_end + content_length
```

Chunked transfer adds states for chunk-size lines, chunk data, CRLF, and the
zero-length trailer. Every state returns either NeedMore, FrameReady, or
ProtocolError and may provide a new required_bytes target.

## Protocol messages

Network reading, protocol parsing, business processing, and response writing
are separate stages. The non-template SubReactor owns the byte buffers and
Channel. A per-connection `ProtocolCodec` produces a `DecodeResult` with payload
offset and typed `FrameMetadata`. The ThreadPool handler receives the common
`FrameMessage` envelope:

```text
connection handle
request sequence
FrameMetadata variant
body payload bytes
```

`FrameMetadata` currently contains `BinaryFrameMetadata` and
`HttpRequestMetadata`. Responses use
`ApplicationResponse = BinaryResponse | HttpResponse`. This typed variant avoids
RTTI and avoids compiling a separate Reactor and ThreadPool for every protocol.
Adding a protocol requires a new metadata/response alternative and Codec, while
read/write buffering and worker scheduling remain unchanged.

`Http1Codec` parses HTTP/1.0 and HTTP/1.1 request lines and headers, extracts
Content-Length and keep-alive, and emits the body through the normal payload
path. `HttpResponse` encoding adds Content-Length and Connection headers. Chunked
request framing returns ProtocolError in this first implementation; its state
machine can be extended without changing SubReactor.

`recv()` writes directly into the connection buffer. A complete payload is
copied once into an owned `FrameMessage` and submitted to the ThreadPool.
Reusable input capacity remains attached to the connection and is compacted
only when the unconsumed tail has insufficient writable space.

## Worker dispatch

SubReactor copies each complete payload into a `FrameMessage` before enqueueing
it. The connection buffer can therefore be consumed and reused immediately.
Each message carries a process-unique `connection_id` and a per-connection
`sequence`.

ThreadPool uses `moodycamel::BlockingConcurrentQueue` as an MPMC queue. Every
Worker keeps an explicit consumer token. Queue occupancy is bounded by an
atomic reservation count so that a disconnected or slow Worker subsystem cannot
grow memory without limit. Dequeue releases a slot before the handler runs, so
the configured limit covers queued work while a bounded number of messages can
also be in flight in Worker calls.

Handlers run concurrently and have no global completion-order guarantee.
SubReactor therefore keeps a bounded reorder buffer per connection and only
appends responses whose sequence is the connection's next expected sequence.
This preserves request order on HTTP/1.1 pipelines and on binary connections
with multiple in-flight requests. Queue submission failure applies backpressure
by closing that connection.

`FrameMessage::connection` is a `ConnectionHandle`. Workers call `Respond()` to
move an application payload and its request sequence into the bound response
queue of the owning SubReactor. Every dispatched frame requires exactly one
accepted response with the same sequence. A missing sequence stalls later
responses for that connection. Duplicate, unknown, or already closed responses
are discarded. The handle is weak: a response for a closed connection fails
instead of writing to an fd that may have been reused.

`SubReactorOptions::max_pending_responses` bounds both the global response
queue and each connection's out-of-order buffer. Exceeding the per-connection
bound closes that connection because its application-level ordering contract
can no longer be maintained safely.

SubReactor drains responses on its own thread, encodes them using the
connection's `ProtocolCodec`, and appends them to the output buffer. It writes
nonblocking until `EAGAIN`; only then does the Channel add `EPOLLOUT`. Once the
buffer drains, write interest is removed. `close_after_write` closes the
connection only after all queued output is flushed.

Producers must stop before `ThreadPool::Stop()`. Stop enqueues one sentinel per
Worker, waits for earlier messages to drain, and joins all Worker threads.

## Lifecycle

`EventLoop::Run()` executes on one owner thread. `Stop()` may be called from
another thread. The owner must stop the loop and join that thread before
destroying the EventLoop or either Reactor. Creating, changing, or destroying a
Channel happens on the EventLoop thread; other threads use
`SubReactor::AdoptConnection` as the handoff boundary.

Producers must be stopped before calling `SubReactor::Stop()`. The ThreadPool
must outlive every SubReactor that references it. An
`AdoptConnection` call that raced with shutdown may have returned true and then
lost its queued descriptor when the reactor was destroyed.

Factory functions return null on setup failure. `MainReactor` owns the listening
socket, while `SubReactor` owns eventfd, pending descriptors, and accepted
connections. This member order keeps dependent Channels alive only while their
descriptors and EventLoop are valid.

Business decoding and execution occur in the ThreadPool handler.
`FrameMessage::payload` is a complete transport payload, not a decoded protobuf,
JSON, or application request.

## Logging

Reactor and ThreadPool factories accept an optional non-owning `log::Logger*`.
The orchestrator must keep the Logger alive until MainReactor, every
SubReactor, and the ThreadPool have stopped. Logs cover lifecycle transitions,
epoll registration failures, accept/recv errors, dispatch failures, oversized
frames, and worker-queue backpressure. Individual epoll events and successful
reads stay out of INFO logging to protect the I/O hot path.
