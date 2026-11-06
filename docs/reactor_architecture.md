# Reactor foundations

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
are read before `recv` returns zero. A Channel invokes at most one callback per
epoll event, and keeps a local callback copy while dispatching. This lets a
SubReactor close a connection from inside its readable callback without
leaving EventLoop with a dangling Channel pointer.

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

Handlers run concurrently and have no global ordering guarantee. Ordering that
matters to an application must use `connection_id` and `sequence`. Queue
submission failure applies backpressure by closing that connection.

`FrameMessage::connection` is a `ConnectionHandle`. Workers call `Respond()` to
move an application payload into the bound response queue of the owning
SubReactor. The handle is weak: responses for a closed connection fail instead
of writing to an fd that may have been reused.

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
