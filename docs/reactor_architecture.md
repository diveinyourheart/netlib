# Reactor foundations

The first layer keeps descriptor ownership, event registration, and thread
handoff separate:

```text
MainReactor
  EventLoop
    listener Channel -> listen fd
  accept4 loop
    -> UniqueFd
    -> SubReactor::AdoptConnection

SubReactor
  pending queue (mutex protected)
  eventfd (EPOLLIN | EPOLLET)
  EventLoop
    client Channels -> client fds
```

## Ownership

`UniqueFd` owns a descriptor. A `Channel` is a non-owning registration view:
the descriptor owner must outlive the Channel, and the EventLoop must outlive
both. Reactors arrange their members in that order so Channels are removed
before descriptors close.

MainReactor owns the listening descriptor and its Channel. A successful
`accept4` creates a `UniqueFd` and transfers ownership to the accept handler.
SubReactor takes that `UniqueFd` into its pending queue, then stores it together
with the client Channel after the loop thread drains the queue.

## Thread handoff

The producer appends an accepted descriptor to the SubReactor queue under a
mutex and writes the eventfd. The consumer runs on the SubReactor thread: it
reads the eventfd to `EAGAIN`, swaps out the whole queue, and registers every
connection. The eventfd is only a doorbell; descriptor ownership moves through
the queue.

The queue is bounded. `AdoptConnection` returns false at capacity and the
caller's `UniqueFd` closes the descriptor on return. A main reactor can use
that failure to pause accepting or apply another explicit overload policy.

## Event processing

All Channels are edge triggered. Readable callbacks drain accept or `recv`
until `EAGAIN`. `EPOLLRDHUP` is included in the readable path so existing bytes
are read before `recv` returns zero. A Channel invokes at most one callback per
epoll event, and keeps a local callback copy while dispatching. This lets a
SubReactor close a connection from inside its readable callback without
leaving EventLoop with a dangling Channel pointer.

## Lifecycle

`EventLoop::Run()` executes on one owner thread. `Stop()` may be called from
another thread. The owner must stop the loop and join that thread before
destroying the EventLoop or either Reactor. Creating, changing, or destroying a
Channel happens on the EventLoop thread; other threads use
`SubReactor::AdoptConnection` as the handoff boundary.

Producers must be stopped before calling `SubReactor::Stop()`. An
`AdoptConnection` call that raced with shutdown may have returned true and then
lost its queued descriptor when the reactor was destroyed.

Factory functions return null on setup failure. `MainReactor` owns the listening
socket, while `SubReactor` owns eventfd, pending descriptors, and accepted
connections. This member order keeps dependent Channels alive only while their
descriptors and EventLoop are valid.

Protocol framing and business execution are intentionally outside this layer.
`SubReactor::ConnectionCallbacks::on_data` receives a byte view, not a complete
application message.
