# Logging

`netlib::log` is a separate static library and does not depend on the Reactor or
ThreadPool implementation.

## Data path

```text
producer thread
  -> collect timestamp, Linux tid, sequence, source location
  -> level short-circuit
  -> compile-time checked fmt formatting
  -> move formatted message into LogRecord
  -> bounded moodycamel BlockingConcurrentQueue

Logger writer thread
  -> bulk dequeue
  -> LogSink::Write
  -> StdoutSink formatting and output
```

The producer never waits for terminal or network I/O. A full queue drops the new
record, increments `dropped_count()`, and reports the pending drop count on the
next successfully queued record.

The macros use fmtlib 11.2.0 with compile-time format checking. Formatting is
performed on the producer thread in one pass and the resulting string is moved
into the queued `LogRecord`. Disabled levels skip formatting and message
allocation. Dynamic format strings can use `Logger::Log()` with an already
formatted `std::string_view`.

## Record fields

Every standard output line contains:

- local timestamp with millisecond precision
- level name aligned for scanning
- process-wide sequence number
- Linux thread ID
- logger name
- source file, line, and function
- optional dropped-record count
- sanitized message

Multi-line messages retain their line structure with aligned continuation
prefixes. ANSI escape and other control bytes from message content are rendered
as visible `\xHH` text.

## Colors

`StdoutSink` uses these level colors:

- TRACE: dim gray
- DEBUG: cyan
- INFO: green
- WARN: yellow
- ERROR: red
- FATAL: bold red

`ColorMode::kAuto` enables color only when the output stream is a TTY. Use
`kAlways` for terminal tooling and `kNever` for redirected output and tests.

## Extension points

Future file, network, or shared-memory outputs implement `LogSink`. The Logger
calls a Sink from one writer thread, so a Sink does not need an internal output
lock. A Sink that stores records beyond `Write()` must copy owned data because
`logger_name` is a view into the Logger.

## Flush and shutdown

`Flush()` obtains a monotonically increasing ticket from `next_flush_id_` and
enqueues a control item carrying that ticket. The writer calls
`LogSink::Flush()` and advances the monotonic `completed_flush_id_` watermark.
Waiters sleep until the watermark reaches their ticket.

Flush and stop control items use one explicit moodycamel producer token while
the lifecycle mutex serializes command submission. Concurrent flushes therefore
have a consistent control-channel order, and the completion watermark never
moves backwards.

Flush is a queue barrier. Records submitted concurrently by other producers can
appear after the flush marker. `Stop()` rejects later flushes, drains previously
queued records, flushes the Sink, and joins the writer thread. Application
producers should stop logging before `Stop()`.
