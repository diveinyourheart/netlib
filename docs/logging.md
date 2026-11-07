# Logging

`netlib::log` is a separate static library and does not depend on the Reactor or
ThreadPool implementation.

## Data path

```text
producer thread
  -> collect timestamp, OS tid, sequence, source location
  -> level short-circuit
  -> compile-time checked fmt formatting
  -> move formatted message into LogRecord
  -> bounded moodycamel ConcurrentQueue

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
- per-logger acceptance sequence number
- OS thread ID
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

Admission, sequence assignment and queue publication share a short mutex with
Flush snapshots and Stop. The mutex is never held during sink I/O, formatting,
or a completion wait. Producers retain independent moodycamel queues; correctness
does not depend on their dequeue order.

A successful Log accepts one gap-free sequence. Flush snapshots the accepted
count under the mutex, then waits for that fixed prefix to be written and a
subsequent Sink::Flush to finish. Logs whose calls have returned true before
Flush starts are included. Log calls overlapping the snapshot are included if
accepted before it; formatting or a not-yet-admitted call is not an accepted log.
Rejected/disabled logs are excluded. Later producers can continue, and do not
extend a pending Flush target. Flush does not guarantee exclusion of later logs.

The writer tracks each outstanding sequence in a bounded completion window and
advances its prefix only through contiguous completed records. It services ready
flush requests between records, so a control request never blocks the writer
from consuming the very logs needed to satisfy it. Queue allocation failures
roll back admission without creating missing sequence numbers. The capacity
bounds the entire outstanding completion window (including a record being
written and out-of-order completions), not just the physical queue length.
queued_count counts accepted records not yet written.

Stop closes admission under the same mutex, stops the timer, drains all accepted
logs, completes already registered Flush calls, performs a final flush and joins
the writer. Concurrent Stop callers all wait for shutdown. No allocating stop
message is required. Calls after Stop are rejected. Keep the Logger alive until
all producer calls have returned; racing destruction is not supported.

Sink exceptions stop admission and wake pending Flush callers with false.
Stop still joins. Sink Write/Flush must not reenter this Logger's Flush/Stop.
The built-in StdoutSink uses fflush, not fsync/FlushFileBuffers; successful
completion is not a power-loss durability guarantee. Its existing void Flush
interface also does not report FILE* write/fflush return codes.

## Periodic flushing

LoggerOptions::flush_interval defaults to 1000 milliseconds. Zero disables it;
negative intervals are rejected. Each enabled logger owns a TimerScheduler
thread in addition to its writer. The timer only registers a nonblocking flush
request; all sink operations still run serially on the writer. At most one
unserviced periodic request is retained, with its first target kept fixed.

WARN and higher levels, and flush_each_record, retain their immediate writer-side
flush behavior. A periodic interval is a scheduling target, not a maximum
delivery latency: slow sinks/backlogs delay completion. A full outstanding
window rejects new logs instead of waiting for I/O.

See [timer.md](timer.md) for the reusable scheduler and future Reactor integration.
