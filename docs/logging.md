# Logging

`netlib::log` is a separate static library and does not depend on the Reactor or
ThreadPool implementation.

中文状态与流程图解：[日志状态与刷新水位](logging_state.md)，包含多线程共用单 producer、
固定 ProducerToken、accepted_/completed_ 双水位，以及 Flush 目标达成的过程。

## Data path

```text
producer thread
  -> level short-circuit
  -> compile-time checked fmt formatting
  -> collect timestamp, OS tid, source location
  -> move formatted message into LogRecord
  -> under the admission mutex: assign sequence, enqueue using producer_
  -> bounded moodycamel ConcurrentQueue

Logger writer thread
  -> bulk dequeue
  -> LogSink::Write
  -> StdoutSink formatting and output
```

The producer may briefly contend on the admission mutex, but does not wait for
sink I/O or a Flush to finish. A full outstanding-record budget drops the new record,
increments `dropped_count()`, and reports the pending drop count on the next
successfully queued record.

The macros use fmtlib 11.2.0 with compile-time format checking. Formatting is
performed on the producer thread in one pass and the resulting string is moved
into the queued `LogRecord`. Disabled levels skip formatting and message
allocation. Dynamic format strings can use `Logger::Log()` with an already
formatted `std::string_view`.

## Default Logger

`Logger::Initialize` and `Logger::InitializeStdout` configure one process-wide
Logger. Initialization succeeds at most once; attempts to replace it fail. The
Logger state and its detached Writer thread intentionally live until process
exit. `NETLIB_LOG_*` uses this singleton and takes no logger argument.

Typical service setup:

```cpp
Logger::InitializeStdout(options);
NETLIB_LOG_INFO("service starting");

// Run the service.

Logger::Flush();
```

`Flush` is a barrier for accepted records but does not close admission or destroy
the Writer thread.

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

All other metadata is colored as well:

- timestamp, separators, and multi-line continuation prefixes: gray
- sequence number (`seq`): blue
- OS thread ID (`tid`): magenta
- dropped-record count (`drops`), when present: yellow
- logger name: cyan
- source file, line, and function: bright blue

Styles are reset between fields and before each business-message line, so the
message uses the terminal's default style and never inherits the level color.

`ColorMode::kAuto` enables color only when the output stream is a TTY. Use
`kAlways` for terminal tooling and `kNever` for redirected output and tests.

## Extension points

Future file, network, or shared-memory outputs implement `LogSink`. The Logger
calls a Sink from one writer thread, so a Sink does not need an internal output
lock. A Sink that stores records beyond `Write()` must copy owned data because
`logger_name` is a view into the Logger.

## Flush and shutdown

Admission, sequence assignment and queue publication share a short mutex with
Flush snapshots. The mutex is never held during sink I/O, formatting, or a
completion wait. Every producer thread uses the same explicit ProducerToken
under this mutex. The queue disables implicit producers, so the writer receives
records in acceptance-sequence order from a single producer stream.

A successful Log accepts one gap-free sequence. Flush snapshots the accepted
count under the mutex, then waits for that fixed prefix to be written and a
subsequent Sink::Flush to finish. Logs whose calls have returned true before
Flush starts are included. Log calls overlapping the snapshot are included if
accepted before it; formatting or a not-yet-admitted call is not an accepted log.
Rejected/disabled logs are excluded. Later producers can continue, and do not
extend a pending Flush target. Flush does not guarantee exclusion of later logs.

The writer increments completed_ after each record is written; no completion
bitmap is needed. It services ready flush requests between records. Enqueue
failures leave accepted_ unchanged, without creating missing sequence numbers.
The capacity bounds accepted_ - completed_, including records already dequeued
into a batch or currently being written. queued_count() returns this difference.

There is no Stop operation. Admission remains open for the process lifetime, and
the detached Writer keeps consuming records. Before process exit, producers
should stop submitting and call `Flush()` to wait for all accepted records and a
Sink Flush. Process teardown does not join the Writer or run another implicit
final flush.

Sink exceptions mark the shared Logger failed, reject later logs, and wake
pending Flush callers with false. The detached Writer exits; no join is
performed. Sink Write/Flush must not reenter Logger::Flush.
The built-in StdoutSink uses fflush, not fsync/FlushFileBuffers; successful
completion is not a power-loss durability guarantee. Its existing void Flush
interface also does not report FILE* write/fflush return codes.

## Periodic flushing

LoggerOptions::flush_interval defaults to 1000 milliseconds. Zero disables it;
negative intervals are rejected. The singleton Logger registers a periodic
TimerId on the process-wide `TimerScheduler`. The timer only registers a
nonblocking flush request; all sink operations still run serially on the
Writer. At most one unserviced periodic request is retained, with its first
target kept fixed.

WARN and higher levels, and flush_each_record, retain their immediate writer-side
flush behavior. A periodic interval is a scheduling target, not a maximum
delivery latency: slow sinks/backlogs delay completion. A full outstanding
window rejects new logs instead of waiting for I/O.

See [timer.md](timer.md) for the reusable scheduler and future Reactor integration.
