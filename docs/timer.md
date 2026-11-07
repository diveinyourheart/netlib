# 可复用定时器

`netlib::timer` 是独立 C++20 静态库，不依赖网络和日志库。
`TimerScheduler` 使用 `steady_clock`、连续存储最小堆和 TimerId 索引和条件变量，
一个后台线程依次执行到期回调；不会每个定时器创建一个线程，也不会忙轮询。
仅在插入更早任务、删除堆顶或停止时唤醒等待线程；其他插入和取消不触发通知。

```cpp
#include "netlib/timer.h"
using namespace std::chrono_literals;

netlib::TimerScheduler timers;
netlib::TimerScheduler::TimerId timeout = timers.ScheduleAfter(5s, [] { /* 提交超时处理任务 */ });
netlib::TimerScheduler::TimerId periodic = timers.ScheduleEvery(1s, [] { /* 提交周期工作 */ });
timers.Cancel(timeout);
timers.Cancel(periodic);
timers.Stop();
```

也可用 `ScheduleAt(steady_clock::time_point, callback)` 指定截止时间。
空回调、非正周期或停止后提交返回 `kInvalidTimer`（0）。一次性过去的截止时间
按立即到期处理。周期采用固定延迟：回调完成后再等待一个 interval；不会补发错过的 tick。

回调在调度线程串行执行，允许重入 Schedule/Cancel；必须尽快返回且不得抛异常。
定时器无 try/catch/throw 和异常恢复路径，接口使用 noexcept。无效参数返回 0/false；
内存、线程等系统资源耗尽采用 fail-fast，不伪装为正常取消。调用方构造 std::function
发生的分配不在接口内部。取消会释放尚未执行的回调资源；
已经被调度线程选中的回调仍可能执行完，后续周期不会重排。需要等待运行中回调结束时，
由外部调用 Stop：它取消待执行任务并 join，多个 Stop 调用都等待完成。
回调内调用 Stop 只请求停止，之后仍须外部 Stop 或析构来 join；不能在回调内销毁调度器。

日志默认拥有一个调度器，周期回调仅登记异步刷新目标，实际刷新由日志写线程完成。
慢 I/O 不会占用定时器线程或日志提交锁。

## 后续连接超时接入

本次未改变连接行为。网络层后续可用连接 ID、请求 sequence 和弱生命周期引用登记
超时任务。正常响应后取消对应 TimerId；超时回调向所属 Reactor 的任务队列提交操作，
由 Reactor 线程再次检查该请求是否仍未完成，再决定关闭连接。
取消可能与正在运行的回调竞态，所以必须有这次状态复核，不能仅依赖 Cancel 返回值。
不要在定时器线程直接访问或关闭 SubReactor 管理的连接；当前 EventLoop 还需补充安全的
跨线程任务提交接口，才能完整接入该用法。

## 性能设计
添加、到期取出和取消均为 O(log n)，ID 查找平均 O(1)。最小堆连续存储任务，
不使用 shared_ptr 或红黑树节点。周期重排复用原有 ID 索引节点；固定任务规模下
不会为每个周期重新分配容器节点。容量增长和较大 std::function 捕获仍可能分配内存。
任务回调及捕获对象的正常释放都在锁外执行；慢回调仍会推迟其他到期任务。

复测单生产者的 10 万定时任务添加和随机取消（不含回调执行时间）：

```powershell
g++ -std=c++20 -O2 -fno-exceptions -pthread -Iinclude benchmarks/timer_benchmark.cc src/timer.cc -o build/timer-benchmark.exe
./build/timer-benchmark.exe
```

本机 MinGW 15.2、相同 O2 参数、三轮中位数：原 map/shared_ptr 版本添加约
96ms、取消约 253ms；最小堆版本约 22ms、42ms。该微基准不代表多线程吞吐、
实际回调延迟或所有工作负载；保留程序便于后续用实际负载继续验证。
