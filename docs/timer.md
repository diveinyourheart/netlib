# 可复用定时器

`netlib::timer` 是独立 C++20 静态库，不依赖网络和日志库。
`TimerScheduler` 使用 `steady_clock`、连续存储最小堆和 TimerId 索引和条件变量，
一个后台线程依次执行到期回调；不会每个定时器创建一个线程，也不会忙轮询。
只在插入更早任务或删除堆顶时唤醒等待线程；其他插入和取消不触发通知。

```cpp
#include "netlib/timer.h"
using namespace std::chrono_literals;

netlib::TimerScheduler::TimerId timeout =
    netlib::TimerScheduler::ScheduleAfter(5s, [] { /* 提交超时处理任务 */ });
netlib::TimerScheduler::TimerId periodic =
    netlib::TimerScheduler::ScheduleEvery(1s, [] { /* 提交周期工作 */ });
netlib::TimerScheduler::Cancel(timeout);
netlib::TimerScheduler::Cancel(periodic);
```

也可用 `ScheduleAt(steady_clock::time_point, callback)` 指定截止时间。
空回调或非正周期返回 `kInvalidTimer`（0）。一次性过去的截止时间
按立即到期处理。周期采用固定延迟：回调完成后再等待一个 interval；不会补发错过的 tick。

回调在调度线程串行执行，允许重入 Schedule/Cancel；必须尽快返回且不得抛异常。
定时器无 try/catch/throw 和异常恢复路径，接口使用 noexcept。无效参数返回 0/false；
内存、线程等系统资源耗尽采用 fail-fast，不伪装为正常取消。调用方构造 std::function
发生的分配不在接口内部。取消会释放尚未执行的回调资源；
已经被调度线程选中的回调仍可能执行完，后续周期不会重排。

## 进程级唯一实例

`TimerScheduler` 是进程级唯一对象，不可构造、替换、停止或销毁。后台线程创建后
直接 detach，内部堆和索引对象保持到进程结束，避免静态析构顺序和 join 状态。
Reactor 请求超时和日志周期刷新共享该调度线程，不再由每个 SubReactor/Logger
创建独立定时器线程。

组件停止时只取消自己持有的 TimerId，不得停止共享调度器。由于一个回调线程服务
所有组件，回调必须只做无阻塞的状态检查、入队和唤醒；耗时业务处理必须投递到
Reactor 或 Worker。单线程最小堆也意味着高 timer 数量下所有组件共享同一把插入、
取消锁；如果后续成为瓶颈，应演进为分片 timer wheel，而不是重新引入每连接线程。

SubReactor 正常响应后取消请求 TimerId；超时回调只通过弱引用向所属响应队列提交
控制消息，最终由 Reactor owner 线程检查请求状态并关闭连接。Logger 同样是进程级
对象，周期回调只设置异步刷新目标。

## 性能设计
添加、到期取出和取消均为 O(log n)，ID 查找平均 O(1)。最小堆连续存储任务，
不使用 shared_ptr 或红黑树节点。周期重排复用原有 ID 索引节点；固定任务规模下
不会为每个周期重新分配容器节点。容量增长和较大 std::function 捕获仍可能分配内存。
任务回调及捕获对象的正常释放都在锁外执行；慢回调仍会推迟所有共享该调度器的
其他任务。

复测单生产者的 10 万定时任务添加和随机取消（不含回调执行时间）：

```powershell
g++ -std=c++20 -O2 -fno-exceptions -pthread -Iinclude benchmarks/timer_benchmark.cc src/timer.cc -o build/timer-benchmark.exe
./build/timer-benchmark.exe
```

本机 MinGW 15.2、相同 O2 参数、三轮中位数：原 map/shared_ptr 版本添加约
96ms、取消约 253ms；最小堆版本约 22ms、42ms。该微基准不代表多线程吞吐、
实际回调延迟或所有工作负载；保留程序便于后续用实际负载继续验证。
