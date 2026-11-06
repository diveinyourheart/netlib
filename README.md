# netlib

一个用于复习 Unix 网络编程、C++ 面向对象设计和性能工程的练习库。

## 当前阶段

当前具备第一层 Reactor 抽象：

- `EventLoop`：封装 epoll 事件循环，通过内部 stop Channel 优雅唤醒退出。
- `Channel`：把一个 fd 上的读写、错误回调注册到 `EventLoop`。
- `UniqueFd`：表达 fd 的唯一所有权，支持跨线程移动。
- `MainReactor`：监听 IPv4 端口，循环 `accept4`，轮询分发给多个 `SubReactor`。
- `SubReactor`：管理每连接 Codec、读写缓冲、响应队列和 ET 事件。
- `ProtocolCodec`：负责字节流分帧和响应编码，默认实现为 4 字节长度前缀。
- `ConnectionHandle`：Worker 回写响应的安全弱连接句柄。
- `frame_protocol.h`：定义 4 字节大端 payload 长度和 16 MiB payload 上限。
- `ThreadPool`：使用 moodycamel MPMC 队列消费拥有数据的 `FrameMessage`。
- `netlib::log`：异步彩色标准输出日志，提供可扩展的 Sink 接口。

- 语言标准：C++20
- 构建系统：CMake
- 编译选项：不使用编译器扩展
- 公开头文件：`include/netlib/`
- 库实现：`src/`
- 单元测试：`tests/`
- 示例程序：`examples/`
- 性能测试：`benchmarks/`
- 设计记录：`docs/`

当前只实现了 Linux epoll 后端。传输帧分帧、Worker 线程池以及非阻塞响应写回已经
加入。架构和生命周期约定见
[`docs/reactor_architecture.md`](docs/reactor_architecture.md)。

## 数据流程

```text
TCP bytes
  -> SubReactor 按长度前缀组装完整 payload
  -> FrameMessage（拥有 payload）
  -> moodycamel BlockingConcurrentQueue
  -> ThreadPool Worker
  -> 业务 Handler
  -> ConnectionHandle::Respond
  -> SubReactor response queue + eventfd
  -> ProtocolCodec::Encode
  -> nonblocking send / EPOLLOUT
```

长度字段为 4 字节网络字节序，只计算 payload，不包含长度头。payload 最大
16 MiB。Worker Handler 并发执行，没有跨连接的全局顺序保证；业务需要顺序时
使用 `FrameMessage::connection_id` 和 `sequence`。

基本组装方式：

```cpp
auto workers = netlib::ThreadPool::Create(
    4, 4096, [](netlib::FrameMessage message) {
      // 解码并处理完整 payload。
      std::vector<char> response = BuildResponse(message.payload);
      message.connection.Respond(std::move(response));
    });

netlib::SubReactorOptions sub_options{
    .max_pending_connections = 1024,
    .max_pending_responses = 4096,
    .max_pending_output_bytes = 16 * 1024 * 1024,
};

auto sub = netlib::SubReactor::Create(
    sub_options, *workers, connection_callbacks);

auto main = netlib::MainReactor::Create(
    8080, {sub.get()});
```

`ThreadPool` 必须比引用它的 `SubReactor` 活得久。正常关闭时先停止 Main 和
Sub 的生产者，再调用 `ThreadPool::Stop()` 排空消息并 join Worker。

`ConnectionHandle::Respond()` 是线程安全的。连接已经关闭时返回 false，不会使用
可能已经复用的 fd。响应队列满时同样返回 false，由业务层选择丢弃、关闭连接或
施加更上层的背压。

`SubReactorOptions` 分别控制待注册连接数、待处理响应数和单连接输出积压字节数。
Codec 通过 `MaxDecodeBufferSize()` 描述输入分帧缓冲上限，单次响应 payload
限制由 `Encode()` 自己执行。输出积压策略不再隐式绑定到 Codec 的输入缓冲上限。

默认 SubReactor 使用 `CreateLengthPrefixedCodec()`。自定义协议可以实现
`ProtocolCodec` 并通过带 `ProtocolCodecFactory` 的 `SubReactor::Create`
重载传入。每个连接拥有独立的 Codec 实例，因此可以维护自定义解析状态。

发送采用循环 `send(..., MSG_NOSIGNAL)` 直到缓冲区为空或 `EAGAIN`。仅在存在
输出积压且遇到 `EAGAIN` 时注册 `EPOLLOUT`，写空后立即恢复为只关注读取，
避免可写 socket 持续触发 epoll。

Reactor 和 ThreadPool 的工厂函数支持可选的 `Logger*` 参数。建议由服务编排层
创建 Logger，并作为最后一个非拥有参数传给三者。当前会记录 EventLoop 启动和
停止、监听地址、连接派发、Channel 注册失败、accept/recv 错误、超大帧、
Worker 队列背压及 Reactor 关闭等事件；逐 epoll 事件和每次成功 `recv` 不记录
INFO 日志，避免热路径产生日志风暴。

EventLoop 的 stop eventfd 也作为普通 Channel 注册。跨线程 `Stop()` 会先设置
停止状态，再写入 stop eventfd 唤醒阻塞的 `epoll_wait()`；当前 ready batch
处理完后，`Run()` 检查停止状态并返回，随后由调用方 join 事件循环线程。

## 日志

日志库使用独立静态库 `netlib::log`。调用线程只采集时间、线程号、序号和消息，
实际格式化及终端写入由单个 Writer 线程批量完成。队列有界，满时默认丢弃新日志
并记录丢弃数量，不会阻塞网络线程。

```cpp
#include "netlib/log/logging.h"

netlib::log::LoggerOptions options{
    .name = "netlib.server",
    .minimum_level = netlib::log::LogLevel::kInfo,
    .queue_capacity = 8192,
    .color_mode = netlib::log::ColorMode::kAuto,
    .flush_each_record = false,
};

auto logger = netlib::log::Logger::CreateStdout(std::move(options));
NETLIB_LOG_INFO(*logger, "server started on port={} workers={}", 8080, 4);
```

标准输出示例（实际输出为单行）：

```text
2026-11-06 16:20:31.427 | INFO  | seq=42 | tid=12831 | netlib.server | logging.cc:120 Run | server started
```

每行包含毫秒时间、级别、全局序号、操作系统线程号、logger 名称、源码文件、
行号、函数名及消息。TTY 输出自动启用 ANSI 颜色，重定向时可使用
`ColorMode::kNever`。消息中的终端控制字符会被转义。

`Flush()` 使用单调递增的票据号和 Writer 完成水位：每个 Flush 控制消息携带
一个 ticket，Writer 执行 Sink Flush 后推进 completed watermark 并唤醒等待者。
Flush 和 Stop 控制消息共用一个显式 moodycamel producer token，保证并发 Flush
时控制消息按 FIFO 处理。Flush 是队列屏障；Flush 期间由其他线程继续写入的日志
可能排在该屏障之后。

日志宏使用固定版本 fmtlib 11.2.0，格式串在编译期检查，例如：

```cpp
NETLIB_LOG_WARN(*logger, "fd={} errno={} reason={}",
                client_fd, error_number, reason);
```

日志级别检查发生在参数格式化之前，因此关闭某个级别时只承担一次原子读取和
分支判断，不执行格式化和消息分配。启用日志时，fmt 在调用线程完成一次格式化，
消息 move 到异步队列；Writer 线程只负责格式前缀、Sink 写入和 Flush。相比
多段 `std::string + std::to_string` 拼接，这避免了多个临时字符串和重复分配，
同时保留异步 I/O 的非阻塞特性。

自定义 `LogSink` 可供后续文件、网络或共享内存 Sink 扩展。设计细节见
[`docs/logging.md`](docs/logging.md)。

运行示例：

```bash
cmake --build --preset debug --target netlib_log_example
./build/debug/netlib_log_example
```

## 构建

```bash
cmake --preset debug
cmake --build --preset debug
ctest --test-dir build/debug --output-on-failure
```

发布构建：

```bash
cmake --preset release
cmake --build --preset release
```

生成结果位于 `build/debug/` 和 `build/release/`。

## IDE 索引

CMake 会生成 `build/debug/compile_commands.json`。仓库级 `.clangd` 指向该
文件，因此 VSCode + clangd 和命令行 clangd 可以直接使用项目的 include
路径、宏和 C++20 参数。CLion 导入 CMake 后使用自身的索引。

项目根目录的 `compile_commands.json` 是可选的兼容链接，指向 Debug 编译数据库：

```bash
ln -sfn build/debug/compile_commands.json compile_commands.json
```

重新 configure 或切换构建目录后，需要确认该链接仍然有效。

## 目录约定

```text
netlib/
├── include/netlib/   # 公开头文件，使用 netlib/... 形式引用
├── src/              # 非公开实现
├── third_party/      # 固定版本第三方依赖
├── tests/            # 单元测试和集成测试
├── examples/         # 可运行示例
├── benchmarks/       # 性能与压测程序
└── docs/             # 设计、实验和复盘记录
```
