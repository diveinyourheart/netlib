# netlib

一个支持 Windows 和 Linux、用于学习网络编程、C++ 面向对象设计和性能工程的练习库。

## 当前阶段

当前具备第一层 Reactor 抽象：

- `EventLoop`：通过平台 Poller 运行事件循环，通过内部 stop Channel 优雅唤醒退出。
- `Channel`：把一个 fd 上的读写、错误回调注册到 `EventLoop`。
- `UniqueFd`：表达原生 socket 句柄的唯一所有权，支持跨线程移动。
- `MainReactor`：监听 IPv4 端口，循环调用平台 accept 接口，轮询分发给多个 `SubReactor`。
- `SubReactor`：管理每连接 Codec、读写缓冲、响应队列和非阻塞就绪事件。
- `ProtocolCodec`：负责字节流分帧和响应编码，默认实现为 4 字节长度前缀。
- `Http1Codec`：解析 HTTP/1.0、HTTP/1.1 Header、Content-Length Body 和 keep-alive。
- `RpcCodec`：解析带版本、请求 ID、服务/方法名称和 TLV 扩展的 RPC 二进制帧。
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

当前支持 Linux epoll 和 Windows Winsock / WSAPoll 后端。系统调用集中到独立的 `netlib::platform` 库。传输帧分帧、Worker 线程池以及非阻塞响应写回已经
加入。Windows 构建、API 迁移和 macOS 扩展说明见
[平台文档](docs/platform.md)。架构和生命周期约定见
[`docs/reactor_architecture.md`](docs/reactor_architecture.md)。

## 数据流程

```text
TCP bytes
  -> SubReactor 按所选 Codec 组装完整帧
  -> FrameMessage（拥有 payload）
  -> moodycamel BlockingConcurrentQueue
  -> ThreadPool Worker
  -> 业务 Handler
  -> ConnectionHandle::Respond
  -> SubReactor response queue + Wakeup
  -> per-connection sequence reorder buffer
  -> ProtocolCodec::Encode
  -> platform::Send / writable readiness
```

协议解析和业务层通过 typed variant 解耦：

```cpp
FrameMetadata =
    std::variant<BinaryFrameMetadata, HttpRequestMetadata, RpcFrameMetadata>;
ApplicationResponse =
    std::variant<BinaryResponse, HttpResponse, RpcResponse>;
```

网络层仍然只有一份非模板 SubReactor。长度前缀协议提交
`BinaryFrameMetadata`，HTTP 提交包含 method、target、version、Headers、
Content-Length 和 keep-alive 的 `HttpRequestMetadata`。
RPC 头部放入 `RpcFrameMetadata`，payload 为独立二进制；格式、扩展规则和接入示例见
[RPC 协议文档](docs/rpc_protocol.md)。protobuf 解析与压缩处理由上层 RPC 框架负责。

默认长度前缀协议使用 4 字节网络字节序长度，只计算 payload，不包含长度头。payload 最大
16 MiB。Worker Handler 并发执行，完成顺序不固定。SubReactor 使用每条连接内的
`sequence` 重排响应，因此业务按乱序完成也能按请求到达顺序写回网络。

基本组装方式：

```cpp
auto workers = netlib::ThreadPool::Create(
    4, 4096, [](netlib::FrameMessage message) {
      // 解码并处理完整 payload。
      std::vector<char> response = BuildResponse(message.payload);
      message.connection.Respond(message.sequence, std::move(response));
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

`ConnectionHandle::Respond()` 是线程安全的。每个 `FrameMessage` 必须且只能
提交一个使用相同 `sequence` 的响应。缺少某个 sequence 会阻塞该连接后续响应；
重复响应会被忽略，这些顺序错误通常表示业务 Handler 违反了响应契约。
SubReactor 会按每条连接的 `max_pending_responses` 限制乱序缓存；溢出时关闭该
连接。全局响应队列满或连接已经关闭时，`Respond()` 返回 false。

`SubReactorOptions` 分别控制待注册连接数、待处理响应数和单连接输出积压字节数。
Codec 通过 `MaxDecodeBufferSize()` 描述输入分帧缓冲上限，单次响应 payload
限制由 `Encode()` 自己执行。输出积压策略不再隐式绑定到 Codec 的输入缓冲上限。

`DecodeResult::required_bytes` 是下一次解析推进所需的总未消费字节数。长度前缀
Codec 在读取 4 字节长度头时返回 4，解析出 payload 长度后返回完整帧大小。
HTTP Codec 在寻找 `\r\n\r\n` 时可以返回 0 表示目标未知，解析出
`Content-Length` 后再返回 Header 长度加 Body 长度。Connection 使用该值控制
下一次读取和几何扩容，不会因为协议最大帧大小较大就一次分配整块最大缓冲。

HTTP 业务处理示例：

```cpp
const auto* request =
    std::get_if<netlib::HttpRequestMetadata>(&message.metadata);
if (request != nullptr && request->target == "/rpc") {
  message.connection.Respond(
      message.sequence,
      netlib::HttpResponse{
          .status_code = 200,
          .reason_phrase = "OK",
          .headers = {{.name = "Content-Type", .value = "application/json"}},
          .body = response_json,
          .keep_alive = request->keep_alive,
      });
}
```

当前 HTTP Codec 支持 Content-Length 请求；chunked transfer encoding 会返回
`ProtocolError`，后续可在同一个状态机中增加 chunk size、chunk data 和 trailer
状态。HTTP 响应由 Codec 自动生成 Content-Length 和 Connection Header。

默认 SubReactor 使用 `CreateLengthPrefixedCodec()`。自定义协议可以实现
`ProtocolCodec` 并通过带 `ProtocolCodecFactory` 的 `SubReactor::Create`
重载传入。每个连接拥有独立的 Codec 实例，因此可以维护自定义解析状态。

发送通过平台接口循环执行，直到缓冲区为空或 would-block。仅在输出积压时关注
可写事件，写空后恢复为只读。Linux 保留 epoll ET，Windows 使用 WSAPoll LT。

Reactor 和 ThreadPool 的工厂函数支持可选的 `Logger*` 参数。建议由服务编排层
创建 Logger，并作为最后一个非拥有参数传给三者。当前会记录 EventLoop 启动和
停止、监听地址、连接派发、Channel 注册失败、accept/recv 错误、超大帧、
Worker 队列背压及 Reactor 关闭等事件；逐 epoll 事件和每次成功 `recv` 不记录
INFO 日志，避免热路径产生日志风暴。

EventLoop 的 Wakeup 也作为普通 Channel 注册。跨线程 Stop() 设置停止状态并唤醒
事件等待；Linux 使用 eventfd，Windows 使用本地 TCP 连接对。当前 ready batch
处理完成后 Run() 返回，再由调用方 join 事件循环线程。

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

Flush() 会记录已接受日志的截止编号，等待这个范围内的日志全部写出后再刷新。
写线程跟踪连续完成进度，不依赖不同 producer 之间的出队顺序；后续日志可继续提交。
Stop() 关闭提交入口、排空已接受日志并完成刷新，然后等待后台线程退出。
默认每 1000ms 自动请求刷新，可通过 LoggerOptions::flush_interval 调整（0 表示关闭）。
刷新仍使用 fflush()，不保证断电持久化。可复用定时器见[定时器文档](docs/timer.md)。

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

以下为 Linux 命令；Windows 的 MinGW 和 MSVC 命令见[平台文档](docs/platform.md)。

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
