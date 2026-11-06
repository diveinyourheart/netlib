# netlib

一个用于复习 Unix 网络编程、C++ 面向对象设计和性能工程的练习库。

## 当前阶段

当前具备第一层 Reactor 抽象：

- `EventLoop`：封装 epoll 事件循环和线程安全的停止通知。
- `Channel`：把一个 fd 上的读写、错误回调注册到 `EventLoop`。
- `UniqueFd`：表达 fd 的唯一所有权，支持跨线程移动。
- `MainReactor`：监听 IPv4 端口，循环 `accept4`，轮询分发给多个 `SubReactor`。
- `SubReactor`：通过有界队列和 eventfd 接收连接，维护每连接接收缓冲区并组装完整帧。
- `frame_protocol.h`：定义 4 字节大端 payload 长度和 16 MiB payload 上限。
- `ThreadPool`：使用 moodycamel MPMC 队列消费拥有数据的 `FrameMessage`。

- 语言标准：C++20
- 构建系统：CMake
- 编译选项：不使用编译器扩展
- 公开头文件：`include/netlib/`
- 库实现：`src/`
- 单元测试：`tests/`
- 示例程序：`examples/`
- 性能测试：`benchmarks/`
- 设计记录：`docs/`

当前只实现了 Linux epoll 后端。传输帧分帧和 Worker 线程池已经加入；写缓冲尚未加入。
架构和生命周期约定见
[`docs/reactor_architecture.md`](docs/reactor_architecture.md)。

## 数据流程

```text
TCP bytes
  -> SubReactor 按长度前缀组装完整 payload
  -> FrameMessage（拥有 payload）
  -> moodycamel BlockingConcurrentQueue
  -> ThreadPool Worker
  -> 业务 Handler
```

长度字段为 4 字节网络字节序，只计算 payload，不包含长度头。payload 最大
16 MiB。Worker Handler 并发执行，没有跨连接的全局顺序保证；业务需要顺序时
使用 `FrameMessage::connection_id` 和 `sequence`。

基本组装方式：

```cpp
auto workers = netlib::ThreadPool::Create(
    4, 4096, [](netlib::FrameMessage message) {
      // 解码并处理完整 payload。
    });

auto sub = netlib::SubReactor::Create(
    1024, *workers, connection_callbacks);

auto main = netlib::MainReactor::Create(
    8080, {sub.get()});
```

`ThreadPool` 必须比引用它的 `SubReactor` 活得久。正常关闭时先停止 Main 和
Sub 的生产者，再调用 `ThreadPool::Stop()` 排空消息并 join Worker。

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
