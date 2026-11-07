# 平台层与 Windows 构建

`netlib::platform` 是独立静态库，接口位于 `include/netlib/platform.h`，
实现位于 `src/platform.cc`。Reactor、Channel 和协议代码只调用该接口。
CMake 检测目标操作系统，编译器的 `_WIN32` 宏选择实现，不需要调用方手工定义平台宏。

| 能力 | Linux | Windows |
| --- | --- | --- |
| 事件等待 | epoll，边缘触发 | WSAPoll，水平触发 |
| 跨线程唤醒 | eventfd | 非阻塞 loopback TCP 连接对 |
| socket 初始化 | 无 | 自动 WSAStartup / WSACleanup |
| 非阻塞模式 | fcntl / SOCK_NONBLOCK | ioctlsocket |
| 句柄关闭 | close | closesocket |
| 错误码 | errno | WSAGetLastError |
| 监听地址选项 | SO_REUSEADDR | SO_EXCLUSIVEADDRUSE |

Windows 基线为 Windows 8 / Server 2012 及以上；推荐在受支持的 Windows 版本运行。
WSAPoll 没有 select 的 FD_SETSIZE 限制，但每次等待会扫描 socket 集合，复杂度为
O(n)，不等同于 IOCP 的吞吐扩展能力。选择它是为了沿用当前同步非阻塞 Reactor
接口；未来采用 IOCP 时需要设计完成事件和缓冲区生命周期，不能仅替换等待函数。

`Wakeup` 的读端可注册到 Poller，多个生产者可并发 Notify，消费者 Drain 到
would-block。TCP 发送缓冲已满意味着已有通知待消费，可以合并通知；不使用可能
丢包的 UDP 唤醒。共享 Wakeup 的所有权使仍在提交响应的 Worker 不会访问已释放句柄。
Windows 唤醒会创建本地 TCP socket，因此受本机网络策略和端口资源限制。

Poller 使用注册 token，而非 fd 作为事件身份，避免旧 ready batch 误派发给复用
同一句柄的新 Channel。一次就绪同时包含读写时，两种回调都可执行；每次回调前
重新查找注册信息，回调删除 Channel 后就不会继续调用。收发仍循环至 would-block，
同时满足 epoll ET 和 WSAPoll LT；只在输出积压时关注可写事件。

## API 迁移

`UniqueFd` 保留原名称。在 Linux 上仍可管理 POSIX fd；Windows 上只管理 SOCKET，
不要传 CRT 文件描述符或普通 HANDLE。`get()`、`Release()`、Channel 和连接通知
回调改为使用 `netlib::SocketHandle`：Linux 为 int，Windows 为 uintptr_t。
请将业务回调的 `[](int fd)` 改为 `[](netlib::SocketHandle fd)` 或 `[](auto fd)`，
避免 64 位 Windows 句柄截断。无效值使用 `netlib::kInvalidSocket`，不要比较 `< 0`。

平台 I/O 返回 ptrdiff_t；错误时返回负数，通过 LastError 获取原生错误码，
Interrupted / WouldBlock 分类，ErrorMessage 格式化。Windows 不用 errno 判断 socket 错误。

## 构建与测试

需要 C++20 编译器。使用 CMake presets 需要 CMake 3.25+；直接 `-S/-B` 构建仍支持 3.20+。

Windows MinGW（g++、ninja、cmake 在 PATH 中）：

```powershell
cmake -S . -B build/windows-debug -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build/windows-debug
ctest --test-dir build/windows-debug --output-on-failure
```

Windows MSVC（在 Visual Studio Developer PowerShell 中）：

```powershell
cmake -S . -B build/msvc-debug -G Ninja -DCMAKE_CXX_COMPILER=cl -DCMAKE_BUILD_TYPE=Debug
cmake --build build/msvc-debug
ctest --test-dir build/msvc-debug --output-on-failure
```

也可使用 Visual Studio 多配置生成器：构建加 `--config Debug`，CTest 加 `-C Debug`。
Release 使用单独构建目录并将 Debug 改为 Release。不同编译器不要共享构建目录。
Linux 继续使用 README 中的 presets。CI 包含 Linux/GCC 和 Windows/MSVC 的
Debug、Release；平台测试与现有 Reactor、HTTP、Codec、日志测试在两种系统运行。

## 扩展 macOS

当前 CMake 会明确拒绝未实现的平台，macOS 尚未支持。后续可以保留所有 Reactor 和
Codec 接口，新增 kqueue Poller、pipe/socketpair Wakeup，并适配 accept、fcntl、
SO_NOSIGPIPE 和日志的线程号接口，最后加入 macOS CI。Windows/Linux 分支当前集中在
平台实现与日志的少量系统调用处；平台增多时可按后端拆分源文件，由 CMake 选择。

参考：[Microsoft WSAPoll 文档](https://learn.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsapoll)、
[Windows 地址复用语义](https://learn.microsoft.com/en-us/windows/win32/winsock/using-so-reuseaddr-and-so-exclusiveaddruse)。
