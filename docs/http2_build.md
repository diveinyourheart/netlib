# HTTP/2 协议依赖构建（T05）

当前完成可选 nghttp2 依赖接入，**尚未实现 HTTP/2 Session，也没有在线 HTTP/1/2 自动选择
或 TLS/ALPN**。开启选项表示编译协议内核及其测试；实际请求处理从 T06 开始。
进度与每轮交接见 [HTTP2_TASKS.md](../HTTP2_TASKS.md)，会话边界见
[http2_design.md](http2_design.md)。

## 开关与版本

| 参数 | 默认值与作用 |
| --- | --- |
| NETLIB_ENABLE_HTTP2 | OFF；开启后启用 C 编译器、获取并构建固定 nghttp2 C 静态库 |
| NETLIB_NGHTTP2_ARCHIVE | 空；可指定本地官方 nghttp2-1.71.0.tar.xz，避免联网下载，仍校验 SHA-256 |
| BUILD_TESTING | ON；始终控制 netlib 自身测试，不会被上游的同名选项覆盖 |

固定 **nghttp2 1.71.0**，使用官方发布附件，SHA-256 为：

```text
c0f55121d88650e4b9227aec28cebaf664d2417ca60fe70be0f8d38829f52116
```

版本与摘要集中在 [NetlibNghttp2.cmake](../cmake/NetlibNghttp2.cmake)。摘要已和
[官方 checksums.txt](https://github.com/nghttp2/nghttp2/releases/download/v1.71.0/checksums.txt)
核对。使用 [v1.71.0 版本化发布包](https://github.com/nghttp2/nghttp2/releases/tag/v1.71.0)，
不使用 GitHub 自动生成的 Source code 归档。

只编译 libnghttp2，不构建 nghttp、nghttpd、nghttpx、h2load、HPACK 工具、示例、
上游测试、文档或 HTTP/3 应用依赖。协议库本身是 C；项目 C++ 标准仍为 C++20。
不需要为此安装 OpenSSL。上游配置可能显示可选依赖探测结果，这些应用依赖不参与链接。

## Windows 和 Linux

默认构建继续只需要 C++ 编译器，不下载 nghttp2：

```bash
cmake -S . -B build/plain -DCMAKE_BUILD_TYPE=Debug -DNETLIB_ENABLE_HTTP2=OFF
cmake --build build/plain --config Debug --parallel 4
ctest --test-dir build/plain -C Debug --output-on-failure --no-tests=error
```

开启协议依赖，首次配置会通过 HTTPS 下载并验证固定归档：

```bash
cmake -S . -B build/http2 -DCMAKE_BUILD_TYPE=Debug -DNETLIB_ENABLE_HTTP2=ON
cmake --build build/http2 --config Debug --parallel 4
ctest --test-dir build/http2 -C Debug --output-on-failure --no-tests=error
```

Linux 可使用默认生成器或加 `-G Ninja`；系统需要配套的 C/C++ 编译器、CMake 和构建工具。
Windows 可在 Visual Studio 开发者终端中使用上述命令并加 `-G Ninja`，也可使用 Visual
Studio 多配置生成器。配置错误测试会启动嵌套 CMake，使用 MSVC/Ninja 时也应在开发者
终端里运行 ctest，确保 SDK/编译器环境完整。

MinGW 用户若同时安装了多套编译器，应为新的构建目录明确选择同一套 C/C++：

```powershell
cmake -S . -B build/http2-mingw -G Ninja -DCMAKE_BUILD_TYPE=Debug -DNETLIB_ENABLE_HTTP2=ON -DCMAKE_C_COMPILER=D:/mingw64/bin/gcc.exe -DCMAKE_CXX_COMPILER=D:/mingw64/bin/g++.exe
cmake --build build/http2-mingw --parallel 4
ctest --test-dir build/http2-mingw --output-on-failure --no-tests=error
```

不要在已有构建目录中切换 MSVC 和 MinGW；分别使用独立构建目录。

## 离线归档与故障提示

可以预先下载官方归档，再用绝对路径传入。下面是 PowerShell 示例：

```powershell
New-Item -ItemType Directory -Force build/deps
curl.exe --fail --location --output build/deps/nghttp2-1.71.0.tar.xz https://github.com/nghttp2/nghttp2/releases/download/v1.71.0/nghttp2-1.71.0.tar.xz
Get-FileHash -Algorithm SHA256 build/deps/nghttp2-1.71.0.tar.xz
cmake -S . -B build/http2-offline -DNETLIB_ENABLE_HTTP2=ON -DNETLIB_NGHTTP2_ARCHIVE="D:/path/to/nghttp2-1.71.0.tar.xz"
cmake --build build/http2-offline --config Debug --parallel 4
```

本地归档路径不存在、指向目录、摘要不匹配或版本不兼容，配置会给出具体错误，不回退
到其他版本或静默关闭功能。只有上述 tar.xz 附件匹配当前摘要；同版本 tar.gz 的摘要
不同，也会被拒绝。关闭功能时不读取归档、不下载、不启用 C 编译器。

在线下载开启 TLS 验证，并限制总超时 120 秒、无进度超时 30 秒。网络失败时可重试配置，
或改用 NETLIB_NGHTTP2_ARCHIVE。离线归档通过正常 FetchContent 解包，不必设置
FETCHCONTENT_FULLY_DISCONNECTED；该变量要求源码已经完成填充。

标准 FetchContent 源码覆盖仍会检查 CMake 中的版本声明，拒绝非 1.71.0；这种源码覆盖
不经过归档摘要校验。需要固定发布包内容时使用默认下载或本地官方归档入口。

## 链接与实现边界

```text
netlib --PRIVATE--> netlib_nghttp2 (INTERFACE) --> nghttp2_static
```

netlib_nghttp2 是内部构建目标，为实现文件提供 nghttp2 头文件、生成的版本头及
NGHTTP2_STATICLIB。内部还启用 NGHTTP2_NO_SSIZE_T，隐藏依赖 POSIX ssize_t 的旧 API，
统一使用 nghttp2_ssize、mem_send2/mem_recv2 等便携接口，兼容 MSVC。
静态库链接要求传给最终可执行文件，依赖头文件路径和编译宏不会
传入只链接 netlib::netlib 的使用方。公开头文件没有 nghttp2 类型。

后续 Http2Session 应在 src 实现中使用该依赖；nghttp2 只负责协议状态与字节，不接管
socket/TLS I/O，不增加线程。全部 session 调用仍发生在所属 SubReactor owner 线程。
上游 CMake 选项在函数作用域内设置；BUILD_TESTING 的 cache 强制写入额外做保存和恢复。

依赖采用 MIT 许可证，仓库保留 [COPYING](../third_party/nghttp2/COPYING) 与
[版本来源](../third_party/nghttp2/README.md)。分发静态链接产物时保留相应版权和许可文本。

## 验证范围

- [nghttp2_test.cc](../tests/nghttp2_test.cc)：只在 ON 时构建；确认头文件/运行库版本，
  创建客户端与服务端内存会话，逐字节传递 preface 和 SETTINGS/ACK。此测试验证依赖可用，
  不代表 netlib 已有 HTTP/2 服务端。
- [nghttp2_configuration_test.cmake](../tests/cmake/nghttp2_configuration_test.cmake)：
  OFF 时即使归档路径/C 编译器无效也可配置，且不产生 _deps；ON 时缺少归档、摘要不符、
  不兼容源码覆盖都必须明确报错。
- [.github/workflows/build.yml](../.github/workflows/build.yml)：Linux GCC、Windows MSVC、
  Windows MinGW UCRT64 三组环境分别覆盖 OFF/ON；GCC/MSVC 保留原有 Debug/Release
  矩阵，MinGW CI 使用 Debug。默认开启 netlib 测试，并让无测试时失败。

本机实测与 CI 执行状态以 [每轮记录](../HTTP2_TASKS.md) 为准。分支推送后会自动触发
该工作流，实际运行结果仍需核对；不能把推送成功等同于 Linux 验证通过。
