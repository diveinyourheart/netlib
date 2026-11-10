# HTTP/2 分阶段任务

分支：`codex/http2-support`。
基线：`main`，`16cc2f95b814ebb455abe5cac77820ae74a383c0`。
执行到第五轮：T01–T07 已完成，本轮新增阶段期限、Worker 取消通知和有期限排空。
下一轮入口为 T08 的全链路预算与背压，之后才进入 T09 自动选择。

## 目标和当前边界

目标是在显式配置的 HTTP 服务入口，让 HTTP/1.0、HTTP/1.1 和 HTTP/2 按连接自动选择
协议。保留长度前缀与 RPC 的显式 Codec 配置，不对任意 TCP 数据猜测 HTTP。

- 明文：HTTP/1 从请求行识别版本；HTTP/2 使用完整 client preface 识别 prior knowledge。
- HTTPS：TLS 完成后使用 ALPN，协商到 h2 才启用 HTTP/2。
- HTTP/2 没有 HTTP/1 风格的文本请求行，不能仅看到 `HTTP/2.0` 就认为连接是 HTTP/2。
- 不将已弃用的 HTTP/1 `Upgrade: h2c` 作为首版接入路径。
- 协议选定后，连接固定为对应协议族；HTTP/1 各请求的版本仍由 HTTP/1 parser 校验。

设计与标准依据见 [HTTP/2 设计](docs/http2_design.md)。当前仍是基础准备阶段，
还不能通过网络实际处理 HTTP/2 请求，也没有将识别器接入在线协议切换。

## 第一批：边界与输入基础

- [x] **T01：固定架构边界和验收范围**
  - 创建 feature 分支、根目录任务清单和设计文档。
  - 明确 ProtocolSession、stream、Worker 的职责，区分已实现与计划能力。
  - 更新 README 与现有架构文档的入口。
- [x] **T02：HTTP 协议识别器**
  - 独立、有状态、无动态分配的连接起始探测器，支持 HTTP/1.0、HTTP/1.1 与 HTTP/2 preface。
  - 支持逐字节输入、跨读取的 CRLF、后续帧粘包，探测不消费输入。
  - 有请求行长度上限；错误、未完成与已识别必须分开表达。
  - 完整 preface 只表示选择 HTTP/2，随后 SETTINGS 等合法性由 HTTP/2 Session 验证。
  - 验收：已知线格式、所有分割点、错误 preface、伪造文本版本、长度边界测试。
- [x] **T03：协议进度与业务请求解耦**
  - DecodeStatus 增加“已消费、无业务消息”的状态。
  - Reactor 校验消费进度并推进缓冲，不投递 Worker、不占 sequence、不创建请求 timer。
  - 验收：控制字节和普通请求粘包、连续控制输入、零进度/越界消费拒绝、原有协议回归。

## 第二批：会话层与第三方协议内核

- [x] **T04：ProtocolSession 与现有 Codec 适配**（依赖 T01、T03）
  - 明确 OnInput、OnResponse、OnTimeout、输出获取与 BeginDrain 的接口。
  - 单次输入允许产生零个或多个拥有数据的请求/事件，以及不依赖业务响应的协议输出。
  - 每连接一个 Session，只有所属 SubReactor 线程访问；简单协议包装现有 Codec。
  - 引入请求上下文，区分本地任务编号、HTTP/1 pipeline sequence 和 HTTP/2 stream ID。
  - 验收：长度前缀、RPC、HTTP/1 的响应排序、超时和错误行为保持兼容。
- [x] **T05：nghttp2 可选依赖**（依赖 T04）
  - 选择并固定稳定版本、明确获取方式和许可证；可选 CMake 开关，缺少依赖时明确报错。
  - 核心头文件不暴露 nghttp2 类型；nghttp2 不负责 socket/TLS I/O。
  - 验收：Windows MinGW/MSVC、Linux 的启用/禁用构建。
  - 第四轮已核对上轮提交的十项 CI 全部通过，含 Linux Debug/Release ON/OFF，证据见第四轮记录。

## 第三批：HTTP/2 明文链路

- [x] **T06：HTTP/2 Session 与完整请求/响应**（依赖 T04、T05）
  - 处理 preface、SETTINGS/ACK、HPACK、HEADERS/CONTINUATION、DATA、PING。
  - 按 stream 聚合有上限的请求，END_STREAM 后向 Worker 投递完整请求。
  - 响应按 stream 寻址，不经过 HTTP/1 的整连接 pipeline 排序。
  - 基本连接/stream 流控必须从此阶段起正确处理，不允许临时忽略对端窗口。
  - 验收：至少两条 stream 交错请求、乱序完成响应、帧分片和控制帧往返。
- [x] **T07：stream 超时、取消和优雅结束**（依赖 T06）
  - 超时接口接收对应请求上下文，结合发送阶段决定错误响应或 RST_STREAM。
  - 收到取消后释放 stream 资源，迟到回复拒绝；其他 stream 可继续使用。
  - GOAWAY、最后接受的 stream ID、排空期限与强制结束分开建模。
  - 验收：超时/响应竞争、取消后回写、单流异常隔离、连接排空。
  - 第五轮已补齐阶段期限、stop_token、外部 Drain、截止期限与竞争验证，见第五轮记录。
- [ ] **T08：内存预算和背压闭环**（依赖 T06、T07）
  - 限制并发 stream、解压后 Header、各流正文、待编码输出和连接累计字节数。
  - 接收窗口与缓冲预算关联，整包聚合不能因等 Worker 才归还窗口而卡死。
  - Worker 满时优先针对单流拒绝/背压，仍能处理必要的连接控制帧。
  - 设置每轮连接处理的帧/字节预算，避免控制帧洪流长期占用事件线程。
  - 验收：零窗口与恢复、慢流隔离、大正文、多流累计预算、队列饱和。
- [ ] **T09：明文 HTTP/1 与 HTTP/2 自动选择**（依赖 T02、T06、T08）
  - 将探测器接入 HTTP Session factory；探测输入原样交给选中的 Session。
  - 连接只选择一次协议族；不能回退解析已选定协议的畸形输入。
  - 验收：同一 HTTP 入口的 HTTP/1.0、HTTP/1.1、prior-knowledge HTTP/2 混合连接。

## 第四批：HTTPS、流式扩展与发布验收

- [ ] **T10：非阻塞 TLS 与 ALPN**（依赖 T04、T09）
  - 传输层支持握手、TLS 读写事件需求、证书配置与关闭。
  - 同一 TLS 入口按 ALPN 选择 h2/http/1.1，不探测加密字节。
  - 验收：TLS 握手分片、协商与不支持协议、HTTP/1 回退策略、关闭和超时。
- [ ] **T11：流式 Body 与 Trailers**（依赖 T07、T08）
  - 有界 Body chunk/消费通知；同一 stream 的写命令有明确串行规则。
  - 不把所有 DATA 帧当成独立业务请求；支持请求与响应 Trailers。
  - 验收：大流式正文、取消传播、跨 producer 命令顺序、内存有界。
  - gRPC 属于后续独立协议层，本任务不自动视为已支持 gRPC。
- [ ] **T12：互操作与用户文档收口**（依赖 T09；HTTPS 验收还依赖 T10）
  - 提供可运行服务示例、清晰的开关与依赖说明、HTTP/1/2 客户端验证方式。
  - 覆盖畸形帧/头部、资源限制、长连接和并发；Linux/Windows 回归。
  - README 分别声明明文、TLS、整包与流式能力，避免将探测基础误报为完整 HTTP/2 支持。

## 分批完成标准

每批结束更新本清单的状态、改动文件、测试结果和下一批入口。没有通过验收的任务不勾选。
每轮完成后总结并更新交接上下文，保留已完成轮次的记录；第五轮到 T07 为止，
不自动继续全部任务。依赖与 TLS 按对应任务阶段引入。

## 第一批执行记录

- T01 完成：main 基线核对、feature 分支、任务拆分、架构设计、README 和文档入口。
- T02 完成：[http_protocol.h](include/netlib/http_protocol.h)、
  [http_protocol.cc](src/http_protocol.cc)。探测器不分配动态内存、不消费输入，
  增量保存请求行扫描位置；结果一旦选择或出错即固定。
- T03 完成：[protocol_codec.h](include/netlib/protocol_codec.h)、
  [sub_reactor.cc](src/sub_reactor.cc)。新增 kConsumed，合法消费后继续解析，
  业务 sequence、请求 timer 和 Worker 投递都不受控制输入影响。
- 新增 [http_protocol_test.cc](tests/http_protocol_test.cc)：所有前缀分割点、
  逐字节输入、逐字节破坏 preface、伪造 HTTP/2 文本请求、请求行上限、粘包和输入搬移。
- 扩展 [reactor_test.cc](tests/reactor_test.cc)：合成控制标记连续消费后跟业务帧，
  在 max_pending_responses=1 时仍只投递一个 sequence=0 的业务请求；
  零消费、消费越界和携带 payload 的控制结果都会关闭连接且不调用 Handler。

本批验证结果：

| 本机配置 | 全量回归 |
| --- | --- |
| MinGW Debug，build/windows-debug | 19/19 通过 |
| MinGW Release，build/windows-release | 19/19 通过 |
| MSVC Debug，build/msvc-debug | 19/19 通过 |

已校验代码格式和文档本地链接，并刷新 clangd 的 build/debug 编译数据库。
Linux 运行验证留待后续 CI。本机 MSVC 仍报告 main 中 FrameMessage 回写接口已有的
payload 参数遮蔽成员警告 C4458，本批不改动该接口。

第一批在 T03 结束。当前没有可用的 HTTP/2 Session、自动在线切换或 TLS/ALPN，
没有引入 nghttp2/TLS 依赖；合成控制测试不等于 HTTP/2 控制帧互操作测试。

第一批交接入口为 **T04**：先定义 owner 线程中的会话输入/请求/输出事件和带请求身份的
回写接口，给现有 Codec 做适配，用旧协议回归与模拟会话验证，再开始 T05 的依赖接入。

## 第二轮执行记录：T04（2026-11-09）

本轮完成 **T04**，没有继续 T05。已完成任务合计 T01–T04。

实现及接口决定：

- 新增 [protocol_session.h](include/netlib/protocol_session.h)、
  [protocol_session.cc](src/protocol_session.cc)：每连接一个 Session，只有 owner Reactor
  线程调用；OnInput 一次可产零个或多个拥有数据的 SessionRequest。
- 新增 [request_context.h](include/netlib/request_context.h)，并扩展
  [frame_message.h](include/netlib/frame_message.h)。本地 request_id 从 0 连续递增，
  与原 sequence 相同；stream_id 独立可选；RPC 线上请求 ID 仍保存在 metadata 中。
- [sub_reactor.cc](src/sub_reactor.cc) 的 Connection 保留 socket、输入缓冲、Channel、
  连接身份与 context/timer 表，响应排序、编码和输出缓冲移入 CodecSession。
  Worker 回复及 timer 都由 Reactor 查表后调用 OnResponse/OnTimeout，使用保存的
  context；重复和超时后迟到的响应不会再次交给 Session。
- 原 Create(..., codec_factory) 自动包装 CreateCodecSession；现有 Codec 子类和
  Handler 回写方法保持兼容。原生 Session 使用 CreateWithSession，factory 接收
  ProtocolSessionOptions，且创建 Reactor 时会先创建一个实例验证。
- Session 的 ProduceOutput/PendingOutput/ConsumeOutput 在连接注册、输入推进、
  回写、超时与可写就绪时驱动，可主动发控制字节，不经 Worker，也不增加传输层输出副本。
  ProduceOutput 的预算包含已有待发字节；socket 部分写仅消费实际成功发送的部分。
- [channel.cc](src/channel.cc) 支持空兴趣暂停注册，再启用时使用新 token；
  恢复读取前会解析 Reactor 中已有的未消费输入，避免等待不存在的新就绪通知。
  同时修正输入缓冲初次扩容在配置上限小于 4 KiB 时越过上限的问题。
- BeginDrain 作为会话钩子：旧 Codec 停止接受新请求，等待已接受请求完成并发送完输出。
  此钩子还未接入外部服务停止命令；SubReactor::Stop 仍立即停止事件循环。

验收与证据：

- 新增 [protocol_session_test.cc](tests/protocol_session_test.cc)：长度前缀、HTTP/1、RPC
  适配；owned payload；身份分离；ordered/unordered 占位与去重；部分输出、限额、
  HTTP 504 的终结响应及 drain。
- 新增 [session_reactor_test.cc](tests/session_reactor_test.cc)：真实 TCP 配合合成 Session；
  启动与输入控制输出不调用 Worker；一次输入产生两个请求；stream 11/7 独立乱序响应；
  Worker 修改 context 副本不改变路由；超时正确携带 stream 19；重复和迟到回复丢弃；
  读兴趣暂停/恢复及粘包余量重放；32 字节输入上限下分批消费 70 字节。
- 非法输入结果包括零消费、消费越界、非法 NeedMore、本地请求 ID 跳号；非法输出包括
  预算越界与 ProduceOutput 失败，均关闭连接而不投递业务。输入期间关闭也已回归，
  读循环会重新查连接，避免发送控制输出关闭后继续引用已销毁状态。
- 旧协议真实 socket 的排序、无序回写、请求超时、线程池和日志测试继续全量通过。

| 本机配置 | 全量回归 |
| --- | --- |
| MinGW Debug，build/windows-debug | 21/21 通过 |
| MinGW Release，build/windows-release | 21/21 通过 |
| MSVC Debug，build/msvc-debug | 21/21 通过 |

README、Reactor 架构和 [会话接口设计](docs/http2_design.md#t04-会话接口与请求身份)
已同步；build/debug 编译数据库已刷新，代码格式、空白和文档本地链接完成检查。
Linux 尚未执行运行验证；MSVC 仍只有第一批已记录的 FrameMessage 参数遮蔽成员警告。
本轮未引入第三方协议依赖，也未提交或 push。

### 第二轮交接入口：T05

1. 从当前 codex/http2-support 工作区继续，保留前两轮实现；先核对本清单和
   protocol_session.h。不要重建分支，也不要把合成 Session 当成真实 HTTP/2 支持。
2. 引入可选 nghttp2 协议内核：拟使用 NETLIB_ENABLE_HTTP2，默认 OFF。关闭时保持
   现有无额外依赖构建，也不发起下载；开启时选择并固定稳定版本/校验值，明确获取方式
   与许可证。若源码构建 nghttp2，注意当前项目只启用了 CXX，需要按实际依赖启用 C。
3. 只使用 nghttp2 库，不引入客户端工具或不必要的 TLS/应用依赖。公开头文件继续不暴露
   nghttp2 类型，库不接管 socket 或线程。缺失依赖或版本不兼容时给出明确 CMake 错误。
4. 验证开关关闭/开启两种构建，覆盖本机 MinGW/MSVC，并补充 Linux CI 或运行证据；
   完成后更新版本、获取命令、目标链接方式、测试结果和下一轮 T06 入口。

后续必须保留的边界：目前没有 HTTP/2 Session、在线协议自动选择、TLS/ALPN 或 GOAWAY。
Session 级 stream 取消、取消事件传播、Worker 饱和时的单流拒绝、解压后 Header/Body
以及待编码输出的累计预算，仍分别属于 T06–T08，不能仅靠现有整连接超时与关闭完成。
每轮到选定任务验收后即总结交接，不自动展开全部剩余任务。

## 第三轮执行记录：T05 依赖接入（2026-11-10）

本轮实现 T05，完成 Windows 上开启/关闭两种配置验证。Linux CI 已配置，尚未实际执行，
所以 T05 在总清单中保留待整体验收标记。本轮不实现 T06。

实现与决定：

- [CMakeLists.txt](CMakeLists.txt) 新增 NETLIB_ENABLE_HTTP2，默认 OFF。
  OFF 时不获取 nghttp2、不启用 C，也不读取本地归档。ON 才启用 C 编译器。
- [NetlibNghttp2.cmake](cmake/NetlibNghttp2.cmake) 固定官方稳定版 **1.71.0** 的
  nghttp2-1.71.0.tar.xz；SHA-256 为
  `c0f55121d88650e4b9227aec28cebaf664d2417ca60fe70be0f8d38829f52116`，
  已核对官方发布附件及 checksums.txt。FetchContent 通过 HTTPS 和 URL_HASH 校验获取；
  支持 NETLIB_NGHTTP2_ARCHIVE 指向同一发布归档进行离线构建。
- 只构建 C 静态库，关闭上游工具、示例、文档、测试和额外协议应用；netlib 仍为 C++20。
  私有依赖链为 netlib -> netlib_nghttp2 -> nghttp2_static，最终可执行文件获得静态链接
  要求，使用方不继承 nghttp2 头文件目录及内部宏。已核对编译数据库中的实现/使用方命令。
- 内部启用 NGHTTP2_NO_SSIZE_T，用 nghttp2_ssize 和 mem_send2/mem_recv2 等便携 API。
  MSVC 不定义 POSIX ssize_t，若忽略此开关，包含上游头文件会因旧接口声明编译失败。
  不修改上游源码，不向 netlib 公开头文件添加兼容 typedef。
- 上游 cmake_dependent_option 会强制写入 BUILD_TESTING cache；已在配置子项目前保存、
  完成后恢复，避免关闭父项目测试。其他构建选项留在函数作用域，保持 netlib 的 C++20
  和编译选项；全新 Release/ON 配置也验证默认 BUILD_TESTING=ON。
- 许可证与版本来源保存在 [third_party/nghttp2](third_party/nghttp2/README.md)，实际源码
  位于各构建目录的 _deps，未整体拷入仓库。分发静态链接产物时需保留 MIT 许可文本。

验证入口和附带修正：

- [nghttp2_test.cc](tests/nghttp2_test.cc)：头文件/运行库的固定版本校验、客户端与服务端
  内存会话、逐字节 preface、SETTINGS 与 ACK 往返，仅 ON 时注册。
- [nghttp2_configuration_test.cmake](tests/cmake/nghttp2_configuration_test.cmake)：
  OFF 配置容忍无效归档和无效 C 编译器，且不产生依赖目录；ON 时归档缺失、校验不匹配、
  源码覆盖版本不兼容均明确报错。这些配置测试不联网。
- MSVC/ON 实际验证了默认在线下载；MinGW Debug/Release ON 实际验证了本地官方归档。
- [build.yml](.github/workflows/build.yml) 保留 Linux GCC / Windows MSVC 的 Debug、
  Release 覆盖，分别加入 OFF/ON；新增 MinGW UCRT64 Debug 的 OFF/ON，共 10 个 job。
  已通过 actionlint 1.7.12 检查，尚未提交或触发远端运行。
- 全量测试曾在 MinGW Release 的 platform_test 并发唤醒用例中误报，ON/OFF 均出现。
  增加子用例诊断后，在四组并发压力下定位到 Drain 后立即检查空 poll 的断言。
  Windows TCP 唤醒有在途字节，该断言不成立。[platform_test.cc](tests/platform_test.cc)
  已改为有截止时间的排空等待，并保留对 Drain 失效的检测；修正后四组各 20 次，
  **80/80 压力运行通过**。只调整测试，未改动 platform 生产实现。

本机最终结果：

| 工具链 / 配置 | HTTP/2 OFF | HTTP/2 ON |
| --- | --- | --- |
| MinGW Debug | 22/22 | 23/23 |
| MinGW Release | 22/22 | 23/23 |
| MSVC Debug | 22/22 | 23/23 |

OFF 比第二轮多一项依赖配置测试，ON 另多一项 nghttp2 内存会话测试。
本机未安装 WSL Linux 发行版，也无 Docker/Podman，Linux ON/OFF 尚未运行；等上述 CI
实际执行后再补充结果并勾选 T05。MSVC 原有 FrameMessage 参数遮蔽成员警告仍在。

构建命令、离线使用、报错和链接边界见 [http2_build.md](docs/http2_build.md)，README
和设计文档已同步。clangd 的 build/debug 编译数据库已配置为 ON，以便识别新增内部测试
及后续 Session；这是本机构建缓存，源码默认值仍为 OFF。格式、文档本地链接/锚点和
代码块、git diff --check 已检查。前三轮成果随 codex/http2-support 分支提交保存，
后续从该分支继续；远端 CI 是否通过需要单独核对，推送本身不代表 Linux 已验收。

### 下一轮明确入口：T06

1. 继续当前 codex/http2-support 分支，保留前三轮已提交成果。先核对 Linux CI 是否已有
   运行证据，有则回填 T05；若尚未运行，明确保留待验收记录，不宣称已在 Linux 实测。
2. 阅读 protocol_session.h、docs/http2_design.md 和 docs/http2_build.md；在 src 内实现
   原生 Http2Session，通过 CreateWithSession 接入。不改造成一个只能产单 payload 的 Codec。
   nghttp2 类型和两个内部宏继续留在实现侧，统一使用不依赖 ssize_t 的便携接口。
3. 每条连接由 owner Reactor 驱动一个 nghttp2 server session，优先完成 preface、
   SETTINGS/ACK、HPACK HEADERS/CONTINUATION、DATA、PING 的输入输出。完整请求按
   stream 聚合，在 END_STREAM 后发出拥有数据的 SessionRequest，context 保留独立 stream_id。
4. 按 stream 路由 HttpResponse，跨 stream 不做 HTTP/1 pipeline 排序；输出需要服从
   对端连接/stream 窗口。明确输入与 Body 限额，不能为尽早跑通而忽略基本流控。
5. mem_send2 返回内存属于 nghttp2，后续 nghttp2 调用可能使其失效；协议输出需正确处理
   生命周期和部分写，不可把内部指针长期留给 socket 发送。等待窗口时应继续处理控制输入。
6. 用真实 nghttp2 客户端内核和 TCP 接入验证至少两条 stream 的交错输入、乱序完成响应、
   分片与控制帧；再回归旧协议。T05 的内存握手测试仅证明依赖可用，不能替代这一步。
7. 阶段结束更新清单、实现接口、测试证据、限制及 T07 入口。明文自动选择在 T09，TLS/ALPN
   在 T10，stream 取消/排空及更完整预算分别在 T07/T08，暂不扩大本轮范围。

## 本日交接封存（2026-11-10）

按用户要求，将前三轮实现、文档和测试整理提交到 codex/http2-support 并推送 origin。
本日工作到此结束，不继续 T06。下次先检查分支的 GitHub Actions 结果，补充 T05 的
Linux ON/OFF 证据；通过验收后再更新清单，随后继续上述 T06 入口。

## 第四轮执行记录：T05 收尾与 T06（2026-11-10）

本轮按难度评估完成两项：补齐 T05 验收，以及实现 T06 原生 HTTP/2 Session。
T06 涉及协议状态、回调生命周期、取消和流控，未合并实施 T09 自动选择，也未将已有
GOAWAY 基础等同于完成 T07 的有期限排空。

### T05 远端证据

已核对 [Actions 42650782593](https://github.com/diveinyourheart/netlib/actions/runs/42650782593)，
对应前三轮提交 4da60510a43f2e0208f6495eb20bb4c0e7deb761，十个作业全部 success：
Ubuntu 24.04 GCC 和 Windows MSVC 的 Debug/Release × ON/OFF，以及 MinGW UCRT64
Debug ON/OFF。Linux 缺失的实际构建和运行证据已补齐，T05 现可勾选。
该 CI 结果不覆盖本轮尚未提交的 T06 代码。

### T06 实现及接口决定

- 新增 [http2_session.h](include/netlib/http2_session.h) 和
  [http2_session.cc](src/http2_session.cc)。工厂 CreateHttp2Session(limits, options)
  供 CreateWithSession 显式接入；关闭构建功能或选项非法时返回 nullptr。
  不对长度前缀/RPC/HTTP/1 入口偷偷启用探测。
- nghttp2 server session 只由 owner Reactor 访问，处理 preface、SETTINGS/ACK、HPACK、
  HEADERS/CONTINUATION、DATA、PING。每流独立聚合，END_STREAM 后发出 owned 请求，
  本地 request_id 按完整请求完成顺序分配，与 stream_id 独立。
- HttpRequestMetadata 新增 scheme/authority，HTTP/2 version 为 HTTP/2；普通 Header 与
  Body 保留独立所有权。旧 HTTP/1 字段与响应接口保持兼容。
- 应用仍返回 HttpResponse；按 context 寻址 stream，忽略跨流排序提示，校验/规范化响应
  Header 和 Content-Length，自动补正文长度，HEAD/204/304 不发正文。拒绝连接专用字段、
  非法字段值/状态、错误响应类型，重置该流而不是让其他流一起失败。
- 接收窗口显式 consume，存入有界缓冲或丢弃数据后立即返还信用，解决大请求在初始窗口
  内收不完的问题。发送遵守 peer 的连接/stream 窗口；零窗口不阻止其他流和控制输入。
- ProduceOutput 使用 send_callback2 复制到 Session 拥有的输出向量，并支持预算内部分
  接收；没有跨 nghttp2 调用保存 mem_send2 内部指针。响应正文只移动一次到稳定 Stream
  对象，已编码字节按 socket 实际发送量消费，流关闭时回收保留正文。
- 增加每流 Header/Body、并发 stream、Session 正文合计及输入分块限制。Header 预算按
  name/value + 每项 32 字节计算；超限流不派发业务。流控、部分写和这些基础限制从首版
  生效，T08 的全链路预算与队列背压仍未完成。
- SessionInputResult 新增 cancelled_requests；ProtocolSession 增加默认空实现的
  TakeCancelledRequests，覆盖协议输出阶段产生的关闭事件。Reactor 两处均按保存的
  RequestContext 复核并取消 timer，取消或超时后的迟到回复不再传给 Session。
- OnTimeout 当前对尚未回应的流发送 504，保持连接可用；BeginDrain 发 GOAWAY，固定
  最后接受的 stream，已有流继续收发。响应的 keep_alive=false 或 close_after_write
  请求进入此会话排空过程，外部 Stop 的行为仍未改成自动排空。

### 验证结果

新增 [http2_factory_test.cc](tests/http2_factory_test.cc)，ON/OFF 都只链接公开 netlib
目标，验证工厂行为、选项与第三方宏隔离。新增
[http2_session_test.cc](tests/http2_session_test.cc)，使用真实 nghttp2 客户端内核：

- 内存链路逐字节拆帧，双流交错 Body/END_STREAM 与本地编号分离、乱序完成响应、控制 ACK。
- 7 字节输出预算与部分写；130000 字节请求及 40000 字节 Header/CONTINUATION；
  小 stream 窗口耗尽、其他流继续响应、WINDOW_UPDATE 后恢复。
- Header、单流及连接正文预算，畸形 preface、HEAD、非法应用响应的流隔离。
- 输入侧 RST 和输出侧 RST 的取消事件，迟到响应、单流 504 与 GOAWAY 完成已有流。
- 实际 TCP + MainReactor/SubReactor + Worker 链路：双流反序响应；仅一个在途槽时 RST
  与新请求连续进入，验证旧 context/timer 及时回收；真实 timer 触发 504 后连接继续服务。

本机全量回归：

| 配置 | HTTP/2 OFF | HTTP/2 ON |
| --- | --- | --- |
| MinGW Debug | 23/23 | 25/25 |
| MinGW Release | 23/23 | 25/25 |
| MSVC Debug | 23/23 | 25/25 |

MSVC OFF 在高并发构建验证中暴露旧无序响应测试的状态发布竞态：收到 socket 响应不代表
生产者已从 RespondUnordered 返回并发布测试标志。已将相关断言移到 Worker join 之后，
修正后 MSVC 全量通过。只改测试同步，不改变业务回写语义。MSVC 仍有先前记录的
FrameMessage payload 参数遮蔽警告。本轮 T06 尚无 Linux 实测，待下一次推送后的 CI 补证据。

接入、元数据、流控、选项与限制见 [http2_session.md](docs/http2_session.md)；README、
构建/设计/架构文档已同步。clangd 编译数据库已刷新；格式、文档链接/锚点、代码块和
git diff --check 已检查。本轮改动保留在工作区，未自动提交或 push。

### 下一轮入口：T07

1. 继续当前分支与本轮工作区，先查看 T06 提交后的 CI 结果（如已推送），不要把 T05 的
   绿色 CI 当成本轮 Linux 验证。
2. 阅读新 http2_session.md 和协议源码。现有业务 timer 仅从投递 Worker 到接受应用响应，
   不覆盖 Header/Body 接收、等待发送窗口和最终写出；分开设计这些阶段的超时。
3. 定义 owner 线程的外部 drain 命令及排空期限，再将服务停止与 BeginDrain/GOAWAY 接通；
   既有流需要继续 WINDOW_UPDATE/DATA 输入，不能关闭读兴趣后等待它们结束。
4. 增加请求级取消状态/通知，让 Worker 可感知 RST/超时而停止无效工作；继续保留 Reactor
   的最终身份复核。对响应/取消/超时竞争、GOAWAY 后新流、排空期限届满进行测试。
5. 完成 T07 后再推进 T08：把 Worker 持有正文、队列、nghttp2 内部状态与向量容量纳入
   字节预算，补 Worker 满时的单流拒绝与每轮字节/帧公平性。当前 Submit 失败仍关闭整连接。
6. T09 再组合已实现的检测器与 HTTP/1/2 Session，实现同端口自适应。CONNECT、trailers、
   流式业务、1xx 和 server push 当前不支持；不要误报为已支持 HTTPS/gRPC。

## 第五轮执行记录：T07（2026-11-10–11）

本轮完成 T07，不展开 T08/T09。本轮继续在 T06 的未提交工作区上实现，没有覆盖或丢弃
前轮成果。T06/T07 按 2026-11-11 的收尾要求统一提交并推送；Linux 运行验证待该提交的 CI。

### 实现及接口决定

- ProtocolSession 新增默认无期限的 NextDeadline/OnDeadline。Http2Session 用有序期限表
  和每流迭代器维护 Header、Body 空闲、响应发送期限；取最早期限 O(1)、更新 O(log N)、
  删除 O(1)。每连接只挂一个协议/写停滞闹钟，不轮询扫描，也不在 timer 线程操作协议。
- Header 期限同时覆盖 preface/首个 SETTINGS、半帧和未完成 HPACK Header 块。轻量帧
  边界计数仅跟踪 9 字节帧头和 payload 长度，不重复解析 HPACK；这些状态无法安全丢弃
  后继续解码，到期关闭整连接。Body 空闲到期只重置相应流，有效 DATA 进度才刷新期限。
- 业务超时仍从提交到 owner 接受回复，HTTP/2 返回单流 504；响应接受后另有绝对发送期限，
  覆盖等待 peer 窗口，过期发 RST。用户态待发输出的 socket 无进展期限是连接级保护。
  Header 默认 10 秒，Body/响应/写停滞/排空默认 30 秒；业务默认 10 分钟。可配置为
  (0, 24 小时]，完整含义见 [http2_lifecycle.md](docs/http2_lifecycle.md)。
- 保存业务绝对截止时间，不能因 moodycamel 跨 producer 乱序让过期回复优先被接受。
  协议/排空闹钟带 generation，取消后已经选中的旧 callback 只会投递无效控制事件。
  控制命令复用隐式 producer，不受普通响应队列容量限制；常驻 Worker 显式 token 不变。
- FrameMessage 新增 stop_token 和 IsCancelled；Reactor 在派发前登记 stop_source/timer，
  peer RST、超时、关闭、强制排空和 Stop 会通知仍在途 Worker。正常回复被 owner 接受
  后该业务结束，不再补发取消。FrameMessage 的回写助手先拒绝已取消消息，原始
  ConnectionHandle 继续保持入队语义，owner 仍作最终复核。
- MainReactor::Drain(timeout) 停止监听接收，向各 SubReactor 请求有期限排空；也能单独
  SubReactor::Drain。拒绝新 Adopt，清理未注册描述符，在 owner 中 BeginDrain。
  重复请求只能缩短期限。既有 HTTP/2 流继续读取 DATA/WINDOW_UPDATE，最后接受的
  stream ID 不再增加；peer GOAWAY 和会话自身排空也受本地期限限制。
- 到期强制关闭剩余连接并停止 loop；正常排空结束自动退出。Stop 保持立即停止，Run
  退出时统一取消 timer/token、清理连接并作废句柄；Main Run 退出解除监听并关闭 socket。
  Worker 取消是协作式的，网络排空期限不能强行终止忽略取消的业务线程。
- 新增 platform::SetSendBuffer，用于设置发送缓冲及构造真实写停滞测试。分发/Worker
  两方向 moodycamel 队列架构、Codec 子类和默认协议入口保持兼容。顺带将回写助手的
  payload 形参更名，消除之前已记录的 MSVC C4458 遮蔽警告。

### 验收与结果

- 新增 [drain_test.cc](tests/drain_test.cc)，ON/OFF 都运行：完整响应后排空退出、拒绝
  新 Adopt、重复请求不延长截止时间、强制排空/Stop 的 stop_token 与回调线程、真实
  socket 写停滞。测试使用小发送缓冲和分块 Codec 输出，避免把 Windows 已接收进
  内核的超大 send 当作用户态停滞。
- 专门阻塞测试进程的 TimerScheduler 回调，让超时控制暂时无法送达；不同 producer 的
  过期回复先进入 owner 后仍触发超时，验证绝对期限复核，而非依赖队列顺序。
- 扩展 [http2_session_test.cc](tests/http2_session_test.cc)：preface/部分帧头/HPACK 块
  确定性到期，Body 续传后旧期限不误杀，Body 空闲及零窗口发送超时隔离，GOAWAY 最后
  stream ID 和新流边界，强制排空期限。真实 TCP 验证外部 Drain 与握手期限调度。
- 扩展工厂参数边界测试；既有 peer RST、业务 timer 和迟到回写验证增加 token 判定。
  旧 Codec、HTTP/RPC、线程池、timer 和日志回归全部继续通过。

| 本机配置 | HTTP/2 OFF | HTTP/2 ON |
| --- | --- | --- |
| MinGW Debug | 24/24 | 26/26 |
| MinGW Release | 24/24 | 26/26 |
| MSVC Debug | 24/24 | 26/26 |

本轮编译没有新增警告，并已消除原有 FrameMessage 参数遮蔽警告。clangd 编译数据库已
刷新；Google 代码格式、文档本地链接/锚点、代码块和 git diff --check 已检查。
本轮 T07 与前轮 T06 一起提交推送，不能把 T05 的绿色 Linux CI 当作新代码的运行证据。

### 下一轮明确入口：T08

1. 从 codex/http2-support 的 T06/T07 提交继续，先检查该提交的 CI，再回填 Linux
   证据。按后续用户指令推进，不要自动把所有后续任务实现或提交。
2. 阅读 [http2_lifecycle.md](docs/http2_lifecycle.md)、http2_session.md 及新期限/取消代码。
   stop_source、timer、FrameMessage 所有权和当前 Header/Body 预算都应纳入后续设计。
3. 目前正文预算按 vector.size 统计，只管 Session 持有的正文，不包含 vector.capacity、
   Worker/响应队列持有的正文与 nghttp2 内部状态。先设计跨所有权移动的字节预算凭证，
   避免搬到队列就漏记，也不要在取消通知发出时提前释放 Worker 仍持有的内存预算。
4. 添加 owner 中的“请求派发拒绝”事件/回调，把 Worker 队列满从整连接关闭改为合适的
   单流拒绝/重置；仍要消费必要的控制输入，不能等 Worker 完成才返还全部接收窗口。
5. 为读取、解码、输出、响应命令处理设置每轮预算和继续调度入口；注意 epoll ET 下
   不能预算用尽就丢掉后续就绪。验证控制洪流、大 Body、慢流、队列饱和与取消后的预算归还。
6. T08 验收后才进入 T09 的明文自动选择。TLS/ALPN、流式业务、trailers、CONNECT 等
   尚未实现，不要扩展现有能力声明。

## 本日交接封存（2026-11-11）

按用户要求，将第四、第五轮的 T06/T07 实现、测试与文档统一提交到
codex/http2-support 并推送远端，今天到此结束。下一轮先核对该提交的 CI，
再从上面的 T08 入口继续；本机验证结果已记录，尚未取得该提交的 Linux CI 结果。
