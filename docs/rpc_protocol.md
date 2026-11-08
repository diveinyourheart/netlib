# RPC 传输协议 v1

`CreateRpcCodec()` 实现 `ProtocolCodec`，与长度前缀 Codec、HTTP Codec 一样通过
`ProtocolCodecFactory` 注入每条连接。解析后的头部保存为
`FrameMessage::metadata` 中的 `RpcFrameMetadata`；`FrameMessage::payload`
只包含 protobuf 序列化后的二进制（压缩时为压缩后的字节）。

Codec 负责分帧和头部校验，不依赖 protobuf、gzip 或 zstd 库，不执行序列化或
压缩。RPC 层必须检查 serialization/compression，自行处理或拒绝不支持的算法，
并对解压后的大小设置独立上限。协议标签不代表本库已经实现这些算法。

## 线格式

所有多字节整数使用大端网络字节序。长度单位均为字节；不发送 C++ struct 内存。

```text
[u32 frame_total_len]       不含自身，等于 header_len + payload 长度
---- 固定头部：24 字节（以下偏移相对于头部起点）----
 0  [4 bytes magic]         ASCII "NRPC"
 4  [u8 version]            1
 5  [u8 flags]              bit0: 0=request，1=response；其他位必须为 0
 6  [u16 header_len]        包含固定头部、名称和 TLV，不含 frame_total_len
 8  [u64 request_id]        由调用方生成，响应原样返回
16  [u8 serialization]     1=protobuf
17  [u8 compression]       0=none，1=gzip，2=zstd
18  [u16 error_code]        请求必须为 0；响应 0=成功，其余由 RPC 层定义
20  [u16 service_name_len]
22  [u16 method_name_len]
---- 变长头部 ----
[service_name bytes]        无结尾 NUL，长度不包含 NUL
[method_name bytes]
[TLV extensions ...]       到 header_len 为止
---- payload ----
[protobuf bytes]           到 frame_total_len 为止；允许为空
```

每个 TLV 为 `[u16 type][u16 value_len][value bytes]`。type 0 保留；
1..0x7fff 为可选扩展，未知类型保留在 metadata 的有序列表中，业务层可以忽略。
重复类型和空 value 允许，具体类型的重复规则由后续 RPC 层定义。
type 最高位为 1 表示必须理解的扩展；当前没有已实现的必选扩展，收到此类字段
返回 `kProtocolError`，避免静默忽略关键语义。未来可以登记 trace context、
调用 deadline、认证等扩展，无需移动 payload 或修改固定头部。

请求必须有非空 service_name、method_name，名称作为字节串传递；编码、合法命名和
服务注册由 RPC 层约定。响应可以省略名称，仅靠 request_id 关联调用，也可携带名称。
request_id 采用 64 位降低长连接的编号回绕频率；0 合法，调用方负责在途 ID 唯一性。
未知版本、保留 flags、未知序列化或压缩编号当前均拒绝，未来需要显式实现新语义。

与最初示例相比，magic 用于识别协议，独立 version 避免挤占 flags，header_len
定义 payload 的可靠边界，TLV 用于兼容扩展；此 v1 不兼容没有这些字段的示例格式。

## 边界和解析状态

`RpcCodecOptions::max_frame_bytes` 默认 16 MiB，计算 header + payload；
`max_header_bytes` 默认 16 KiB，包括固定头部、名称和 TLV。
配置须满足 `24 <= max_header_bytes <= 65535`，且不超过 max_frame_bytes；
max_frame_bytes 还必须可由 u32 和本机加上前缀后的 size_t 表示。
`MaxDecodeBufferSize()` 返回 max_frame_bytes + 4。

长度前缀超限时只读 4 字节即可拒绝；读取固定头部后检查 magic、版本、字段编号和
各长度关系，再读取变长头部和 payload。TLV 必须完整位于头部内，不能借用 payload
补足长度。畸形帧返回 `kProtocolError`，SubReactor 关闭连接；Codec 保持错误状态。
解码器分为 length/header/payload 状态；头部只在完整时构造一次，分段到达的 payload
不会反复分配名称或扫描 TLV。不会仅凭声明长度预先分配整帧内存。

`required_bytes` 表示下一步需要的总未消费字节数，依次为 4、28、4 + header_len、
4 + frame_total_len（某些步骤可一次完成）。完整帧只消费自身长度，可继续解析粘包。
头部字符串及 TLV 由 metadata 拥有，移交 Worker 后不引用网络读缓冲。
编码先验证全部字段和大小，再追加到输出；校验失败不会改变已有输出。

## 接入和响应

```cpp
#include "netlib/rpc_codec.h"

std::unique_ptr<netlib::SubReactor> reactor = netlib::SubReactor::Create(
    options, workers, callbacks, [] { return netlib::CreateRpcCodec(); });

// 在 ThreadPool handler 内：
const netlib::RpcFrameMetadata* request =
    std::get_if<netlib::RpcFrameMetadata>(&message.metadata);
if (request != nullptr &&
    request->message_type == netlib::RpcMessageType::kRequest) {
  // 由 RPC 层根据 service_name/method_name 分派，按 compression 解压，
  // 再反序列化 message.payload；这里只示意回复一个空 protobuf 消息。
  netlib::RpcResponse response;
  response.metadata.request_id = request->request_id;
  response.metadata.error_code = 0;
  message.connection.Respond(message.sequence, std::move(response));
}
```

`ProtocolCodec::Encode()` 仅接受 `RpcResponse`，且 message_type 必须为 response；
默认 metadata 已设为 response。`EncodeRpcFrame(metadata, payload, output, options)`
是共用线格式编码入口，可供后续客户端编码 request，也可编码 response。源数据不得与
输出 vector 别名。解码支持两种方向；使用同一 envelope 时，上层必须验证接收方向，
服务端收到 response 不能当成调用执行。它不是握手、协商或完整 RPC 客户端实现。

`request_id` 是线上关联 ID，`FrameMessage::sequence` 是本地 Reactor 的每连接
响应顺序，回写时仍须传原 sequence，不要用 request_id 替代。
当前 SubReactor 仍按 sequence 写回，慢请求会阻塞后面的响应；实现此 Codec
不会启用 RPC 按 request_id 乱序回包。将来可独立扩展 Reactor 的响应排序策略。
每个投递给 Worker 的帧仍必须产生一个响应；单向调用、流式 RPC、心跳和取消尚未实现，
不能通过保留 flags 私自启用。远端业务错误通过 error_code 表达，不自动关闭连接。

Codec 帧上限与 `SubReactorOptions::max_pending_output_bytes` 分别控制单帧和输出积压。
若希望允许最大编码响应，输出上限至少应为 max_frame_bytes + 4；多帧积压还需另设预算。

运行协议及 Reactor 回环测试：

```bash
cmake --build --preset debug --target netlib_rpc_codec_test
ctest --test-dir build/debug -R netlib_rpc_codec_test --output-on-failure
```
