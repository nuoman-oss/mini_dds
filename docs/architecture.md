# mini_dds 架构设计

## 1. 项目目标

mini_dds 是一个用于学习 DDS 与 RTPS 的小型实现。项目优先保证协议边界清晰、代码可测试、演进过程可观察，而不是一次性覆盖完整 DDS 规范。

第一个可用版本（MVP）的目标是：

- 支持 Windows 与 Linux 上的 IPv4 UDP 通信；
- 两个进程可以通过固定对端地址发布和订阅一个 Topic；
- 使用 CDR 序列化应用数据；
- 使用 RTPS Message Header 与 DATA Submessage 承载数据；
- 同时提供 Best-Effort 与同步确认的最小 Reliable 路径；
- 每个协议组件都有独立的编解码测试。

后续版本继续增加异步可靠传输、更多 QoS 和第三方 DDS 互操作。

## 2. 当前范围与非目标

### 2.1 当前范围

- IPv4 UDP 单播与组播；
- RTPS 2.x 基础消息格式；
- 手写 TypeSupport 和基础 CDR 类型；
- 单进程一个 DomainParticipant；
- 固定对端发布/订阅；
- Best-Effort DataWriter/DataReader；
- SPDP/SEDP 自动发现；
- Best-Effort 与最小 Reliable 一对多 Writer/Reader。

### 2.2 暂不实现

- DDS Security；
- TCP、共享内存等传输；
- 完整 IDL 编译器与代码生成器；
- Persistence、Ownership 等高级 QoS；
- DDS-XTypes 的完整动态类型系统；
- 一开始就追求与所有商业 DDS 实现完全互操作。

## 3. 分层架构

```mermaid
flowchart TB
    APP[Application]
    DDS[DDS API\nDomainParticipant / Topic\nPublisher / Subscriber\nDataWriter / DataReader]
    CACHE[History Cache\nWriterHistory / ReaderHistory]
    DISCOVERY[Discovery\nSPDP / SEDP]
    RTPS[RTPS Protocol\nMessage / Submessage\nWriter / Reader]
    SERIALIZATION[Serialization\nCDR / TypeSupport]
    TRANSPORT[Transport\nUDP unicast / multicast]
    OS[Operating System\nWinsock / POSIX Socket]

    APP --> DDS
    DDS --> CACHE
    DDS --> SERIALIZATION
    CACHE --> RTPS
    DISCOVERY --> RTPS
    RTPS --> SERIALIZATION
    RTPS --> TRANSPORT
    TRANSPORT --> OS
```

依赖只能自上向下。Transport 不认识 RTPS，RTPS 不依赖具体业务类型，DDS API 不直接操作 Socket。

## 4. 模块职责

### 4.1 `transport`

负责字节数据报的收发，不解析 RTPS 内容。

核心对象：

- `Endpoint`：IP 地址与端口；
- `Datagram`：负载与来源地址；
- `ITransport`：传输层抽象；
- `UdpTransport`：Winsock/POSIX Socket 实现。

约束：

- Socket 对象不可复制，避免重复关闭；
- 接收必须支持超时，不能让上层永久卡死；
- 网络错误通过结构化结果返回；
- 组播加入与退出由传输层管理。

### 4.2 `serialization`

负责 CDR 字节序、对齐和基础类型编解码。

第一阶段支持：

- `uint8/16/32/64`；
- 有符号整数；
- 字符串；
- 原始字节序列；
- Little-Endian 与 Big-Endian。

业务类型通过 `TypeSupport<T>` 转换为 CDR Payload，RTPS 层只处理 Payload 字节。

### 4.3 `rtps`

负责 RTPS 线协议，不提供面向业务的 DDS API。

内部拆分为：

- `types`：GUID、EntityId、SequenceNumber、Locator；
- `message`：RTPS Header、Submessage Header；
- `messages`：DATA、HEARTBEAT、ACKNACK 等具体 Submessage；
- `reader` / `writer`：RTPS 端点状态机；
- `ParticipantMessageRouter`：单点收包并按 Submessage 类型与 EntityId 分发到端点队列；
- `discovery`：SPDP 与 SEDP。

协议解析必须先检查长度、标志位和字节序，再读取字段。任何网络输入都不能假定合法。

### 4.4 `dds`

为应用提供较稳定的接口：

- `DomainParticipant`：持有 Domain、GUID Prefix、传输、用户数据路由和发现服务；
- `Topic`：Topic 名与类型名；
- `Publisher` / `Subscriber`：端点容器；
- `DataWriter<T>`：序列化并写入 WriterHistory；
- `DataReader<T>`：从 ReaderHistory 取样并反序列化。

DDS 层负责对象生命周期与配置，网络协议细节不能泄漏到应用接口。

### 4.5 `history` 与 `qos`

`WriterHistory` 保存待发送或待确认的 CacheChange，`ReaderHistory` 保存已接收样本。

首批 QoS：

- Reliability：Best-Effort / Reliable；
- History：KeepLast；
- Durability：Volatile。

实现顺序先稳定 Best-Effort 数据面，再在相同传输与 History 边界上叠加 Reliable 状态机。

## 5. 目录规划

```text
mini_dds/
├── docs/
│   └── architecture.md
├── include/mini_dds/
│   ├── dds/
│   ├── discovery/
│   ├── history/
│   ├── qos/
│   ├── rtps/
│   ├── serialization/
│   └── transport/
├── src/
│   ├── dds/
│   ├── discovery/
│   ├── history/
│   ├── rtps/
│   ├── serialization/
│   └── transport/
├── examples/
└── tests/
```

目录按模块逐步创建，不为尚未设计的模块提交无行为的空类。

## 6. 数据发送流程

```text
Application value
  -> DataWriter<T>::write
  -> TypeSupport<T>::serialize (CDR)
  -> WriterHistory::add_change
  -> RTPS DATA Submessage
  -> RTPS Message Header
  -> ITransport::send
  -> UDP datagram
```

接收方向执行完全相反的过程：

```text
UDP datagram
  -> RTPS Message validation
  -> Reader endpoint matching
  -> ReaderHistory::add_change
  -> TypeSupport<T>::deserialize
  -> DataReader callback / take
```

## 7. 线程模型

MVP 使用简单且可控的线程模型：

- SPDP/SEDP 服务拥有独立的公告与接收线程；
- 每个 Participant 由一个 `ParticipantMessageRouter` 线程接收用户数据报；
- 路由线程按 EntityId 把报文放入端点队列，`DataReader::take()` 从自己的队列同步解析；
- Best-Effort `write()` 只负责发送；Reliable 默认同步等待 ACKNACK，也可配置后台发送队列；
- 当前没有用户回调线程，样本统一通过 `take()` 取出；
- 停止时通过有限接收超时退出，不依赖强制终止线程。

异步 Reliable Writer 使用一个可停止的工作线程，按提交顺序逐条发送并确认。`write()` 表示样本已进入队列，`wait_for_acknowledgments()` 用于观察后台送达结果。

## 8. 标识与端口策略

- 一个 Participant 对应一个 12 字节 `GuidPrefix`；
- 每个 Writer/Reader 使用一个 4 字节 `EntityId`；
- 完整 GUID 为 `GuidPrefix + EntityId`；
- Writer 的每个 CacheChange 使用单调递增 `SequenceNumber`；
- 固定对端阶段允许显式配置 IP 与端口；
- 发现阶段按照 RTPS Domain 端口映射计算内建端点端口。

在获得正式 Vendor ID 之前使用未分配/实验标识，并且不宣称完全互操作。

## 9. 错误处理与可观测性

- 配置错误在创建对象时返回失败；
- 网络超时与网络错误必须区分；
- 协议解析返回明确错误，不允许越界读取；
- 日志至少包含模块、远端地址、EntityId 和 SequenceNumber；
- 测试中可以注入假的 `ITransport`，避免所有测试依赖真实网络。

## 10. 测试策略

### 单元测试

- CDR 对齐、字节序、字符串长度与非法输入；
- RTPS Header/Submessage Header 的已知字节向量；
- DATA、HEARTBEAT、ACKNACK 编解码；
- History 去重、排序与深度限制；
- Reader/Writer 状态机。

### 集成测试

- 本机 UDP loopback；
- 两个进程固定端点发布订阅；
- 丢包、乱序、重复包；
- Participant 自动发现；
- 同一 Participant 上多个 Reader/Writer 的并发路由；
- 使用 Wireshark 检查报文字段；
- 最终与 Fast DDS 或 Cyclone DDS 做有限互操作验证。

## 11. 里程碑

### M0：UDP 原型（已完成）

- [x] UDP Socket 创建、绑定、发送、接收；
- [x] sender/receiver 本机通信示例；
- [x] CMake 基础工程。

### M1：可测试的协议地基（本轮）

- [x] 跨平台 `ITransport` 与 `UdpTransport`；
- [x] 接收超时和来源地址；
- [x] CDR 基础 Reader/Writer；
- [x] RTPS Message Header 与 Submessage Header；
- [x] 单元测试和 UDP loopback 测试。

### M2：固定端点 Best-Effort 数据面（已完成）

- [x] GUID、EntityId、SequenceNumber 基础行为；
- [x] DATA Submessage 编解码；
- [x] `CacheChange`、WriterHistory、ReaderHistory；
- [x] StatelessWriter/StatelessReader；
- [x] 固定对端 `DataWriter`/`DataReader` 示例。

验收：subscriber 能收到 publisher 发布的结构化样本，而不是裸字符串 UDP 包。

### M3：DDS API（已完成最小版本）

- [x] DomainParticipant；
- [x] Topic 与 StringTypeSupport；
- [x] Publisher/Subscriber；
- [x] DataWriter/DataReader；
- [x] RAII 生命周期与资源释放。

当前 M3 API 状态：

- 一个 Participant 可同时持有多个 DataReader/DataWriter，用户数据由 M8 路由层隔离；
- Writer 可以使用固定远端，也可以通过 SEDP 自动匹配全部同 Topic、同类型 Reader；
- Best-Effort 与 Reliable 均已接入统一的 `DataWriter`/`DataReader` API。

### M4：自动发现（已完成最小版本）

- [x] UDP 组播；
- [x] ParameterList；
- [x] SPDP Participant Discovery 与租约过期；
- [x] SEDP Endpoint Discovery；
- [x] Topic 与类型匹配。

当前 M4 限制：

- SEDP 尚未使用 Reliable 内建端点状态机，而是通过周期性重复公告提高容错；
- 尚未处理第三方实现常用的复合 Submessage、更多 PID 和兼容性规则；
- Endpoint 删除尚未发送 dispose，远端记录会随 Participant 租约清理；
- 使用未分配的实验 VendorId，因此不宣称第三方互操作。

### M5：可靠性与 QoS（已完成最小版本）

- [x] `SequenceNumberSet` 与 HEARTBEAT、ACKNACK、GAP 编解码；
- [x] Reliable Writer/Reader 状态机；
- [x] ACK 超时、缺口反馈与有界重传；
- [x] KeepLast、Volatile 最小 QoS；
- [x] SEDP 可靠性匹配；
- [x] 首包丢失恢复、确认超时与双进程集成测试。

当前 Reliable 实现的单条样本仍采用 stop-and-wait 模型：Writer 向全部匹配 Reader 发送 DATA 与 HEARTBEAT，分别记录每个 Reader 的 ACKNACK；收到 NACK 或确认超时后，只向尚未确认的 Reader 在 `max_retries` 范围内重传。同步发布由调用线程执行该过程，异步发布由后台工作线程执行。Reader 会缓存乱序 DATA，只把连续序列交给 ReaderHistory。

当前 M5 限制：

- SEDP 当前要求 Writer/Reader 可靠性精确相等，尚未实现完整的 requested/offered QoS 兼容规则；
- 没有独立的空闲期 HEARTBEAT 定时器；仅在存在待确认样本时周期发送 HEARTBEAT；
- GAP 已支持编解码和 Reader 处理，但 Writer 尚未为已淘汰样本主动生成 GAP；
- History 目前只有 KeepLast，Durability 目前只有 Volatile；
- 尚未实现 DATA_FRAG、HEARTBEAT_FRAG 与 NACK_FRAG。

### M6：一对多发布（已完成最小版本）

- [x] Discovery 返回全部匹配 Reader，并在每次写入前刷新匹配集合；
- [x] Best-Effort Writer 将同一 CacheChange fan-out 给全部 Reader；
- [x] Reliable Writer 维护逐 Reader 确认状态；
- [x] 已确认 Reader 不参与其他 Reader 触发的重传；
- [x] 双 Reader 自动发现与选择性丢包集成测试。

新加入或租约过期的 Reader 会在下一次 `write()` 刷新时加入或移出匹配集合；已入异步队列的样本保留其提交时的 Reader 快照。

### M7：异步 Reliable 发布（已完成最小版本）

- [x] `PublishModeKind::synchronous/asynchronous` QoS；
- [x] Reliable Writer 后台 FIFO 队列与可停止工作线程；
- [x] `write()` 入队语义与 `wait_for_acknowledgments()` 确认屏障；
- [x] 待处理样本计数和后台失败回传；
- [x] 连续样本顺序、ACK 超时和 DDS 类型化 API 集成测试。

当前 M7 限制：

- 后台只有一个工作线程，每条样本仍需完成全部 Reader 确认后才处理下一条；
- 队列暂无限长，尚未实现容量上限、背压、批处理或多样本在途窗口；
- 异步发布目前只用于 Reliable DataWriter，Best-Effort 仍由调用线程直接发送；
- `write()` 成功只表示入队成功，后台错误必须通过 `wait_for_acknowledgments()` 获取；
- 销毁 Writer 会停止工作线程并放弃尚未完成的队列，要求送达时需先显式等待。

### M8：Participant 多端点接收与路由（已完成最小版本）

- [x] Participant 级单一用户数据接收线程；
- [x] 每个 Reader/Writer 独立的 `ITransport` 接收队列适配器；
- [x] DATA、HEARTBEAT、GAP 按 ReaderId 路由；
- [x] ACKNACK 按 WriterId 路由；
- [x] Unknown ReaderId 向全部本地 Reader 广播；
- [x] 端点注册、重复 EntityId 检查和 RAII 注销；
- [x] 双 Reliable Reader 与双 Reliable Writer 并发集成测试。

当前 M8 限制：

- 路由器目前只处理每个 RTPS Message 中的第一个 Submessage；
- 尚未处理 INFO_DST、INFO_TS 等上下文 Submessage 和复合消息；
- 端点接收队列暂无限长，尚未提供容量限制和溢出策略；
- 路由阶段会调用现有解码器提取 EntityId，后续可改为轻量头部解析以减少 DATA Payload 复制。

## 12. 完成定义

一个模块只有同时满足以下条件才算完成：

1. 对外接口和依赖方向明确；
2. 正常路径与错误路径都有实现；
3. 编解码具有已知字节测试；
4. 跨平台代码至少能在当前 Windows 工具链编译；
5. 文档中的状态与代码保持一致；
6. 没有依靠无限忙循环或不可停止线程维持运行。

## 13. 规范依据

线协议字段以 OMG [DDSI-RTPS 2.5](https://www.omg.org/spec/DDSI-RTPS/2.5/PDF) 为依据。当前项目只实现规范的一个子集，因此只能描述为“基于 RTPS”，不能宣称完整符合或通过互操作认证。
