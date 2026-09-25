# mini_dds

一个用于学习 DDS、RTPS 与实时通信的轻量级 C++17 项目。

当前进度：完成带自动发现、Best-Effort/可靠一对多发布、异步 Reliable 写入和 Participant 多端点路由的 MVP，包括跨平台 UDP/组播、CDR、RTPS DATA/HEARTBEAT/ACKNACK/GAP、History Cache、SPDP/SEDP，以及 `DomainParticipant`、`Topic`、`Publisher/Subscriber`、`DataWriter/DataReader`。

完整设计与进度见 [架构文档](docs/architecture.md)。

## 构建

```bash
cmake -S . -B build -G Ninja
cmake --build build
ctest --test-dir build --output-on-failure
```

项目位于包含中文字符的 Windows 路径时，建议使用 Ninja；部分 MinGW Make 版本无法正确处理这类源文件路径。

## DDS 发布订阅示例

先启动接收端：

```bash
./build/subscriber
```

再启动发送端：

```bash
./build/publisher
```

Windows 多配置生成器的可执行文件通常位于 `build/Debug/` 或 `build/Release/`。

`publisher` 和 `subscriber` 使用 SPDP/SEDP 自动发现与 Reliable QoS，不需要配置彼此的数据端口。当前示例把 `advertised_address` 设为 `127.0.0.1`；跨机器运行时需要改为对端可访问的网卡 IPv4 地址。

`sender`/`receiver` 可用于单独验证裸 UDP 传输层。固定地址方式仍可通过 `DataWriterConfig::remote_endpoint` 使用。

当前发现实现是 RTPS 发现协议的最小子集，能够让两个 mini_dds 进程互相发现，但尚不保证与第三方 DDS 实现互操作。

每个 `DomainParticipant` 由一个用户数据接收线程读取底层 UDP Socket，再根据 Submessage 类型和 EntityId 把 DATA/HEARTBEAT/GAP 分发给 Reader，把 ACKNACK 分发给 Writer。因此同一 Participant 可以同时运行多个 DataReader/DataWriter，不再由端点线程竞争同一个 Socket。

Reliable 路径支持同步和异步两种发布模式。默认同步模式下，`write()` 会等待全部匹配 Reader 确认；把 `EndpointQos::publish_mode` 设为 `PublishModeKind::asynchronous` 后，`write()` 在样本进入后台 FIFO 队列后返回，应用可通过 `wait_for_acknowledgments()` 等待当前已提交样本完成。两种模式都只向未确认 Reader 按 `acknowledgment_timeout` 与 `max_retries` 有界重传。

异步模式当前使用单工作线程逐条确认，没有批处理、队列容量限制或并行在途窗口。销毁 `DataWriter` 会停止后台发送；需要确保送达时，应在销毁前调用 `wait_for_acknowledgments()` 并检查返回值。
