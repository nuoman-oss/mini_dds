# mini_dds

一个用于学习 DDS、RTPS 与实时通信的轻量级 C++17 项目。

当前进度：完成带自动发现、Best-Effort/可靠一对多发布的 MVP，包括跨平台 UDP/组播、CDR、RTPS DATA/HEARTBEAT/ACKNACK/GAP、History Cache、SPDP/SEDP，以及 `DomainParticipant`、`Topic`、`Publisher/Subscriber`、`DataWriter/DataReader`。

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

Reliable 路径当前是多 Reader、逐 Reader 同步确认模型：`write()` 会等待所有匹配 Reader 的 ACKNACK，并仅对未确认 Reader 按 `EndpointQos::acknowledgment_timeout` 与 `max_retries` 有界重传。它适合学习协议和验证一对多丢包恢复，还不是完整 DDS 实现中的异步高吞吐可靠通道。
