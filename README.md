# mini_dds

一个用于学习 DDS、RTPS 与实时通信的轻量级 C++17 项目。

当前进度：完成固定端点 Best-Effort MVP，包括跨平台 UDP、CDR、RTPS Header/DATA、History Cache，以及 `DomainParticipant`、`Topic`、`Publisher/Subscriber`、`DataWriter/DataReader`。自动发现和可靠传输仍在后续里程碑中。

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

当前示例使用固定地址 `127.0.0.1:9000`，尚未实现 SPDP/SEDP 自动发现。`sender`/`receiver` 可用于单独验证裸 UDP 传输层。
