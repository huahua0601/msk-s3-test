# MSK 测试程序

Amazon Managed Streaming for Apache Kafka (MSK) 的 Python 测试程序，支持 JSON 和 Protobuf 二进制格式。

## 📁 项目结构

```
msk-test/
├── config.py           # MSK 连接配置
├── environment.yml     # Conda 环境配置
├── requirements.txt    # Python 依赖
├── message.proto       # Protobuf 消息定义
├── message_pb2.py      # 生成的 Protobuf Python 类
├── msk_test.py         # MSK 综合测试程序
├── producer.py         # JSON 格式生产者
├── consumer.py         # JSON 格式消费者
├── bytes-producer.py   # Protobuf 二进制生产者（支持长度前缀）
└── s3-bytes.py         # S3 数据解析程序
```

## 🚀 快速开始

### 1. 环境设置

```bash
# 使用 Conda 创建环境
conda env create -f environment.yml
conda activate msk-test

# 或手动安装依赖
pip install -r requirements.txt
pip install boto3 grpcio-tools

# 编译 Protobuf 文件
python -m grpc_tools.protoc --python_out=. -I. message.proto
```

### 2. 配置

MSK 连接信息在 `config.py` 中配置：

```python
BOOTSTRAP_SERVERS = 'boot-nm1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092,...'
```

也可以通过环境变量覆盖：
```bash
export KAFKA_BOOTSTRAP_SERVERS="your-broker1:9092,your-broker2:9092"
```

## 📖 使用指南

### JSON 格式测试

#### 综合测试
```bash
# 完整测试（连接 + 创建Topic + 发送/消费）
python msk_test.py

# 仅测试连接
python msk_test.py --test connection

# 发送消息
python msk_test.py --test produce --count 10
```

#### 单独使用 Producer/Consumer
```bash
# 发送消息
python producer.py --topic my-topic --count 20

# 消费消息（Ctrl+C 停止）
python consumer.py --topic my-topic
```

---

### Protobuf 二进制格式

#### 数据格式

`bytes-producer.py` 支持两种格式：

**1. 带长度前缀格式（推荐）**
```
┌─────────────────┬───────────────┬────────────────────┐
│ 4字节大端长度   │ 1字节消息类型  │ Protobuf 数据      │
│ (不含自身)      │ 0x01=User     │                    │
│                 │ 0x02=Order    │                    │
└─────────────────┴───────────────┴────────────────────┘
```

**2. 原始格式（无前缀）**
```
┌────────────────────┐
│ Protobuf 数据      │
└────────────────────┘
```

#### Protobuf 消息定义

```protobuf
// UserEvent
message UserEvent {
    int64 user_id = 1;
    string username = 2;
    string action = 3;      // login, logout, purchase, view, click
    int64 timestamp = 4;
    map<string, string> metadata = 5;
}

// OrderEvent
message OrderEvent {
    string order_id = 1;
    int64 user_id = 2;
    repeated OrderItem items = 3;
    double total_amount = 4;
    OrderStatus status = 5;
    int64 created_at = 6;
}
```

#### 发送 Protobuf 消息

```bash
# 发送带长度前缀的消息（默认，推荐）
python bytes-producer.py --count 10

# 发送无前缀的原始消息
python bytes-producer.py --count 10 --no-prefix

# 只发送 UserEvent
python bytes-producer.py --type user --count 20

# 只发送 OrderEvent
python bytes-producer.py --type order --count 10

# 演示序列化/反序列化
python bytes-producer.py --type demo
```

---

### S3 数据解析

当使用 MSK S3 Sink Connector 将数据存储到 S3 后，可以使用 `s3-bytes.py` 解析：

```bash
# 解析 S3 文件
python s3-bytes.py --bucket my-bucket --key topics/my-topic/partition=0/file.bin

# 解析本地文件
python s3-bytes.py --local /path/to/file.bin

# 显示更多 hexdump
python s3-bytes.py --hexdump 1024

# 强制使用原始模式解析
python s3-bytes.py --raw
```

#### 输出示例

```
============================================================
解析带长度前缀的消息
============================================================
✓ 成功解析 20 条消息
  消耗字节: 1580/1580
  消息类型统计: {'UserEvent': 10, 'OrderEvent': 10}

[消息 #1] UserEvent (67 bytes)
  user_id: 1000
  username: user_0
  action: login
  timestamp: 1764315040942

[消息 #11] OrderEvent (89 bytes)
  order_id: e6e8d490-a435-4232-b72a-b2880c97d8e2
  user_id: 2000
  total_amount: 999.99
  items (1):
    - iPhone 15 x1 @ $999.99
```

---

## ⚙️ MSK S3 Sink Connector 配置

推荐的 Connector 配置（ByteArray 格式）：

```json
{
  "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "s3.region": "us-east-1",
  "s3.bucket.name": "your-bucket",
  "topics": "my-bytes-topic",
  "flush.size": "10",
  "rotate.interval.ms": "60000",
  "format.class": "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
  "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
  "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
  "storage.class": "io.confluent.connect.s3.storage.S3Storage",
  "tasks.max": "2",
  "errors.tolerance": "all",
  "errors.log.enable": "true"
}
```

> **提示**: 使用带长度前缀的格式可以让 S3 文件更容易解析，因为 ByteArrayFormat 会将多条消息直接拼接存储。

---

## 🔧 故障排查

### 连接问题
```bash
# 测试网络连通性
telnet boot-nm1.democluster.xxx.kafka.us-east-1.amazonaws.com 9092

# 检查安全组是否开放 9092 端口
```

### Protobuf 编译错误
```bash
# 安装 grpcio-tools
pip install grpcio-tools

# 重新编译
python -m grpc_tools.protoc --python_out=. -I. message.proto
```

### S3 访问错误
```bash
# 检查 AWS 凭证
aws sts get-caller-identity

# 检查 S3 权限
aws s3 ls s3://your-bucket/
```

---

## 📋 依赖列表

| 包名 | 用途 |
|------|------|
| kafka-python | Kafka 客户端 |
| protobuf | Protobuf 序列化 |
| grpcio-tools | Protobuf 编译器 |
| boto3 | AWS S3 访问 |

---

## 📝 注意事项

1. **网络配置**: EC2 实例需要与 MSK 集群在同一 VPC 或有正确的网络路由
2. **安全组**: 确保允许访问 9092 端口
3. **Topic 创建**: MSK 默认不自动创建 Topic，需要手动创建或使用 `msk_test.py`
4. **数据格式**: 推荐使用带长度前缀的格式，便于后续解析

---

## 📄 License

MIT
