#!/usr/bin/env python3
"""
MSK Protobuf Bytes Producer - 发送 protobuf 序列化的二进制数据到 Kafka

使用方法:
1. 安装依赖: pip install kafka-python protobuf
2. 编译 proto 文件: protoc --python_out=. message.proto
3. 运行: python bytes-producer.py

数据格式（带长度前缀）:
  [4字节大端长度][1字节消息类型][protobuf数据]
  
消息类型:
  0x01 = UserEvent
  0x02 = OrderEvent
"""
import struct
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import BOOTSTRAP_SERVERS

# 消息类型标识
MSG_TYPE_USER_EVENT = 0x01
MSG_TYPE_ORDER_EVENT = 0x02

# 导入生成的 protobuf 类
try:
    import message_pb2
except ImportError:
    print("错误: 未找到 message_pb2.py")
    print("请先编译 proto 文件: protoc --python_out=. message.proto")
    exit(1)


# Topic 名称
TOPIC = 'my-bytes-topic'


def pack_message(msg_type: int, data: bytes) -> bytes:
    """
    打包消息：添加长度前缀和消息类型
    
    格式: [4字节大端长度][1字节消息类型][protobuf数据]
    长度 = 1(类型) + len(data)
    """
    payload = bytes([msg_type]) + data
    length = len(payload)
    return struct.pack('>I', length) + payload


def unpack_message(packed: bytes) -> tuple:
    """
    解包消息
    
    返回: (msg_type, data, bytes_consumed)
    """
    if len(packed) < 5:
        raise ValueError("数据太短")
    
    length = struct.unpack('>I', packed[:4])[0]
    if len(packed) < 4 + length:
        raise ValueError("数据不完整")
    
    msg_type = packed[4]
    data = packed[5:4 + length]
    
    return msg_type, data, 4 + length


def create_producer():
    """创建 Kafka Producer (发送原始 bytes)"""
    print(f"正在连接到 MSK: {BOOTSTRAP_SERVERS}")
    
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(','),
        # 不使用序列化器，直接发送 bytes
        value_serializer=None,
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        retry_backoff_ms=1000,
        request_timeout_ms=30000
    )
    
    return producer


def create_user_event(user_id: int, username: str, action: str) -> bytes:
    """创建 UserEvent protobuf 消息并序列化为 bytes"""
    event = message_pb2.UserEvent()
    event.user_id = user_id
    event.username = username
    event.action = action
    event.timestamp = int(datetime.now().timestamp() * 1000)  # 毫秒时间戳
    event.metadata['source'] = 'bytes-producer'
    event.metadata['version'] = '1.0'
    
    # 序列化为 bytes
    return event.SerializeToString()


def create_order_event(user_id: int, items: list) -> bytes:
    """创建 OrderEvent protobuf 消息并序列化为 bytes"""
    event = message_pb2.OrderEvent()
    event.order_id = str(uuid.uuid4())
    event.user_id = user_id
    event.status = message_pb2.CONFIRMED
    event.created_at = int(datetime.now().timestamp() * 1000)
    
    total = 0.0
    for item_data in items:
        item = event.items.add()
        item.product_id = item_data['product_id']
        item.product_name = item_data['product_name']
        item.quantity = item_data['quantity']
        item.price = item_data['price']
        total += item.quantity * item.price
    
    event.total_amount = total
    
    # 序列化为 bytes
    return event.SerializeToString()


def send_user_events(producer, count=5, topic=TOPIC, use_prefix=True):
    """发送 UserEvent 消息"""
    prefix_info = "带长度前缀" if use_prefix else "无前缀"
    print(f"\n发送 {count} 条 UserEvent 消息到 '{topic}' ({prefix_info})")
    print("-" * 60)
    
    actions = ['login', 'logout', 'purchase', 'view', 'click']
    
    for i in range(count):
        user_id = 1000 + i
        username = f'user_{i}'
        action = actions[i % len(actions)]
        
        # 创建 protobuf bytes
        proto_data = create_user_event(user_id, username, action)
        
        # 根据选项决定是否添加长度前缀
        if use_prefix:
            data = pack_message(MSG_TYPE_USER_EVENT, proto_data)
        else:
            data = proto_data
        
        try:
            # 发送 bytes 数据
            future = producer.send(
                topic,
                value=data,
                key=f'user-{user_id}'
            )
            record = future.get(timeout=10)
            
            print(f"✓ UserEvent #{i+1}")
            print(f"  user_id: {user_id}, action: {action}")
            print(f"  protobuf: {len(proto_data)} bytes, 发送: {len(data)} bytes")
            print(f"  partition: {record.partition}, offset: {record.offset}")
            
        except KafkaError as e:
            print(f"✗ 发送失败: {e}")
        
        time.sleep(0.3)


def send_order_events(producer, count=3, topic=TOPIC, use_prefix=True):
    """发送 OrderEvent 消息"""
    prefix_info = "带长度前缀" if use_prefix else "无前缀"
    print(f"\n发送 {count} 条 OrderEvent 消息到 '{topic}' ({prefix_info})")
    print("-" * 60)
    
    sample_items = [
        {'product_id': 'P001', 'product_name': 'iPhone 15', 'quantity': 1, 'price': 999.99},
        {'product_id': 'P002', 'product_name': 'AirPods Pro', 'quantity': 2, 'price': 249.99},
        {'product_id': 'P003', 'product_name': 'MacBook Pro', 'quantity': 1, 'price': 2499.99},
    ]
    
    for i in range(count):
        user_id = 2000 + i
        # 随机选择商品
        items = sample_items[:((i % 3) + 1)]
        
        # 创建 protobuf bytes
        proto_data = create_order_event(user_id, items)
        
        # 根据选项决定是否添加长度前缀
        if use_prefix:
            data = pack_message(MSG_TYPE_ORDER_EVENT, proto_data)
        else:
            data = proto_data
        
        try:
            future = producer.send(
                topic,
                value=data,
                key=f'order-{user_id}'
            )
            record = future.get(timeout=10)
            
            print(f"✓ OrderEvent #{i+1}")
            print(f"  user_id: {user_id}, items: {len(items)}")
            print(f"  protobuf: {len(proto_data)} bytes, 发送: {len(data)} bytes")
            print(f"  partition: {record.partition}, offset: {record.offset}")
            
        except KafkaError as e:
            print(f"✗ 发送失败: {e}")
        
        time.sleep(0.3)


def demo_deserialize():
    """演示如何反序列化 protobuf bytes（带长度前缀）"""
    print("\n" + "=" * 60)
    print("演示: 带长度前缀的 Protobuf 序列化/反序列化")
    print("=" * 60)
    
    # 创建并序列化
    original_data = create_user_event(12345, 'test_user', 'login')
    print(f"原始 protobuf bytes: {original_data.hex()}")
    print(f"原始长度: {len(original_data)} bytes")
    
    # 打包（添加长度前缀）
    packed_data = pack_message(MSG_TYPE_USER_EVENT, original_data)
    print(f"\n打包后的 bytes: {packed_data.hex()}")
    print(f"打包后长度: {len(packed_data)} bytes")
    print(f"  前4字节（长度）: {packed_data[:4].hex()} = {struct.unpack('>I', packed_data[:4])[0]}")
    print(f"  第5字节（类型）: {packed_data[4]:02x} = {'UserEvent' if packed_data[4] == MSG_TYPE_USER_EVENT else 'OrderEvent'}")
    
    # 解包
    msg_type, data, consumed = unpack_message(packed_data)
    print(f"\n解包结果:")
    print(f"  消息类型: {msg_type} ({'UserEvent' if msg_type == MSG_TYPE_USER_EVENT else 'OrderEvent'})")
    print(f"  数据长度: {len(data)} bytes")
    print(f"  消耗字节: {consumed}")
    
    # 反序列化 protobuf
    parsed_event = message_pb2.UserEvent()
    parsed_event.ParseFromString(data)
    
    print(f"\n反序列化 Protobuf 结果:")
    print(f"  user_id: {parsed_event.user_id}")
    print(f"  username: {parsed_event.username}")
    print(f"  action: {parsed_event.action}")
    print(f"  timestamp: {parsed_event.timestamp}")
    print(f"  metadata: {dict(parsed_event.metadata)}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='MSK Protobuf Bytes Producer')
    parser.add_argument('--topic', '-t', default=TOPIC, help='Kafka topic名称')
    parser.add_argument('--count', '-c', type=int, default=5, help='消息数量')
    parser.add_argument('--type', choices=['user', 'order', 'both', 'demo'],
                        default='both', help='消息类型')
    parser.add_argument('--no-prefix', action='store_true', 
                        help='不添加长度前缀（默认添加）')
    args = parser.parse_args()
    
    topic = args.topic
    use_prefix = not args.no_prefix
    
    # 演示序列化
    if args.type == 'demo':
        demo_deserialize()
        return
    
    producer = None
    try:
        producer = create_producer()
        print("Producer 连接成功！")
        print(f"长度前缀模式: {'开启' if use_prefix else '关闭'}")
        
        if args.type in ['user', 'both']:
            send_user_events(producer, args.count, topic, use_prefix)
        
        if args.type in ['order', 'both']:
            send_order_events(producer, args.count, topic, use_prefix)
        
        producer.flush()
        print("\n所有消息发送完成！")
        
    except Exception as e:
        print(f"错误: {e}")
        raise
    finally:
        if producer:
            producer.close()
            print("Producer 已关闭")


if __name__ == '__main__':
    main()

