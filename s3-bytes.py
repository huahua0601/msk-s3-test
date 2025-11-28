#!/usr/bin/env python3
"""
S3 Bytes 解析程序 - 从 S3 下载并解析 Kafka 存储的 protobuf bytes 数据

支持的数据格式:
1. 带长度前缀格式: [4字节大端长度][1字节消息类型][protobuf数据]
2. 原始 protobuf 格式（无前缀，直接拼接）

使用方法:
    python s3-bytes.py
    python s3-bytes.py --bucket afd-data78998 --key topics/my-bytes-topic/partition=0/my-bytes-topic+0+0000000000.bin
    python s3-bytes.py --local /path/to/local/file.bin
"""
import argparse
import struct
import sys

import boto3
from botocore.exceptions import ClientError

# 导入 protobuf 消息类
try:
    import message_pb2
except ImportError:
    print("警告: 未找到 message_pb2.py，某些解析功能可能不可用")
    message_pb2 = None

# 消息类型标识（与 bytes-producer.py 保持一致）
MSG_TYPE_USER_EVENT = 0x01
MSG_TYPE_ORDER_EVENT = 0x02

MSG_TYPE_NAMES = {
    MSG_TYPE_USER_EVENT: 'UserEvent',
    MSG_TYPE_ORDER_EVENT: 'OrderEvent',
}

# 默认 S3 路径
DEFAULT_BUCKET = 'afd-data78998'
DEFAULT_KEY = 'topics/my-bytes-topic/partition=0/my-bytes-topic+0+0000000100.bin'


def download_from_s3(bucket: str, key: str) -> bytes:
    """从 S3 下载文件"""
    print(f"从 S3 下载文件...")
    print(f"  Bucket: {bucket}")
    print(f"  Key: {key}")
    
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        print(f"  下载成功！文件大小: {len(data)} bytes")
        return data
    except ClientError as e:
        print(f"  S3 错误: {e}")
        raise


def read_local_file(path: str) -> bytes:
    """读取本地文件"""
    print(f"读取本地文件: {path}")
    with open(path, 'rb') as f:
        data = f.read()
    print(f"  文件大小: {len(data)} bytes")
    return data


def parse_raw_bytes(data: bytes):
    """解析原始 bytes 数据"""
    print("\n" + "=" * 60)
    print("原始数据分析")
    print("=" * 60)
    
    print(f"总大小: {len(data)} bytes")
    print(f"前 100 bytes (hex): {data[:100].hex()}")
    
    # 检查是否有可打印字符
    printable = sum(1 for b in data if 32 <= b < 127)
    print(f"可打印字符比例: {printable}/{len(data)} ({100*printable/len(data):.1f}%)")
    
    # 检查是否是带长度前缀的格式
    if len(data) >= 5:
        first_length = struct.unpack('>I', data[:4])[0]
        first_type = data[4]
        if first_length < len(data) and first_type in MSG_TYPE_NAMES:
            print(f"\n检测到可能的长度前缀格式:")
            print(f"  第一条消息长度: {first_length}")
            print(f"  第一条消息类型: 0x{first_type:02x} ({MSG_TYPE_NAMES.get(first_type, '未知')})")


def parse_length_prefixed_format(data: bytes) -> list:
    """
    解析带长度前缀的格式
    格式: [4字节大端长度][1字节消息类型][protobuf数据]
    
    返回: [(msg_type, protobuf_data), ...]
    """
    messages = []
    offset = 0
    
    while offset + 5 <= len(data):  # 至少需要 4字节长度 + 1字节类型
        # 读取长度
        length = struct.unpack('>I', data[offset:offset+4])[0]
        
        # 验证长度合理性
        if length <= 0 or length > 10 * 1024 * 1024:  # 最大10MB
            break
        if offset + 4 + length > len(data):
            break
        
        # 读取消息类型
        msg_type = data[offset + 4]
        
        # 验证消息类型
        if msg_type not in MSG_TYPE_NAMES:
            break
        
        # 读取 protobuf 数据
        proto_data = data[offset + 5:offset + 4 + length]
        
        messages.append((msg_type, proto_data))
        offset += 4 + length
    
    return messages, offset


def parse_protobuf_message(msg_type: int, data: bytes):
    """根据消息类型解析 protobuf"""
    if not message_pb2:
        return None, "message_pb2 未加载"
    
    try:
        if msg_type == MSG_TYPE_USER_EVENT:
            event = message_pb2.UserEvent()
            event.ParseFromString(data)
            return event, None
        elif msg_type == MSG_TYPE_ORDER_EVENT:
            event = message_pb2.OrderEvent()
            event.ParseFromString(data)
            return event, None
        else:
            return None, f"未知消息类型: {msg_type}"
    except Exception as e:
        return None, str(e)


def print_user_event(event, indent="  "):
    """打印 UserEvent"""
    print(f"{indent}user_id: {event.user_id}")
    print(f"{indent}username: {event.username}")
    print(f"{indent}action: {event.action}")
    print(f"{indent}timestamp: {event.timestamp}")
    if event.metadata:
        print(f"{indent}metadata: {dict(event.metadata)}")


def print_order_event(event, indent="  "):
    """打印 OrderEvent"""
    print(f"{indent}order_id: {event.order_id}")
    print(f"{indent}user_id: {event.user_id}")
    print(f"{indent}total_amount: {event.total_amount:.2f}")
    print(f"{indent}status: {event.status}")
    print(f"{indent}items ({len(event.items)}):")
    for item in event.items:
        print(f"{indent}  - {item.product_name} x{item.quantity} @ ${item.price:.2f}")


def try_parse_with_length_prefix(data: bytes):
    """尝试使用长度前缀格式解析"""
    print("\n" + "=" * 60)
    print("解析带长度前缀的消息")
    print("格式: [4字节长度][1字节类型][protobuf数据]")
    print("=" * 60)
    
    messages, bytes_consumed = parse_length_prefixed_format(data)
    
    if not messages:
        print("\n✗ 未能解析出任何消息（可能不是长度前缀格式）")
        return False
    
    print(f"\n✓ 成功解析 {len(messages)} 条消息")
    print(f"  消耗字节: {bytes_consumed}/{len(data)}")
    if bytes_consumed < len(data):
        print(f"  剩余字节: {len(data) - bytes_consumed} (可能是不完整的消息)")
    
    # 统计消息类型
    type_counts = {}
    for msg_type, _ in messages:
        type_name = MSG_TYPE_NAMES.get(msg_type, f'Unknown({msg_type})')
        type_counts[type_name] = type_counts.get(type_name, 0) + 1
    print(f"  消息类型统计: {type_counts}")
    
    print("\n" + "-" * 60)
    print("消息详情 (前 20 条)")
    print("-" * 60)
    
    for i, (msg_type, proto_data) in enumerate(messages[:20]):
        type_name = MSG_TYPE_NAMES.get(msg_type, f'Unknown({msg_type})')
        print(f"\n[消息 #{i+1}] {type_name} ({len(proto_data)} bytes)")
        
        event, error = parse_protobuf_message(msg_type, proto_data)
        if error:
            print(f"  ✗ 解析错误: {error}")
            print(f"  bytes: {proto_data[:50].hex()}...")
        else:
            if msg_type == MSG_TYPE_USER_EVENT:
                print_user_event(event)
            elif msg_type == MSG_TYPE_ORDER_EVENT:
                print_order_event(event)
    
    if len(messages) > 20:
        print(f"\n... 还有 {len(messages) - 20} 条消息未显示")
    
    return True


def try_parse_raw_protobuf(data: bytes):
    """尝试解析原始 protobuf（无长度前缀，直接拼接）"""
    print("\n" + "=" * 60)
    print("尝试解析原始 Protobuf（无长度前缀）")
    print("=" * 60)
    
    if not message_pb2:
        print("跳过（message_pb2 未加载）")
        return
    
    # 扫描可能的消息起始位置
    found = []
    
    for i in range(min(len(data), 2000)):
        for length in [50, 70, 100, 150, 200, 300]:
            if i + length > len(data):
                continue
            
            chunk = data[i:i + length]
            
            # 尝试 UserEvent
            try:
                event = message_pb2.UserEvent()
                event.ParseFromString(chunk)
                if event.user_id and event.username:
                    found.append((i, 'UserEvent', event))
                    break
            except:
                pass
            
            # 尝试 OrderEvent
            try:
                event = message_pb2.OrderEvent()
                event.ParseFromString(chunk)
                if event.order_id and len(event.order_id) > 10:
                    found.append((i, 'OrderEvent', event))
                    break
            except:
                pass
    
    if not found:
        print("未找到可识别的 protobuf 消息")
        return
    
    print(f"扫描发现 {len(found)} 条可能的消息:\n")
    
    for offset, msg_type, event in found[:20]:
        print(f"偏移 {offset}: {msg_type}")
        if msg_type == 'UserEvent':
            print(f"  user_id={event.user_id}, username={event.username}, action={event.action}")
        elif msg_type == 'OrderEvent':
            print(f"  order_id={event.order_id}, user_id={event.user_id}")
    
    if len(found) > 20:
        print(f"\n... 还有 {len(found) - 20} 条可能的消息")


def hexdump(data: bytes, length: int = 256):
    """打印 hexdump"""
    print("\n" + "=" * 60)
    print(f"Hexdump (前 {min(length, len(data))} bytes)")
    print("=" * 60)
    
    for i in range(0, min(length, len(data)), 16):
        chunk = data[i:i+16]
        hex_part = ' '.join(f'{b:02x}' for b in chunk)
        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        print(f"  {i:08x}  {hex_part:<48}  |{ascii_part}|")


def main():
    parser = argparse.ArgumentParser(description='S3 Bytes 解析程序')
    parser.add_argument('--bucket', '-b', default=DEFAULT_BUCKET, help='S3 bucket 名称')
    parser.add_argument('--key', '-k', default=DEFAULT_KEY, help='S3 object key')
    parser.add_argument('--local', '-l', help='本地文件路径（如果指定则不从S3下载）')
    parser.add_argument('--hexdump', '-x', type=int, default=256, 
                        help='Hexdump 显示的字节数（0表示不显示）')
    parser.add_argument('--raw', '-r', action='store_true',
                        help='强制使用原始模式解析（不尝试长度前缀）')
    args = parser.parse_args()
    
    print("=" * 60)
    print("S3 Bytes 解析程序")
    print("=" * 60)
    
    # 获取数据
    try:
        if args.local:
            data = read_local_file(args.local)
        else:
            data = download_from_s3(args.bucket, args.key)
    except Exception as e:
        print(f"获取数据失败: {e}")
        sys.exit(1)
    
    # 分析数据
    parse_raw_bytes(data)
    
    if args.hexdump > 0:
        hexdump(data, args.hexdump)
    
    # 尝试解析
    if args.raw:
        # 强制原始模式
        try_parse_raw_protobuf(data)
    else:
        # 优先尝试长度前缀格式
        success = try_parse_with_length_prefix(data)
        
        if not success:
            # 回退到原始模式
            print("\n尝试原始 protobuf 模式...")
            try_parse_raw_protobuf(data)
    
    print("\n" + "=" * 60)
    print("解析完成")
    print("=" * 60)


if __name__ == '__main__':
    main()
