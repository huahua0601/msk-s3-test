#!/usr/bin/env python3
"""
MSK Producer - 向Kafka发送测试消息
"""
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import BOOTSTRAP_SERVERS, DEFAULT_TOPIC


def create_producer():
    """创建Kafka Producer"""
    print(f"正在连接到 MSK: {BOOTSTRAP_SERVERS}")
    
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',  # 等待所有副本确认
        retries=3,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=30000
    )
    
    return producer


def send_messages(producer, topic, num_messages=10):
    """发送测试消息"""
    print(f"\n开始发送 {num_messages} 条消息到 topic: {topic}")
    
    for i in range(num_messages):
        message = {
            'id': i + 1,
            'timestamp': datetime.now().isoformat(),
            'content': f'测试消息 #{i + 1}',
            'data': {
                'source': 'msk-test-producer',
                'sequence': i + 1
            }
        }
        
        key = f'key-{i % 3}'  # 使用3个不同的key进行分区
        
        try:
            future = producer.send(topic, value=message, key=key)
            # 等待发送完成
            record_metadata = future.get(timeout=10)
            print(f"✓ 消息 {i + 1} 发送成功 - "
                  f"topic: {record_metadata.topic}, "
                  f"partition: {record_metadata.partition}, "
                  f"offset: {record_metadata.offset}")
        except KafkaError as e:
            print(f"✗ 消息 {i + 1} 发送失败: {e}")
        
        time.sleep(0.5)  # 每条消息间隔0.5秒
    
    producer.flush()
    print(f"\n发送完成！共发送 {num_messages} 条消息")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='MSK Producer 测试程序')
    parser.add_argument('--topic', '-t', default=DEFAULT_TOPIC, help='Kafka topic名称')
    parser.add_argument('--count', '-c', type=int, default=10, help='发送消息数量')
    args = parser.parse_args()
    
    producer = None
    try:
        producer = create_producer()
        print("Producer 连接成功！")
        send_messages(producer, args.topic, args.count)
    except Exception as e:
        print(f"错误: {e}")
        raise
    finally:
        if producer:
            producer.close()
            print("Producer 已关闭")


if __name__ == '__main__':
    main()

