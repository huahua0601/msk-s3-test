#!/usr/bin/env python3
"""
MSK Consumer - 从Kafka消费测试消息
"""
import json
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import BOOTSTRAP_SERVERS, DEFAULT_TOPIC, CONSUMER_GROUP_ID


# 全局变量用于优雅退出
running = True


def signal_handler(signum, frame):
    """处理中断信号"""
    global running
    print("\n收到退出信号，正在停止消费者...")
    running = False


def create_consumer(topic, group_id):
    """创建Kafka Consumer"""
    print(f"正在连接到 MSK: {BOOTSTRAP_SERVERS}")
    print(f"Topic: {topic}")
    print(f"Consumer Group: {group_id}")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(','),
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset='earliest',  # 从最早的消息开始消费
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        session_timeout_ms=30000,
        request_timeout_ms=40000,
        consumer_timeout_ms=10000  # 10秒无消息则返回
    )
    
    return consumer


def consume_messages(consumer):
    """消费消息"""
    global running
    message_count = 0
    
    print("\n开始消费消息 (按 Ctrl+C 退出)...\n")
    print("-" * 80)
    
    while running:
        try:
            # 轮询消息
            messages = consumer.poll(timeout_ms=1000)
            
            if not messages:
                continue
            
            for topic_partition, records in messages.items():
                for record in records:
                    message_count += 1
                    print(f"消息 #{message_count}")
                    print(f"  Topic: {record.topic}")
                    print(f"  Partition: {record.partition}")
                    print(f"  Offset: {record.offset}")
                    print(f"  Key: {record.key}")
                    print(f"  Value: {json.dumps(record.value, ensure_ascii=False, indent=4)}")
                    print(f"  Timestamp: {record.timestamp}")
                    print("-" * 80)
                    
        except StopIteration:
            print("没有更多消息")
            break
        except KafkaError as e:
            print(f"Kafka错误: {e}")
            break
    
    print(f"\n消费完成！共消费 {message_count} 条消息")
    return message_count


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='MSK Consumer 测试程序')
    parser.add_argument('--topic', '-t', default=DEFAULT_TOPIC, help='Kafka topic名称')
    parser.add_argument('--group', '-g', default=CONSUMER_GROUP_ID, help='消费者组ID')
    args = parser.parse_args()
    
    # 设置信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer = None
    try:
        consumer = create_consumer(args.topic, args.group)
        print("Consumer 连接成功！")
        consume_messages(consumer)
    except Exception as e:
        print(f"错误: {e}")
        raise
    finally:
        if consumer:
            consumer.close()
            print("Consumer 已关闭")


if __name__ == '__main__':
    main()

