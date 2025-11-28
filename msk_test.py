#!/usr/bin/env python3
"""
MSK 综合测试程序 - 包含连接测试、生产者、消费者功能
"""
import json
import sys
import time
import threading
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from config import BOOTSTRAP_SERVERS, DEFAULT_TOPIC, CONSUMER_GROUP_ID


class MSKTester:
    """MSK 测试类"""
    
    def __init__(self, bootstrap_servers=None):
        self.bootstrap_servers = (bootstrap_servers or BOOTSTRAP_SERVERS).split(',')
        self.producer = None
        self.consumer = None
        self.admin_client = None
    
    def test_connection(self):
        """测试MSK连接"""
        print("=" * 60)
        print("测试 MSK 连接")
        print("=" * 60)
        print(f"Bootstrap Servers: {', '.join(self.bootstrap_servers)}")
        
        try:
            # 使用AdminClient测试连接
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=10000
            )
            
            # 获取集群信息
            cluster_metadata = self.admin_client.describe_cluster()
            print(f"\n✓ 连接成功!")
            print(f"  Cluster ID: {cluster_metadata.get('cluster_id', 'N/A')}")
            
            # 获取topic列表
            topics = self.admin_client.list_topics()
            print(f"  现有 Topics ({len(topics)}): {', '.join(topics) if topics else '无'}")
            
            return True
            
        except Exception as e:
            print(f"\n✗ 连接失败: {e}")
            return False
    
    def create_topic(self, topic_name, num_partitions=3, replication_factor=2):
        """创建Topic"""
        print(f"\n创建 Topic: {topic_name}")
        
        try:
            if not self.admin_client:
                self.admin_client = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers
                )
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            
            self.admin_client.create_topics([topic])
            print(f"✓ Topic '{topic_name}' 创建成功")
            return True
            
        except TopicAlreadyExistsError:
            print(f"Topic '{topic_name}' 已存在")
            return True
        except Exception as e:
            print(f"✗ 创建Topic失败: {e}")
            return False
    
    def produce_messages(self, topic, count=5):
        """发送测试消息"""
        print(f"\n发送 {count} 条测试消息到 '{topic}'")
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            
            sent_count = 0
            for i in range(count):
                message = {
                    'id': i + 1,
                    'timestamp': datetime.now().isoformat(),
                    'content': f'MSK测试消息 #{i + 1}'
                }
                
                future = self.producer.send(topic, value=message, key=f'test-{i}')
                record = future.get(timeout=10)
                sent_count += 1
                print(f"  ✓ 发送消息 {i + 1}: partition={record.partition}, offset={record.offset}")
            
            self.producer.flush()
            print(f"✓ 成功发送 {sent_count} 条消息")
            return sent_count
            
        except Exception as e:
            print(f"✗ 发送消息失败: {e}")
            return 0
        finally:
            if self.producer:
                self.producer.close()
    
    def consume_messages(self, topic, timeout_seconds=10):
        """消费测试消息"""
        print(f"\n从 '{topic}' 消费消息 (超时: {timeout_seconds}秒)")
        
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=f'{CONSUMER_GROUP_ID}-{int(time.time())}',  # 唯一的group id
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='earliest',
                consumer_timeout_ms=timeout_seconds * 1000
            )
            
            consumed_count = 0
            for message in self.consumer:
                consumed_count += 1
                print(f"  ✓ 收到消息 {consumed_count}: "
                      f"key={message.key}, value={message.value}")
                
                if consumed_count >= 100:  # 最多消费100条
                    break
            
            print(f"✓ 成功消费 {consumed_count} 条消息")
            return consumed_count
            
        except Exception as e:
            print(f"消费结束或出错: {e}")
            return 0
        finally:
            if self.consumer:
                self.consumer.close()
    
    def run_full_test(self, topic=None):
        """运行完整测试"""
        topic = topic or DEFAULT_TOPIC
        
        print("\n" + "=" * 60)
        print("MSK 完整测试")
        print("=" * 60)
        print(f"测试时间: {datetime.now().isoformat()}")
        print(f"Bootstrap Servers: {', '.join(self.bootstrap_servers)}")
        print(f"测试 Topic: {topic}")
        print("=" * 60)
        
        results = {
            'connection': False,
            'topic_creation': False,
            'produce': 0,
            'consume': 0
        }
        
        # 1. 测试连接
        results['connection'] = self.test_connection()
        if not results['connection']:
            print("\n连接失败，无法继续测试")
            return results
        
        # 2. 创建Topic
        results['topic_creation'] = self.create_topic(topic)
        
        # 3. 发送消息
        results['produce'] = self.produce_messages(topic, count=5)
        
        # 等待消息同步
        time.sleep(2)
        
        # 4. 消费消息
        results['consume'] = self.consume_messages(topic, timeout_seconds=15)
        
        # 打印测试结果
        print("\n" + "=" * 60)
        print("测试结果汇总")
        print("=" * 60)
        print(f"  连接测试: {'✓ 通过' if results['connection'] else '✗ 失败'}")
        print(f"  Topic创建: {'✓ 通过' if results['topic_creation'] else '✗ 失败'}")
        print(f"  消息发送: {results['produce']} 条")
        print(f"  消息消费: {results['consume']} 条")
        print("=" * 60)
        
        return results
    
    def close(self):
        """关闭所有连接"""
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()
        if self.admin_client:
            self.admin_client.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='MSK 综合测试程序')
    parser.add_argument('--topic', '-t', default=DEFAULT_TOPIC, help='测试Topic名称')
    parser.add_argument('--test', choices=['connection', 'produce', 'consume', 'full'],
                        default='full', help='测试类型')
    parser.add_argument('--count', '-c', type=int, default=5, help='消息数量')
    args = parser.parse_args()
    
    tester = MSKTester()
    
    try:
        if args.test == 'connection':
            tester.test_connection()
        elif args.test == 'produce':
            tester.test_connection()
            tester.create_topic(args.topic)
            tester.produce_messages(args.topic, args.count)
        elif args.test == 'consume':
            tester.consume_messages(args.topic, timeout_seconds=30)
        else:  # full
            tester.run_full_test(args.topic)
    except KeyboardInterrupt:
        print("\n用户中断")
    except Exception as e:
        print(f"\n测试失败: {e}")
        raise
    finally:
        tester.close()


if __name__ == '__main__':
    main()

