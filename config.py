"""
MSK Configuration
"""
import os

# Kafka Bootstrap Servers - 从环境变量读取，如果没有则使用默认值
BOOTSTRAP_SERVERS = os.environ.get(
    'KAFKA_BOOTSTRAP_SERVERS',
    'boot-nm1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092,'
    'boot-y8y.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092,'
    'boot-dw1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092'
)

# 默认测试Topic
DEFAULT_TOPIC = 'msk-test-topic'

# 消费者组ID
CONSUMER_GROUP_ID = 'msk-test-consumer-group'

