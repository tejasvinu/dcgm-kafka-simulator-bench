from kafka.admin import KafkaAdminClient
from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
import json

def check_kafka_setup():
    # Check broker connectivity
    client = KafkaClient(bootstrap_servers=['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094'])
    print("Connected to Kafka brokers successfully")
    
    # List topics
    consumer = KafkaConsumer(
        bootstrap_servers=['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094'],
        auto_offset_reset='earliest'
    )
    topics = consumer.topics()
    print(f"\nAvailable topics: {topics}")
    
    # Check specific topic
    topic = 'dcgm-metrics-test'
    if topic in topics:
        print(f"\nTopic '{topic}' exists")
        partitions = consumer.partitions_for_topic(topic)
        print(f"Number of partitions: {len(partitions)}")
        
        # Check messages
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        message_count = 0
        for _ in consumer:
            message_count += 1
            if message_count == 1:
                print("\nFound messages in topic")
                break
        
        if message_count == 0:
            print("\nNo messages found in topic")
    else:
        print(f"\nTopic '{topic}' does not exist!")

if __name__ == "__main__":
    check_kafka_setup()
