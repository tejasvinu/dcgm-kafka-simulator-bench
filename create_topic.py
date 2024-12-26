import asyncio
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import logging
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    TOPIC_CONFIG
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def create_topic():
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='dcgm-admin'
    )

    topic_list = [
        NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=TOPIC_CONFIG['num_partitions'],
            replication_factor=TOPIC_CONFIG['replication_factor'],
            topic_configs={
                'min.insync.replicas': str(TOPIC_CONFIG['min_insync_replicas']),
                'compression.type': TOPIC_CONFIG['compression_type'],
                'cleanup.policy': TOPIC_CONFIG['cleanup_policy'],
                'retention.ms': str(TOPIC_CONFIG['retention_ms']),
                'retention.bytes': str(TOPIC_CONFIG['retention_bytes'])
            }
        )
    ]

    try:
        await admin_client.start()
        await admin_client.create_topics(topic_list)
        logger.info(f"Successfully created topic {KAFKA_TOPIC}")
    except Exception as e:
        if "already exists" in str(e):
            logger.warning(f"Topic {KAFKA_TOPIC} already exists")
        else:
            logger.error(f"Failed to create topic: {e}")
            raise
    finally:
        await admin_client.close()

async def main():
    await create_topic()

if __name__ == "__main__":
    asyncio.run(main())
