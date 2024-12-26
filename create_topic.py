import asyncio
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import logging
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    TOPIC_CONFIG
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_topic_exists(admin_client):
    """Verify that topic exists and is properly configured"""
    try:
        topics = await admin_client.list_topics()
        logger.info(f"Available topics: {topics}")
        
        if KAFKA_TOPIC not in topics:
            return False
            
        topic_metadata = await admin_client.describe_topics([KAFKA_TOPIC])
        logger.info(f"Topic metadata: {topic_metadata}")
        
        if not topic_metadata or KAFKA_TOPIC not in topic_metadata:
            return False
            
        partitions = topic_metadata[KAFKA_TOPIC].partitions
        if len(partitions) != TOPIC_CONFIG['num_partitions']:
            logger.warning(f"Topic has {len(partitions)} partitions, expected {TOPIC_CONFIG['num_partitions']}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Error verifying topic: {e}")
        return False

async def create_topic():
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='dcgm-admin'
    )

    try:
        await admin_client.start()
        
        # First verify if topic exists and is properly configured
        if await verify_topic_exists(admin_client):
            logger.info(f"Topic {KAFKA_TOPIC} already exists and is properly configured")
            return
            
        # Create topic if it doesn't exist or isn't properly configured
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

        await admin_client.create_topics(topic_list)
        logger.info(f"Successfully created topic {KAFKA_TOPIC}")
        
        # Verify topic was created properly
        if not await verify_topic_exists(admin_client):
            raise RuntimeError(f"Topic {KAFKA_TOPIC} was not created properly")
            
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
