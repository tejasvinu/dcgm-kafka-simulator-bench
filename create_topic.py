import asyncio
import subprocess
import logging
import shlex
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    TOPIC_CONFIG
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_command(command):
    """Run shell command asynchronously"""
    process = await asyncio.create_subprocess_exec(
        *shlex.split(command),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()

async def create_topic():
    """Create Kafka topic using kafka-topics.sh"""
    bootstrap_servers = ','.join(KAFKA_BOOTSTRAP_SERVERS)
    command = f"/usr/local/kafka/bin/kafka-topics.sh " \
             f"--bootstrap-server {bootstrap_servers} " \
             f"--create " \
             f"--replication-factor {TOPIC_CONFIG['replication_factor']} " \
             f"--partitions {TOPIC_CONFIG['num_partitions']} " \
             f"--config retention.ms={TOPIC_CONFIG['retention_ms']} " \
             f"--config retention.bytes={TOPIC_CONFIG['retention_bytes']} " \
             f"--config cleanup.policy={TOPIC_CONFIG['cleanup_policy']} " \
             f"--config min.insync.replicas={TOPIC_CONFIG['min_insync_replicas']} " \
             f"--config compression.type={TOPIC_CONFIG['compression_type']} " \
             f"--topic {KAFKA_TOPIC}"

    try:
        # First check if topic exists
        check_command = f"/usr/local/kafka/bin/kafka-topics.sh --bootstrap-server {bootstrap_servers} --describe --topic {KAFKA_TOPIC}"
        returncode, stdout, stderr = await run_command(check_command)
        
        if returncode == 0:
            logger.info(f"Topic {KAFKA_TOPIC} already exists")
            return True

        # Create topic if it doesn't exist
        logger.info(f"Creating topic with command:\n{command}")
        returncode, stdout, stderr = await run_command(command)
        
        if returncode != 0:
            logger.error(f"Failed to create topic: {stderr}")
            raise RuntimeError(f"Failed to create topic: {stderr}")
            
        logger.info(f"Topic creation output: {stdout}")
        
        # Verify topic was created
        returncode, stdout, stderr = await run_command(check_command)
        if returncode == 0:
            logger.info(f"Topic {KAFKA_TOPIC} created successfully")
            logger.info(f"Topic details:\n{stdout}")
            return True
        else:
            raise RuntimeError(f"Topic creation verification failed: {stderr}")
            
    except Exception as e:
        logger.error(f"Error creating topic: {str(e)}")
        raise

async def main():
    await create_topic()

if __name__ == "__main__":
    asyncio.run(main())
