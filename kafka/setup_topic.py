from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "payment-events"
NUM_PARTITIONS = 4
REPLICATION_FACTOR = 1

def create_topic():
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    topic = NewTopic(
        topic=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )

    fs = admin_client.create_topics([topic])

    for topic_name, future in fs.items():
        try:
            future.result()
            print(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"Topic '{topic_name}' may already exist or error occurred: {e}")

if __name__ == "__main__":
    create_topic()