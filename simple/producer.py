import socket
from confluent_kafka import Producer
# Kafka settings
BROKER = 'localhost:9092'  # Change this to your Kafka broker address
TOPIC = 'simple_topic'  # Replace with your Kafka topic

# Function to create a Kafka Producer
def create_kafka_producer(broker):
    conf = {
        'bootstrap.servers': broker,
        'client.id': socket.gethostname()

    }
    producer = Producer(conf)
    return producer

def publish_simple_message():
    producer = create_kafka_producer(BROKER)
    producer.produce(TOPIC, key="message", value="Yes, it's me!")
    producer.flush()

if __name__ == "__main__":
    publish_simple_message()
