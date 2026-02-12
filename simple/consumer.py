from confluent_kafka import Consumer, KafkaError
import socket

# Kafka settings
BROKER = 'localhost:9092'  # Change this to your Kafka broker address
GROUP_ID = 'HR'
TOPIC = 'simple_topic'  # Replace with your Kafka topic

# Function to create a Kafka consumer
def create_kafka_consumer(broker, group_id, topic):
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'client.id': socket.gethostname()

    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer

# Display data from Kafka
def display_kafka_data():
    consumer = create_kafka_consumer(BROKER, GROUP_ID, TOPIC)    
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        
        key = msg.key().decode('utf-8')
        value = msg.value().decode('utf-8')
        
        # Display message with an icon
        print ("New Message Alert!!")
        print (f"Key: {key} and Value: {value}")
    consumer.close()

if __name__ == "__main__":
    display_kafka_data()
