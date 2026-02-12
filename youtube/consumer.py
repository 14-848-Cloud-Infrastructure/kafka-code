import streamlit as st
from confluent_kafka import Consumer, KafkaError
import socket

# Kafka settings
BROKER = 'localhost:9092'  # Change this to your Kafka broker address
GROUP_ID = 'analytics'
TOPIC = 'youtube_topic'  # Replace with your Kafka topic
# Icon HTML (you can use any icon you want, here we use a simple Unicode icon)
ICON_HTML = """
<span style="font-size: 1.5em; margin-right: 10px;">📩</span>
"""

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
    st.write("Listening to Kafka topic:", TOPIC)
    button = st.button("Stop listening")
    while True:
        if button:
            break
        msg = consumer.poll(timeout=1.0)       
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                st.error(msg.error())
                break
        
        key = msg.key().decode('utf-8')
        value = msg.value().decode('utf-8')
        style = ""
        # Display message with an icon
        st.markdown(f"{ICON_HTML}<span {style}>{key} has made the comment: {value}</span>", unsafe_allow_html=True)
    consumer.close()

if __name__ == "__main__":
    # Streamlit app title
    st.title("Kafka Streamlit Consumer")
    display_kafka_data()