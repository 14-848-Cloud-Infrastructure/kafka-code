import socket
from confluent_kafka import Producer, KafkaError
import os
import time 
from googleapiclient.discovery import build
import json

# Kafka settings
BROKER = 'localhost:9092'  # Change this to your Kafka broker address
TOPIC = 'youtube_topic'  # Replace with your Kafka topic
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="" #update this with your JSON file
youtube = build('youtube', 'v3')
# Example video ID
VIDEO_ID = 'fQX56Jd9X4s' # Replace with the YouTube video ID you want to monitor
def get_comments(video_id):
    video_response = youtube.videos().list(
        part="liveStreamingDetails",
        id=video_id
    ).execute()

    #Uncomment the line below to understand the object structure
    #print(json.dumps(video_response, indent=2))

    live_chat_id = video_response["items"][0]["liveStreamingDetails"]["activeLiveChatId"]

    # STEP 2: Fetch live chat messages
    chat_response = youtube.liveChatMessages().list(
        liveChatId=live_chat_id,
        part="id,snippet,authorDetails",
        maxResults=2 # update max results to fetch more data
    ).execute()    
    comments = []
    #Uncomment the line below to understand the object structure
    #print(json.dumps(chat_response, indent=2))
    for item in chat_response['items']:
        #print(json.dumps(item, indent=2))
        comment_data = {
            'author': item['authorDetails']['displayName'],
            'published_at': item['snippet']['publishedAt'],
            'text': item['snippet']['displayMessage']
        }
        comments.append(comment_data)

    return comments
# Function to create a Kafka consumer
def create_kafka_producer(broker):
    conf = {
        'bootstrap.servers': broker,
        'client.id': socket.gethostname()

    }
    producer = Producer(conf)
    return producer

def stream_youtube_comments(video_id):
    seen_comments = set()
    producer = create_kafka_producer(BROKER)
    while True:
        comments = get_comments(video_id)        
        for comment in comments:
            comment_id = comment['published_at'] + ' ' + comment['author']
            if comment_id not in seen_comments:
                seen_comments.add(comment_id)
                # Send the new comment to Kafka
                producer.produce(TOPIC, key=str(comment['author']), value=str(comment['text']))
                producer.flush()
        time.sleep(30)  # Poll for new comments every 30 seconds

if __name__ == "__main__":
    stream_youtube_comments(VIDEO_ID)