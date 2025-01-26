# Updated Consumer Script
from google.cloud import pubsub_v1
import glob
import json
import os

# Set the Google Cloud credentials
gcp_credential_files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_credential_files[0]

# Set your project ID and subscription details
project_id = "velvety-study-448822-h6"
topic_name = "LabelsTopic"  # Updated topic name
subscription_id = "LabelsTopic-sub"  # Updated subscription name

# Create a subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
print(f"Listening for messages on {subscription_path}...\n")

# Define the callback function to process messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # Deserialize the message data from JSON
    record = json.loads(message.data.decode("utf-8"))
    print("Consumed record:")
    for key, value in record.items():
        print(f"  {key}: {value}")

    # Acknowledge the message
    message.ack()

# Subscribe to the topic and process incoming messages
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Waiting for messages... Press Ctrl+C to exit.")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Stopped listening for messages.")
