import json
import boto3
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# Initialize Kafka consumer

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Initialize MinIO client
minio_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY
    )

# Create Bucket in MiniO if not exists
try:
    minio_client.head_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket {MINIO_BUCKET} already exists.")
except:
    minio_client.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket {MINIO_BUCKET} created.")

batch_size = 20
batch = []



for message in consumer:
    track_data = message.value
    batch.append(track_data)

    if len(batch) >= batch_size:
        now = datetime.utcnow()
        file_name = f"spotify_top_tracks_batch_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        json_data = "\n".join([json.dumps(e) for e in batch])

        minio_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=file_name,
            Body=json_data.encode('utf-8')
        )
        print(f"Uploaded {len(batch)} events to MinIO: {file_name}")
        batch = []


        
