import os
import json
import time
from dotenv import load_dotenv
from kafka import KafkaProducer
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Load environment variables
load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","spotify-events")
print("BOOTSTRAP =", KAFKA_BOOTSTRAP_SERVERS)
print("TOPIC =", KAFKA_TOPIC)
sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            redirect_uri=SPOTIFY_REDIRECT_URI,
            scope="user-top-read")
        )

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    allow_auto_create_topics=True,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

top_tracks = sp.current_user_top_tracks(limit=20,time_range = 'medium_term')
print("Total tracks:", len(top_tracks["items"]))



for i,track in enumerate(top_tracks['items'],start=1):
    track_data = {
        'rank': i,
        'track_id': track['id'],
        'track_name': track['name'],
        'artist_id': track['artists'][0]['id'],
        'artist_name': track['artists'][0]['name'],
        'album_name': track['album']['name'],
        'popularity': track['popularity'],
        'duration_ms': track['duration_ms'],
        "ingested_at": time.time()
    }

    producer.send(KAFKA_TOPIC,track_data)
    print(f"Sent track data to Kafka: {track_data}")

producer.flush()
print("All messages flushed to Kafka.")
time.sleep(30)