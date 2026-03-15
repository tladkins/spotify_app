import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
from db import insert_track

load_dotenv()

scope = "user-read-recently-played"

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        scope="user-read-recently-played",
        client_id=os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIPY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIPY_REDIRECT_URI")
    )
)

def fetch_recent_tracks():

    results = sp.current_user_recently_played(limit=50)

    for item in results['items']:

        track = item['track']

        track_name = track['name']
        artist = track['artists'][0]['name']
        played_at = item['played_at']
        spotify_id = track['id']

        insert_track(track_name, artist, played_at, spotify_id)

        print(f"Saved: {track_name} - {artist}")


def main():

    fetch_recent_tracks()


if __name__ == "__main__":
    main()