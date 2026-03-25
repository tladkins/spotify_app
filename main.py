import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
from db import insert_track, get_latest_played_at_ms
from datetime import datetime, timezone

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
    # Fetch only tracks newer than the latest stored played_at to avoid gaps
    latest_ms = get_latest_played_at_ms()

    all_items = []

    # First fetch: newest items after latest_ms (if present)
    if latest_ms:
        print(f"Fetching plays after {latest_ms} ms since epoch")
        results = sp.current_user_recently_played(limit=50, after=latest_ms)
    else:
        results = sp.current_user_recently_played(limit=50)

    batch = results.get('items', [])
    all_items.extend(batch)

    # If we got a full batch, there may be older items we still need to fetch.
    # Page backwards using the 'before' parameter until we reach the latest_ms.
    while len(batch) == 50:
        # find oldest played_at in the batch
        oldest_iso = min(i['played_at'] for i in batch)
        # convert to ms
        if oldest_iso.endswith('Z'):
            oldest_iso_t = oldest_iso.replace('Z', '+00:00')
        else:
            oldest_iso_t = oldest_iso
        oldest_dt = datetime.fromisoformat(oldest_iso_t)
        oldest_ms = int(oldest_dt.timestamp() * 1000)

        # fetch older items before the oldest_ms
        batch_resp = sp.current_user_recently_played(limit=50, before=oldest_ms)
        batch = batch_resp.get('items', [])
        if not batch:
            break
        # stop if the oldest in this new batch is <= latest_ms
        min_ms_in_batch = int(datetime.fromisoformat(batch[-1]['played_at'].replace('Z', '+00:00')).timestamp() * 1000)
        all_items.extend(batch)
        if latest_ms and min_ms_in_batch <= latest_ms:
            break

    if not all_items:
        print("No recent plays returned.")
        return

    # Convert items to (played_at_ms, item) and filter out any <= latest_ms
    processed = []
    for item in all_items:
        iso = item['played_at']
        if iso.endswith('Z'):
            iso_t = iso.replace('Z', '+00:00')
        else:
            iso_t = iso
        dt = datetime.fromisoformat(iso_t)
        played_ms = int(dt.timestamp() * 1000)
        if latest_ms and played_ms <= latest_ms:
            continue
        processed.append((played_ms, item))

    # insert from oldest -> newest to preserve chronology
    processed.sort(key=lambda x: x[0])

    for _, item in processed:
        track = item['track']
        track_name = track['name']
        artist = track['artists'][0]['name']
        played_at = item['played_at']
        spotify_id = track['id']

        inserted = insert_track(track_name, artist, played_at, spotify_id)
        if inserted:
            print(f"Saved: {track_name} - {artist}")


def main():

    fetch_recent_tracks()


if __name__ == "__main__":
    main()