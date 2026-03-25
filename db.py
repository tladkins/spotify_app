import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
        sslmode='require'
    )


def insert_track(track_name, artist, played_at, spotify_id):

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO listening_history (track_name, artist, played_at, spotify_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (spotify_id, played_at) DO NOTHING
        """, (track_name, artist, played_at, spotify_id))

        inserted = cur.rowcount
        conn.commit()
        cur.close()
        if inserted:
            print(f"Inserted: {track_name} - {artist} at {played_at}")
        else:
            print(f"Skipped (conflict): {track_name} - {artist} at {played_at}")
        return bool(inserted)
    except Exception as e:
        print(f"Error inserting track {track_name} ({spotify_id}) at {played_at}: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_latest_played_at_ms():
    """Return the max played_at in the DB as milliseconds since epoch, or None."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT EXTRACT(EPOCH FROM MAX(played_at)) * 1000 FROM listening_history;")
        row = cur.fetchone()
        cur.close()
        if row and row[0]:
            return int(row[0])
        return None
    except Exception as e:
        print(f"Error fetching latest played_at: {e}")
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
