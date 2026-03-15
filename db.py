import psycopg2
import os

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))


def insert_track(track_name, artist, played_at, spotify_id):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO listening_history
        (track_name, artist, played_at, spotify_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (spotify_id) DO NOTHING
    """, (track_name, artist, played_at, spotify_id))

    conn.commit()

    cur.close()
    conn.close()