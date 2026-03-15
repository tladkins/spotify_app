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

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO listening_history (track_name, artist, played_at, spotify_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (spotify_id, played_at) DO NOTHING
    """, (track_name, artist, played_at, spotify_id))

    conn.commit()
    cur.close()
    conn.close()
