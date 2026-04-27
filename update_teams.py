import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

def update_database():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    try:
        print("Adding new settings columns to the 'teams' table...")
        cursor.execute("""
            ALTER TABLE teams 
            ADD COLUMN IF NOT EXISTS max_shifts INTEGER DEFAULT 3,
            ADD COLUMN IF NOT EXISTS sat_coverage INTEGER DEFAULT 3,
            ADD COLUMN IF NOT EXISTS sun_coverage INTEGER DEFAULT 2,
            ADD COLUMN IF NOT EXISTS min_preferences INTEGER DEFAULT 6;
        """)
        conn.commit()
        print("✅ Database updated successfully!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Update failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    update_database()
