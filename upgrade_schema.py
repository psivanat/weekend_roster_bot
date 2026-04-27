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

def upgrade_database():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    try:
        print("1. Adding Shift Time columns to 'teams' table...")
        cursor.execute("""
            ALTER TABLE teams 
            ADD COLUMN IF NOT EXISTS shift_start_time TIME DEFAULT '09:00:00',
            ADD COLUMN IF NOT EXISTS shift_end_time TIME DEFAULT '17:00:00';
        """)

        print("2. Creating new unlimited 'roster_assignments' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS roster_assignments (
                id SERIAL PRIMARY KEY,
                shift_date DATE NOT NULL,
                team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
                engineer_id INTEGER REFERENCES engineers(id) ON DELETE CASCADE,
                UNIQUE(shift_date, team_id, engineer_id)
            );
        """)
        
        # We can safely drop the old rigid roster table
        cursor.execute("DROP TABLE IF EXISTS roster;")

        conn.commit()
        print("✅ Database upgraded successfully for unlimited engineers and shift times!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Upgrade failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    upgrade_database()
