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

def upgrade_users():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    try:
        print("1. Creating user_teams mapping table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_teams (
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
                PRIMARY KEY (user_id, team_id)
            )
        """)

        print("2. Migrating existing user team access...")
        # Move existing team_id data into the new mapping table
        cursor.execute("""
            INSERT INTO user_teams (user_id, team_id)
            SELECT id, team_id FROM users WHERE team_id IS NOT NULL
            ON CONFLICT DO NOTHING
        """)

        print("3. Updating users table (Adding is_active, dropping old team_id)...")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE")
        cursor.execute("ALTER TABLE users DROP COLUMN IF EXISTS team_id")

        conn.commit()
        print("✅ Database upgraded successfully for Multi-Team Users!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Upgrade failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    upgrade_users()
