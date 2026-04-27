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

def migrate():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    try:
        print("Starting database migration for Multi-Team support...")
        
        # 1. Create Teams table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            )
        """)

        # 2. Create a default team for your existing data
        cursor.execute("INSERT INTO teams (name) VALUES ('Alpha Team') ON CONFLICT DO NOTHING RETURNING id")
        result = cursor.fetchone()
        if result:
            default_team_id = result[0]
        else:
            cursor.execute("SELECT id FROM teams WHERE name = 'Alpha Team'")
            default_team_id = cursor.fetchone()[0]

        # 3. Add team_id to Engineers
        cursor.execute("ALTER TABLE engineers ADD COLUMN IF NOT EXISTS team_id INTEGER")
        # Link existing engineers to the default team
        cursor.execute("UPDATE engineers SET team_id = %s WHERE team_id IS NULL", (default_team_id,))
        # Add foreign key constraint
        cursor.execute("""
            ALTER TABLE engineers 
            DROP CONSTRAINT IF EXISTS fk_team_id,
            ADD CONSTRAINT fk_team_id FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
        """)
        cursor.execute("ALTER TABLE engineers ALTER COLUMN team_id SET NOT NULL")

        # 4. Add team_id to Roster and update Primary Key
        cursor.execute("ALTER TABLE roster ADD COLUMN IF NOT EXISTS team_id INTEGER")
        cursor.execute("UPDATE roster SET team_id = %s WHERE team_id IS NULL", (default_team_id,))
        cursor.execute("""
            ALTER TABLE roster 
            DROP CONSTRAINT IF EXISTS fk_roster_team_id,
            ADD CONSTRAINT fk_roster_team_id FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
        """)
        cursor.execute("ALTER TABLE roster ALTER COLUMN team_id SET NOT NULL")
        
        # Drop old primary key (shift_date) and create composite key (shift_date + team_id)
        cursor.execute("ALTER TABLE roster DROP CONSTRAINT IF EXISTS roster_pkey")
        cursor.execute("ALTER TABLE roster ADD PRIMARY KEY (shift_date, team_id)")

        conn.commit()
        print("✅ Database successfully migrated!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Migration failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate()
