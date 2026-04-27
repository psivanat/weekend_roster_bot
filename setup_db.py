import os
import psycopg2
from werkzeug.security import generate_password_hash
from dotenv import load_dotenv

load_dotenv()
DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

def setup_database():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    try:
        print("Dropping old tables to build enterprise schema...")
        cursor.execute("""
            DROP TABLE IF EXISTS roster_assignments CASCADE;
            DROP TABLE IF EXISTS leave_blockouts CASCADE;
            DROP TABLE IF EXISTS availability CASCADE;
            DROP TABLE IF EXISTS engineers CASCADE;
            DROP TABLE IF EXISTS users CASCADE;
            DROP TABLE IF EXISTS teams CASCADE;
        """)

        print("1. Creating Teams table...")
        cursor.execute("""
            CREATE TABLE teams (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                sat_coverage INTEGER DEFAULT 3,
                sun_coverage INTEGER DEFAULT 2,
                min_preferences INTEGER DEFAULT 6,
                shift_start_time TIME DEFAULT '09:00:00',
                shift_end_time TIME DEFAULT '17:00:00'
            )
        """)

        print("2. Creating Users table (For Authentication & RBAC)...")
        cursor.execute("""
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(50) NOT NULL CHECK (role IN ('super_admin', 'team_admin', 'viewer')),
                team_id INTEGER REFERENCES teams(id) ON DELETE SET NULL
            )
        """)

        print("3. Creating Engineers table (With Individual Shift Limits)...")
        cursor.execute("""
            CREATE TABLE engineers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                webex_email VARCHAR(255),
                team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                is_active BOOLEAN DEFAULT TRUE,
                max_shifts INTEGER DEFAULT 3,
                UNIQUE(name, team_id)
            )
        """)

        print("4. Creating Availability & Leave Blockouts tables...")
        cursor.execute("""
            CREATE TABLE availability (
                id SERIAL PRIMARY KEY,
                engineer_id INTEGER NOT NULL REFERENCES engineers(id) ON DELETE CASCADE,
                year_month VARCHAR(7) NOT NULL,
                preferences TEXT NOT NULL,
                UNIQUE (engineer_id, year_month)
            );
            
            CREATE TABLE leave_blockouts (
                id SERIAL PRIMARY KEY,
                engineer_id INTEGER NOT NULL REFERENCES engineers(id) ON DELETE CASCADE,
                block_date DATE NOT NULL,
                UNIQUE (engineer_id, block_date)
            );
        """)

        print("5. Creating Roster Assignments table...")
        cursor.execute("""
            CREATE TABLE roster_assignments (
                id SERIAL PRIMARY KEY,
                shift_date DATE NOT NULL,
                team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                engineer_id INTEGER NOT NULL REFERENCES engineers(id) ON DELETE CASCADE,
                UNIQUE(shift_date, team_id, engineer_id)
            )
        """)

        # --- SEED INITIAL DATA ---
        print("Seeding initial Super-Admin and Default Team...")
        cursor.execute("INSERT INTO teams (name) VALUES ('Alpha Team') RETURNING id")
        team_id = cursor.fetchone()[0]

        # Create your Super Admin account (Password: admin123)
        hashed_pw = generate_password_hash("admin123")
        cursor.execute("""
            INSERT INTO users (username, password_hash, role, team_id) 
            VALUES ('superadmin', %s, 'super_admin', NULL)
        """, (hashed_pw,))

        conn.commit()
        print("✅ Database setup complete! You can log in with username: 'superadmin', password: 'admin123'")

    except Exception as e:
        conn.rollback()
        print(f"❌ Setup failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_database()
