import psycopg2

def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS engineers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            webex_email VARCHAR(255) UNIQUE,
            is_active BOOLEAN DEFAULT TRUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS availability (
            id SERIAL PRIMARY KEY,
            engineer_id INTEGER NOT NULL,
            year_month VARCHAR(7) NOT NULL, -- Format: YYYY-MM
            preferences TEXT NOT NULL,      -- Store as comma-separated string e.g., "14, 15, 7, 8"
            FOREIGN KEY (engineer_id)
                REFERENCES engineers (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            UNIQUE (engineer_id, year_month) -- Ensures 1 entry per engineer per month
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS roster (
            shift_date DATE PRIMARY KEY,
            engineer_id_1 INTEGER REFERENCES engineers (id),
            engineer_id_2 INTEGER REFERENCES engineers (id),
            engineer_id_3 INTEGER REFERENCES engineers (id) -- Nullable for Sundays
        )
        """
    )
    
    conn = None
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(
            host="localhost",
            database="roster_db",
            user="roster_bot",
            password="City@6696" # <-- UPDATE THIS
        )
        cursor = conn.cursor()
        
        # Execute the table creation commands
        for command in commands:
            cursor.execute(command)
            
        # Commit the changes
        cursor.close()
        conn.commit()
        print("✅ PostgreSQL tables created successfully!")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("❌ Error creating tables:", error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_tables()
