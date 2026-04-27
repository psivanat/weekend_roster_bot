import psycopg2

DB_PARAMS = {
    "host": "localhost",
    "database": "roster_db",
    "user": "roster_bot",
    "password": "City@6696"  # <-- UPDATE THIS
}

def seed_database():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # 1. Insert Engineers
    engineers = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    for eng in engineers:
        cursor.execute(
            "INSERT INTO engineers (name, webex_email) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING",
            (eng, f"{eng.lower()}@yourcompany.com")
        )

    # 2. Insert Availability for March 2026
    # Format: (Engineer Name, Preferences)
    avail_data = [
        ('A', '1, 7, 8, 14, 15, 28, 29'),
        ('B', '21, 22, 28, 29'),
        ('C', '1, 7, 8, 14, 15, 21, 22, 28, 29'),
        ('D', '1, 7, 8, 14, 15, 21, 22, 28, 29'),
        ('E', '1, 7, 8, 14, 15, 28, 29'),
        ('F', '1, 7, 8, 14, 15, 28, 29'),
        ('G', '1, 7, 8, 15, 21'),
        ('H', '1, 7, 8, 14, 15, 21, 22, 28, 29')
    ]

    for name, prefs in avail_data:
        # Get the engineer's ID
        cursor.execute("SELECT id FROM engineers WHERE name = %s", (name,))
        eng_id = cursor.fetchone()[0]

        # Insert availability
        cursor.execute(
            """
            INSERT INTO availability (engineer_id, year_month, preferences) 
            VALUES (%s, '2026-03', %s) 
            ON CONFLICT (engineer_id, year_month) DO UPDATE SET preferences = EXCLUDED.preferences
            """,
            (eng_id, prefs)
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Test data successfully inserted into PostgreSQL!")

if __name__ == "__main__":
    seed_database()
