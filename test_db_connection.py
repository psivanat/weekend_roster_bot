import psycopg2

try:
    # Connect to your postgres DB
    connection = psycopg2.connect(
        host="localhost",
        database="roster_db",
        user="roster_bot",
        password="City@6696"
    )
    
    cursor = connection.cursor()
    
    # Print PostgreSQL version to confirm it works
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record)

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
