import psycopg2 # type: ignore

def create_database():
    # Database connection parameters
    dbname = "postgres"  # Default database name
    user = "your_user"   # Your PostgreSQL username
    password = "your_password"  # Your PostgreSQL password
    host = "your_host"   # Your PostgreSQL host address
    port = "your_port"   # Your PostgreSQL port number

    # Connect to the default database
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = True  # Enable autocommit mode

    # Create a new database named 'LoanDatabase'
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE LoanDatabase")
        print("Database 'LoanDatabase' created successfully!")
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        # Close the database connection
        conn.close()

if __name__ == "__main__":
    create_database()
