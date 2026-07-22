import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def create_table_example():
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_NAME", "postgres")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT", "5432")

    if not all([host, user, password]):
        return False

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            connect_timeout=5
        )
        cursor = conn.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        
        query_create_table = """
            CREATE TABLE IF NOT EXISTS raw.example_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
            );
        """
        cursor.execute(query_create_table)
        
        conn.commit()
        
        cursor.close()
        conn.close()
        return True

    except Exception as e:
        return False

if __name__ == "__main__":
    create_table_example()
