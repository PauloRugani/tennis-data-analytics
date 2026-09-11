import os
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

class DBHandler:
    def __init__(self):
        ...

    def get_db_connection(self):
        host = os.getenv("DB_HOST")
        database = os.getenv("DB_NAME", "postgres")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        port = os.getenv("DB_PORT", "5432")

        if not all([host, user, password]):
            raise ValueError("fail to load all env variables")

        try:
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port,
                connect_timeout=10
            )
            return conn
        except Exception as e:
            print(f"fail to connect to the database {e}")
            raise e

    @contextmanager
    def get_db_cursor(self, commit=True):
        conn = self.get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"execution failed {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

    def execute_query(self, query: str, params: tuple = None, commit: bool = True):
        with self.get_db_cursor(commit=commit) as cursor:
            cursor.execute(query, params)

    def fetch_query(self, query: str, params: tuple = None) -> list[dict]:
        with self.get_db_cursor(commit=False) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall