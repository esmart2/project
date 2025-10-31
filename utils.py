import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor


def run_sql(sql: str, params: tuple = ()):
    # Load environment variables from .env
    load_dotenv()

    USER = os.getenv("user")
    PASSWORD = os.getenv("password")
    HOST = os.getenv("host")
    PORT = os.getenv("port")
    DBNAME = os.getenv("dbname")

    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME,
            sslmode="require",   # add SSL for Supabase
        )
        print("Connection successful!")

        # Use a dict cursor so you get column names in results
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params)

            # If the query returns rows (e.g. SELECT)
            if cursor.description:
                result = cursor.fetchall()
                print(f"Returned {len(result)} rows")
                return result
            else:
                # For INSERT/UPDATE/DELETE, commit and return nothing
                connection.commit()
                print("Query executed successfully (no return rows).")
                return []

    except Exception as e:
        print(f"Failed to connect or run query: {e}")
        return []

    finally:
        if 'connection' in locals() and not connection.closed:
            connection.close()
            print("Connection closed.")
