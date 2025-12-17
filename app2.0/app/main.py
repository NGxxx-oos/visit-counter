import os
from datetime import datetime
from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2 import sql

app = FastAPI()

# Database connection details from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Construct the connection string
DATABASE_URL = f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Could not connect to the database")

def initialize_db(conn):
    """Creates the visits table if it does not exist."""
    with conn.cursor() as cur:
        # Use sql.SQL and sql.Identifier for safe query construction
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS visits (
                id SERIAL PRIMARY KEY,
                visited_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.commit()

@app.get("/")
def visit_counter():
    """Handles the visit counter logic: insert new visit and count total visits."""
    conn = get_db_connection()
    try:
        # 1. Initialize table
        initialize_db(conn)

        with conn.cursor() as cur:
            # 2. Insert new visit
            cur.execute(sql.SQL("INSERT INTO visits DEFAULT VALUES;"))
            
            # 3. Count total visits
            cur.execute(sql.SQL("SELECT COUNT(*) FROM visits;"))
            count = cur.fetchone()[0]
            
            conn.commit()
            
            # 4. Return result
            return {"message": f"Hello! I have been visited {count} times"}
            
    except Exception as e:
        conn.rollback()
        print(f"An error occurred during database operation: {e}")
        raise HTTPException(status_code=500, detail="Database operation failed")
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    # This block is mainly for local testing, the container will use the command in Dockerfile
    uvicorn.run(app, host="0.0.0.0", port=8000)
