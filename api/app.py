from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

# Get database connection details from environment variables
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_host = os.getenv("POSTGRES_HOST", "destination_postgres")
db_port = os.getenv("POSTGRES_PORT", "5432")

def get_db_connection():
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    return conn

def fetch_data_with_headers(query):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()

    # Fetch the column headers using cursor.description
    headers = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    # Return data as a list of dictionaries (column name as key)
    data = [dict(zip(headers, row)) for row in rows]

    return {"headers": headers, "data": data}

@app.get("/actors")
def read_actors():
    return fetch_data_with_headers("SELECT * FROM actors;")

@app.get("/films")
def read_films():
    return fetch_data_with_headers("SELECT * FROM films;")

@app.get("/filmactors")
def read_film_actors():
    return fetch_data_with_headers("SELECT * FROM film_actors;")

@app.get("/filmclassification")
def read_film_classification():
    return fetch_data_with_headers("SELECT * FROM film_classification;")

@app.get("/filmratings")
def read_film_ratings():
    return fetch_data_with_headers("SELECT * FROM film_ratings;")
