import psycopg

def connect_to_postgresql(database="monitoramento"):
    conn = psycopg.connect(
        dbname=database,
        user="postgres",
        password="root",
        host="localhost",
        port="5432"
    )
    return conn
