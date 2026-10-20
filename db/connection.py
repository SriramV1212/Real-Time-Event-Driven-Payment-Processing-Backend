import psycopg2

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def get_connection(database=None):
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=database if database is not None else DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
