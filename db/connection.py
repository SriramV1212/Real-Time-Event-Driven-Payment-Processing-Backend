import psycopg2

from config import DB_HOST
from config import DB_NAME
from config import DB_PASSWORD
from config import DB_PORT
from config import DB_USER


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
