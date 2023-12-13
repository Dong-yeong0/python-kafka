import os
import psycopg2
from psycopg2 import pool, extras

pg_connection_params = {
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
    "database": os.environ.get("POSTGRES_NAME"),
}

def create_connection_pool(
    minconn=os.environ.get("POSTGRES_MIN_CONN"),
    maxconn=os.environ.get("POSTGRES_MAX_CONN"),
):
    return pool.ThreadedConnectionPool(
        minconn=minconn,
        maxconn=maxconn,
        **pg_connection_params,
        cursor_factory=extras.RealDictCursor,
    )


def put_conn(pool, conn):
    pool.putconn(conn)
