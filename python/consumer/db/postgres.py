import os
import psycopg2
from psycopg2 import pool, extras, errors

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


def insert_messages(conn, cur, message_list: list)->bool:
    insert_query = "INSERT INTO messages (data) VALUES %s"
    insert_args = [(message["data"], ) for message in message_list]
    try:
        extras.execute_values(cur, insert_query, insert_args)
        print(cur.query.decode())
        print(
            f"insert data: {insert_args}"
        )
    except errors.DatabaseError as e:
        print(e)
        return False
    else:
        conn.commit()
        return True
