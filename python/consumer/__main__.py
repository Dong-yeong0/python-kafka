import os
from db.postgres import create_connection_pool, put_conn

if __name__ == '__main__':
    pg_connection_pool = create_connection_pool()
    conn = pg_connection_pool.getconn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            *
        FROM
            messages
        """
    )

    query_result = cur.fetchall()
    put_conn(pg_connection_pool, conn)
    cur.close()
