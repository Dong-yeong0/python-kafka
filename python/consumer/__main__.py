import os
import time
import json
from kafka.consumer import KafkaConsumer
from db.postgres import create_connection_pool, put_conn, insert_messages

if __name__ == "__main__":
    consumer = KafkaConsumer(
        os.environ.get("KAFKA_TOPIC"),
        group_id=os.environ.get("KAFKA_CONSUMER_GROUP_ID"),
        bootstrap_servers=[
            f"{os.environ.get('KAFKA_HOST')}:{os.environ.get('KAFKA_PORT')}"
        ],
        key_deserializer=lambda x: x.decode("utf-8"),
        value_deserializer=lambda x: json.loads(x),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ.get("KAFKA_USERNAME"),
        sasl_plain_password=os.environ.get("KAFKA_PASSWORD"),
        max_poll_records=1_000,
    )

    while True:
        msg_pack = consumer.poll(timeout_ms=3_000)
        for tp, messages in msg_pack.items():
            for message in messages:
                print(
                    f"Received message from topic '{tp.topic}'\n"
                    f"Partition {tp.partition}\nKey: {message.key}\nOffset {message.offset}\nData: {json.dumps(message.value, indent=2)}\n"
                )

        # pg_connection_pool = create_connection_pool()
        # conn = pg_connection_pool.getconn()
        # cur = conn.cursor()
        # insert_messages(conn, cur, msg_pack)
        # put_conn(pg_connection_pool, conn)
        # cur.close()
