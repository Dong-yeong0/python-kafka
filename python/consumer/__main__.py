import os
import time
import json
from kafka.consumer import KafkaConsumer
from db.postgres import create_connection_pool, put_conn, insert_messages

def process_and_insert_messages(consumer):
    while True:
        msg_pack = consumer.poll(timeout_ms=3_000)
        converted_messages = [
            {
                "topic": tp.topic,
                "partition": tp.partition,
                "key": message.key,
                "offset": message.offset,
                "data": message.value
            }
            for tp, messages in msg_pack.items()
            for message in messages
        ]

        for msg in converted_messages:
            print(
                f"Received message from topic '{msg['topic']}'\n"
                f"Partition {msg['partition']}\nKey: {msg['key']}\nOffset {msg['offset']}\nData: {json.dumps(msg['data'], indent=2)}\n"
            )


        insert_values = [msg["data"] for msg in converted_messages]
        if insert_values != []:
            pg_connection_pool = create_connection_pool()
            conn = pg_connection_pool.getconn()
            cur = conn.cursor()
            if insert_messages(conn, cur, insert_values):
                consumer.commit()
            else:
                print("Failed to insert messages")
            put_conn(pg_connection_pool, conn)
            cur.close()

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

    process_and_insert_messages(consumer)
