import json
import os
import random
import time
from kafka import KafkaProducer


def generate_message():
    while True:
        yield {
            "topic": f"data_{random.randint(1, 10)}",
            "data": 1
        }
        time.sleep(1)



if  __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=f"{os.environ.get('KAFKA_HOST')}:{os.environ.get('KAFKA_PORT')}",
        compression_type="gzip",
        key_serializer=lambda v: str(v).encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        security_protocol="SASL_PLAINTEXT",
        sasl_plain_username="kafka",
        sasl_plain_password="1234",
        sasl_mechanism="PLAIN",
        acks="all",
    )
    topic_generator = generate_message()

    while True:
        message = next(topic_generator)
        producer.send(message["topic"], value=message["data"])
        producer.flush()
        print(f"Sent message to topic: {message['topic']}")
