import json
import os
import random
import time
from kafka import KafkaProducer, errors

def generate_message():
    while True:
        yield {
            "data": random.randint(1, 99)
        }
        time.sleep(1)

if __name__ == "__main__":
    try:
        producer = KafkaProducer(
            bootstrap_servers=f"{os.environ.get('KAFKA_HOST')}:{os.environ.get('KAFKA_PORT')}",
            compression_type="gzip",
            key_serializer=lambda v: str(v).encode("utf-8"),
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=os.environ.get("KAFKA_USERNAME"),
            sasl_plain_password=os.environ.get("KAFKA_PASSWORD"),
            acks="all",
        )
        topic_generator = generate_message()

        while True:
            message = next(topic_generator)
            producer.send(os.environ.get("KAFKA_TOPIC"), value=message)
            producer.flush()
            print(f"Sent message to topic: {message }")
    except errors.KafkaError as ke:
        print(f"KafkaError: {ke}")

