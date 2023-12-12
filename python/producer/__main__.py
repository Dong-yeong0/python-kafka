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
            api_version=(0,11,5),
            key_serializer=lambda v: str(v).encode("utf-8"),
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="admin",
            sasl_plain_password="1234",
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
    finally:
        producer.close()  # Always close the producer when done
