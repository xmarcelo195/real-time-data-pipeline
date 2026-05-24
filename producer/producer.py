import time
import json
from faker import Faker
from kafka import KafkaProducer
import uuid
import random

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'test-topic'

def generate_data():
    return {
        'name': fake.name(),
        'email': fake.email(),
        'address': fake.address(),
        'timestamp': fake.iso8601(),
        "country": fake.country(),
        "currency": random.choice(["USD", "EUR", "GBP", "JPY", "AUD"]),
        "value": round(random.uniform(10.0, 5000.0), 2),
        "item_description": fake.bs(),
        "transaction_id": str(fake.uuid4()),
    }

if __name__ == '__main__':
    while True:
        try:
            data = generate_data()
            future = producer.send(topic, value=data)
            result = future.get(timeout=60)
            time.sleep(1)
        except Exception as e:
            print(f"Erro ao enviar mensagem: {e}")
            time.sleep(5)