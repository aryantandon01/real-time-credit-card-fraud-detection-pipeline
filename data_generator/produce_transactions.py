from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    transaction = {
        'transaction_id': random.randint(1000, 10000),
        'user_id': random.randint(1, 100),
        'amount': random.uniform(10.0, 1000.0),
        'timestamp': int(time.time())
    }
    producer.send('transactions', transaction)
    print(f"✅ Produced: {transaction}")
    time.sleep(1)
