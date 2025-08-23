from confluent_kafka import Consumer, KafkaError
import json
import logging

# Setup logging
logging.basicConfig(
    filename="db_changes.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:29092',  # external listener
    'group.id': 'db-change-logger',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Subscribe to Debezium topic(s)
topics = ["bookings_db_server.bookings_db.bookings"]
consumer.subscribe(topics)

print("Listening for DB changes...")

try:
    while True:
        msg = consumer.poll(1.0)  # 1 second timeout
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                continue

        # Decode message
        try:
            record = json.loads(msg.value().decode('utf-8'))
            print(f"Change event: {json.dumps(record, indent=2)}")
            print(json.dumps(record, indent=2))
        except Exception as e:
            print(f"Failed to parse message: {e}")
finally:
    consumer.close()
