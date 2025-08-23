from confluent_kafka import Consumer, KafkaError
import json

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:29092',  # external listener
    'group.id': 'db-change-logger',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topics = ["bookings_db_server.bookings_db.bookings"]
consumer.subscribe(topics)

print("Listening for DB changes...")

# Counter for new records
record_count = 0
RETRAIN_THRESHOLD = 1000

def retrain_model_placeholder():
    # Placeholder function for model retraining
    print("=== Retraining model with new data ===")
    # TODO: Add your retraining logic here

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
            # print(json.dumps(record, indent=2))

            # Increment counter
            record_count += 1
            if record_count >= RETRAIN_THRESHOLD:
                retrain_model_placeholder()
                record_count = 0  # reset counter

        except Exception as e:
            print(f"Failed to parse message: {e}")
finally:
    consumer.close()
