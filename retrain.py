from kafka import KafkaConsumer
import json
import os

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mysql_server.bookings_db.bookings")
KAFKA_BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")]
RETRAIN_THRESHOLD = int(os.getenv("RETRAIN_THRESHOLD", "1000"))

counter = 0

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ml_retrain_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def retrain_model():
    print("Retraining model...")
    # Load latest bookings or batch from Kafka
    # Train ML model
    print("Retraining completed.")

for message in consumer:
    counter += 1
    print(f"New booking event: {message.value}")
    
    if counter >= RETRAIN_THRESHOLD:
        retrain_model()
        counter = 0
