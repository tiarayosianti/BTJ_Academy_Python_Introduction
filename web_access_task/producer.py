import os
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Kafka config
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# URL and IP address
IP_ADDRESS = os.getenv("IP_ADDRESS")
URL_ADDRESS = os.getenv("URL_ADDRESS")

# Setup Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

while True:
    message = {
        "timestamp": datetime.utcnow().isoformat(),
        "url": URL_ADDRESS,
        "ip_address": IP_ADDRESS
    }
    producer.send(KAFKA_TOPIC, value=message)
    print(f"[SEND] {message}")
    producer.flush()
    time.sleep(5)  # kirim tiap 5 detik → total 12 kali per menit
