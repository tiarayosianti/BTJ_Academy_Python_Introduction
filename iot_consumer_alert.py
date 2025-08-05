from kafka import KafkaConsumer, KafkaProducer
import json
from main import KAFKA_SERVER, KAFKA_TOPIC, ALERT_TOPIC, CONSUMER_GROUP

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

try:
    for msg in consumer:
        data = msg.value
        print(f"[RECV] {data}")

        temperature = data.get("temperature")
        if temperature and temperature > 95:
            print(f"[ALERT] Temp {temperature} > 95! Forwarding to alert topic.")
            producer.send(ALERT_TOPIC, value=data)
            producer.flush()
except Exception as e:
    print(f"Error: {e}")
