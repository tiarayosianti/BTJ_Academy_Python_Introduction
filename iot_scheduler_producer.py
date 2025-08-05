import json
import random
import time
import sys
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
from main import KAFKA_SERVER, KAFKA_TOPIC


# Konfigurasi Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

# Membuat ID perangkat IOT
device_ids = [f"device_{i}" for i in range(1, 6)]

def generate_iot_data():
    for device_id in device_ids:
        temperature = round(random.uniform(1, 100), 2)
        data = {
            "device_id": device_id,
            "timestamp": datetime.utcnow().isoformat(),
            "temperature": temperature, # celcius
            "humidity": round(random.uniform(20, 80), 2), # persen
            "wind_speed": round(random.uniform(1, 20), 2), # mil per jam
            "rainfall": round(random.uniform(1, 20), 2), # inchi
            "status": "OK" if temperature <=95 else "OVERHEAT"
        }
        print(f"[SEND] {data}")
        producer.send(KAFKA_TOPIC, value=data)
    producer.flush()

# Scheduler setiap 1 menit
scheduler = BackgroundScheduler()
scheduler.add_job(generate_iot_data, "cron", minute="*")  
print("Sending IoT data ...")
scheduler.start()

# Durasi runtime: 10 menit (600 detik) sebagai contoh
start_time = time.time()
duration_seconds = 600

try:
    while True:
        time.sleep(1)
        if time.time() - start_time > duration_seconds:
            print("Waktu produce selesai. Scheduler dihentikan.")
            scheduler.shutdown()
            producer.close()
            sys.exit()
except KeyboardInterrupt:
    print("Program dihentikan manual.")
    scheduler.shutdown()
    producer.close()
