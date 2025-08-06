import os
import json
import redis
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# Kafka config
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")

# Posgres config
POSGRES_SERVER = os.getenv("POSGRES_SERVER")
POSGRES_PORT = os.getenv("POSGRES_PORT")
USER_DB = os.getenv("USER_DB")
PASSWORD_DB = os.getenv("PASSWORD_DB")
NAME_DB = os.getenv("NAME_DB")

# Redis config
REDIS_SERVER = os.getenv("REDIS_SERVER")
REDIS_PORT = os.getenv("REDIS_PORT")

# Setup redis
redis_client = redis.Redis(host=REDIS_SERVER, port=int(REDIS_PORT), decode_responses=True)

# Connect to PostgreSQL
try:
    pg_conn = psycopg2.connect(
        dbname=NAME_DB,
        user=USER_DB,
        password=PASSWORD_DB,
        host=POSGRES_SERVER,
        port=POSGRES_PORT
    )
    pg_cur = pg_conn.cursor()
except psycopg2.Error as e:
    print(f"Error connect to database! {e}")

# Consumer Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    enable_auto_commit=True,
    group_id=CONSUMER_GROUP
)

# Insert to database from redis
def blocked_ip(ip_address):
    current_time = datetime.utcnow()
    query_insert = "INSERT INTO blocked_ip_address (ip_address, blocked_at) VALUES (%s, %s)"
    insert_to_db = (ip_address, current_time)
    pg_cur.execute(query_insert, insert_to_db)
    pg_conn.commit()
    print("[SUCCESS] Blocked IP inserted to database.")

try:
    for msg in consumer:
        data = msg.value
        ip = data.get("ip_address")
        print("[MESSAGE] ", data)

        # Key Redis → ip_address:timestamp-minute
        key = f"count:{ip}:{datetime.utcnow().strftime('%Y-%m-%d-%H-%M')}"
        count = redis_client.incr(key)
        # print(f"IP {ip} accessed the web {count} times.")
        redis_client.expire(key, 60)  # TTL 1 menit (6 detik)
        print(f"[INFO] IP {ip} akses ke-{count} dalam 1 menit")

        if count > 10 and not redis_client.get(f"blocked:{ip}"):  # maksimal akses per menit
            print(f"[BLOCK] IP {ip} melebihi batas akses, masukkan ke database.")
            blocked_ip(ip)
            redis_client.set(f"blocked:{ip}", 1, ex=3600)
except KeyboardInterrupt:
    print("Dihentikan manual.")
finally:
    consumer.close()
    pg_cur.close()
    pg_conn.close()
