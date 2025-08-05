import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
ALERT_TOPIC = os.getenv("ALERT_TOPIC")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")

