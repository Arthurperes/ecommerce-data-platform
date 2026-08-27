import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC_ECOMMERCE_EVENTS = os.getenv(
    "TOPIC_ECOMMERCE_EVENTS",
    "ecommerce_events_raw"
)

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommercedb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ecommerceuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ecommercepass")

POSTGRES_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
