import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "stockdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "stockuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "stockpass")

TOPIC_STOCK_PRICES = os.getenv("TOPIC_STOCK_PRICES", "stock_prices_raw")
TOPIC_STOCK_ORDERS = os.getenv("TOPIC_STOCK_ORDERS", "stock_orders_raw")

POSTGRES_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
