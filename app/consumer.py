import json
import hashlib
from app.data_lake_writer import save_json_to_datalake
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_STOCK_PRICES,
    TOPIC_STOCK_ORDERS,
    POSTGRES_URL
)

# conexão com banco
engine = create_engine(POSTGRES_URL)

# consumer kafka
consumer = KafkaConsumer(
    TOPIC_STOCK_PRICES,
    TOPIC_STOCK_ORDERS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="stock-consumer-group"
)

def mask_client_id(client_id: str) -> str:
    return hashlib.sha256(client_id.encode()).hexdigest()

def process_price(data):
    if data["price"] <= 0:
        return None

    price_band = "LOW" if data["price"] < 30 else "HIGH"

    return {
        "ticker": data["ticker"],
        "price": data["price"],
        "volume": data["volume"],
        "event_time": data["event_time"],
        "price_band": price_band
    }

def process_order(data):
    if data["quantity"] <= 0:
        return None

    return {
        "order_id": data["order_id"],
        "client_id_masked": mask_client_id(data["client_id"]),
        "ticker": data["ticker"],
        "order_type": data["order_type"],
        "quantity": data["quantity"],
        "price": data["price"],
        "total_value": data["quantity"] * data["price"],
        "event_time": data["event_time"]
    }

def insert_price(conn, data):
    conn.execute(text("""
        INSERT INTO trusted_stock_prices
        (ticker, price, volume, event_time, price_band)
        VALUES (:ticker, :price, :volume, :event_time, :price_band)
    """), data)

def insert_order(conn, data):
    conn.execute(text("""
        INSERT INTO trusted_stock_orders
        (order_id, client_id_masked, ticker, order_type, quantity, price, total_value, event_time)
        VALUES (:order_id, :client_id_masked, :ticker, :order_type, :quantity, :price, :total_value, :event_time)
    """), data)


def run():
    print("🚀 Consumer iniciado...")

    for message in consumer:
        topic = message.topic
        data = message.value

        try:
            with engine.begin() as conn:
                if topic == TOPIC_STOCK_PRICES:
                    processed = process_price(data)
                    if processed:
                        insert_price(conn, processed)
                        object_name = save_json_to_datalake(
                            bucket="raw",
                            prefix="prices",
                            data=processed
                        )
                        print(f"💾 PRICE salvo no banco e MinIO: {processed}")
                        print(f"📁 Arquivo MinIO: {object_name}")

                elif topic == TOPIC_STOCK_ORDERS:
                    processed = process_order(data)
                    if processed:
                        insert_order(conn, processed)
                        object_name = save_json_to_datalake(
                            bucket="raw",
                            prefix="orders",
                            data=processed
                        )
                        print(f"💾 ORDER salvo no banco e MinIO: {processed}")
                        print(f"📁 Arquivo MinIO: {object_name}")

        except Exception as e:
            print(f"❌ Erro ao processar: {e}")

if __name__ == "__main__":
    run()
