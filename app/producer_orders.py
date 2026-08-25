import json
import time
from kafka import KafkaProducer
from app.simulator import generate_order
from app.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_STOCK_ORDERS

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def run():
    print("🚀 Producer de ordens iniciado...")
    
    while True:
        data = generate_order()
        producer.send(TOPIC_STOCK_ORDERS, value=data)
        
        print(f"Enviado ORDER: {data}")
        
        time.sleep(2)

if __name__ == "__main__":
    run()
