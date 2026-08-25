import json
import time
from kafka import KafkaProducer
from app.simulator import generate_price
from app.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_STOCK_PRICES

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def run():
    print("🚀 Producer de preços iniciado...")
    
    while True:
        data = generate_price()
        producer.send(TOPIC_STOCK_PRICES, value=data)
        
        print(f"Enviado PRICE: {data}")
        
        time.sleep(1)

if __name__ == "__main__":
    run()
