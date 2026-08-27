import json
import time
from kafka import KafkaProducer
from app.simulator import generate_ecommerce_event
from app.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ECOMMERCE_EVENTS

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def run():
    print("Producer de eventos de e-commerce iniciado...")

    while True:
        data = generate_ecommerce_event()

        producer.send(
            TOPIC_ECOMMERCE_EVENTS,
            value=data
        )

        print(f"Evento enviado: {data}")

        time.sleep(2)

if __name__ == "__main__":
    run()
