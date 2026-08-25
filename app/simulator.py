import random
import time
from datetime import datetime
from faker import Faker

fake = Faker()

TICKERS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3"]

def generate_price():
    return {
        "ticker": random.choice(TICKERS),
        "price": round(random.uniform(10, 100), 2),
        "volume": random.randint(100, 10000),
        "event_time": datetime.utcnow().isoformat()
    }

def generate_order():
    return {
        "order_id": fake.uuid4(),
        "client_id": fake.uuid4(),
        "ticker": random.choice(TICKERS),
        "order_type": random.choice(["BUY", "SELL"]),
        "quantity": random.randint(1, 1000),
        "price": round(random.uniform(10, 100), 2),
        "event_time": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    while True:
        print(generate_price())
        print(generate_order())
        print("-" * 50)
        time.sleep(2)
