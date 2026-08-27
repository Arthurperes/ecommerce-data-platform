import random
import uuid
from datetime import datetime

EVENT_TYPES = ["view", "cart", "purchase"]

PRODUCTS = [
    {
        "product_id": 1001,
        "category_id": 10,
        "category_code": "electronics.smartphone",
        "brand": "samsung",
        "price": 2499.90
    },
    {
        "product_id": 1002,
        "category_id": 10,
        "category_code": "electronics.smartphone",
        "brand": "apple",
        "price": 4999.90
    },
    {
        "product_id": 2001,
        "category_id": 20,
        "category_code": "electronics.audio.headphone",
        "brand": "sony",
        "price": 799.90
    },
    {
        "product_id": 3001,
        "category_id": 30,
        "category_code": "computers.notebook",
        "brand": "lenovo",
        "price": 3599.90
    }
]


def generate_ecommerce_event():

    product = random.choice(PRODUCTS)

    event = {
        "event_time": datetime.utcnow().isoformat(),
        "event_type": random.choices(
            EVENT_TYPES,
            weights=[70, 20, 10]
        )[0],
        "product_id": product["product_id"],
        "category_id": product["category_id"],
        "category_code": product["category_code"],
        "brand": product["brand"],
        "price": product["price"],
        "user_id": random.randint(100000, 999999),
        "user_session": str(uuid.uuid4())
    }

    return event
