from locust import HttpUser, task, between
import random

PRODUCT_IDS = [
    "0PUK6V6EV0",
    "1YMWWN1N4O",
    "2ZYFJ3GM2N",
    "66VCHSJNUP",
    "6E92ZMYYFZ",
    "9SIQT8TOJO",
    "L9ECAV7KIM",
    "LS4PSXUNUM",
    "OLJCESPC7Z"
]

class OnlineBoutiqueUser(HttpUser):
    wait_time = between(1, 3)

    @task(5)
    def test(self):
        self.client.get("/", timeout=5)

    def on_start(self):
        pass

    
    def visit_homepage(self):
        with self.client.get("/", catch_response=True) as r:
            if r.status_code != 200:
                r.failure(f"Homepage failed: {r.status_code}")

    
    @task(10)
    def debug_ping(self):
        self.client.get("/")

    @task(3)
    def browse_products(self):
        self.visit_homepage()

        product_id = random.choice(PRODUCT_IDS)
        self.client.get(f"/product/{product_id}")

    @task(2)
    def add_to_cart(self):
        product_id = random.choice(PRODUCT_IDS)

        with self.client.post("/cart", data={
            "product_id": product_id,
            "quantity": random.randint(1, 3)
        }, catch_response=True) as r:
            if r.status_code != 200:
                r.failure(f"Cart failed: {r.status_code}")
    

    @task(1)
    def checkout(self):
        with self.client.post(
            "/cart/checkout",
            data={
                "email": "user@example.com",
                "street_address": "123 Test Street",
                "zip_code": "12345",
                "city": "TestCity",
                "state": "TestState",
                "country": "TestCountry",
                "credit_card_number": "4111111111111111",
                "credit_card_expiration_month": "12",
                "credit_card_expiration_year": "2030",
                "credit_card_cvv": "123"
            },
            catch_response=True
        ) as r:
            if r.status_code != 200:
                r.failure(f"Checkout failed: {r.status_code}")