from locust import HttpUser, task, between

class OtelDemoUser(HttpUser):
    wait_time = between(1, 3)

    @task(3)
    def load_homepage(self):
        self.client.get("/")

    @task(2)
    def browse_products(self):
        self.client.get("/api/products")

    # @task(1)
    # def view_product(self):
    #     # Example product ID (these exist in demo)
    #     self.client.get("/api/product/OLJCESPC7Z")

    @task(1)
    def add_to_cart(self):
        self.client.post("/cart", json={
            "product_id": "OLJCESPC7Z",
            "quantity": 1
        })

    @task(1)
    def view_cart(self):
        self.client.get("/cart")

    # @task(1)
    # def checkout(self):
    #     self.client.post("/api/checkout", json={
    #         "email": "test@example.com",
    #         "address": {
    #             "street_address": "123 Test St",
    #             "city": "Test City",
    #             "state": "TS",
    #             "zip_code": "12345",
    #             "country": "NL"
    #         },
    #         "credit_card": {
    #             "number": "4242424242424242",
    #             "expiration_month": 12,
    #             "expiration_year": 2030,
    #             "cvv": "123"
    #         }
    #     })