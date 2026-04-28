import base64
from random import choice

from locust import HttpUser, constant, task


class Web(HttpUser):
    wait_time = constant(0)

    @task
    def load(self):
        base64string = base64.b64encode(b"user:password").decode("ascii")

        with self.client.get("/catalogue", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"GET /catalogue returned HTTP {response.status_code}")
                return

            try:
                catalogue = response.json()
            except ValueError as exc:
                response.failure(f"GET /catalogue did not return JSON: {exc}")
                return

            if not isinstance(catalogue, list) or not catalogue:
                response.failure("GET /catalogue returned an empty or invalid catalogue payload")
                return

            response.success()

        category_item = choice(catalogue)
        item_id = category_item["id"]

        self.client.get("/")
        self.client.get("/login", headers={"Authorization": f"Basic {base64string}"})
        self.client.get("/category.html")
        self.client.get("/detail.html?id={}".format(item_id))
        self.client.delete("/cart")
        self.client.post("/cart", json={"id": item_id, "quantity": 1})
        self.client.get("/basket.html")
        self.client.post("/orders")