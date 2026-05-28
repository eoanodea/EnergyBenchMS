from locust import HttpUser, task, between
import random

class SocialNetworkUser(HttpUser):
    wait_time = between(1, 3)

    @task(3)
    def load_home(self):
        self.client.get("/")

    # @task(2)
    # def compose_post(self):
    #     user_id = random.randint(1, 100)
    #     username = f"user_{user_id}"

    #     data = {
    #         "username": username,
    #         "user_id": user_id,
    #         "text": "hello from locust"
    #     }

    #     self.client.post(
    #         "/wrk2-api/post/compose",
    #         json={
    #             "user_id": user_id,
    #             "username": username,
    #             "text": "hello from locust",
    #             "media_ids": [],
    #             "media_types": [],
    #             "post_type": 0,
    #             "req_id": str(random.randint(1, 100000000))
    #         }
