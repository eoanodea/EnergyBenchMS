from locust import HttpUser, task

class SimpleWebUser(HttpUser):
    @task
    def index(self):
        self.client.get("/")