from locust import HttpUser, task, between


class TrainTicketUser(HttpUser):
    wait_time = between(1, 3)

    @task(3)
    def homepage(self):
        self.client.get("/")

    # @task(2)
    # def routes(self):
    #     self.client.get("/api/v1/routes")

    # @task(1)
    # def trips(self):
    #     self.client.get("/api/v1/travelservice/trips")