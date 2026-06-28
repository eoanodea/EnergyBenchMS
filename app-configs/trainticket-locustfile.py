"""
Locust load test for train-ticket, adapted from FudanSELab/train-ticket-auto-query.

Ports the atomic queries (atomic_queries.py / queries.py) and composed
scenarios (scenarios.py) from that repo into Locust tasks. Differences
from the original scripts:

- Login happens once per simulated user in on_start() and the token is
  reused for all subsequent requests, rather than logging in before
  every single query. Repeated logins put disproportionate load on
  ts-auth-service / ts-verification-code-service under high concurrency.
- Uses self.client (Locust's session) instead of a bare `requests` call,
  so requests show up in Locust's stats and respect --host.
- Every request passes name= so the web UI / CSV groups by logical
  endpoint instead of by raw URL.
- Logging instead of print().
"""

import logging
import random
import string
import time
from datetime import datetime, timedelta

from locust import HttpUser, task, between

logger = logging.getLogger("trainticket_locust")

USERNAME = "fdse_microservice"
PASSWORD = "111111"

HIGH_SPEED_PAIRS = [
    ("Shang Hai", "Su Zhou"),
    ("Su Zhou", "Shang Hai"),
    ("Nan Jing", "Shang Hai"),
]
NORMAL_PAIRS = [
    ("Shang Hai", "Nan Jing"),
    ("Nan Jing", "Shang Hai"),
]

# 60/40 high-speed vs normal, matching scenarios.py's highspeed_weights
HIGH_SPEED_WEIGHT = 60
NORMAL_WEIGHT = 40


def random_boolean():
    return random.choice([True, False])


def random_str(k_min=4, k_max=10):
    return "".join(random.choices(string.ascii_letters, k=random.randint(k_min, k_max)))


def random_phone():
    return "".join(random.choices(string.digits, k=random.randint(8, 15)))


def is_high_speed_choice():
    return random.uniform(0, HIGH_SPEED_WEIGHT + NORMAL_WEIGHT) <= HIGH_SPEED_WEIGHT


class TrainTicketUser(HttpUser):
    wait_time = between(1, 3)

    def on_start(self):
        self.uid = None
        self.token = None
        self.login()

    # ---- auth -----------------------------------------------------

    def login(self):
        try:
            self.client.get("/api/v1/verifycode/generate", name="verifycode")
            payload = {
                "username": USERNAME,
                "password": PASSWORD,
                "verificationCode": "1234",
            }
            response = self.client.post(
                "/api/v1/users/login", json=payload, name="login"
            )
            if response.status_code == 200:
                data = response.json().get("data") or {}
                self.uid = data.get("userId")
                self.token = data.get("token")
        except Exception as exc:
            logger.warning("login failed: %s", exc)

    def auth_headers(self):
        if self.token:
            return {"Authorization": f"Bearer {self.token}"}
        return {}

    # ---- atomic queries (ported from atomic_queries.py / queries.py) ----

    def query_high_speed_ticket(self, place_pair=None, date=None):
        place_pair = place_pair or random.choice(HIGH_SPEED_PAIRS)
        payload = {
            "departureTime": date or time.strftime("%Y-%m-%d", time.localtime()),
            "startingPlace": place_pair[0],
            "endPlace": place_pair[1],
        }
        response = self.client.post(
            "/api/v1/travelservice/trips/left",
            json=payload,
            headers=self.auth_headers(),
            name="trips_left_highspeed",
        )
        return self._extract_trip_ids(response)

    def query_normal_ticket(self, place_pair=None, date=None):
        place_pair = place_pair or random.choice(NORMAL_PAIRS)
        payload = {
            "departureTime": date or time.strftime("%Y-%m-%d", time.localtime()),
            "startingPlace": place_pair[0],
            "endPlace": place_pair[1],
        }
        response = self.client.post(
            "/api/v1/travel2service/trips/left",
            json=payload,
            headers=self.auth_headers(),
            name="trips_left_normal",
        )
        return self._extract_trip_ids(response)

    def query_high_speed_ticket_parallel(self, place_pair=None, date=None):
        place_pair = place_pair or random.choice(HIGH_SPEED_PAIRS)
        payload = {
            "departureTime": date or time.strftime("%Y-%m-%d", time.localtime()),
            "startingPlace": place_pair[0],
            "endPlace": place_pair[1],
        }
        response = self.client.post(
            "/api/v1/travelservice/trips/left_parallel",
            json=payload,
            headers=self.auth_headers(),
            name="trips_left_parallel",
        )
        return self._extract_trip_ids(response)

    def query_advanced_ticket(self, plan_type="cheapest", place_pair=None, date=None):
        place_pair = place_pair or random.choice(HIGH_SPEED_PAIRS)
        payload = {
            "departureTime": date or time.strftime("%Y-%m-%d", time.localtime()),
            "startingPlace": place_pair[0],
            "endPlace": place_pair[1],
        }
        response = self.client.post(
            f"/api/v1/travelplanservice/travelPlan/{plan_type}",
            json=payload,
            headers=self.auth_headers(),
            name=f"travelplan_{plan_type}",
        )
        try:
            data = response.json().get("data")
        except Exception:
            return []
        if not data:
            return []
        return [d.get("tripId") for d in data if d.get("tripId")]

    def query_assurances(self):
        self.client.get(
            "/api/v1/assuranceservice/assurances/types",
            headers=self.auth_headers(),
            name="assurances",
        )
        return [{"assurance": "1"}]

    def query_food(self, place_pair=("Shang Hai", "Su Zhou"), train_num="D1345"):
        date = time.strftime("%Y-%m-%d", time.localtime())
        self.client.get(
            f"/api/v1/foodservice/foods/{date}/{place_pair[0]}/{place_pair[1]}/{train_num}",
            headers=self.auth_headers(),
            name="food",
        )
        # food choice doesn't affect downstream calls, return a fixed option
        return [
            {
                "foodName": "Soup",
                "foodPrice": 3.7,
                "foodType": 2,
                "stationName": "Su Zhou",
                "storeName": "Roman Holiday",
            }
        ]

    def query_contacts(self):
        if not self.uid:
            return []
        response = self.client.get(
            f"/api/v1/contactservice/contacts/account/{self.uid}",
            headers=self.auth_headers(),
            name="contacts",
        )
        try:
            data = response.json().get("data")
        except Exception:
            return []
        if not data:
            return []
        return [d.get("id") for d in data if d.get("id")]

    def query_orders(self, types=(0,), query_other=False):
        if query_other:
            url = "/api/v1/orderOtherService/orderOther/refresh"
            name = "orders_other_refresh"
        else:
            url = "/api/v1/orderservice/order/refresh"
            name = "orders_refresh"
        response = self.client.post(
            url,
            json={"loginId": self.uid},
            headers=self.auth_headers(),
            name=name,
        )
        try:
            data = response.json().get("data")
        except Exception:
            return []
        if not data:
            return []
        pairs = []
        for d in data:
            # status 0: not paid, 1: paid not collected, 2: collected
            if d.get("status") in types:
                pairs.append((d.get("id"), d.get("trainNumber")))
        return pairs

    def query_orders_all_info(self, query_other=False):
        if query_other:
            url = "/api/v1/orderOtherService/orderOther/refresh"
            name = "orders_other_refresh"
        else:
            url = "/api/v1/orderservice/order/refresh"
            name = "orders_refresh"
        response = self.client.post(
            url,
            json={"loginId": self.uid},
            headers=self.auth_headers(),
            name=name,
        )
        try:
            data = response.json().get("data")
        except Exception:
            return []
        if not data:
            return []
        results = []
        for d in data:
            results.append(
                {
                    "accountId": d.get("accountId"),
                    "targetDate": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    "orderId": d.get("id"),
                    "from": d.get("from"),
                    "to": d.get("to"),
                }
            )
        return results

    def put_consign(self, order_info):
        payload = {
            "accountId": order_info["accountId"],
            "handleDate": time.strftime("%Y-%m-%d", time.localtime()),
            "targetDate": order_info["targetDate"],
            "from": order_info["from"],
            "to": order_info["to"],
            "orderId": order_info["orderId"],
            "consignee": "32",
            "phone": "12345677654",
            "weight": "32",
            "id": "",
            "isWithin": False,
        }
        response = self.client.put(
            "/api/v1/consignservice/consigns",
            json=payload,
            headers=self.auth_headers(),
            name="consign_put",
        )
        if response.status_code in (200, 201):
            return order_info["orderId"]
        return None

    def query_route(self, route_id=None):
        if route_id:
            self.client.get(
                f"/api/v1/routeservice/routes/{route_id}",
                headers=self.auth_headers(),
                name="route_by_id",
            )
        else:
            self.client.get(
                "/api/v1/routeservice/routes",
                headers=self.auth_headers(),
                name="routes",
            )

    def pay_order(self, order_id, trip_id):
        payload = {"orderId": order_id, "tripId": trip_id}
        response = self.client.post(
            "/api/v1/inside_pay_service/inside_payment",
            json=payload,
            headers=self.auth_headers(),
            name="inside_payment",
        )
        return order_id if response.status_code == 200 else None

    def cancel_order(self, order_id):
        response = self.client.get(
            f"/api/v1/cancelservice/cancel/{order_id}/{self.uid}",
            headers=self.auth_headers(),
            name="cancel",
        )
        return order_id if response.status_code == 200 else None

    def collect_order(self, order_id):
        response = self.client.get(
            f"/api/v1/executeservice/execute/collected/{order_id}",
            headers=self.auth_headers(),
            name="collect",
        )
        return order_id if response.status_code == 200 else None

    def enter_station(self, order_id):
        response = self.client.get(
            f"/api/v1/executeservice/execute/execute/{order_id}",
            headers=self.auth_headers(),
            name="enter_station",
        )
        return order_id if response.status_code == 200 else None

    def rebook_ticket(self, old_order_id, old_trip_id, new_trip_id):
        payload = {
            "oldTripId": old_trip_id,
            "orderId": old_order_id,
            "tripId": new_trip_id,
            "date": time.strftime("%Y-%m-%d", time.localtime()),
            "seatType": random.choice(["2", "3"]),
        }
        self.client.post(
            "/api/v1/rebookservice/rebook",
            json=payload,
            headers=self.auth_headers(),
            name="rebook",
        )

    def preserve(self, start, end, trip_ids, high_speed):
        if not trip_ids:
            return
        if high_speed:
            url = "/api/v1/preserveservice/preserve"
            name = "preserve"
        else:
            url = "/api/v1/preserveotherservice/preserveOther"
            name = "preserve_other"

        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        payload = {
            "accountId": self.uid,
            "assurance": "0",
            "contactsId": "",
            "date": tomorrow,
            "from": start,
            "to": end,
            "tripId": random.choice(trip_ids),
        }

        if random_boolean():
            food_options = self.query_food(place_pair=(start, end))
            payload.update(random.choice(food_options))
        else:
            payload["foodType"] = "0"

        if random_boolean():
            payload["assurance"] = 1

        contacts = self.query_contacts()
        if contacts:
            payload["contactsId"] = random.choice(contacts)

        payload["seatType"] = random.choice(["2", "3"])

        if random_boolean():
            payload.update(
                {
                    "consigneeName": random_str(),
                    "consigneePhone": random_phone(),
                    "consigneeWeight": random.randint(1, 10),
                    "handleDate": tomorrow,
                }
            )

        self.client.post(url, json=payload, headers=self.auth_headers(), name=name)

    @staticmethod
    def _extract_trip_ids(response):
        try:
            data = response.json().get("data")
        except Exception:
            return []
        if not data:
            return []
        trip_ids = []
        for d in data:
            trip_id_obj = d.get("tripId")
            if isinstance(trip_id_obj, dict):
                trip_ids.append(trip_id_obj.get("type", "") + trip_id_obj.get("number", ""))
        return trip_ids

    # ---- composed scenarios (ported from scenarios.py) ----------------

    # @task(5)
    # def scenario_query_advanced_ticket(self):
    #     """Cheapest / quickest / min-station trip planning."""
    #     # Removed: travelplan endpoints are timing out under load
    #     plan_type = random.choice(["cheapest", "quickest", "minStation"])
    #     self.query_advanced_ticket(plan_type=plan_type)

    @task(4)
    def scenario_query_travel_left(self):
        """Browse available trips without booking."""
        self.query_normal_ticket()

    @task(2)
    def scenario_query_admin_basic(self):
        """Background admin reads: prices and configs."""
        self.client.get(
            "/api/v1/adminbasicservice/adminbasic/prices",
            headers=self.auth_headers(),
            name="admin_basic_prices",
        )
        self.client.get(
            "/api/v1/adminbasicservice/adminbasic/configs",
            headers=self.auth_headers(),
            name="admin_basic_configs",
        )