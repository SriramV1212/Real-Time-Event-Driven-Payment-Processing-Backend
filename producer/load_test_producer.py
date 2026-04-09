import logging
import os
import random
import time

import requests

from utils.logging_config import configure_logging

API_BASE_URL = os.getenv("PAYMENT_API_BASE_URL", "http://127.0.0.1:8000")
PAYMENTS_ENDPOINT = f"{API_BASE_URL}/payments"

logger = logging.getLogger(__name__)

users = ["user_1", "user_2", "user_3", "user_4", "user_5"]


def generate_payment_request():
    user_id = random.choice(users)
    payload = {
        "user_id": user_id,
        "amount": random.randint(10, 500),
    }

    return payload


def create_payment(payload):
    response = requests.post(PAYMENTS_ENDPOINT, json=payload, timeout=5)
    response.raise_for_status()
    return response.json()


def main():
    configure_logging()

    start_time = time.time()
    success_count = 0
    try:
        for _ in range(1000):
            payload = generate_payment_request()

            try:
                result = create_payment(payload)
                success_count += 1
                logger.info(
                    "Created payment %s for user %s with status %s",
                    result.get("payment_id"),
                    payload["user_id"],
                    result.get("status"),
                )
            except requests.HTTPError as exc:
                details = exc.response.text if exc.response is not None else str(exc)
                logger.error(
                    "API rejected payment for user %s with status %s: %s",
                    payload["user_id"],
                    exc.response.status_code if exc.response is not None else "unknown",
                    details,
                )
            except requests.RequestException as exc:
                logger.error("Failed to reach %s: %s", PAYMENTS_ENDPOINT, exc)
                break

            time.sleep(0.001)

        logger.info(
            "Created %s payments via API in %.2fs",
            success_count,
            time.time() - start_time,
        )

    except KeyboardInterrupt:
        logger.info("Stopping load test...")


if __name__ == "__main__":
    main()
