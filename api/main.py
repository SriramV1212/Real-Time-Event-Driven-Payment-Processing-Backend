import logging
import time
import uuid

from fastapi import FastAPI, HTTPException

from api.models import CreatePaymentRequest
from api.producer import produce_event
from db.connection import get_connection
from utils.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI()


@app.get("/")
def health_check():
    return {"message": "API is running"}


@app.post("/payments")
def create_payment(request: CreatePaymentRequest):
    user_id = request.user_id
    amount = request.amount

    if not user_id.startswith("user_"):
        raise HTTPException(
            status_code=400,
            detail="Invalid user_id format. Must start with 'user_'."
        )

    conn = get_connection()
    cur = conn.cursor()

    try:
        payment_id = str(uuid.uuid4())
        event_id = str(uuid.uuid4())

        cur.execute("""
            INSERT INTO payments (payment_id, user_id, amount, status)
            VALUES (%s, %s, %s, %s)
        """, (payment_id, user_id, amount, "pending"))

        conn.commit()

        event = {
            "event_id": event_id,
            "payment_id": payment_id,
            "user_id": user_id,
            "amount": amount,
            "event_type": "payment_created",
            "timestamp": time.time()
        }

        produce_event(event)

        return {
            "payment_id": payment_id,
            "status": "pending"
        }

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to create payment for user %s", user_id)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()


@app.get("/payments/{payment_id}")
def get_payment_status(payment_id: str):
    logger.info("Fetching payment status for %s", payment_id)

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT payment_id, user_id, amount, status, created_at
            FROM payments
            WHERE payment_id = %s
        """, (payment_id,))

        result = cur.fetchone()

        if result is None:
            raise HTTPException(
                status_code=404,
                detail="Payment not found"
            )

        payment = {
            "payment_id": result[0],
            "user_id": result[1],
            "amount": result[2],
            "status": result[3],
            "created_at": result[4]
        }

        return payment

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to fetch payment status for %s", payment_id)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()
