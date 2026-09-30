import hashlib
import json
import logging
import os
import time
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, Header, HTTPException, Response
from psycopg2.extras import Json

from api.models import CreatePaymentRequest
from db.connection import get_connection
from utils.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI()

IDEMPOTENCY_RETENTION_HOURS = 24


@app.get("/")
def health_check():
    return {"message": "API is running"}


@app.post("/payments")
def create_payment(
    request: CreatePaymentRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),):

    user_id = request.user_id
    amount = request.amount

    canonical_request = json.dumps(
    request.model_dump(),
    sort_keys=True,
    separators=(",", ":"),
    )

    request_hash = hashlib.sha256(
        canonical_request.encode("utf-8")
    ).hexdigest()

    if not user_id.startswith("user_"):
        raise HTTPException(
            status_code=400,
            detail="Invalid user_id format. Must start with 'user_'."
        )

    conn = get_connection()
    cur = conn.cursor()

    try:

        cur.execute("""
            SELECT request_hash, response_body
            FROM idempotency_keys
            WHERE idempotency_key = %s
            AND expires_at > CURRENT_TIMESTAMP
            ORDER BY created_at ASC
            LIMIT 1
        """, (idempotency_key,))

        existing_record = cur.fetchone()

        if existing_record is not None:
            stored_request_hash, stored_response = existing_record

            if stored_request_hash != request_hash:
                raise HTTPException(
                    status_code=409,
                    detail="Idempotency-Key already used with a different request payload."
                )

            return Response(
                    content=stored_response,
                    media_type="application/json",
                )
        race_delay_ms = os.getenv("FAULT_INJECT_IDEMPOTENCY_RACE_DELAY_MS")

        if race_delay_ms is not None:
            time.sleep(int(race_delay_ms) / 1000)

        payment_id = str(uuid.uuid4())
        event_id = str(uuid.uuid4())

        event = {
            "event_id": event_id,
            "payment_id": payment_id,
            "user_id": user_id,
            "amount": amount,
            "event_type": "payment_created",
            "timestamp": time.time()
        }

        cur.execute("""
            INSERT INTO payments (payment_id, user_id, amount, status)
            VALUES (%s, %s, %s, %s)""", (payment_id, user_id, amount, "pending"))

        cur.execute("""
            INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
            VALUES (%s, %s, %s, %s)""", (event_id, payment_id, event["event_type"], Json(event),))

        response_body = json.dumps(
            {
                "payment_id": payment_id,
                "status": "pending",
            },
            separators=(",", ":"),
        )

        expires_at = datetime.now(UTC) + timedelta(
            hours=IDEMPOTENCY_RETENTION_HOURS
        )

        cur.execute("""
            INSERT INTO idempotency_keys (
                idempotency_key,
                request_hash,
                payment_id,
                response_body,
                expires_at
            )
            VALUES (%s, %s, %s, %s, %s)
        """, (
            idempotency_key,
            request_hash,
            payment_id,
            response_body,
            expires_at,
        ))

        conn.commit()

        if os.getenv("FAULT_INJECT_CRASH_AFTER_PAYMENT_COMMIT") == "true":
            os._exit(1)

        return  Response(
            content=response_body,
            media_type="application/json",
            )
            
    except HTTPException:
        conn.rollback()
        raise

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
