import pytest
from pydantic import ValidationError

from api.models import CreatePaymentRequest


def test_valid_payment_request():
    payment = CreatePaymentRequest(
        user_id="user_123",
        amount=100
    )

    assert payment.user_id == "user_123"
    assert payment.amount == 100

def test_payment_request_rejects_zero_amount():
    with pytest.raises(ValidationError):
        CreatePaymentRequest(
            user_id="user_123",
            amount=0
        )

def test_payment_request_rejects_negative_amount():
    with pytest.raises(ValidationError):
        CreatePaymentRequest(
            user_id="user_123",
            amount=-50
        )

def test_payment_request_rejects_short_user_id():
    with pytest.raises(ValidationError):
        CreatePaymentRequest(
            user_id="usr",
            amount=100
        )