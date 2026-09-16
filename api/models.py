from pydantic import BaseModel, Field


class CreatePaymentRequest(BaseModel):
    user_id: str = Field(
        ...,
        min_length=5,
        max_length=50,
        json_schema_extra={"example": "user_123"},
    )

    amount: int = Field(
        ...,
        gt=0,
        json_schema_extra={"example": 100},
    )