from pydantic import BaseModel, Field

class CreatePaymentRequest(BaseModel):
    user_id: str = Field(..., min_length=5, max_length=50, example="user_123")
    amount: int = Field(..., gt=0, example=100)