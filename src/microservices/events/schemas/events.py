from typing import Any, Dict, Optional

from pydantic import BaseModel, EmailStr, Field


class Event(BaseModel):
    id: str = Field(..., description="Уникальный идентификатор события")
    type: str = Field(..., description="Тип события")
    timestamp: str = Field(..., description="Время события в ISO-8601")
    payload: Dict[str, Any] = Field(default_factory=dict)


class EventResponse(BaseModel):
    status: str = "success"
    partition: int
    offset: int
    event: Event


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[list[str]] = None
    description: Optional[str] = None


class UserEvent(BaseModel):
    user_id: int
    action: str
    timestamp: str
    username: Optional[str] = None
    email: Optional[EmailStr] = None


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: str
    method_type: Optional[str] = None
