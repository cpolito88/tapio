"""Message models shared across the unit tests."""

from pydantic import BaseModel

from tapio import Message
from tapio.actor import ActorRef


class Greeted(Message):
    whom: str


class Greet(Message):
    whom: str
    count: int
    reply_to: ActorRef[Greeted]


class Increment(Message):
    by: int = 1


class GetCount(Message):
    reply_to: ActorRef[Greeted]


class NotAMessage(BaseModel):
    """A plain BaseModel, to prove the delivery-time guarantee is not silent."""

    n: int
