"""The base class every tapio message must subclass."""

from pydantic import BaseModel, ConfigDict

__all__ = ["Message"]


class Message(BaseModel):
    """Base class for messages: frozen, and re-validated on every delivery.

    Subclassing this rather than `BaseModel` is a real constraint on user
    code, and it is what makes the delivery-time guarantee work. Pydantic
    defaults `revalidate_instances` to `"never"`, so re-validating a plain
    `BaseModel` instance returns it untouched and re-checks no field. A
    library that advertised validation on delivery while inheriting that
    default would be shipping a no-op.

    `frozen=True` is the other half. A message that has been sent is shared
    with its recipient, and changing it afterwards would be a data race that
    no amount of validation could catch.

    Example:
        ```python
        class Greet(Message):
            whom: str
            reply_to: ActorRef["Greeted"]
        ```
    """

    model_config = ConfigDict(frozen=True, revalidate_instances="always")
