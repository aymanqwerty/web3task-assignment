from pydantic import BaseModel, HttpUrl


class DispatchRequest(BaseModel):
    destinationUrl: HttpUrl
    eventId: str
    payload: dict
