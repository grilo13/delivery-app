from typing import Union
from pydantic import BaseModel
from datetime import datetime


class Order(BaseModel):
    name: str
    quantity: int
    status: str
    order_id: Union[str, None] = None
    request_date: Union[datetime, None] = None
    update_date: Union[datetime, None] = None
