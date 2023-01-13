from datetime import datetime
from pydantic import BaseModel, validator
from uuid import UUID, uuid5, NAMESPACE_X500


class SProductNames(BaseModel):
    hk_product_names_pk: UUID
    h_product_pk: UUID
    name: str
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src = "kafka"

class SRestaurantNames(BaseModel):
    hk_restaurant_names_pk: UUID
    h_restaurant_pk: UUID
    name: str
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src = "kafka"

class SUserNames(BaseModel):
    hk_user_names_pk: UUID
    h_user_pk: UUID
    username: str
    userlogin: str
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src = "kafka"

class SOrderCost(BaseModel):
    hk_order_cost_pk: UUID
    h_order_pk: UUID
    cost: str
    payment: str
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src = "kafka"

class SOrderStatus(BaseModel):
    hk_order_status_pk: UUID
    h_order_pk: UUID
    status: str
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src = "kafka"