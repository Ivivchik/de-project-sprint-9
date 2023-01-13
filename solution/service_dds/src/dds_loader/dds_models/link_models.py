from datetime import datetime
from pydantic import BaseModel
from uuid import UUID


class LOrderProduct(BaseModel):
    hk_order_product_pk: UUID
    h_order_pk: UUID
    h_product_pk: UUID
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src: str = "kafka"

    def get_table_name() -> str:
        return 'l_order_product'

    def get_unique_keys() -> list[str]:
        return ['hk_order_product_pk']

class LProductRestaurant(BaseModel):
    hk_product_restaurant_pk: UUID
    h_restaurant_pk: UUID
    h_product_pk: UUID
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src: str = "kafka"

    def get_table_name() -> str:
        return 'l_product_restaurant'

    def get_unique_keys() -> list[str]:
        return ['hk_product_restaurant_pk']

class LProductCategory(BaseModel):
    hk_product_category_pk: UUID
    h_category_pk: UUID
    h_product_pk: UUID
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src: str = "kafka"

    def get_table_name() -> str:
        return 'l_product_category'

    def get_unique_keys() -> list[str]:
        return ['hk_product_category_pk']

class LOrderUser(BaseModel):
    hk_order_user_pk: UUID
    h_order_pk: UUID
    h_user_pk: UUID
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src = "kafka"

    def get_table_name() -> str:
        return 'l_order_user'

    def get_unique_keys() -> list[str]:
        return ['hk_order_user_pk']

