from datetime import datetime
from pydantic import BaseModel
from uuid import UUID

class HProduct(BaseModel):
    product_id: str
    h_product_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src = 'kafka'

    def get_table_name() -> str:
        return 'h_product'
    
    def get_unique_keys() -> list[str]:
        return ['h_product_pk']

class HOrder(BaseModel):
    order_id: int
    order_dt: datetime
    h_order_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name() -> str:
        return 'h_order'

    def get_unique_keys() -> list[str]:
        return ['h_order_pk']

class HCategory(BaseModel):
    category_name: str
    h_category_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name() -> str:
        return 'h_category'

    def get_unique_keys() -> list[str]:
        return ['h_category_pk']

class HRestaurant(BaseModel):
    restaurant_id: str
    h_restaurant_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name() -> str:
        return 'h_restaurant'

    def get_unique_keys() -> list[str]:
        return ['h_restaurant_pk']

class HUser(BaseModel):
    user_id: str
    h_user_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name() -> str:
        return 'h_user'

    def get_unique_keys() -> list[str]:
        return ['h_user_pk']

__all__ = [HProduct, HOrder, HCategory, HRestaurant, HUser]