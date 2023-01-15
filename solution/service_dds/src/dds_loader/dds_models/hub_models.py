from uuid import UUID
from datetime import datetime

from dds_loader.dds_models.generic_model import DDSGenericModel

class HProduct(DDSGenericModel):
    product_id: str
    h_product_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src = 'kafka'

    def get_table_name(self) -> str:
        return 'h_product'
    
    def get_unique_keys(self) -> list[str]:
        return ['h_product_pk']

class HOrder(DDSGenericModel):
    order_id: int
    order_dt: datetime
    h_order_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 'h_order'

    def get_unique_keys(self) -> list[str]:
        return ['h_order_pk']

class HCategory(DDSGenericModel):
    category_name: str
    h_category_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 'h_category'

    def get_unique_keys(self) -> list[str]:
        return ['h_category_pk']

class HRestaurant(DDSGenericModel):
    restaurant_id: str
    h_restaurant_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 'h_restaurant'

    def get_unique_keys(self) -> list[str]:
        return ['h_restaurant_pk']

class HUser(DDSGenericModel):
    user_id: str
    h_user_pk: UUID
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 'h_user'

    def get_unique_keys(self) -> list[str]:
        return ['h_user_pk']

__all__ = ['HProduct', 'HOrder', 'HCategory', 'HRestaurant', 'HUser']
