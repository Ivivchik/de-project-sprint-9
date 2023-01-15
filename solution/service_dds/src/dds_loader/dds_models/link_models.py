from uuid import UUID
from datetime import datetime

from dds_loader.dds_models.generic_model import DDSGenericModel

class LOrderProduct(DDSGenericModel):
    hk_order_product_pk: UUID
    h_order_pk: UUID
    h_product_pk: UUID
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src: str = "kafka"

    def get_table_name(self) -> str:
        return 'l_order_product'

    def get_unique_keys(self) -> list[str]:
        return ['hk_order_product_pk']

class LProductRestaurant(DDSGenericModel):
    hk_product_restaurant_pk: UUID
    h_restaurant_pk: UUID
    h_product_pk: UUID
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src: str = "kafka"

    def get_table_name(self) -> str:
        return 'l_product_restaurant'

    def get_unique_keys(self) -> list[str]:
        return ['hk_product_restaurant_pk']

class LProductCategory(DDSGenericModel):
    hk_product_category_pk: UUID
    h_category_pk: UUID
    h_product_pk: UUID
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src: str = "kafka"

    def get_table_name(self) -> str:
        return 'l_product_category'

    def get_unique_keys(self) -> list[str]:
        return ['hk_product_category_pk']

class LOrderUser(DDSGenericModel):
    hk_order_user_pk: UUID
    h_order_pk: UUID
    h_user_pk: UUID
    load_dt: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_src = "kafka"

    def get_table_name(self) -> str:
        return 'l_order_user'

    def get_unique_keys(self) -> list[str]:
        return ['hk_order_user_pk']

__all__ = ['LOrderProduct', 'LProductRestaurant', 'LProductCategory', 'LOrderUser']
