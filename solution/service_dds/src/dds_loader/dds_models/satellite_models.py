from uuid import UUID
from datetime import datetime

from dds_loader.dds_models.generic_model import DDSGenericModel

class SProductNames(DDSGenericModel):
    hk_product_names_pk: UUID
    h_product_pk: UUID
    name: str
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 's_product_names'
    
    def get_unique_keys(self) -> list[str]:
        return ['hk_product_names_pk']

class SRestaurantNames(DDSGenericModel):
    hk_restaurant_names_pk: UUID
    h_restaurant_pk: UUID
    name: str
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 's_restaurant_names'
    
    def get_unique_keys(self) -> list[str]:
        return ['hk_restaurant_names_pk']

class SUserNames(DDSGenericModel):
    hk_user_names_pk: UUID
    h_user_pk: UUID
    username: str
    userlogin: str
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 's_user_names'
    
    def get_unique_keys(self) -> list[str]:
        return ['hk_user_names_pk']

class SOrderCost(DDSGenericModel):
    hk_order_cost_pk: UUID
    h_order_pk: UUID
    cost: str
    payment: str
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 's_order_cost'
    
    def get_unique_keys(self) -> list[str]:
        return ['hk_order_cost_pk']

class SOrderStatus(DDSGenericModel):
    hk_order_status_pk: UUID
    h_order_pk: UUID
    status: str
    load_dt: datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_src: str = 'kafka'

    def get_table_name(self) -> str:
        return 's_order_status'
    
    def get_unique_keys(self) -> list[str]:
        return ['hk_order_status_pk']

__all__ = ['SProductNames', 'SRestaurantNames', 'SUserNames', 'SOrderCost', 'SOrderStatus']
