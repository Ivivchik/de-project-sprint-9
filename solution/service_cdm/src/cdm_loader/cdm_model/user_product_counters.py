from uuid import UUID

from cdm_loader.cdm_models.generic_model import CDMGenericModel

class UserProductCounters(CDMGenericModel):
    user_id: UUID
    product_id: UUID
    product_name: str
    order_cnt: int

    def get_table_name(self) -> str:
        return 'user_product_counters'
    
    def get_unique_keys(self) -> list[str]:
        return ['user_id', 'product_id']