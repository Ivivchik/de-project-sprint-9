from uuid import UUID

from cdm_loader.cdm_models.generic_model import CDMGenericModel

class UserCategoryCounters(CDMGenericModel):
    user_id: UUID
    category_id: UUID
    category_name: str
    order_cnt: int

    def get_table_name(self) -> str:
        return 'user_category_counters'
    
    def get_unique_keys(self) -> list[str]:
        return ['user_id', 'category_id']