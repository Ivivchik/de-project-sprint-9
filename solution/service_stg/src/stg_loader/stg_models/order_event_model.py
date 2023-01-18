from datetime import datetime

from stg_loader.stg_models.generic_model import STGGenericModel

class StgOrderEvents(STGGenericModel):
    object_id: int
    payload: str
    object_type: str
    sent_dttm: datetime

    def get_table_name(self) -> str:
        return 'order_events'
    
    def get_unique_keys(self) -> list[str]:
        return ['object_id']