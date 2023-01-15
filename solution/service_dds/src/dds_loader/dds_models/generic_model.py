from pydantic import BaseModel

class DDSGenericModel(BaseModel):

    def get_table_name(self) -> str:
        pass
    
    def get_unique_keys(self) -> list[str]:
        pass