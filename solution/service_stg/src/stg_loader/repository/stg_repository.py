from lib.pg import PgConnect

from stg_loader.stg_models.order_event_model import STGGenericModel

class StgRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def __create_pattern_insert(self,
                              table_name: str, 
                              lst_name: list[str],
                              unique_keys: list[str]) -> str:

        column_name_str = ', '.join(lst_name)
        values_name_str = ', '.join([f"%({name})s" for name in lst_name])
        update_name_str = ', '.join([f"{name}=excluded.{name}" for name in lst_name])
        unique_name_str = ', '.join(unique_keys)

        inser_into = f"""INSERT INTO dds.{table_name}({column_name_str})"""
        values_into = f"""VALUES({values_name_str})"""
        on_coflict_into = f"""ON CONFLICT ({unique_name_str})"""
        do_update = f"""DO UPDATE SET {update_name_str};"""
        return '\n'.join([inser_into, values_into, on_coflict_into, do_update])

    def insert(self, model: STGGenericModel) -> None:

        table_name = model.get_table_name()
        list_column = list(model.__fields__.keys())
        unique_keys = model.get_unique_keys()
        query = self.__create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query ,model.dict())