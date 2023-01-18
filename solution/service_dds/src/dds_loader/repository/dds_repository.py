from uuid import UUID
from lib.pg import PgConnect

from dds_loader.dds_models.generic_model import DDSGenericModel

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def _create_pattern_insert(self,
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

    def insert(self, model: DDSGenericModel) -> None:

        table_name = model.get_table_name()
        list_column = list(model.__fields__.keys())
        unique_keys = model.get_unique_keys()
        query = self._create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query ,model.dict())

    def get_grouped_data(self, user_id: UUID, column_lst: list[str]) -> list:

        column_names = ', '.join(column_lst)
        cte_query = """WITH t AS 
                    (SELECT lou.h_user_pk,
                            hp.h_product_pk,
                            hc.h_category_pk,
                            spn."name",
                            hc.category_name
                       FROM dds.h_product hp 
                       JOIN dds.s_product_names spn ON hp.h_product_pk = spn.h_product_pk
                       JOIN dds.l_product_category lpc ON lpc.h_product_pk = hp.h_product_pk 
                       JOIN dds.h_category hc ON lpc.h_category_pk = hc.h_category_pk
                       JOIN dds.l_order_product lop ON lop.h_product_pk = hp.h_product_pk
                       JOIN dds.h_order ho ON ho.h_order_pk = lop.h_order_pk
                       JOIN dds.l_order_user lou ON lou.h_order_pk = ho.h_order_pk)"""

        grouped_query = f"""SELECT h_user_pk, {column_names}, COUNT(*) FROM t
                             WHERE h_user_pk = '{user_id}'
                          GROUP BY h_user_pk, {column_names};"""
        
        query = '\n'.join([cte_query, grouped_query])

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()

        return result