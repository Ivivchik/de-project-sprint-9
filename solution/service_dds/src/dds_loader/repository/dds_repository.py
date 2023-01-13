from lib.pg import PgConnect
from dds_models.hub_models import *
from dds_models.link_models import *

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def create_pattern_insert(table_name: str, 
                              lst_name: list[str],
                              unique_keys: list[str]) -> str:

        column_name_str = ', '.join(lst_name)
        values_name_str = ', '.join([f"%({name})s" for name in lst_name])
        update_name_str = ', '.join([f"{name}=excluded.{name}" for name in lst_name])
        unique_name_str = ', '.join(unique_keys)

        inser_into = f"""INSERT INTO dds.{table_name}({column_name_str})"""
        values_into = f"""VALUES({values_name_str})"""
        on_coflict_into = f"""ON CONFLICT ({update_name_str})"""
        do_update = f"""DO UPDATE SET {unique_name_str};"""
        return '\n'.join([inser_into, values_into, on_coflict_into, do_update])


    def h_order_insert(self, h_order: HOrder) -> None:

        table_name = h_order.get_table_name()
        list_column = list(h_order.__fields__.keys())
        unique_keys = h_order.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query ,h_order.dict())

    def h_restaurant_insert(self, h_restaurant: HRestaurant) -> None:

        table_name = h_restaurant.get_table_name()
        list_column = list(h_restaurant.__fields__.keys())
        unique_keys = h_restaurant.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, h_restaurant.dict())

    def h_category_insert(self, h_category: HCategory) -> None:

        table_name = h_category.get_table_name()
        list_column = list(h_category.__fields__.keys())
        unique_keys = h_category.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, h_category.dict())

    def h_product_insert(self, h_product: HProduct) -> None:

        table_name = h_product.get_table_name()
        list_column = list(h_product.__fields__.keys())
        unique_keys = h_product.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, h_product.dict())

    def h_user_insert(self, h_user: HUser) -> None:

        table_name = h_user.get_table_name()
        list_column = list(h_user.__fields__.keys())
        unique_keys = h_user.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, h_user.dict())

    def l_order_product_insert(self, l_order_product: LOrderProduct) -> None:

        table_name = l_order_product.get_table_name()
        list_column = list(l_order_product.__fields__.keys())
        unique_keys = l_order_product.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, l_order_product.dict())

    def l_product_restaurant_insert(self, l_product_restaurant: LProductRestaurant) -> None:

        table_name = l_product_restaurant.get_table_name()
        list_column = list(l_product_restaurant.__fields__.keys())
        unique_keys = l_product_restaurant.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, l_product_restaurant.dict())

    def l_product_category_insert(self, l_product_category: LProductCategory) -> None:

        table_name = l_product_category.get_table_name()
        list_column = list(l_product_category.__fields__.keys())
        unique_keys = l_product_category.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, l_product_category.dict())

    def l_order_user_insert(self, l_order_user: LOrderUser) -> None:

        table_name = l_order_user.get_table_name()
        list_column = list(l_order_user.__fields__.keys())
        unique_keys = l_order_user.get_unique_keys()
        query = self.create_pattern_insert(table_name, list_column, unique_keys)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, l_order_user.dict())

