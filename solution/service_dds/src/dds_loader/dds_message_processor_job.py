from logging import Logger
from datetime import datetime
from uuid import UUID, uuid5, NAMESPACE_X500

from dds_loader.dds_models.hub_models import *
from dds_loader.dds_models.link_models import *
from dds_loader.dds_models.satellite_models import *
from lib.kafka_connect.kafka_connectors import KafkaConsumer
from lib.kafka_connect.kafka_connectors import KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:

        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = 30

    def __create_output_message(self, user_id: UUID, lst_products: list, lst_categories: list) -> dict:

        products = [{'id': item[1], 'name': item[2], 'cnt': item[3]} for item in lst_products]
        categories = [{'id': item[1], 'name': item[2], 'cnt': item[3]} for item in lst_categories]

        return {'user_id': user_id, 'products': products, 'categories': categories}

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        message = self._consumer.consume()

        payload = message['payload']
        products = payload['products']
        
        user = payload['user']
        restaurant = payload['restaurant']
        
        # filed for dds.h_order
        order_dt = payload['date']
        order_id = message['object_id']
        h_order_pk = uuid5(NAMESPACE_X500, str(order_id))

        # filed for dds.s_order_cost
        cost = payload['cost']
        payment = payload['payment']
        hk_order_cost_pk = uuid5(NAMESPACE_X500, str(order_id) + str(cost))

        # filed for dds.s_order_status
        status = payload['status']
        hk_order_status_pk = uuid5(NAMESPACE_X500, str(order_id) + status)
        
        # filed for dds.h_restaurant
        restaurant_id = restaurant['id']
        h_restaurant_pk = uuid5(NAMESPACE_X500, restaurant_id)

        # filed for dds.s_restaurant_names
        restaurant_name = restaurant['name']
        hk_restaurant_names_pk = uuid5(NAMESPACE_X500, restaurant_id + restaurant_name)
        
        # filed for dds.h_user
        user_id = user['id']
        h_user_pk = uuid5(NAMESPACE_X500, user_id)

        # filed for dds.s_user_names
        user_name = user['name']
        user_login = user['login']
        hk_user_names_pk = uuid5(NAMESPACE_X500, user_id + user_name + user_login)

        # filed for dds.l_user_order
        hk_order_user_pk = uuid5(NAMESPACE_X500, str(order_id) + user_id)

        h_order = HOrder(order_id=order_id,
                         order_dt=order_dt,
                         h_order_pk=h_order_pk)
        
        s_order_cost = SOrderCost(cost=cost,
                                  payment=payment,
                                  h_order_pk=h_order_pk,
                                  hk_order_cost_pk=hk_order_cost_pk)

        s_order_status = SOrderStatus(status=status,
                                      h_order_pk=h_order_pk,
                                      hk_order_status_pk=hk_order_status_pk)

        h_restaurant = HRestaurant(restaurant_id=restaurant_id,
                                   h_restaurant_pk=h_restaurant_pk)

        s_restaurant_names = SRestaurantNames(name=restaurant_name,
                                              h_restaurant_pk=h_restaurant_pk,
                                              hk_restaurant_names_pk=hk_restaurant_names_pk)

        h_user = HUser(user_id=user_id, h_user_pk=h_user_pk)

        s_user_names = SUserNames(username=user_name,
                                  h_user_pk=h_user_pk,
                                  userlogin=user_login,
                                  hk_user_names_pk=hk_user_names_pk)

        l_user_order = LOrderUser(h_user_pk=h_user_pk,
                                  h_order_pk=h_order_pk,
                                  hk_order_user_pk=hk_order_user_pk)

        for item in products:
            # filed for dds.h_product
            product_id = item['id']
            h_product_pk = uuid5(NAMESPACE_X500, product_id)

            # filed for dds.s_product_names
            product_name = item['name']
            hk_product_names_pk = uuid5(NAMESPACE_X500, product_id + product_name)

            # filed for dds.h_category
            category_name = item['category']
            h_category_pk = uuid5(NAMESPACE_X500, category_name)

            # filed for dds.l_product_category
            hk_product_category_pk = uuid5(NAMESPACE_X500, product_id + category_name)

            # filed for dds.l_product_restaurant
            hk_product_restaurant_pk = uuid5(NAMESPACE_X500, product_id + restaurant_id)

            # filed for dds.l_order_product
            hk_order_product_pk = uuid5(NAMESPACE_X500, str(order_id) + product_id)

            h_product = HProduct(product_id=product_id,
                                 h_product_pk=h_product_pk)

            s_poduct_names = SProductNames(name=product_name,
                                           h_product_pk=h_product_pk,
                                           hk_product_names_pk=hk_product_names_pk)

            h_category = HCategory(h_category_pk=h_category_pk,
                                   category_name=category_name)

            l_product_category = LProductCategory(h_product_pk=h_product_pk,
                                                  h_category_pk=h_category_pk,
                                                  hk_product_category_pk=hk_product_category_pk)

            l_product_restaurant = LProductRestaurant(h_product_pk=h_product_pk,
                                                      h_restaurant_pk=h_restaurant_pk,
                                                      hk_product_restaurant_pk=hk_product_restaurant_pk)

            l_order_product = LOrderProduct(h_order_pk=h_order_pk,
                                            h_product_pk=h_product_pk,
                                            hk_order_product_pk=hk_order_product_pk)

            self._dds_repository.insert(h_product)
            self._dds_repository.insert(s_poduct_names)
            self._dds_repository.insert(h_category)
            self._dds_repository.insert(l_product_category)
            self._dds_repository.insert(l_product_restaurant)
            self._dds_repository.insert(l_order_product)


        self._dds_repository.insert(h_order)
        self._dds_repository.insert(s_order_cost)
        self._dds_repository.insert(s_order_status)
        self._dds_repository.insert(h_restaurant)
        self._dds_repository.insert(s_restaurant_names)
        self._dds_repository.insert(h_user)
        self._dds_repository.insert(s_user_names)
        self._dds_repository.insert(l_user_order)


        lst_products = self._dds_repository.get_grouped_data(h_user_pk, ['h_product_pk', 'name'])
        lst_categories = self._dds_repository.get_grouped_data(h_user_pk, ['h_category_pk', 'category_name'])

        output_message = self.__create_output_message(h_user_pk, lst_products, lst_categories)
        
        self._producer.produce(output_message)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
