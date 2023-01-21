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
                 logger: Logger,
                 batch_size: int = 30) -> None:

        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size

    def _insert_h_order(self,
                        order_id: int,
                        order_dt: str,
                        h_order_pk: UUID) -> None:
    
        h_order = HOrder(order_id=order_id,
                         order_dt=order_dt,
                         h_order_pk=h_order_pk)

        self._dds_repository.insert(h_order)
        
    def _insert_s_order_cost(self,
                             cost: str,
                             payment: str,
                             h_order_pk: UUID,
                             hk_order_cost_pk: UUID) -> None:

        s_order_cost = SOrderCost(cost=cost,
                                  payment=payment,
                                  h_order_pk=h_order_pk,
                                  hk_order_cost_pk=hk_order_cost_pk)
        
        self._dds_repository.insert(s_order_cost)
            
    def _insert_s_order_status(self,
                               status: str,
                               h_order_pk: UUID,
                               hk_order_status_pk: UUID) -> None:

        s_order_status = SOrderStatus(status=status,
                                      h_order_pk=h_order_pk,
                                      hk_order_status_pk=hk_order_status_pk)

        self._dds_repository.insert(s_order_status)


    def _insert_h_restaurant(self,
                             restaurant_id: str,
                             h_restaurant_pk: UUID) -> None:

        h_restaurant = HRestaurant(restaurant_id=restaurant_id,
                                   h_restaurant_pk=h_restaurant_pk)

        self._dds_repository.insert(h_restaurant)

    def _insert_s_restaurant_names(self,
                                   name: str,
                                   h_restaurant_pk: UUID,
                                   hk_restaurant_names_pk: UUID) -> None:

        s_restaurant_names = SRestaurantNames(name=name,
                                              h_restaurant_pk=h_restaurant_pk,
                                              hk_restaurant_names_pk=hk_restaurant_names_pk)

        self._dds_repository.insert(s_restaurant_names)


    def _insert_s_user_names(self,
                             user_name: str,
                             h_user_pk: UUID,
                             user_login: str,
                             hk_user_names_pk: UUID) -> None:

        s_user_names = SUserNames(username=user_name,
                                  h_user_pk=h_user_pk,
                                  userlogin=user_login,
                                  hk_user_names_pk=hk_user_names_pk)

        self._dds_repository.insert(s_user_names)


    def _insert_l_user_order(self,
                             h_user_pk: UUID,
                             h_order_pk: UUID,
                             hk_order_user_pk: UUID) -> None:

        l_user_order = LOrderUser(h_user_pk=h_user_pk,
                                  h_order_pk=h_order_pk,
                                  hk_order_user_pk=hk_order_user_pk)

        self._dds_repository.insert(l_user_order)

    def _insert_h_product(self,
                          product_id: str,
                          h_product_pk: UUID) -> None:

        h_product = HProduct(product_id=product_id,
                             h_product_pk=h_product_pk)

        self._dds_repository.insert(h_product)

    def _insert_s_poduct_names(self,
                               name: str,
                               h_product_pk: UUID,
                               hk_product_names_pk: UUID) -> None:

        s_poduct_names = SProductNames(name=name,
                                       h_product_pk=h_product_pk,
                                       hk_product_names_pk=hk_product_names_pk)

        self._dds_repository.insert(s_poduct_names)

    def _insert_h_category(self,
                           category_name: str,
                           h_category_pk: UUID) -> None:
    
        h_category = HCategory(h_category_pk=h_category_pk,
                               category_name=category_name)
        
        self._dds_repository.insert(h_category)

    def _insert_l_product_category(self,
                                   h_product_pk: UUID,
                                   h_category_pk: UUID,
                                   hk_product_category_pk: UUID) -> None:

        l_product_category = LProductCategory(h_product_pk=h_product_pk,
                                              h_category_pk=h_category_pk,
                                              hk_product_category_pk=hk_product_category_pk)

        self._dds_repository.insert(l_product_category)

    def _insert_h_user(self,
                       user_id: str,
                       h_user_pk: UUID) -> None:

        h_user = HUser(user_id=user_id, h_user_pk=h_user_pk)

        self._dds_repository.insert(h_user)

    def _insert_l_product_restaurant(self,
                                     h_product_pk: UUID,
                                     h_restaurant_pk: UUID,
                                     hk_product_restaurant_pk: UUID) -> None:

        l_product_restaurant = LProductRestaurant(h_product_pk=h_product_pk,
                                                  h_restaurant_pk=h_restaurant_pk,
                                                  hk_product_restaurant_pk=hk_product_restaurant_pk)

        self._dds_repository.insert(l_product_restaurant)
        
    def _insert_l_order_product(self,
                                h_order_pk: UUID,
                                h_product_pk: UUID,
                                hk_order_product_pk: UUID) -> None:

        l_order_product = LOrderProduct(h_order_pk=h_order_pk,
                                        h_product_pk=h_product_pk,
                                        hk_order_product_pk=hk_order_product_pk)

        self._dds_repository.insert(l_order_product)

    def __create_output_message(self, user_id: UUID) -> dict:

        lst_products = self._dds_repository.get_grouped_data(user_id, ['h_product_pk', 'name'])
        lst_categories = self._dds_repository.get_grouped_data(user_id, ['h_category_pk', 'category_name'])

        products = [{'id': str(item[1]), 'name': item[2], 'cnt': item[3]} for item in lst_products]
        categories = [{'id': str(item[1]), 'name': item[2], 'cnt': item[3]} for item in lst_categories]

        return {'user_id': str(user_id), 'products': products, 'categories': categories}

    def _message_processing(self, message: dict) -> UUID:

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

        self._insert_h_order(order_id=order_id,
                             order_dt=order_dt,
                             h_order_pk=h_order_pk)
        
        self._insert_s_order_cost(cost=cost,
                                  payment=payment,
                                  h_order_pk=h_order_pk,
                                  hk_order_cost_pk=hk_order_cost_pk)

        self._insert_s_order_status(status=status,
                                    h_order_pk=h_order_pk,
                                    hk_order_status_pk=hk_order_status_pk)

        self._insert_h_restaurant(restaurant_id=restaurant_id,
                                  h_restaurant_pk=h_restaurant_pk)

        self._insert_s_restaurant_names(name=restaurant_name,
                                        h_restaurant_pk=h_restaurant_pk,
                                        hk_restaurant_names_pk=hk_restaurant_names_pk)

        self._insert_h_user(user_id=user_id, h_user_pk=h_user_pk)

        self._insert_s_user_names(user_name=user_name,
                                  h_user_pk=h_user_pk,
                                  user_login=user_login,
                                  hk_user_names_pk=hk_user_names_pk)

        self._insert_l_user_order(h_user_pk=h_user_pk,
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

            self._insert_h_product(product_id=product_id,
                                   h_product_pk=h_product_pk)

            self._insert_s_poduct_names(name=product_name,
                                        h_product_pk=h_product_pk,
                                        hk_product_names_pk=hk_product_names_pk)

            self._insert_h_category(h_category_pk=h_category_pk,
                                    category_name=category_name)

            self._insert_l_product_category(h_product_pk=h_product_pk,
                                            h_category_pk=h_category_pk,
                                            hk_product_category_pk=hk_product_category_pk)

            self._insert_l_product_restaurant(h_product_pk=h_product_pk,
                                              h_restaurant_pk=h_restaurant_pk,
                                              hk_product_restaurant_pk=hk_product_restaurant_pk)

            self._insert_l_order_product(h_order_pk=h_order_pk,
                                         h_product_pk=h_product_pk,
                                         hk_order_product_pk=hk_order_product_pk)

        return h_user_pk



    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):

            message = self._consumer.consume()

            if not message:
                self._logger.info(f"{datetime.utcnow()}: NO messages. Quitting.")
                break

            h_user_pk = self._message_processing(message)
            output_message = self.__create_output_message(h_user_pk)

            self._producer.produce(output_message)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
