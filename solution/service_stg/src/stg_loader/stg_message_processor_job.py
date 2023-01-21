import json

from datetime import datetime
from logging import Logger

from lib.redis.redis_client import RedisClient
from lib.kafka_connect.kafka_connectors import KafkaConsumer
from lib.kafka_connect.kafka_connectors import KafkaProducer
from stg_loader.repository.stg_repository import StgRepository
from stg_loader.stg_models.order_event_model import StgOrderEvents


class StgMessageProcessor:
    def __init__(self,
                 logger: Logger,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int = 100) -> None:

        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
    # функция, которая будет вызываться по расписанию.

    def _user_info_processing(self, payload: dict) -> dict:

        user_id = payload['user']['id']
        user_info = self._redis.get(user_id)
        user = payload['user']
        user.update( {'name': user_info['name'], 'login': user_info['login']} )

        return user
    
    def _get_restaurant_info_from_redis(self, payload: dict) -> dict:

        restaurant_id = payload['restaurant']['id']
        restaurant_info = self._redis.get(restaurant_id)

        return restaurant_info

    def _restaurant_info_processing(self,
                                    payload: dict,
                                    restaurant_info: dict) -> dict:

        restaurant = payload['restaurant']
        restaurant.update({'name': restaurant_info['name']})

        return restaurant

    def _product_info_processing(self,
                                 payload: dict,
                                 restaurant_info: dict) -> list:

        products = [dict(x, **{'category': y['category']}) 
                    for y in restaurant_info['menu'] 
                        for x in payload['order_items']
                        if x['id'] == y['_id']]

        return products

    def _insert_order_events(self,
                             payload: dict,
                             object_id: int,
                             object_type: str,
                             sent_dttm: datetime) -> None:
                             
        payload_str = json.dumps(payload, ensure_ascii=False)

        stg_order_events = StgOrderEvents(object_id=object_id,
                                          payload=payload_str,
                                          object_type=object_type,
                                          sent_dttm=sent_dttm)

        self._stg_repository.insert(stg_order_events)        


    def _message_processing(self, message: dict) -> dict:

        payload = message['payload']

        object_id = message['object_id']
        object_type = message['object_type']
        
        cost = payload['cost']
        payment = payload['payment']
        date_order = payload['date']
        status = payload['final_status']
        sent_dttm = datetime.strptime(message['sent_dttm'], '%Y-%m-%d %H:%M:%S')
        
        self._insert_order_events(payload=payload,
                                  sent_dttm=sent_dttm,
                                  object_id=object_id,
                                  object_type=object_type)

        restaurant_info = self._get_restaurant_info_from_redis(payload)

        user = self._user_info_processing(payload)
        products = self._product_info_processing(payload, restaurant_info)
        restaurant = self._restaurant_info_processing(payload, restaurant_info)


        output_message = {"object_id": object_id, "object_type": object_type, 
                          "payload": {"id": object_id, "date": date_order,
                                "cost": cost, "payment": payment, 
                                "status": status, "restaurant": restaurant, 
                                "user": user, "products": products}}

        return output_message

    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            message = self._consumer.consume()

            if not message:
                self._logger.info(f"{datetime.utcnow()}: NO messages. Quitting.")
                break

            output_message = self._message_processing(message)

            self._producer.produce(output_message)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
