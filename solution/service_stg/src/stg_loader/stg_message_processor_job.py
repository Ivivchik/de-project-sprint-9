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
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        message = self._consumer.consume()

        object_id = message['object_id']
        object_type = message['object_type']
        payload = message['payload']
        date_order = payload['date']
        cost = payload['cost']
        payment = payload['payment']
        status = payload['final_status']

        sent_dttm = datetime.strptime(message['sent_dttm'], '%Y-%m-%d %H:%M:%S')
        payload_str = json.dumps(payload, ensure_ascii=False)

        stg_order_events = StgOrderEvents(object_id=object_id,
                                          payload=payload_str,
                                          object_type=object_type,
                                          sent_dttm=sent_dttm)

        self._stg_repository.order_events_insert(stg_order_events)

        user_id = payload['user']['id']
        restaurant_id = payload['restaurant']['id']

        user_info = self._redis.get(user_id)
        restaurant_info = self._redis.get(restaurant_id)

        products = [dict(x, **{'category': y['category']}) 
                    for y in restaurant_info['menu'] 
                        for x in message['payload']['order_items']
                        if x['id'] == y['_id']]

        user = payload['user']
        user.update( {'name': user_info['name'], 'login': user_info['login']} )

        restaurant = payload['restaurant']
        restaurant.update({'name': restaurant_info['name']})

        result = {"object_id": object_id, "object_type": object_type, 
                    "payload": {"id": object_id, "date": date_order,
                                "cost": cost, "payment": payment, 
                                "status": status, "restaurant": restaurant, 
                                "user": user, "products": products}}

        self._producer.produce(result)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
