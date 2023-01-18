from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.cdm_models.user_product_counters import UserProductCounters
from cdm_loader.cdm_models.user_category_counters import UserCategoryCounters

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger,
                 batch_size: int = 100
                 ) -> None:

        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size

    def _insert_():



    def _messsage_processing(self, message: dict) -> None:

        user_id = message['user_id']
        products = message['products']
        categories = message['categories']


        for item in products:
            id = item['id']
            name = item['name']
            count = item['ctn']

            res = UserProductCounters(user_id=user_id,
                                      product_id=id,
                                      product_name=name,
                                      order_cnt=count)

            self._cdm_repository.insert(res)

        for item in categories:
            id = item['id']
            name = item['name']
            count = item['ctn']

            res = UserCategoryCounters(user_id=user_id,
                                      category_id=id,
                                      category_name=name,
                                      order_cnt=count)

            self._cdm_repository.insert(res)


    def fun(lst, class_type) -> None:

        for item in lst:
            id = item['id']
            name = item['name']
            count = item['ctn']

            res = class_type(user_id=user_id,
                                      category_id=id,
                                      category_name=name,
                                      order_cnt=count)

            self._cdm_repository.insert(res)







    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for i in range(0, self._batch_size):
            message = self._consumer.consume()

            if not message:
                break
            


        self._logger.info(f"{datetime.utcnow()}: FINISH")
