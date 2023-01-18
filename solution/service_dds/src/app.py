import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository.dds_repository import DdsRepository


app = Flask(__name__)

config = AppConfig()


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()

    consumer = config.kafka_consumer()
    producer = config.kafka_producer()
    pg_connect = config.pg_warehouse_db()

    dds_repository = DdsRepository(pg_connect)
    
    proc = DdsMessageProcessor(
        logger=app.logger,
        consumer=consumer,
        producer=producer,
        dds_repository=dds_repository
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
