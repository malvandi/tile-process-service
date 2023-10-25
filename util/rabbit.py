import logging

from dotenv import load_dotenv
import pika
from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

from model.rabbit_config import RabbitConfig


class Rabbit:
    configs: RabbitConfig
    connection: BlockingConnection
    channel: BlockingChannel
    _logger: logging.Logger

    def __init__(self, configs: RabbitConfig):
        self.configs = configs
        self._logger = logging.getLogger(__name__)
        self._logger.info('Connecting to RabbitMQ ...')
        credentials = pika.credentials.PlainCredentials(self.configs.username, self.configs.password)
        rabbit_parameters = pika.ConnectionParameters(self.configs.host, self.configs.port, credentials=credentials,
                                                      connection_attempts=self.configs.connection_attempts,
                                                      retry_delay=self.configs.retry_delay,
                                                      socket_timeout=self.configs.socket_timeout)
        self.connection = pika.BlockingConnection(rabbit_parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(configs.exchange, 'direct', durable=True)
