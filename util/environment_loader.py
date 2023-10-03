from model.rabbit_config import RabbitConfig
from dotenv import load_dotenv
import os

load_dotenv()


def load_rabbit_config() -> RabbitConfig:

    config = RabbitConfig()
    config.host = str(os.environ.get('RABBIT_HOST'))
    config.port = int(os.environ.get('RABBIT_PORT'))
    config.username = str(os.environ.get('RABBIT_USERNAME'))
    config.password = str(os.environ.get('RABBIT_PASSWORD'))
    config.exchange = str(os.environ.get('RABBIT_EXCHANGE'))
    config.connection_attempts = int(os.environ.get('RABBIT_CONNECTION_ATTEMPTS'))
    config.retry_delay = int(os.environ.get('RABBIT_RETRY_DELAY'))
    config.socket_timeout = int(os.environ.get('RABBIT_SOCKET_TIMEOUT'))

    return config


def load_base_directory() -> str:
    return str(os.environ.get('BASE_DIRECTORY'))


def load_upload_base_directory() -> str:
    return str(os.environ.get('UPLOAD_BASE_DIRECTORY'))

