class RabbitConfig:
    host: str = ''
    port: int = 5672
    username: str = 'guest'
    password: str = 'guest'
    exchange: str = 'mf-exchange-python'

    connection_attempts: int = 100
    retry_delay: int = 2  # In seconds
    socket_timeout: int = 10  # In seconds
