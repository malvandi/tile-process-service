import logging
import time
import json

from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

from model.rabbit_config import RabbitConfig
from model.rabbit_message import TileCreateRequest, LayerInfoRequest, LayerInfoResponse, FileTileCreate
from util.rabbit import Rabbit
from util.environment_loader import load_rabbit_config
from util.raster_info import fetch_info
from util.tile_creator import TileCreator


class Runner:
    _configs: RabbitConfig
    _tile_creator: TileCreator
    _rabbit: Rabbit
    _logger: logging.Logger

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._logger.info('Initializing Runner ...')

        self._configs = load_rabbit_config()
        self._tile_creator = TileCreator()
        self._connect_to_rabbit()

    def _receive_tile_create_message(self, ch: BlockingChannel, method, properties: BasicProperties, body: bytes):
        data_str = body.decode('utf-8')
        data_dict = json.loads(data_str)
        try:
            tile_request = TileCreateRequest(**data_dict)
            self._logger.debug('Receive new "TILE_CREATE_REQUEST" message: ' + tile_request.model_dump_json())
            self._tile_creator.create_tile(tile_request)
        except Exception as exception:
            self._logger.error('Occur Error in creating tile: %s with error: %s' % (data_dict, repr(exception)))
            import traceback
            traceback.print_exc()

    def _init_listen_to_tile_create_messages(self):
        self._logger.info('Listening to "TILE_CREATE_REQUEST" messages ...')
        tile_create_request_queue = self._configs.exchange + '.tile-create-request'
        self._rabbit.channel.queue_declare(tile_create_request_queue, durable=True)
        self._rabbit.channel.queue_bind(tile_create_request_queue, self._configs.exchange, 'TILE_CREATE_REQUEST')
        self._rabbit.channel.basic_consume(tile_create_request_queue, self._receive_tile_create_message, True)

    def _receive_raster_info_message(self, ch: BlockingChannel, method, properties: BasicProperties, body: bytes):
        data_str = body.decode('utf-8')
        data_dict = json.loads(data_str)
        try:
            info_request = LayerInfoRequest(**data_dict)
            self._logger.debug('Receive new "INFO_REQUEST" message: %s' % info_request.model_dump_json())

            info: LayerInfoResponse = fetch_info(info_request)
            json_info: str = info.model_dump_json()

            self._rabbit.channel.basic_publish(self._configs.exchange, 'INFO_RESPONSE', json_info)
        except Exception as exception:
            self._logger.error('Occur Error in getting raster info: %s with error: %s' % (data_dict, repr(exception)))
            import traceback
            traceback.print_exc()

    def _init_listen_to_raster_info_messages(self):
        self._logger.info('Listening to "INFO_REQUEST" messages ...')
        raster_info_queue = self._configs.exchange + '.info-request'
        self._rabbit.channel.queue_declare(raster_info_queue, durable=True)
        self._rabbit.channel.queue_bind(raster_info_queue, self._configs.exchange, 'INFO_REQUEST')
        self._rabbit.channel.basic_consume(raster_info_queue, self._receive_raster_info_message, True)

    def _run_test(self):
        self._logger.warning('Sending test request to rabbit ...')
        tile = TileCreateRequest()
        # tile.z = 18
        # tile.x = 168583
        # tile.y = 158911
        # tile.y = 103232  # TOP_LEFT
        tile.z = 17
        tile.x = 84291
        tile.y = 79455
        tile.startCreateTileZoom = 19
        tile.resampling = 'average'
        tile.startPoint = 'BOTTOM_LEFT'
        tile.directory = '{base_directory}'

        east_file = FileTileCreate()
        east_file.name = 'east.tif'
        tile.files.append(east_file)

        west_file = FileTileCreate()
        west_file.name = 'west.tif'
        tile.files.append(west_file)


        self._rabbit.channel.basic_publish(self._configs.exchange, 'TILE_CREATE_REQUEST', tile.model_dump_json())

        tile.y = tile.y + 1
        # self._rabbit.channel.basic_publish(self._configs.exchange, 'TILE_CREATE_REQUEST', tile.model_dump_json())

        tile.z = 17
        tile.x = 83276
        tile.y = 77557
        # rabbit.channel.basic_publish(configs.exchange, 'TILE_CREATE_REQUEST', tile.model_dump_json())

        info_request = LayerInfoRequest(**{})
        # info_request.file = tile.file
        info_request.directory = tile.directory
        # info: LayerInfoResponse = fetch_info(info_request)
        # print(info.model_dump_json())

    def _connect_to_rabbit(self):
        try:
            self._rabbit = Rabbit(self._configs)
            self._init_listen_to_raster_info_messages()
            self._init_listen_to_tile_create_messages()

            # Test
            self._run_test()

            self._rabbit.channel.start_consuming()

        except Exception as exception:
            self._logger.error('Occur error in connecting to Rabbit: ' + repr(exception))
            import traceback
            traceback.print_exc()
            time.sleep(self._configs.socket_timeout)
            self._connect_to_rabbit()

