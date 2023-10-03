import json

from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

from model.rabbit_message import TileCreateRequest, LayerInfoRequest, LayerInfoResponse
from rabbit import Rabbit
from util.raster_info import fetch_info
from util.tile_creator import TileCreator
from util.environment_loader import load_rabbit_config

if __name__ != '__main__':
    exit()

configs = load_rabbit_config()
rabbit = Rabbit(configs)


def init_tile_create_requests():
    def receive_data(ch: BlockingChannel, method, properties: BasicProperties, body: bytes):
        data_str = body.decode('utf-8')
        data_dict = json.loads(data_str)
        try:
            tile_request = TileCreateRequest(**data_dict)
            print('Creating tile: ' + tile_request.model_dump_json(), flush=True)
            tile_creator.create_tile(tile_request)
        except Exception as exception:
            exception.with_traceback()
            print('Occur Error in creating tile: %s with error: %s' % (data_dict, repr(exception)), flush=True)

    print('Listening to TILE_CREATE_REQUEST messages ...', flush=True)
    tile_creator = TileCreator()
    tile_create_request_queue = configs.exchange + '.tile-create-request'
    rabbit.channel.queue_declare(tile_create_request_queue, durable=True)
    rabbit.channel.queue_bind(tile_create_request_queue, configs.exchange, 'TILE_CREATE_REQUEST')
    rabbit.channel.basic_consume(tile_create_request_queue, receive_data, True)


def init_raster_info_requests():
    def receive_new_request(ch: BlockingChannel, method, properties: BasicProperties, body: bytes):
        data_str = body.decode('utf-8')
        data_dict = json.loads(data_str)
        try:
            info_request = LayerInfoRequest(**data_dict)

            info: LayerInfoResponse = fetch_info(info_request)
            print('Raster File Info: ' + info.model_dump_json(), flush=True)
            rabbit.channel.basic_publish(configs.exchange, 'INFO_RESPONSE', info.model_dump_json())
        except Exception as exception:
            print('Occur Error in creating tile: %s with error: %s' % (data_dict, repr(exception)), flush=True)

    print('Listening to INFO_REQUEST messages ...', flush=True)
    raster_info_queue = configs.exchange + '.info-request'
    rabbit.channel.queue_declare(raster_info_queue, durable=True)
    rabbit.channel.queue_bind(raster_info_queue, configs.exchange, 'INFO_REQUEST')
    rabbit.channel.basic_consume(raster_info_queue, receive_new_request, True)


def run_test():
    tile = TileCreateRequest()
    # tile.z = 15
    # tile.x = 21067
    # tile.y = 12903

    tile.z = 15
    tile.x = 21067
    tile.y = 12900
    # tile.z = 16
    # tile.x = 42134
    # tile.y = 25800
    tile.startCreateTileZoom = 17
    tile.resampling = 'max'
    tile.file = "tehran-now.tif"
    tile.directory = "{base_directory}"
    tile.startPoint = 'TOP_LEFT'
    rabbit.channel.basic_publish(configs.exchange, 'TILE_CREATE_REQUEST', tile.model_dump_json())

    tile.y = 155118
    # rabbit.channel.basic_publish(configs.exchange, 'TILE_CREATE_REQUEST', tile.model_dump_json())

    tile.z = 17
    tile.x = 83276
    tile.y = 77557
    # rabbit.channel.basic_publish(configs.exchange, 'TILE_CREATE_REQUEST', tile.model_dump_json())

    info_request = LayerInfoRequest(**{})
    info_request.file = tile.file
    info_request.directory = tile.directory
    # info: LayerInfoResponse = fetch_info(info_request)
    # print(info.model_dump_json())


init_raster_info_requests()
init_tile_create_requests()

# Test
run_test()

rabbit.channel.start_consuming()
