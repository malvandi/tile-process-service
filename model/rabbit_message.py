import os

from pydantic import BaseModel
from typing import List

from util.environment_loader import load_base_directory, load_upload_base_directory

base_directory = load_base_directory()
upload_base_directory = load_upload_base_directory()


class RabbitMessage(BaseModel):
    directory: str = ''

    def get_directory_path(self) -> str:
        converted = (self.directory.replace('{base_directory}', base_directory)
                     .replace('{upload_base_directory}', upload_base_directory))
        if converted.endswith('/'):
            self.directory = converted[:-1]
            return self.get_directory_path()

        return converted


class FileTileCreate(BaseModel):
    name: str = ''
    resampling: str = 'near'


class TileCreateRequest(RabbitMessage):
    z: int = 0
    x: int = 0
    y: int = 0
    startCreateTileZoom: int = 0
    files: List[FileTileCreate] = []
    startPoint: str = 'TOP_LEFT'
    pattern: str = 'morteza/{z}/{x}/{y}.png'

    def get_raster_file_path(self, file: FileTileCreate) -> str:
        return self.get_directory_path() + "/" + file.name

    """
    Get y index start from bottom
    """

    # Get y index start from bottom
    def get_tms_position(self) -> tuple[int, int, int]:
        if self.startPoint == 'BOTTOM_LEFT' or self.startPoint == 'BOTTOM_RIGHT':
            return self.z, self.x, self.y

        y = 2 ** self.z - 1 - self.y
        return self.z, self.x, y

    def get_tile_path(self) -> str:
        tile_path = self.get_directory_path() + '/' + self.pattern
        return tile_path.replace('{z}', str(self.z)) \
            .replace('{x}', str(self.x)) \
            .replace('{y}', str(self.y))

    def get_file_tile_path(self, file: FileTileCreate):
        return self.get_tile_path() + '_' + file.name + '.temp'

    def get_file_temp_directory(self, file: FileTileCreate) -> str:
        return self.get_directory_path()

    def get_children(self) -> List['TileCreateRequest']:
        returned = []
        for i in range(2):
            for j in range(2):
                child = TileCreateRequest()
                child.z = self.z + 1
                child.x = self.x * 2 + i
                child.y = self.y * 2 + j

                child.files += self.files
                child.startPoint = self.startPoint
                child.pattern = self.pattern
                child.directory = self.directory

                returned.append(child)

        return returned

    def exist(self) -> bool:
        tile_path = self.get_tile_path()
        return os.path.exists(tile_path)

    def exist_file_tile(self, file: FileTileCreate) -> bool:
        file_tile_path = self.get_file_tile_path(file)
        return os.path.exists(file_tile_path)

class LayerInfoRequest(RabbitMessage):
    id: str = ''
    file: str = 'origin.tif'

    def get_raster_file_path(self) -> str:
        return self.get_directory_path() + "/" + self.file


class LayerInfoResponse(BaseModel):
    id: str = ''
    file: str = ''
    minZoom: int = 4
    maxZoom: int = 21
    bbox: list = list()

    def __init__(self, response_id: str, file: str, min_zoom: int, max_zoom: int, bbox: list):
        super().__init__()
        self.id = response_id
        self.file = file
        self.minZoom = min_zoom
        self.maxZoom = max_zoom
        self.bbox = bbox
