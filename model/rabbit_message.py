from pydantic import BaseModel

from util.environment_loader import load_base_directory, load_upload_base_directory

base_directory = load_base_directory()
upload_base_directory = load_upload_base_directory()


class RabbitMessage(BaseModel):
    file: str = 'origin.tif'
    directory: str = ''

    def get_raster_file_path(self) -> str:
        return self.get_directory_path() + "/" + self.file

    def get_directory_path(self) -> str:
        converted = (self.directory.replace('{base_directory}', base_directory)
                     .replace('{upload_base_directory}', upload_base_directory))
        if converted.endswith('/'):
            self.directory = converted[:-1]
            return self.get_directory_path()

        return converted


class TileCreateRequest(RabbitMessage):
    z: int = 0
    x: int = 0
    y: int = 0
    startPoint: str = 'TOP_LEFT'
    pattern: str = 'morteza/{z}/{x}/{y}.png'

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


class LayerInfoRequest(RabbitMessage):
    id: str = ''


class LayerInfoResponse(BaseModel):
    id: str = ''
    minZoom: int = 4
    maxZoom: int = 21
    bbox: list = list()

    def __init__(self, response_id: str, min_zoom: int, max_zoom: int, bbox: list):
        super().__init__()
        self.id = response_id
        self.minZoom = min_zoom
        self.maxZoom = max_zoom
        self.bbox = bbox
