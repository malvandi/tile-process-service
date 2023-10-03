from gdal2tiles import GDAL2Tiles, TileJobInfo


class TileCreatorInstance:
    key: str
    gdal2tiles: GDAL2Tiles
    tile_job_info: TileJobInfo

    def __init__(self, key: str, gdal2tiles: GDAL2Tiles, tile_job_info: TileJobInfo):
        self.key = key
        self.gdal2tiles = gdal2tiles
        self.tile_job_info = tile_job_info
