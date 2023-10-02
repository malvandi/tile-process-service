from pathlib import Path
import numpy as np
from pyproj import Transformer
from rioxarray import rioxarray
from PIL import Image
from model.rabbit_message import TileCreateRequest
import rasterio
import gdal2tiles as g2t

mercator = g2t.GlobalMercator()
transparent_image = Image.new('RGB', (256, 256), (255, 255, 255, 0))


def create_tile(tile_request: TileCreateRequest):
    if Path(tile_request.get_tile_path()).exists():
        return

    tms = tile_request.get_tms_position()
    tile_bound: tuple[float, float, float, float] = mercator.TileBounds(tms[1], tms[2], tms[0])

    raster_file = rioxarray.open_rasterio(tile_request.get_raster_file_path())
    transformer: Transformer = Transformer.from_crs('epsg:3857', raster_file.rio.crs, always_xy=True)
    file_tile_bound = transformer.transform_bounds(tile_bound[0], tile_bound[1], tile_bound[2], tile_bound[3])

    with rasterio.open(tile_request.get_raster_file_path()) as src:
        window = src.window(file_tile_bound[0], file_tile_bound[1], file_tile_bound[2], file_tile_bound[3])
        subset = src.read(window=window)

        try:
            resized_image = Image.fromarray(subset.transpose(1, 2, 0)).resize((256, 256))
        except:
            subset = subset.astype(np.uint8)

            resized_image = transparent_image if subset.size == 0 else Image.fromarray(
                subset.transpose(1, 2, 0)).resize((256, 256))

        tile_directory = Path(tile_request.get_tile_path())
        tile_directory.parent.mkdir(parents=True, exist_ok=True)
        resized_image.save(tile_request.get_tile_path(), 'PNG', save_all=True)
