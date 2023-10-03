import rasterio
from pyproj import Transformer
from rasterio import DatasetReader, CRS
from rasterio.coords import BoundingBox
import gdal2tiles as g2t
from model.rabbit_message import LayerInfoRequest, LayerInfoResponse


mercator = g2t.GlobalMercator()


# Max Boundary of EPSG:3857 is [-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244]
def fetch_info(request: LayerInfoRequest) -> LayerInfoResponse:
    raster_dataset: DatasetReader = rasterio.open(request.get_raster_file_path())
    raster_crs: CRS = raster_dataset.crs

    bounding_box: BoundingBox = raster_dataset.bounds
    bbox = [bounding_box.left, bounding_box.bottom, bounding_box.right, bounding_box.top]
    transformer = Transformer.from_crs(raster_crs, 'epsg:3857', always_xy=True)
    mercator_bounding_box: tuple = transformer.transform_bounds(bbox[0], bbox[1], bbox[2], bbox[3])

    min_zoom = 0
    max_zoom = 0
    for zoom in range(1, 33):

        tile_start: tuple[int, int] = mercator.MetersToTile(mercator_bounding_box[0], mercator_bounding_box[1], zoom)
        tile_end: tuple[int, int] = mercator.MetersToTile(mercator_bounding_box[2], mercator_bounding_box[3], zoom)

        dif_x = tile_end[0] - tile_start[0] + 1
        dif_y = tile_end[1] - tile_start[1] + 1
        tile_number = dif_x * dif_y

        if tile_number > 1 and min_zoom == 0:
            min_zoom = max(zoom - 1, 1)

        size_y = raster_dataset.height / dif_y
        size_x = raster_dataset.width / dif_x

        if size_x < 256 or size_y < 256:
            max_zoom = zoom
            break

    return LayerInfoResponse(request.id, request.file, min_zoom, max_zoom, list(mercator_bounding_box))
