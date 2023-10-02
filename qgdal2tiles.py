import os

import numpy
from PIL import Image
from gdal2tiles import GDAL2Tiles, TileJobInfo
from osgeo import gdal
import osgeo.gdal_array as gdalarray
from osgeo_utils.gdal2tiles import numpy_available

from model.gdal_2_tiles_options import GDAL2TilesOptions
from model.rabbit_message import TileCreateRequest
from model.raster_file import RasterFile
from model.tile_info import TileInfo
from util.tile_creator import TileCreator


def _get_creation_options(options):
    return ["QUALITY=" + str(options.webp_quality)]


def create_tile(info: TileInfo) -> None:
    if os.path.exists(info.tile_directory):
        return

    tile_file_path = os.path.dirname(info.tile_directory)
    os.makedirs(tile_file_path, exist_ok=True)

    tile_size = info.tile_size

    tile_bands_count = info.raster.data_bands_count + 1

    ds = gdal.Open(info.raster.directory, gdal.GA_ReadOnly)

    mem_drv = gdal.GetDriverByName("MEM")
    out_drv = gdal.GetDriverByName(info.driver)
    alpha_band = ds.GetRasterBand(1).GetMaskBand()

    # Tile dataset in memory
    dataset_tile = mem_drv.Create("", tile_size, tile_size, tile_bands_count)

    data = alpha = None

    if info.tile_file_width != 0 and info.tile_file_height != 0 and info.content_width != 0 and info.content_height != 0:
        alpha = alpha_band.ReadRaster(info.rx, info.ry, info.tile_file_width, info.tile_file_height, info.content_width,
                                      info.content_height)

        data = ds.ReadRaster(info.rx, info.ry, info.tile_file_width, info.tile_file_height, info.content_width,
                             info.content_height, band_list=list(range(1, tile_bands_count)))

    # The tile in memory is a transparent file by default. Write pixel values into it if
    # any
    if data:
        if tile_size == info.query_size:
            # Use the ReadRaster result directly in tiles ('nearest neighbour' query)
            dataset_tile.WriteRaster(
                info.start_content_x, info.start_content_y, info.content_width, info.content_height, data,
                band_list=list(range(1, tile_bands_count))
            )
            dataset_tile.WriteRaster(info.start_content_x, info.start_content_y, info.content_width,
                                     info.content_height, alpha, band_list=[tile_bands_count])

        else:
            dataset_query = mem_drv.Create("", info.query_size, info.query_size, tile_bands_count)
            dataset_query.WriteRaster(
                info.start_content_x, info.start_content_y, info.content_width, info.content_height, data,
                band_list=list(range(1, tile_bands_count))
            )
            dataset_query.WriteRaster(info.start_content_x, info.start_content_y, info.content_width,
                                      info.content_height, alpha, band_list=[tile_bands_count])

            scale_query_to_tile(dataset_query, dataset_tile, info.options, info.tile_directory)
            del dataset_query
    del data

    if info.options.resampling != "antialias":
        # Write a copy of tile to png/jpg
        out_drv.CreateCopy(info.tile_directory, dataset_tile, strict=0, options=[])
        # pass

    del dataset_tile


def scale_query_to_tile(dsquery, dstile, options, tilefilename=""):
    """Scales down query dataset to the tile dataset"""

    querysize = dsquery.RasterXSize
    tile_size = dstile.RasterXSize
    tilebands = dstile.RasterCount

    if options.resampling == "average":

        # Function: gdal.RegenerateOverview()
        for i in range(1, tilebands + 1):
            # Black border around NODATA
            res = gdal.RegenerateOverview(
                dsquery.GetRasterBand(i), dstile.GetRasterBand(i), "average"
            )
            if res != 0:
                # exit_with_error(
                #     "RegenerateOverview() failed on %s, error %d" % (tilefilename, res)
                # )
                return

    elif options.resampling == "antialias" and numpy_available:

        if tilefilename.startswith("/vsi"):
            raise Exception(
                "Outputting to /vsi file systems with antialias mode is not supported"
            )

        # Scaling by PIL (Python Imaging Library) - improved Lanczos
        array = numpy.zeros((querysize, querysize, tilebands), numpy.uint8)
        for i in range(tilebands):
            array[:, :, i] = gdalarray.BandReadAsArray(
                dsquery.GetRasterBand(i + 1), 0, 0, querysize, querysize
            )
        im = Image.fromarray(array, "RGBA")  # Always four bands
        im1 = im.resize((tile_size, tile_size), Image.LANCZOS)
        if os.path.exists(tilefilename):
            im0 = Image.open(tilefilename)
            im1 = Image.composite(im1, im0, im1)

        params = {}
        if options.tiledriver == "WEBP":
            if options.webp_lossless:
                params["lossless"] = True
            else:
                params["quality"] = options.webp_quality
        im1.save(tilefilename, options.tiledriver, **params)

    else:

        if options.resampling == "near":
            gdal_resampling = gdal.GRA_NearestNeighbour

        elif options.resampling == "bilinear":
            gdal_resampling = gdal.GRA_Bilinear

        elif options.resampling == "cubic":
            gdal_resampling = gdal.GRA_Cubic

        elif options.resampling == "cubicspline":
            gdal_resampling = gdal.GRA_CubicSpline

        elif options.resampling == "lanczos":
            gdal_resampling = gdal.GRA_Lanczos

        elif options.resampling == "mode":
            gdal_resampling = gdal.GRA_Mode

        elif options.resampling == "max":
            gdal_resampling = gdal.GRA_Max

        elif options.resampling == "min":
            gdal_resampling = gdal.GRA_Min

        elif options.resampling == "med":
            gdal_resampling = gdal.GRA_Med

        elif options.resampling == "q1":
            gdal_resampling = gdal.GRA_Q1

        elif options.resampling == "q3":
            gdal_resampling = gdal.GRA_Q3

        # Other algorithms are implemented by gdal.ReprojectImage().
        dsquery.SetGeoTransform(
            (
                0.0,
                tile_size / float(querysize),
                0.0,
                0.0,
                0.0,
                tile_size / float(querysize),
            )
        )
        dstile.SetGeoTransform((0.0, 1.0, 0.0, 0.0, 0.0, 1.0))

        res = gdal.ReprojectImage(dsquery, dstile, None, None, gdal_resampling)
        if res != 0:
            # exit_with_error(
            #     "ReprojectImage() failed on %s, error %d" % (tilefilename, res)
            # )
            return


def get_valid_zooms(gdal_2_tiles: GDAL2Tiles):
    min_zoom = 0
    # TODO
    max_zoom = 32
    for zoom in range(1, 32):
        min_tile_x, min_tile_y, max_tile_x, max_tile_y = gdal_2_tiles.tminmax[zoom]
        width_tile_size = max_tile_x - min_tile_x + 1
        height_tile_size = max_tile_y - min_tile_y + 1
        if width_tile_size * height_tile_size > 1 and min_zoom == 0:
            min_zoom = max(zoom - 1, 1)

        print('Zoom[' + str(zoom) + ']: ' + str(gdal_2_tiles.tminmax[zoom]))
    print('Min Zoom: ' + str(min_zoom))


def get_tile_info(gdal_2_tiles: GDAL2Tiles, zoom: int, x: int, y: int) -> TileInfo:
    dataset = gdal_2_tiles.warped_input_dataset
    query_size = gdal_2_tiles.querysize

    tile_bound = gdal_2_tiles.mercator.TileBounds(x, y, zoom)

    # Tile bounds in raster coordinates for ReadRaster query
    rb, wb = gdal_2_tiles.geo_query(dataset, tile_bound[0], tile_bound[3], tile_bound[2], tile_bound[1],
                                    querysize=query_size)

    info = TileInfo()

    info.driver = gdal_2_tiles.tiledriver

    info.options = gdal_2_tiles.options
    info.raster = RasterFile()
    info.raster.directory = raster_file
    info.raster.data_bands_count = gdal_2_tiles.dataBandsCount

    info.x = x
    info.y = y
    info.zoom = zoom

    info.query_size = query_size
    info.start_content_x, info.start_content_y, info.content_width, info.content_height = wb

    info.rx, info.ry, info.tile_file_width, info.tile_file_height = rb

    info.tile_size = gdal_2_tiles.tilesize
    info.tile_directory = ('/home/malvandi/Projects/Tiles/generated/' + str(info.zoom) + '/x' + str(info.x) + '/' +
                           str(info.y) + '.png')

    return info


raster_file = '/home/malvandi/Projects/Tiles/shomale_qharb3.tif'
tile_directory = '/home/malvandi/Projects/Tiles/generated'

tile_creator = TileCreator()

request_str = {"z": 11, "x": 1280, "y": 1248, "directory": "/home/malvandi/Projects/Tiles",
               "file": "shomale_qharb3.tif"}

tile_request = TileCreateRequest(**request_str)
print(tile_request.model_dump_json())

tile_creator.create_tile(tile_request)

# options = GDAL2TilesOptions()
# gdal_to_tiles = GDAL2Tiles(raster_file, tile_directory, options)
# gdal_to_tiles.open_input()
# gdal_to_tiles.generate_metadata()

# print(gdal_to_tiles.tminmax[11])

# tile_info_1 = get_tile_info(gdal_to_tiles, 11, 1280, 1248)
#
# create_tile(tile_info_1)
