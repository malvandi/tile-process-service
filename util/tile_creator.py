import os
import random

import numpy
from PIL import Image
from gdal2tiles import GDAL2Tiles, TileJobInfo
from osgeo import gdal
import osgeo.gdal_array as gdalarray
from osgeo_utils.gdal2tiles import numpy_available, TileDetail

from model.gdal_2_tiles_options import GDAL2TilesOptions
from model.rabbit_message import TileCreateRequest, FileTileCreate
from model.tile_creator_instance import TileCreatorInstance
from typing import List, Literal


class TileCreator:
    _gdal2tilesEntries = dict()
    _tile_creators = dict()

    # _transparent_image = Image.new('RGBA', (256, 256), (255, 255, 255, 0))

    def create_tile(self, tile_request: TileCreateRequest):

        tile_file_path = tile_request.get_tile_path()

        # if os.path.exists(tile_file_path):  # TODO(remove)
        #     os.remove(tile_request.get_tile_path())

        if os.path.exists(tile_file_path):
            return

        tile_request.files[:] = [file for file in tile_request.files if
                                 not self._is_empty_file_tile(tile_request, file)]

        if tile_request.z >= tile_request.startCreateTileZoom:
            self._new_create_tile_by_origin_file(tile_request, 4)
        else:
            self._create_tile_by_child(tile_request, 4)
        # for file in tile_request.files:
        #     self._create_tile_file(tile_request, file)

        # self._create_tile_if_file_tiles_exist(tile_request)

    def _create_tile_by_child(self, tile_request: TileCreateRequest, max_create_tile: int) -> int:
        if tile_request.exist():
            return 0

        if max_create_tile <= 0:
            return 0

        created_tiles = 0
        children = tile_request.get_children()
        for child in children:
            if tile_request.z + 1 >= tile_request.startCreateTileZoom:
                created_tiles += self._new_create_tile_by_origin_file(child, max_create_tile - created_tiles)
            else:
                created_tiles += self._create_tile_by_child(child, max_create_tile - created_tiles)

        self._create_tile_if_file_tiles_exist(tile_request)
        return created_tiles

    def _new_create_tile_by_origin_file(self, tile_request: TileCreateRequest, max_create_tile: int) -> int:
        if tile_request.exist():
            return 0

        created_tiles = 0
        for file in tile_request.files:
            if tile_request.exist_file_tile(file):
                continue

            if self._is_empty_file_tile(tile_request, file):
                continue

            if created_tiles > max_create_tile:
                return created_tiles

            self._create_file_tile_by_origin_file(tile_request, file)
            created_tiles += 1

        self._create_tile_if_file_tiles_exist(tile_request)
        return created_tiles

    def _create_tile_file(self, tile_request: TileCreateRequest, file: FileTileCreate):

        tile_path = tile_request.get_tile_path()
        tile_file_path = tile_request.get_file_tile_path(file)
        if os.path.exists(tile_path) or os.path.exists(tile_file_path):
            return

        if self._is_empty_file_tile(tile_request, file):
            os.makedirs(os.path.dirname(tile_file_path), exist_ok=True)
            self._get_transparent_tile().save(tile_file_path, format="png")
            return

        if file.startCreateTileZoom > tile_request.z:
            self._create_file_tile_by_child(tile_request, file, 4)
            return

        self._create_file_tile_by_origin_file(tile_request, file)

    def _create_file_tile_by_child(self, tile_request: TileCreateRequest, file: FileTileCreate,
                                   max_create_tile: int) -> int:
        if max_create_tile <= 0:
            return 0

        tile_path = tile_request.get_tile_path()
        file_tile_path = tile_request.get_file_tile_path(file)
        if os.path.exists(tile_path) or os.path.exists(file_tile_path):
            return 0

        if self._is_empty_file_tile(tile_request, file):
            return 0

        if tile_request.z >= file.startCreateTileZoom:
            self._create_file_tile_by_origin_file(tile_request, file)
            return 1

        created_tiles = 0
        children: List[TileCreateRequest] = tile_request.get_children()
        random.shuffle(children)
        for child in children:
            if max_create_tile - created_tiles > 0:
                created_tiles += self._create_file_tile_by_child(child, file, max_create_tile - created_tiles)

        self._create_file_tile_if_child_exists(tile_request, file)

        return created_tiles

    def _create_file_tile_if_child_exists(self, tile_request: TileCreateRequest, file: FileTileCreate):
        children: List[TileCreateRequest] = tile_request.get_children()
        images = []
        for child in children:
            image = self._get_file_tile_image_if_exists(child, file)
            if image is None:
                return

            images.append(image)

        concatenated_tile = Image.new('RGBA', (512, 512))
        if tile_request.startPoint == 'TOP_LEFT' or tile_request.startPoint == 'TOP_RIGHT':
            concatenated_tile.paste(images[0], (0, 0))
            concatenated_tile.paste(images[1], (0, 256))
            concatenated_tile.paste(images[2], (256, 0))
            concatenated_tile.paste(images[3], (256, 256))
        else:
            concatenated_tile.paste(images[0], (0, 256))
            concatenated_tile.paste(images[1], (0, 0))
            concatenated_tile.paste(images[2], (256, 256))
            concatenated_tile.paste(images[3], (256, 0))

        # Resize the horizontally concatenated image to 256x256
        resampling = TileCreator._get_pillow_image_resize_resampling(file.resampling)
        resized_image = concatenated_tile.resize((256, 256), resampling)
        concatenated_tile.close()

        tile_file_path = tile_request.get_file_tile_path(file)
        print('Generating file tile by childs: %s' % tile_file_path)
        os.makedirs(os.path.dirname(tile_file_path), exist_ok=True)
        resized_image.save(tile_file_path, format="png")
        resized_image.close()
        self._create_tile_if_file_tiles_exist(tile_request)

    def _get_file_tile_image_if_exists(self, child: TileCreateRequest, file: FileTileCreate) -> Image:
        file_tile_path = child.get_file_tile_path(file)
        if os.path.exists(file_tile_path):
            return Image.open(file_tile_path)

        if self._is_empty_file_tile(child, file):
            return self._get_transparent_tile()

        return None

    def _create_file_tile_by_origin_file(self, tile_request: TileCreateRequest, file_tile: FileTileCreate):
        tile_file_path = tile_request.get_file_tile_path(file_tile)
        print('Creating tile by origin: %s' % tile_file_path, flush=True)
        tile_creator = self._get_file_tile_creator_instance(tile_request, file_tile)

        tile_job_info = tile_creator.tile_job_info

        os.makedirs(os.path.dirname(tile_file_path), exist_ok=True)

        tile_detail = self._get_tile_detail(tile_creator.gdal2tiles, tile_request)

        data_bands_count = tile_job_info.nb_data_bands
        tile_size = tile_job_info.tile_size
        options = tile_job_info.options

        tile_bands = data_bands_count + 1
        ds = gdal.Open(tile_job_info.src_file, gdal.GA_ReadOnly)

        mem_drv = gdal.GetDriverByName("MEM")
        out_drv = gdal.GetDriverByName(tile_job_info.tile_driver)
        alpha_band = ds.GetRasterBand(1).GetMaskBand()

        # Tile dataset in memory
        tile_dataset = mem_drv.Create("", tile_size, tile_size, tile_bands)

        data = alpha = None

        if tile_detail.rxsize != 0 and tile_detail.rysize != 0 and tile_detail.wxsize != 0 and tile_detail.wysize != 0:
            alpha = alpha_band.ReadRaster(tile_detail.rx, tile_detail.ry, tile_detail.rxsize, tile_detail.rysize,
                                          tile_detail.wxsize, tile_detail.wysize)

            data = ds.ReadRaster(
                tile_detail.rx, tile_detail.ry, tile_detail.rxsize, tile_detail.rysize,
                tile_detail.wxsize, tile_detail.wysize, band_list=list(range(1, data_bands_count + 1)),
            )

        if data:
            if tile_size == tile_detail.querysize:
                # Use the ReadRaster result directly in tiles ('nearest neighbour' query)
                tile_dataset.WriteRaster(
                    tile_detail.wx, tile_detail.wy, tile_detail.wxsize, tile_detail.wysize, data,
                    band_list=list(range(1, data_bands_count + 1)),
                )
                tile_dataset.WriteRaster(tile_detail.wx, tile_detail.wy, tile_detail.wxsize, tile_detail.wysize, alpha,
                                         band_list=[tile_bands])

                # Note: For source drivers based on WaveLet compression (JPEG2000, ECW,
                # MrSID) the ReadRaster function returns high-quality raster (not ugly
                # nearest neighbour)
                # TODO: Use directly 'near' for WaveLet files
            else:
                # Big ReadRaster query in memory scaled to the tile_size - all but 'near'
                # algo
                ds_query = mem_drv.Create("", tile_detail.querysize, tile_detail.querysize, tile_bands)
                # TODO: fill the null value in case a tile without alpha is produced (now
                # only png tiles are supported)
                ds_query.WriteRaster(
                    tile_detail.wx, tile_detail.wy, tile_detail.wxsize, tile_detail.wysize, data,
                    band_list=list(range(1, data_bands_count + 1)),
                )
                ds_query.WriteRaster(
                    tile_detail.wx, tile_detail.wy, tile_detail.wxsize, tile_detail.wysize, alpha,
                    band_list=[tile_bands]
                )

                self._scale_query_to_tile(ds_query, tile_dataset, options, tile_file_path)
                del ds_query

        del data

        if options.resampling != "antialias":
            # Write a copy of tile to png/jpg
            out_drv.CreateCopy(tile_file_path, tile_dataset, strict=0, options=[])

        del tile_dataset

    def _get_file_tile_creator_instance(self, tile_request: TileCreateRequest,
                                        file: FileTileCreate) -> TileCreatorInstance:
        raster_file_path = tile_request.get_raster_file_path(file)
        entry = self._tile_creators.get(raster_file_path)
        if entry:
            return entry

        print('Instance not found. exist instances are: ' + str(self._tile_creators.keys()))
        print('Reading %s file for tiling ...' % file.name, flush=True)
        options = GDAL2TilesOptions()
        options.zoom = [1, 23]
        options.resampling = file.resampling

        file_temp_directory = tile_request.get_file_temp_directory(file)
        gdal_to_tiles = GDAL2Tiles(raster_file_path, file_temp_directory, options)
        gdal_to_tiles.open_input()
        gdal_to_tiles.generate_metadata()

        tile_job_info = self._get_tile_job_info(gdal_to_tiles)

        instance = TileCreatorInstance(raster_file_path, gdal_to_tiles, tile_job_info)
        self._tile_creators[raster_file_path] = instance
        return instance

    def _is_empty_file_tile(self, tile_request: TileCreateRequest, file: FileTileCreate) -> bool:
        creator: TileCreatorInstance = self._get_file_tile_creator_instance(tile_request, file)
        zoom_info = creator.tile_job_info.tminmax[tile_request.z]
        tms = tile_request.get_tms_position()

        return not (zoom_info[0] <= tms[1] <= zoom_info[2] and zoom_info[1] <= tms[2] <= zoom_info[3])

    @staticmethod
    def _get_tile_job_info(gdal_2_tiles: GDAL2Tiles) -> TileJobInfo:
        return TileJobInfo(
            src_file=gdal_2_tiles.tmp_vrt_filename,
            nb_data_bands=gdal_2_tiles.dataBandsCount,
            output_file_path=gdal_2_tiles.output_folder,
            tile_extension=gdal_2_tiles.tileext,
            tile_driver=gdal_2_tiles.tiledriver,
            tile_size=gdal_2_tiles.tilesize,
            kml=gdal_2_tiles.kml,
            tminmax=gdal_2_tiles.tminmax,
            tminz=gdal_2_tiles.tminz,
            tmaxz=gdal_2_tiles.tmaxz,
            in_srs_wkt=gdal_2_tiles.in_srs_wkt,
            out_geo_trans=gdal_2_tiles.out_gt,
            ominy=gdal_2_tiles.ominy,
            is_epsg_4326=gdal_2_tiles.isepsg4326,
            options=gdal_2_tiles.options,
        )

    @staticmethod
    def _get_tile_detail(gdal2tiles: GDAL2Tiles, tile_request: TileCreateRequest) -> TileDetail:
        ds = gdal2tiles.warped_input_dataset
        query_size = gdal2tiles.querysize

        tms = tile_request.get_tms_position()
        bound_tile = gdal2tiles.mercator.TileBounds(tms[1], tms[2], tms[0])
        # b = gdal2tiles.mercator.TileBounds(tile_request.x, tile_request.y, tile_request.z)
        rb, wb = gdal2tiles.geo_query(ds, bound_tile[0], bound_tile[3], bound_tile[2], bound_tile[1])

        # Tile bounds in raster coordinates for ReadRaster query
        rb, wb = gdal2tiles.geo_query(ds, bound_tile[0], bound_tile[3], bound_tile[2], bound_tile[1],
                                      querysize=query_size)

        rx, ry, rxsize, rysize = rb
        wx, wy, wxsize, wysize = wb

        return TileDetail(
            tx=tile_request.x, ty=tile_request.y, tz=tile_request.z, rx=rx, ry=ry, rxsize=rxsize, rysize=rysize, wx=wx,
            wy=wy, wxsize=wxsize, wysize=wysize, querysize=query_size,
        )

    @staticmethod
    def _scale_query_to_tile(dataset_query, dataset_tile, options, tile_file_name=""):
        """Scales down query dataset to the tile dataset"""

        query_size = dataset_query.RasterXSize
        tile_size = dataset_tile.RasterXSize
        tile_bands = dataset_tile.RasterCount

        if options.resampling == "average":

            # Function: gdal.RegenerateOverview()
            for i in range(1, tile_bands + 1):
                # Black border around NODATA
                res = gdal.RegenerateOverview(
                    dataset_query.GetRasterBand(i), dataset_tile.GetRasterBand(i), "average"
                )
                if res != 0:
                    # exit_with_error(
                    #     "RegenerateOverview() failed on %s, error %d" % (tilefilename, res)
                    # )
                    return

        elif options.resampling == "antialias" and numpy_available:

            if tile_file_name.startswith("/vsi"):
                raise Exception(
                    "Outputting to /vsi file systems with antialias mode is not supported"
                )

            # Scaling by PIL (Python Imaging Library) - improved Lanczos
            array = numpy.zeros((query_size, query_size, tile_bands), numpy.uint8)
            for i in range(tile_bands):
                array[:, :, i] = gdalarray.BandReadAsArray(
                    dataset_query.GetRasterBand(i + 1), 0, 0, query_size, query_size
                )
            im = Image.fromarray(array, "RGBA")  # Always four bands
            im1 = im.resize((tile_size, tile_size), Image.LANCZOS)
            if os.path.exists(tile_file_name):
                im0 = Image.open(tile_file_name)
                im1 = Image.composite(im1, im0, im1)

            params = {}
            if options.tiledriver == "WEBP":
                if options.webp_lossless:
                    params["lossless"] = True
                else:
                    params["quality"] = options.webp_quality
            im1.save(tile_file_name, options.tiledriver, **params)

        else:

            gdal_resampling = TileCreator._get_resampling(options.resampling)

            # Other algorithms are implemented by gdal.ReprojectImage().
            dataset_query.SetGeoTransform(
                (0.0, tile_size / float(query_size), 0.0, 0.0, 0.0, tile_size / float(query_size))
            )
            dataset_tile.SetGeoTransform((0.0, 1.0, 0.0, 0.0, 0.0, 1.0))

            res = gdal.ReprojectImage(dataset_query, dataset_tile, None, None, gdal_resampling)
            if res != 0:
                # exit_with_error(
                #     "ReprojectImage() failed on %s, error %d" % (tilefilename, res)
                # )
                pass

    @staticmethod
    def _get_resampling(str_resampling: str) -> int:
        if str_resampling == "near":
            return gdal.GRA_NearestNeighbour

        if str_resampling == "bilinear":
            return gdal.GRA_Bilinear

        if str_resampling == "cubic":
            return gdal.GRA_Cubic

        if str_resampling == "cubicspline":
            return gdal.GRA_CubicSpline

        if str_resampling == "lanczos":
            return gdal.GRA_Lanczos

        if str_resampling == "mode":
            return gdal.GRA_Mode

        if str_resampling == "max":
            return gdal.GRA_Max

        if str_resampling == "min":
            return gdal.GRA_Min

        if str_resampling == "med":
            return gdal.GRA_Med

        if str_resampling == "q1":
            return gdal.GRA_Q1

        if str_resampling == "q3":
            return gdal.GRA_Q3

        return 0

    @staticmethod
    def _get_pillow_image_resize_resampling(resampling: str) -> Literal[int]:
        if resampling == 'nearest' or resampling == 'near':
            return Image.NEAREST

        if resampling == 'average' or resampling == 'bilinear':
            return Image.BILINEAR

        if resampling == 'cubic' or resampling == 'cubicspline':
            return Image.BICUBIC

        if resampling == 'lanczos':
            return Image.LANCZOS

        if resampling == 'antialias':
            return Image.ANTIALIAS

        if resampling == 'max':
            return Image.MAXCOVERAGE

        if resampling == 'med':
            return Image.MEDIANCUT

        return Image.NEAREST

    @staticmethod
    def _get_transparent_tile() -> Image:
        return Image.new('RGBA', (256, 256), (255, 255, 255, 0))

    def _create_tile_if_file_tiles_exist(self, tile_request: TileCreateRequest):
        images: list[Image] = []
        for file in tile_request.files:
            image = self._get_file_tile_image_if_exists(tile_request, file)
            if image is None:
                return
            images.append(image)

        tile = Image.new('RGBA', (256, 256), (255, 255, 255, 0))
        for image in images:
            tile.paste(image, (0, 0), image)
            image.close()

        tile_path = tile_request.get_tile_path()
        os.makedirs(os.path.dirname(tile_path), exist_ok=True)
        tile.save(tile_path, "png")
        tile.close()
        for file in tile_request.files:
            tile_file_path = tile_request.get_file_tile_path(file)
            if os.path.exists(tile_file_path):
                os.remove(tile_file_path)
