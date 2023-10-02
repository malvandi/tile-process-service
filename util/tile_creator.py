import os

import numpy
from PIL import Image
from gdal2tiles import GDAL2Tiles, TileJobInfo
from osgeo import gdal
import osgeo.gdal_array as gdalarray
from osgeo_utils.gdal2tiles import numpy_available, TileDetail

from model.gdal_2_tiles_options import GDAL2TilesOptions
from model.rabbit_message import TileCreateRequest


class TileCreator:
    _gdal2tilesEntries = dict()

    def create_tile(self, tile_request: TileCreateRequest):

        tile_file_path = tile_request.get_tile_path()

        if os.path.exists(tile_file_path):
            return

        os.makedirs(os.path.dirname(tile_file_path), exist_ok=True)

        gdal_2_tiles: GDAL2Tiles = self._get_gdal_2_tile_entry(tile_request)

        tile_job_info = self._get_tile_job_info(gdal_2_tiles)

        tile_detail = self._get_tile_detail(gdal_2_tiles, tile_request)
        self._create_tile(tile_job_info, tile_detail, tile_request)

    def _create_tile(self, tile_job_info: TileJobInfo, tile_detail: TileDetail, tile_request: TileCreateRequest):

        tile_file_path = tile_request.get_tile_path()

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

    def _get_gdal_2_tile_entry(self, tile_request: TileCreateRequest) -> GDAL2Tiles:
        key = tile_request.get_raster_file_path() + '_' + tile_request.get_directory_path()

        entry = self._gdal2tilesEntries.get(key)
        if entry:
            return entry

        options = GDAL2TilesOptions()
        options.zoom = [tile_request.z, tile_request.z]
        gdal_to_tiles = GDAL2Tiles(tile_request.get_raster_file_path(), tile_request.get_directory_path(), options)
        gdal_to_tiles.open_input()
        gdal_to_tiles.generate_metadata()
        self._gdal2tilesEntries[key] = gdal_to_tiles
        return gdal_to_tiles

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
