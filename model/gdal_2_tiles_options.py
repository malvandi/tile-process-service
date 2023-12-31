class GDAL2TilesOptions:
    tiledriver = 'PNG'
    tile_size: int = 256

    # average, near, bilinear, cubic, cubicspline, lanczos, mode, max, min, med, q1, q3
    resampling = 'average'
    zoom = [1, 26]
    kml = False
    verbose = False
    srcnodata = False
    s_srs = False
    profile = "mercator"
    webviewer = "none"
    resume = True
    title = ""
    quiet = False
    exclude_transparent = True
    url: str = ''