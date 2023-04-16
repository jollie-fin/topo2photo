#!/usr/bin/python3
import sys
import array
import numpy as np
import argparse
import os.path
import math
import traceback
import importlib
import sentinel2.cloudless
from datetime import datetime
from affine import Affine

from shapely.affinity import affine_transform

from shapely import box
import rasterio as rio
from utils.rasterio import LOSSLESS_JPEG2000, allowedextension, makeintoshapelymatrix
from utils.grid import tileFixedTiles, equigrid
import utils.geo
import utils.misc as misc
from drivers.raster import Raster2Raster
from drivers.shape import Shape2Raster
from drivers.gmt import Gmt2Raster

from timeit import default_timer as timer

#import usgs2raster
driver_factory = misc.dotdict(
    gmt = lambda : Gmt2Raster(),
    raster = lambda : Raster2Raster(),
    shape = lambda : Shape2Raster(),
    sentinel2 = None,
)

allowed_extension_writing = ' ,'.join(map(lambda x:f'.{x}', utils.rasterio.allowedextension('w')))

def buildParser():
    parser = argparse.ArgumentParser(
        prog = sys.argv[0],
        description = 'Take multiple geotiff and shape, and warp them and export them to a folder file')
    parser.add_argument('--output', type=str, required=True, help="Folder output")
    parser.add_argument('--projection', type=str, default='EPSG:4326', help="Projection for GDAL")
    parser.add_argument('--area', dest='orig_bounds', type=float,
        metavar=("LONG_MIN", "LAT_MIN", "LONG_MAX", "LAT_MAX"), nargs=4,
        help="Area of interest")
    parser.add_argument('--deduce-area', dest='deduce_bounds', type=str,
        metavar=("SHAPE_FILE", "WHERE_REQUEST"), nargs=2,
        help="Area of interest, deduced from shape_file")
    parser.add_argument('--widen-area', dest='dilate', type=float,
        metavar="DILATE_COEFFICIENT", nargs=1, default=0,
        help="Dilate area of interest, in %")    
    parser.add_argument('--size', required=True, type=misc.str2ByteSize, metavar="SIZE", nargs=1,
        help="Size of the resulting image in pixels (k and )")
    parser.add_argument('--tiling', dest='tiling', type=int,
        metavar=("TILE_WIDTH", "TILE_HEIGHT", "MIN_OVERLAP"), nargs=3,
        help="Produce tiles of size TILE_WIDTHxTILE_HEIGHT overlapping by at least MIN_OVERLAP pixels",
        default=(-1,-1,0))
    parser.add_argument('--filename-format', type=str, dest='format_name', metavar='PYTHON_FORMAT_STRING', nargs=1,
        help="Describe what filename to use for output files. It will be parsed by str.format, "
             "with variables id, tile_x, tile_y, tile_x_count, tile_y_count,"
             "layer, width, height, total_width, total_height, now, extension. Supported extensions are "
             + allowed_extension_writing,
             default="{layer}.{extension}")
    parser.add_argument('--layer', dest='layer', type=str,
        metavar=("DRIVER KEY|FILENAME DTYPE LAYER BANDS EXTENSION", "OPTION[=VALUE]"),
        nargs='+', action="append",
        help=f"Raster to add to exr. Valid drivers are : {misc.getValid(driver_factory)}.")
    return parser

def main(argv):
    parser = buildParser()
    args = parser.parse_args(argv[1:])

    try:
        print(f"Creating drivers")

        def validate_layer(parameters):
            if len(parameters) < 6:
                raise Exception(f"At least 6 arguments are necessary for a layer")
            driver, key, dtype, layer, bands, extension, *options = parameters

            if driver not in driver_factory:
                raise Exception(f"Unknown driver {driver}")
            if driver_factory[driver] is None:
                raise Exception(f"Unsupported driver {driver}")

            bands = misc.decodeBands(bands)
            return misc.dotdict(
                driver=driver,
                key=key,
                dtype=np.dtype(dtype),
                layer=layer,
                count=max(bands),
                bands=bands,
                extension=extension,
                options=misc.dictFromOptions(options))

        layers = list(map(validate_layer, args.layer))

        print(f"Building drivers")
        drivers = {
            name:driver_factory[name]()
            for name in set(layer.driver for layer in layers)
        }

        print(f"Computing bounds for final image")
        projection_crs = utils.geo.getCrsFromInput(args.projection)
        shape_bounds,gps_bounds = utils.geo.deduceBoundsFromArgs(args, projection_crs)
        bounds = shape_bounds.bounds
        print(f"-- found : {bounds} with projection {projection_crs}")

        print(f"Computing final image dimensions")
        width, height = misc.computeWidthHeight(args.size[0], bounds)
        print(f"-- found : {width}x{height}")

        main_transform = rio.transform.from_bounds(
            *bounds,
            width,
            height)

        main_transform_gps = rio.transform.from_bounds(
            *gps_bounds.bounds,
            width,
            height)

        format_name = args.format_name[0]

        print("Preparing tiling")
        *tile_size, tile_overlap = args.tiling

        if tile_size == [-1,-1]:
            tile_size = (width, height)

        tiling = tileFixedTiles((0,0,width,height), tile_size, tile_overlap)
        _,_,(tile_x_count, tile_y_count),(tile_width, tile_height) = tiling

        option_rasterio = misc.dotdict(
            mode='w+',
            driver=None,
            sharing=False,
            width=tile_width,
            height=tile_height,
            crs=projection_crs,
        )

        print(f"{tile_x_count}x{tile_y_count} tiles will be used")
    except Exception as e:
        traceback.print_exception(e)
        parser.print_usage()
        sys.exit(1)
    
    output = args.output
    misc.createHierachy(output)

    shape_bounds_pixel_space = affine_transform(shape_bounds, makeintoshapelymatrix(~main_transform))

    starting_date = datetime.now().replace(microsecond=0).isoformat()

    for layer in layers:
        print(f"processing layer : {layer.layer}")

        for tile_id,tile in enumerate(equigrid(*tiling,
                shape_bounds=None and shape_bounds_pixel_space)):
            start = timer()
            (idx, idy), (l, b), (r, u) = tile
            tile_id += 1
            idx += 1
            idy += 1
            print(f"processing tile nb {tile_id}({idx},{idy}) out of {tile_x_count}x{tile_y_count}")
            filename = format_name.format(
                    id = tile_id,
                    tile_x = idx, tile_y = idy,
                    tile_x_count = tile_x_count, tile_y_count = tile_y_count,
                    layer = layer.layer,
                    width = tile_width, height = tile_height,
                    total_width = width, total_height = height,
                    now = starting_date,
                    extension = layer.extension,
                )
            path = os.path.join(output, filename)
            print(f"At filename {path}")

            # trans = Affine.translation(-l, -b)   
            # scale = Affine.scale(width / (r - l), height / (u - b))
            # sub_transform = main_transform * trans

            window_px = rio.windows.Window(l,b,width,height)
            sub_transform = rio.windows.transform(window_px, main_transform)
            window_box_px = box(l,b,r,u)
            window_box = affine_transform(window_box_px, makeintoshapelymatrix(~sub_transform))

            shall_patch_holes = 'fillnodata' in layer.options
            shall_scale_01 = 'scale01' in layer.options
            shall_scale_11 = 'scale11' in layer.options
            layer.options.pop('fillnodata', None)
            layer.options.pop('scale01', None)
            layer.options.pop('scale11', None)
            # if shall_scale_01 or shall_scale_11:
            #     if 'min_value' not in layer.dtype:
            #         raise Exception('impossible to scale down with this type')
            #     min_value, max_value = (layer.dtype['min_value'], layer.dtype['max_value'])

            print(f"-- Warping {layer.key}")
            option_image = (
                option_rasterio
                | misc.dotdict(
                    transform=sub_transform,
                    count=layer.count,
                    dtype=layer.dtype)
                | LOSSLESS_JPEG2000)
    
            with rio.open(path, **option_image) as dst_ds:
                drivers[layer.driver](
                    dst_ds,
                    layer.bands,
                    shape_bounds,
                    gps_bounds,# & window_box_gps,
                    layer.key,
                    layer.layer,
                    layer.options)

                if False:
                    data = {}
                    if shall_patch_holes:
                        print("-- patching holes in data")
                    if shall_scale_01:
                        print("-- scaling to [0,1]")
                    elif shall_scale_11:
                        print("-- scaling to [-1,1]")

                    for i, name in enumerate(layer_name):
                        np_array = dst_ds.read(i + 1)
                        if 'force_view' in layer.dtype:
                            np_array = np_array.view(layer.dtype['force_view'])
                        if shall_patch_holes:
                            no_data = db.GetNoDataValue()
                            np_array = util.fill(np_array, no_data)
                        np_array = np_array.astype(layer.dtype['np_dtype'])
                        if shall_scale_01:
                            np_array = (np_array - min_value)/(max_value - min_value)
                        elif shall_scale_11:
                            np_array = 2*(np_array - min_value)/(max_value - min_value)-1

                        histogram, base = np.histogram(np_array)
                        print(f"-- histogram from {base[0]} to {base[-1]} : "
                            f"{', '.join(str(i) for i in histogram)}")
                        data[name] = np_array.tobytes()
                    exr = OpenEXR.OutputFile(path, header)
                    exr.writePixels(data)
                print(f"-- finished in {timer()-start:.2f}s. Please check {path}")
            



if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "submodule":
        submodule = importlib.import_module(sys.argv[2])
        argv = list(sys.argv[2:])
        submodule.main(argv)
    else:
        main(sys.argv)
