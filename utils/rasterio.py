import utils.geo
import rasterio as rio
import numpy as np

from utils.misc import str2Resampling
from rasterio import vrt, merge, features, warp
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from rasterio.drivers import raster_driver_extensions, is_blacklisted

import warnings
from timeit import default_timer as timer
import contextlib

LOSSLESS_JPEG2000 = dict(QUALITY=100, REVERSIBLE='YES',TILED='YES',COMPRESS='DEFLATE')
def temporarydataset(
        width,
        height,
        crs,
        nodata=None,
        transform=None,
        bounds=None,
        count=1,
        dtype=rio.uint8,
        *args,
        **kwargs):
    memfile = MemoryFile()

    transform = rio.transform.from_bounds(*bounds, width, height) if transform is None else transform
    ds = memfile.open(
            width=width,
            height=height,
            count=count,
            crs=crs,
            transform=transform,
            dtype=dtype,
            *args,
            **kwargs,
        )
    return ds

def warpmerge(filenames, dst_ds, bands=None, resampling=None, **kwargs):
    src_bands = list(range(1, dst_ds.count + 1))
    dst_bands = src_bands if bands is None else bands

    resampling = Resampling.nearest if resampling is None else resampling
    if isinstance(resampling, str):
        resampling = str2Resampling(resampling)
    resampling_str = resampling.name
    if not filenames:
        out = np.zeros(dst_ds.shape, dtype=dst_ds.dtypes[0])
        dst_ds.write(out, range(1,1+dst_ds.count))
        return dst_ds

    with contextlib.ExitStack() as stack:
#        print(f'-- warping {",".join(filenames)} into {dst_ds.name} with crs "{dst_ds.crs}" with bounds "{dst_ds.bounds}"')
        args = ['gdalwarp', '-t_srs', dst_ds.crs, *filenames, dst_ds.name]
        args_str = map(lambda x:f"'{x}'",args)
        # print(' '.join(args_str))
        dss = [
            stack.enter_context(
                rio.open(filename))
            for filename in filenames]
        warped = [
            stack.enter_context(
                vrt.WarpedVRT(
                    ds,
                    crs=dst_ds.crs,
                    resampling=resampling,
                    )) for ds in dss]
        src_dtype = dss[0].dtypes[0]
        dst_dtype = dst_ds.dtypes[0]
        for src_band, dst_band in zip(src_bands, dst_bands):
            print(f'--       Warping band {src_band} into {dst_band}')
            dest, _ = merge.merge(
                warped,
                bounds=dst_ds.bounds,
                resampling=resampling,
                indexes=[src_band],
                dtype=np.float32,
                **kwargs)
            if (src_dtype != dst_dtype):
                min_src = np.iinfo(src_dtype).min
                max_src = np.iinfo(src_dtype).max
                min_dst = np.iinfo(dst_dtype).min
                max_dst = np.iinfo(dst_dtype).max
                print(min_src,max_src,min_dst, max_dst)
                print(np.histogram(dest))
                dest = (np.maximum(dest,min_src) - min_src) / (max_src - min_src) * (max_dst - min_dst) + min_dst
                print(np.histogram(dest))
            dst_ds.write(dest[0], dst_band)
    return dst_ds

def warp_and_rasterize(src, dst_ds, bands=[1], where=None, getattribute=None, **rasterize_options):
    features_list = list(utils.geo.readFeaturesFromShapeFile(
            src,
#            where=where,
#            bounds=dst_ds.bounds,
            crs=dst_ds.crs,
            getattribute=getattribute))
    print(features_list)    
    if 'default_value' in rasterize_options:
        rasterize_options['default_value'] = int(rasterize_options['default_value'])
    if 'fill' in rasterize_options:
        rasterize_options['fill'] = int(rasterize_options['fill'])

    if not features_list:
        out = np.zeros(dst_ds.shape, dtype=dst_ds.dtypes[0])
    else:
        out = features.rasterize(
            features_list,
            out_shape=dst_ds.shape,
            transform=dst_ds.transform,
            dtype=dst_ds.dtypes[0],
            **rasterize_options
        )
    for band in bands:
        dst_ds.write(out, band)
    return dst_ds

def allowedextension(mode):
    return filter(lambda extension: not is_blacklisted(extension, mode),
           raster_driver_extensions().keys())

def makeintoshapelymatrix(transform):
    rasterio_to_shapely_order = [0,1,3,4,2,5]
    return list(transform[i] for i in rasterio_to_shapely_order)
