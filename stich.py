import utils.rasterio
import glob
import sys
import rasterio as rio
from rasterio import CRS, warp

dst_crs = CRS.from_epsg(4326)

width=10000
height=10000

def method(merged_data, new_data, merged_mask, new_mask, index, roff, coff):
    print('merged',merged_data.shape)
    print('new_data',new_data.shape)
    print('merged_msk',merged_mask.shape, merged_mask.dtype)
    print('new_data_msk',new_mask.shape)
    print('index', index, 'roff', roff, 'coff', coff)
    print()

bounds = None
files = list(glob.glob("strip/T*_TCI_60m.jp2"))
for f in files:
    with rio.open(f, "r") as ds:
        new_bounds = warp.transform_bounds(ds.crs,dst_crs,*ds.bounds)
        if bounds is None:
            bounds = list(new_bounds)
        print(f, new_bounds)
        bounds[0] = min(bounds[0], new_bounds[0])
        bounds[1] = min(bounds[1], new_bounds[1])
        bounds[2] = max(bounds[2], new_bounds[2])
        bounds[3] = max(bounds[3], new_bounds[3])
print(bounds)
transform = rio.transform.from_bounds(*bounds, width, height)

with rio.open("stich.jp2","w+",crs=dst_crs,width=width,height=height,count=3,dtype=rio.uint8,transform=transform) as dst_ds:
    utils.rasterio.warpmerge(files, dst_ds)
