from utils.rasterio import warpmerge
import drivers.driver as driver
from rasterio.warp import Resampling

class Raster2Raster(driver.RasterDriver):
    def __init__(self):
        driver.RasterDriver.__init__(self, 'raster_layer')
        print(f"-- creating raster2raster driver")

    def renderToRaster(
        self,
        dst_ds,
        bands,
        shape_bounds,
        gps_bounds,
        key,
        options
    ):
        warpmerge(
            [key],
            dst_ds=dst_ds,
            bands=bands,
            **options)
