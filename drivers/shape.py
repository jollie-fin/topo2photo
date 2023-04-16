import drivers.driver as driver
from utils.rasterio import warp_and_rasterize

class Shape2Raster(driver.RasterDriver):
    def __init__(self):
        driver.RasterDriver.__init__(self, 'rasterize')

    def renderToRaster(
        self,
        dst_ds,
        bands,
        shape_bounds,
        gps_bounds,
        key,
        options
    ):
        warp_and_rasterize(key, dst_ds=dst_ds, bands=bands, **options)

