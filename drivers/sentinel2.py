from osgeo import gdal
from datetime import datetime, timedelta
import drivers.driver as driver
import utils.misc as misc
import dotenv
import os
import time
import pprint
from sentinel2.google import GoogleStorageSentinel2
from sentinel2.scihub        import ScihubSentinel2Description, ScihubSentinel2Downloader
import sentinel2.utils as s2utils
from utils.grid import Sent2Grid

class Sentinel22Raster(driver.RasterDriver):
    def __init__(self):
        driver.RasterDriver.__init__(self, 'sentinel2')
        dotenv.load_dotenv()

        self.scihub = ScihubSentinel2Description()
        source = os.environ.get('SENTINEL2_SOURCE', 'google')
        if source == 'google':
            self.retriever = GoogleStorageSentinel2()
        elif source == 'scihub':
            self.retriever = ScihubSentinel2Downloader()
        else:
            raise Exception('source should be "google" or "scihub"')

    def computeBandResolutionToKeep(self, min_resolution):
        return [
            (band,resolution)
            for band in BANDS_TO_KEEP
            for resolution in bands_S2[band].resolutions
            if resolution >= min_resolution]

    def retrieveScene(self, scene, min_resolution):
        bands_resolutions = self.computeBandResolutionToKeep(min_resolution)
        retval = self.retriever(scene, bands_resolutions)
        return retval

    def retrieveScenes(self, scenes, min_resolution):
        return {scene['identifier'] : self.retrieveScene(scene, min_resolution)
                for scene in scenes}

    def getScenes(self, bbox):
        now = datetime.now()
        before = now - timedelta(days=30)
        scenes = self.scihub.getScenesDescription(bbox=bbox, dates=(datetime(2022,8,17),datetime(2022,8,19)), limit=10000, producttype=s2misc.productTypeFromDataset('1C'))
        return self.retrieveScenes(scenes, 10)

    def renderToRaster(
        self,
        dst_ds,
        bands,
        shape_bounds,
        gps_bounds,
        key,
        options
    ):
        tiles = Sent2Grid().getexactoverlap(
                shape_bounds,
                table='tile',
                getattribute='name')
        print(list(tiles))
        pass

def main(argv):
    dotenv.load_dotenv()
    sentinel2 = Sentinel22Raster(bands='1,2,3', layer='satview', dtype='uint16', source='google')
    sentinel2.getScenes(bbox = (35.5018,33.8938,35.5019,33.8939))
#    sentinel2.getScenes(bbox = (2.3522,48.8566,2.3523,48.8567))

