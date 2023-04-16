import sentinel2.utils as s2utils
from sentinel2.utils import BandEnum, Dataset
from sentinel2.retriever import Sentinel2Retriever
import utils.lru as lru
from datetime import datetime
from s2cloudless import S2PixelCloudDetector
import numpy as np
import rasterio as rio
from rasterio.warp import reproject, Resampling, Affine
import math
from skimage import filters

import matplotlib.pyplot as plt

from timeit import default_timer as timer

BANDS_FOR_CLOUDLESS = (
    BandEnum.B01,
    BandEnum.B02,
    BandEnum.B04,
    BandEnum.B05,
    BandEnum.B08,
    BandEnum.B8A,
    BandEnum.B09,
    BandEnum.B10,
    BandEnum.B11,
    BandEnum.B12,
)

DN_TO_REFLECTANCE = 10000
TILESIZE = 109800
EXTENSION = '.tif'
AVERAGE_OVER = 220
DILATATION_SIZE = 110

class Sentinel2Cloudless(object):
    def __init__(self, source, resolution=160):
        self.retriever = Sentinel2Retriever(source)
        self.lru = lru.get('sentinel2CLD')
        self.dataset = Dataset.L1C
        self.resolution = resolution
        self.cloud_detector = S2PixelCloudDetector(
            threshold=0.4,
            average_over=math.ceil(AVERAGE_OVER/self.resolution),
            dilation_size=math.ceil(DILATATION_SIZE/self.resolution),
        )

    def getBias(self, scene):
        return self.retriever.getBoaOrRadioOffset(scene=scene, bands=BANDS_FOR_CLOUDLESS)

    def buildStack(self,l1scenelocation, bias):
        width = TILESIZE // self.resolution
        band_to_slice = {b:i for i,b in enumerate(BANDS_FOR_CLOUDLESS)}

        transform = None
        crs = None

        print(f'--   building stack')
        retval = np.empty((1,width,width,len(BANDS_FOR_CLOUDLESS)),dtype=np.float32)
        for band, path in l1scenelocation.items():
            with rio.open(path) as scl:
                if transform is None:
                    transform = scl.transform
                    transform = Affine(
                        transform[0] * scl.width / width,
                        transform[1],
                        transform[2],
                        transform[3],
                        transform[4] * scl.height / width,
                        transform[5])
                    transform = scl.transform
                    crs = scl.crs
                retval[0,:,:,band_to_slice[band]] = scl.read(out_shape=retval.shape[1:-1],resampling=Resampling.bilinear)
        bias = np.array([bias[b] for b in BANDS_FOR_CLOUDLESS]).reshape([1,1,1,len(BANDS_FOR_CLOUDLESS)])
        retval = (retval.astype(np.float32) + bias) / DN_TO_REFLECTANCE

        print(f'--     done')
        return retval, transform, crs

    def computeCloudPrb(self, l2scene, probability_path):
        print(f'-- computing cloud probability')
        print(f'--   retrieving matching l1scene')
        l1scene, l1scenelocation = self.retriever.getL1SceneFromL2Scene(scene=l2scene,bands=BANDS_FOR_CLOUDLESS)
        bias = self.getBias(l1scene)

        data, transform, crs = self.buildStack(l1scenelocation, bias)

        print(f'--   computing cloud probability')
        cloud_probs = self.cloud_detector.get_cloud_probability_maps(data)

        with rio.open(
                probability_path,
                "w",
                compress="lzw",
                height=cloud_probs.shape[2],
                width=cloud_probs.shape[1],
                count=1,
                crs=crs,
                transform=transform,
                dtype=rio.float32) as dest:
            dest.write(cloud_probs*10000,indexes=[1])
        return True

    def getCloudPrbPath(self, scene):
        dataset = s2misc.datasetFromScene(scene)
        fn = f"{scene['level1cpdiidentifier']}/CLDPRB{EXTENSION}"
        return self.lru.retrieve(
            fn,
            retrieve_func=lambda path, fn: self.computeCloudPrb(scene, path),
            )
    
    def getCloudMask(self, scene, shape=None, threshold=None):
        path = self.getCloudPrbPath(scene)
        with rio.open(path) as probability:
            clouds = probability.read(1, out_shape=shape, resampling=Resampling.bilinear)
            threshold = filters.threshold_otsu(clouds) if threshold is None else threshold
            return self.cloud_detector.get_mask_from_prob(clouds[np.newaxis,...], threshold=threshold)


def main(argv):
    print(-1)
    cloudless = Sentinel2Cloudless(source='google',resolution=160)
    scene = {
        'level1cpdiidentifier': 'S2B_OPER_MSI_L1C_TL_2BPS_20220818T091321_A028461_T36SYC_N04.00',
        'processinglevel': 'Level-2A',
        'identifier': 'S2B_MSIL2A_20220818T081609_N0400_R121_T36SYC_20220818T100935',
    }

    path = cloudless.getCloudPrbPath(scene)
    print(path)
