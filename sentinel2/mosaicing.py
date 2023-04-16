from suncalc import get_position
from enum import Enum
import numpy as np
#import cv2
import glob
from numpy.lib.stride_tricks import sliding_window_view
import re
from datetime import datetime
import time
import math
import pickle
import sentinel2.utils as s2utils
from sentinel2.utils import SclEnum, BandEnum
import pprint
from sentinel2cloudless import Sentinel2Cloudless
import rasterio as rio
import matplotlib.pyplot as plt
from rasterio.plot import show

class ImageStacking(object):
    def __init__(self, dtype=None, invalid_values=None, authorized_scl_values=None):
        self.dtype = dtype
        self.invalid_values = invalid_values
        self.authorized_scl_values = authorized_scl_values
        self.cloudlessretriever = Sentinel2Cloudless('google')

    def open(self, fn, output_shape=None):
        with rio.open(fn) as f:
            return f.read(out_shape=output_shape)

    def shrink(self, selection, radius):
        kernel = np.array([[x**2+y**2 for x in range(-radius,radius+1)]
                        for y in range(-radius,radius+1)]) < (radius+1)**2
        kernel = np.uint8(kernel)
        return cv2.erode(selection.astype(np.uint8), kernel, iterations=1) != 0
    
    def collapseMask(self, array):
        if len(array.shape) > 2:
            array = np.amax(array, axis=2)
        return np.repeat(array[:,:,np.newaxis],3,axis=2)

    def getvalid(self, array):
        if self.invalid_values:
            return np.invert(self.collapseMask(np.isin(array, self.invalid_values)))
        else:
            return True

    def getscl(self, scl, shape):
        if scl and self.authorized_scl_values:
            array = cv2.resize(self.open(scl), shape[:2], cv2.INTER_NEAREST)
#            self.showSCL(array)
            isin = self.collapseMask(np.isin(array, [scl_value.value for scl_value in self.authorized_scl_values]))
            return isin
        else:
            return True

    def getmask(self, scl, array):
        print(f'Computing mask')
        a = self.getvalid(array)
        b = self.getscl(scl, array.shape)
#        print(np.count_nonzero(a), np.count_nonzero(b), np.count_nonzero(array))
#        self.showImg('a',a)
#        self.showImg('b',b)
        retval = np.bitwise_and(b,a)
        print(f'  Done')
        return retval

    def openarray_and_filter(self, filename, scl):
        array = self.open(filename)
        return array, self.getmask(scl, array)

    def show(self, numerator, denominator):
        denominator[denominator == 0] = 1
        cv2.imshow('merged', numerator/denominator/256)
        cv2.waitKey(0)
    def showSCL(self, mask):
        cv2.imshow('merged', (mask.astype(np.uint16)*255/11).astype(np.uint8))
        cv2.waitKey(0)
    def showImg(self, name, img):
        if img.dtype == np.bool_:
            cv2.imshow(name, img.astype(np.uint8)*255)
        elif img.dtype == np.int16:
            cv2.imshow(name, (img//256).astype(np.int8))
        elif img.dtype == np.uint16:
            cv2.imshow(name, (img//256).astype(np.uint8))
        elif img.dtype == np.float64:
            cv2.imshow(name, img.astype(np.uint8))
        else:
            cv2.imshow(name, img)

    def merge2(self, dest, filenames, scls = None):
        scls = scls or [None] * len(filenames)
        original = self.open(filenames[0])
        numerator = np.zeros(original.shape,self.dtype)
        total_pixels_count =  np.product(numerator.shape)
        is_valid = self.getmask(scls[0], original).view(np.uint8)
        # cv2.imshow('merged',(is_valid.astype(np.uint8)*255)[...,None])
        # cv2.waitKey(1000)
        denominator = is_valid
        print(np.histogram(denominator))
        print(is_valid.shape, original.shape)
        numerator += cv2.bitwise_and(original,original, mask=is_valid)
        self.show(numerator, denominator)
        print(np.count_nonzero(denominator) * 100 / total_pixels_count)
        
        for filename,scl in zip(filenames[1:], scls[1:]):
#            self.show(numerator, denominator)
            array, is_valid = self.openarray_and_filter(filename, scl)
            is_valid = is_valid.view(np.uint8)
            numerator += cv2.bitwise_and(array,array, mask=is_valid)
            denominator += is_valid
            self.show(numerator, denominator)

        invalid_pixels = denominator == 0
        invalid_pixels_count = np.count_nonzero(invalid_pixels)
        total_pixels_count =  np.product(numerator.shape)
        print(invalid_pixels_count * 100 / total_pixels_count)

        denominator[invalid_pixels.squeeze()] = 1
        numerator[invalid_pixels] = original[invalid_pixels]
        retval = (numerator/denominator).astype(np.uint8)
        print(np.histogram(numerator))
        print(np.histogram(denominator))
        print(np.histogram(retval))
        cv2.imshow('merged', retval/256)
        cv2.waitKey(5000)
        return retval

    def mergeSCL(self, filenames, scls = None, cldprbs = None):
        scls = scls or [None] * len(filenames)
        cldprbs = cldprbs or [None] * len(filenames)
        result = self.open(filenames[0])
        is_invalid = ~self.getmask(scls[0], result)
        total_pixels_count =  np.product(is_invalid.shape)

        result = np.zeros(result.shape, dtype=result.dtype)
        is_invalid = np.ones(is_invalid.shape, dtype=is_invalid.dtype)

        for filename,scl,cldprb in zip(filenames, scls, cldprbs):
            cld_prb = self.open(cldprb)
            if not np.count_nonzero(cld_prb):
                continue
            array, local_is_valid = self.openarray_and_filter(filename, scl)          

            local_is_valid = self.shrink(local_is_valid, 5)

            self.showImg('new_image',array)
            self.showImg('accumul',result)
            self.showImg('accumul_invalid',is_invalid)
#            (h, w) = array.shape[:2]
            print('Merging')
#            result = cv2.seamlessClone(array, result, is_invalid, (w//2, h//2), cv2.MIXED_CLONE)
            result = np.where(is_invalid & local_is_valid, array, result)
            self.showImg('result',result)
            local_is_invalid = ~local_is_valid
            self.showImg('local_invalid',local_is_invalid)
            is_invalid = is_invalid & local_is_invalid
            print('  Done')
            self.showImg('new_accumul_invalid',is_invalid)
            cv2.waitKey(0)
        invalid_pixels_count = np.count_nonzero(is_invalid)

        print(invalid_pixels_count * 100 / total_pixels_count)
        return result

    def sensorTime(self, filename):
        timere = re.compile(r'S2._MSIL2A_(\d{8}T\d{6})_')
        timestr = timere.search(filename).group(1)
        return datetime(*time.strptime(timestr, "%Y%m%dT%H%M%S")[:-2])
        
    def merge4(self, filenames, nirs = None, scls = None, cldprbs = None):
        CLOUD_FILTER = 60
        CLD_PRB_THRESH = 1
        NIR_DRK_THRESH = 0.15
        CLD_PRJ_DIST = 1000/60
        BUFFER = 50

        result = np.zeros(self.open(filenames[0]).shape, dtype=np.uint8)
        is_invalid = np.ones(self.open(cldprbs[0]).shape, np.bool_)

        last_array = None
        for filename,nir,scl,cldprb in zip(filenames, nirs, scls,cldprbs):
            array = self.open(filename)
            if last_array is not None:
                self
            last_array = array


        for filename,nir,scl,cldprb in zip(filenames, nirs, scls,cldprbs):
            cld_prb = self.open(cldprb)
            if not np.count_nonzero(cld_prb):
                continue
            array = self.open(filename)
            nir = self.open(nir)
            scl = self.open(scl)

            not_water = scl != 6
            clouds = cld_prb > CLD_PRB_THRESH
            SR_BAND_SCALE = 1e4
            dark = np.bitwise_and(nir < SR_BAND_SCALE * NIR_DRK_THRESH, not_water)

            date = self.sensorTime(filename)
            print(date)
            lon = 35.5018
            lat = 33.8938
            shadow_azimuth = 90 - get_position(date, lon, lat)['azimuth']

            M = np.float32([
                [1, 0, -CLD_PRJ_DIST*math.sin(math.pi * shadow_azimuth / 180)],
                [0, 1, -CLD_PRJ_DIST*math.cos(math.pi * shadow_azimuth / 180)]
            ])

            shadow = np.bitwise_and(cv2.warpAffine(clouds.astype(np.uint8), M, clouds.shape[:2]) > 0, dark)

            self.showImg('src',array)
            print(array.dtype, array.shape)         
            array = np.where(clouds, np.uint8([255,0,0]), array)
            print(array.dtype, array.shape)         
            self.showImg('masked_clouds',array)
            array = np.where(shadow, np.uint8([0,255,0]), array)           
            self.showImg('masked_shadow',array)


            cv2.waitKey(0)
        invalid_pixels_count = np.count_nonzero(is_invalid)

        print(invalid_pixels_count * 100 / total_pixels_count)
        return result

    def mergemedian(self, filenames, nirs = None, scls = None, cldprbs = None):
        arrays = np.stack([self.open(filename) for filename in filenames[:10]])
        print(arrays.shape)
        result = np.percentile(arrays, 20, axis=0)
#        result = np.amin(arrays, axis=0)
        print(result.dtype)
        self.showImg('result', result)
        cv2.waitKey(0)
        return result

    def mergetomiddle(self, filenames, nirs = None, scls = None, cldprbs = None):
        target = 128
        accumul = np.zeros([1830,1830,3], dtype=np.int16)

        for filename in filenames:
            array = self.open(filename).astype(np.int16)
            distance_array = np.abs(array - target)
            distance_accu = np.abs(accumul - target)
            accumul = np.where(distance_array < distance_accu, array, accumul)
            self.showImg('current', accumul.astype(np.uint8))
            cv2.waitKey(0)

        return accumul.astype(np.uint8)

    def mergeWithCloudPrb(self, scenes_filelocations):
        shape = (3, 10980, 10980)
        valid = np.zeros(shape=shape[1:], dtype=np.bool_)
        result = np.zeros(shape=shape, dtype=np.uint8)

        def showmask(img,ax=None):
            convert_to_rgb = np.repeat(img[np.newaxis,...]*255, 3, axis=0)
            show(convert_to_rgb, ax=ax)

        black = np.zeros(shape=(1,shape[1],shape[2]), dtype=np.uint8)
        for scene_location in scenes_filelocations:
            good = np.count_nonzero(valid)
            total = valid.size
            print(f"{100*good//total}%")
            if good/total > .995:
                break
            scene = scene_location['scene']
            location = scene_location['file_location']
            tci = self.open(location[(BandEnum.TCI, 10)], output_shape=shape)
            is_cloud = self.cloudlessretriever.getCloudMask(scene, shape=shape[1:]) != 0
            is_invalid = is_cloud | np.all(tci == black, axis=0)
            new_result = np.where(valid, result, tci)
            new_valid = valid | ~is_invalid

            result = new_result
            valid = new_valid
        return result

def main(argv):
    with open('scenes.pickle','rb') as f:
        scenes_filelocations = pickle.load(f)
    scenes_filelocations.sort(key=lambda s:s['scene']['beginposition'])
    for scene_filelocation in scenes_filelocations:
        scene = scene_filelocation['scene']
        print(scene['relativeorbitnumber'])
        print(scene['beginposition'])
    
    image_stacking = ImageStacking()
    result = image_stacking.mergeWithCloudPrb(scenes_fildelocations)
    print(result)
    with rio.open(
        'test.jp2',
        'w',
        compress="lzw",
        height=result.shape[2],
        width=result.shape[1],
        count=result.shape[0],
        crs=None,
        transform=None,
        dtype=rio.uint8) as f:
        #f.write(result)
        pass
        