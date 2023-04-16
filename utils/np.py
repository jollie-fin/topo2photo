from osgeo import gdal
import numpy as np
from enum import Enum
import cv2

class MultibandImage(object):
    #dtype should only be wider, not smaller than original dtype
    def __init__(self, dtype, filenames):
        self.ds = []
        self.idx_to_ds = {}
        self.totallayer_count = 0
        self.width = 0
        self.height = 0

        for filename in filenames:
            ds = gdal.Open(filename['path'])

            self.ds.append(ds)
            for i in range(ds.RasterCount):
                idx = len(self.idx_to_ds)
                self.idx_to_ds[idx] = (
                    ds,
                    i + 1,
                    filename.get('interpolation',None),
                    filename.get('scale',None))
    
        self.totallayer_count = len(self.idx_to_ds)
        self.width = max(ds.RasterXSize for ds in self.ds)
        self.height = max(ds.RasterYSize for ds in self.ds)
        self.shape = (self.width, self.height, self.totallayer_count)
        self.np_array = np.zeros(shape=self.shape, dtype=dtype)
    
        for idx,(ds,i,interpolation,scale) in self.idx_to_ds.items():
            db = ds.GetRasterBand(i)
            array = db.ReadAsArray()
            src_type = array.dtype
            array = array.astype(dtype)

            if scale is not None:
                src_scale = np.iinfo(src_type).min, np.iinfo(src_type).max
                array = np.interp(array, src_scale, scale)

            dsize = self.np_array.shape[:-1]
            if array.shape != self.np_array.shape[:-1]:
                array = cv2.resize(array, dsize=dsize, interpolation=interpolation or cv2.INTER_NEAREST)               
            self.np_array[...,idx] = array

    def closeGdal():
        del self.ds
