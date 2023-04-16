from datetime import datetime, timedelta, date
import drivers.driver as driver
import utils.misc as misc
import os
from collections import defaultdict
from enum import Enum, auto
from sentinel2.google import GoogleStorageSentinel2
from sentinel2.scihub        import ScihubSentinel2Description, ScihubSentinel2Downloader
import sentinel2.utils as s2utils
import pickle
from sentinel2.utils import Dataset, BandEnum
from shapely import wkt
import pprint

class Sentinel2Retriever(object):
    def __init__(self, source):
        self.scihub = ScihubSentinel2Description()

        if source == 'google':
            self.retriever = GoogleStorageSentinel2()
        elif source == 'scihub':
            self.retriever = ScihubSentinel2Downloader()
        else:
            raise Exception('source should be "google" or "scihub"')

    def getDatasetMinresolution(self, bands, **kwargs):
        dataset = kwargs.get('dataset', None)
        min_resolution = kwargs.get('min_resolution', s2utils.MIN_RESOLUTION)
        if dataset is not None:
            if set(bands) - set(s2misc.getValidBands(dataset)):
                raise Exception(f'Invalid bands for {dataset}')
            if dataset == Dataset.L1C and min_resolution > s2utils.MIN_RESOLUTION:
                raise Exception(f'Min resolution can only be 10m with {dataset}')
        return dataset, min_resolution

    def getL1SceneFromL2Scene(self, bands, scene):
        dataset = s2misc.datasetFromScene(scene)
        if dataset == Dataset.L2A:
            l1scenes = self.scihub.getScenesDescription(
                level1cpdiidentifier=scene['level1cpdiidentifier'],
                producttype=s2misc.productTypeFromDataset(Dataset.L1C),
            )
            if len(l1scenes) != 1:
                raise Exception("Impossible to find scene as L1")
            l1scene = l1scenes[0]
        else:
            l1scene = scene 
        dataset = s2misc.datasetFromScene(l1scene)

        l1scenelocation = self.retrieveScene(l1scene, bands, s2utils.MIN_RESOLUTION)
        return (l1scene,
        {
            band:location
            for (band,_),location in l1scenelocation.items()
        })
   
    def getBoaOrRadioOffset(self, scene, bands):
        self.retrieveScene(scene, (), None)
        dataset = s2misc.datasetFromScene(scene)
        tree = s2misc.xmlTreeMTDFromScene(scene, s2misc.getLRUPath(dataset))
        if dataset == Dataset.L2A:
            key = 'BOA_ADD_OFFSET'
        else:
            key = 'RADIO_ADD_OFFSET'
        retval = defaultdict(int)
        for band in bands:
            elements = tree.findall(f".//{key}[@band_id='{s2misc.getBandId(dataset,band)}']")
            for element in elements:
                retval[band] = int(element.text)
        return retval

    def retrieveScene(self, scene, bands, min_resolution):
        print(f"--   Retrieving {scene['identifier']}")
        bands_resolutions = s2misc.computeBandResolutionToKeep(
            scene=scene,
            min_resolution=min_resolution,
            bands=bands)

        retval = self.retriever(scene, bands_resolutions)
        return retval

    def retrieveScenes(self, scenes, bands, min_resolution):
        return [{'scene':scene,
                 'file_location':
                    self.retrieveScene(
                        scene=scene,
                        min_resolution=min_resolution,
                        bands=bands)
                }
                for scene in scenes]

    def getScenesDescription(self, bands, **kwargs):
        dataset, min_resolution = self.getDatasetMinresolution(bands, **kwargs)
        kwargs.pop('dataset',None)
        kwargs.pop('min_resolution',None)
        if dataset is not None:
            kwargs['producttype'] = s2misc.productTypeFromDataset(dataset)
        return self.scihub.getScenesDescription(**kwargs)

    def getScenes(self, bands, **kwargs):
        _, min_resolution = self.getDatasetMinresolution(bands, **kwargs)
        scenes = self.getScenesDescription(bands, **kwargs)
        return self.retrieveScenes(scenes, bands, min_resolution)

def main(argv):
    sentinel2 = Sentinel2Retriever(source='google')

    if True:
        result = sentinel2.getScenes(
            bands=[BandEnum.TCI],#getValidBands(Dataset.L1C),
            bbox=(-90,39,90,39.5),
            dataset=Dataset.L2A,
            relativeorbitnumber=107,#36,
            dates=(datetime(2022,1,1), datetime(2022,1,5)),
            min_resolution=60,
            )
        pprint.pprint(result)
        with open('relative_orbit.pickle','wb') as f:
            pickle.dump(result, f)    
    if False:
        result = sentinel2.getScenes(
            bands=[BandEnum.TCI],#getValidBands(Dataset.L1C),
            bbox=(4.8357,45.7640,4.8358,45.7641),
            cloudcoverpercentage=(0.,2.),
            dataset=Dataset.L2A,
#            dates=(datetime.now()-timedelta(days=30), datetime.now()),
            months=[7],
            min_resolution=10,
            )
        with open('scenes.pickle','wb') as f:
            pickle.dump(result, f)    
    if False:
        result = sentinel2.getScenesDescription(
            bands=[BandEnum.TCI, BandEnum.B02, BandEnum.B03, BandEnum.B04, BandEnum.SCL],#getValidBands(Dataset.L1C),
            bbox=(4.8357,45.7640,4.8358,45.7641),
            dataset=Dataset.L2A,
            min_resolution=60,
            limit=10000)
        with open('lyon_over_a_year.pickle','wb') as f:
            pickle.dump(result, f)    
#    sentinel2.getScenes(bbox = (2.3522,48.8566,2.3523,48.8567))

