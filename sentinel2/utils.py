import os
import utils.misc as misc
import utils.lru as lru
import re
import shapely as shp
from enum import Enum, auto
from collections import namedtuple
import xml.etree.ElementTree as ET

class SclEnum(Enum):
    NO_DATA = 0
    SATURATED_OR_DEFECTIVE = 1 
    CAST_SHADOWS = 2 
    CLOUD_SHADOWS = 3 
    VEGETATION = 4 
    NOT_VEGETATED = 5 
    WATER = 6 
    UNCLASSIFIED = 7 
    CLOUD_MEDIUM_PROBABILITY = 8 
    CLOUD_HIGH_PROBABILITY = 9 
    THIN_CIRRUS = 10
    SNOW_OR_ICE = 11

class Dataset(Enum):
    L2A = auto()
    L1C = auto()

class BandEnum(Enum):
    B01 = auto()
    B02 = auto()
    B03 = auto()
    B04 = auto()
    B05 = auto()
    B06 = auto()
    B07 = auto()
    B08 = auto()
    B8A = auto()
    B09 = auto()
    B10 = auto()
    B11 = auto()
    B12 = auto()
    AOT = auto()
    TCI = auto()
    WVP = auto()
    SCL = auto()
#    CLDPRB = auto()

class SclEnum(Enum):
    NO_DATA = 0
    SATURATED_OR_DEFECTIVE = 1 
    CAST_SHADOWS = 2 
    CLOUD_SHADOWS = 3 
    VEGETATION = 4 
    NOT_VEGETATED = 5 
    WATER = 6 
    UNCLASSIFIED = 7 
    CLOUD_MEDIUM_PROBABILITY = 8 
    CLOUD_HIGH_PROBABILITY = 9 
    THIN_CIRRUS = 10
    SNOW_OR_ICE = 11

Band = namedtuple('band', ('id', 'band', 'resolutions', 'central_wl', 'bandwidth', 'description'))

# From https://en.wikipedia.org/wiki/Sentinel-2
# From https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi/definitions
# From https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-2_Level-2A_specs.html
valid_bands_S2 = {
Dataset.L2A:{
    BandEnum.B01 : Band(0,    'B01', [      60], 442.7 ,  21  , 'Coastal aerosol'),
    BandEnum.B02 : Band(1,    'B02', [10,20,60], 492.4 ,  66  , 'Blue'),
    BandEnum.B03 : Band(2,    'B03', [10,20,60], 559.8 ,  36  , 'Green'),
    BandEnum.B04 : Band(3,    'B04', [10,20,60], 664.6 ,  31  , 'Red'),
    BandEnum.B05 : Band(4,    'B05', [   20,60], 704.1 ,  15  , 'Vegetation red edge'),
    BandEnum.B06 : Band(5,    'B06', [   20,60], 740.5 ,  15  , 'Vegetation red edge'),
    BandEnum.B07 : Band(6,    'B07', [   20,60], 782.8 ,  20  , 'Vegetation red edge'),
    BandEnum.B08 : Band(7,    'B08', [10      ], 832.8 ,  106 , 'NIR'),
    BandEnum.B8A : Band(8,    'B8A', [   20,60], 864.7 ,  21  , 'Narrow NIR'),
    BandEnum.B09 : Band(9,    'B09', [      60], 945.1 ,  20  , 'Water vapour'),
    BandEnum.B11 : Band(11,   'B11', [   20,60], 1613.7,  91  , 'SWIR'),
    BandEnum.B12 : Band(12,   'B12', [   20,60], 2202.4,  175 , 'SWIR'),
    BandEnum.AOT : Band(None, 'AOT', [10,20,60], None, None, 'Aerosol Optical Thickness'),
    BandEnum.TCI : Band(None, 'TCI', [10,20,60], None, None, 'True Colour Image'),
    BandEnum.WVP : Band(None, 'WVP', [10,20,60], None, None, 'Water Vapour'),
    BandEnum.SCL : Band(None, 'SCL', [   20,60], None, None, 'Scene Classification map (SCL)'),
#    BandEnum.CLDPRB : Band(None, 'CLDPRB', [   20,60], None, None, 'Cloud probability'),
},
Dataset.L1C:{
    BandEnum.B01 : Band(0,  'B01', [      60], 442.7 ,  21  , 'Coastal aerosol'),
    BandEnum.B02 : Band(1,  'B02', [10      ], 492.4 ,  66  , 'Blue'),
    BandEnum.B03 : Band(2,  'B03', [10      ], 559.8 ,  36  , 'Green'),
    BandEnum.B04 : Band(3,  'B04', [10      ], 664.6 ,  31  , 'Red'),
    BandEnum.B05 : Band(4,  'B05', [   20   ], 704.1 ,  15  , 'Vegetation red edge'),
    BandEnum.B06 : Band(5,  'B06', [   20   ], 740.5 ,  15  , 'Vegetation red edge'),
    BandEnum.B07 : Band(6,  'B07', [   20   ], 782.8 ,  20  , 'Vegetation red edge'),
    BandEnum.B08 : Band(7,  'B08', [10      ], 832.8 ,  106 , 'NIR'),
    BandEnum.B8A : Band(8,  'B8A', [   20   ], 864.7 ,  21  , 'Narrow NIR'),
    BandEnum.B09 : Band(9,  'B09', [      60], 945.1 ,  20  , 'Water vapour'),
    BandEnum.B10 : Band(10, 'B10', [      60], 1373.5,  31  , 'SWIR – Cirrus'),
    BandEnum.B11 : Band(11, 'B11', [   20   ], 1613.7,  91  , 'SWIR'),
    BandEnum.B12 : Band(12, 'B12', [   20   ], 2202.4,  175 , 'SWIR'),
}
}

MIN_RESOLUTION = 10
FILETYPE = 'jp2'

def getLRU(dataset):
    return lru.get(f'sentinel2{dataset.name}')

def getLRUPath(dataset):
    return getLRU(dataset).getPath()

get_tile_re = re.compile(r"_T(\d{2})([A-Z])([A-Z]{2})_")
def getTileInfoFromIdentifier(identifier):
    global get_tile_re
    (utmzone, latitude_band, grid_square) = get_tile_re.search(identifier).groups()
    return utmzone, latitude_band, grid_square

def identifier2Safe(identifier, dataset):
    (utmzone, latitude_band, grid_square) = getTileInfoFromIdentifier(identifier)
    base = 'L2/' if dataset == Dataset.L2A else ''
    return f"{base}tiles/{utmzone}/{latitude_band}/{grid_square}/{identifier}.SAFE"

def identifier2Basedir(identifier,dataset,add_safe=False):
    (utmzone, latitude_band, grid_square) = getTileInfoFromIdentifier(identifier)

    base = os.path.join("L2","tiles") if dataset == Dataset.L2A else "tiles"
    retval = os.path.join(base,utmzone,latitude_band,grid_square)
    if add_safe:
        retval = os.path.join(retval, f"{identifier}.SAFE")
    return retval

def getSuffix(band, resolution, dataset):
    return f"_{band.name}_{resolution}m.jp2" if dataset == Dataset.L2A else f"_{band.name}.jp2"

def datasetFromScene(scene):
    dataset_str = scene['processinglevel'][-2:]
    return Dataset.L2A if dataset_str == '2A' else Dataset.L1C

def productTypeFromDataset(dataset):
    return f"S2MSI{dataset.name[1:]}"

def xmlMTDNameFromDataset(dataset):
    return f"MTD_MSI{dataset.name}.xml"

def xmlMTDPathFromScene(scene, cache_dir=''):
    identifier = scene['identifier']
    dataset = datasetFromScene(scene)
    base = identifier2Safe(identifier, dataset)
    xml_path = os.path.join(cache_dir, base, xmlMTDNameFromDataset(dataset))
    return xml_path

def xmlTreeMTDFromScene(scene, cache_dir=''):
    return ET.parse(xmlMTDPathFromScene(scene, cache_dir))

def getGoodPixelScl():
    return (SclEnum.VEGETATION, SclEnum.NOT_VEGETATED, SclEnum.WATER, SclEnum.SNOW_OR_ICE)

def getSensorBands():
    return valid_bands_S2[Dataset.L1C]

def getBandId(dataset, band):
    return valid_bands_S2[dataset][band].id

def getBandResolutions(dataset, band):
    return valid_bands_S2[dataset][band].resolutions

def getValidBands(dataset):
    return set(valid_bands_S2[dataset].keys())

def getValidDatasets():
    return tuple(valid_bands_S2.keys())

def computeBandResolutionToKeep(scene, min_resolution, bands):
    dataset = datasetFromScene(scene)
    if dataset not in getValidDatasets():
        raise Exception(f"Invalid dataset for scene {scene['identifier']}")
    if dataset == Dataset.L1C:
        min_resolution = 10
    return [
        (band,resolution)
        for band in bands
        for resolution in valid_bands_S2[dataset][band].resolutions
        if resolution >= min_resolution]

def getProcessingBaseline(scene):
    return scene['processingbaseline']

def additionalFilesFromScene(scene):
    return [xmlMTDNameFromDataset(datasetFromScene(scene))]
