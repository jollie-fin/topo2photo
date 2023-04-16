import math
import numpy as np
import re
from pathlib import Path
from collections import OrderedDict
import os
import pprint
import time
from pathlib import Path
from rasterio.enums import Resampling
import hashlib
import warnings

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

warnings.warn('cleanup options')
valid_options = {
'raster_layer' : {
    "fillnodata": {"real_option":"fillNoData","documentation":"replace nodata value by neighboring valid values"},
    "scale01": {"real_option":"scale01","documentation":"scale integer to range 0 1 while reading int8 or int16"},
    "scale11": {"real_option":"scale11","documentation":"scale integer to range -1 1 while reading int8 or int16"},
    "resampling": {"real_option":"resampling","documentation":"algorithm used for resampling"},
},
'rasterize' : {
    "nodata": {"real_option":"noData","documentation":"nodata value"},
    "inverse": {"real_option":"inverse","documentation":"whether to invert rasterization, i.e. burn the fixed burn value, or the burn value associated with the first feature into all parts of the image not inside the provided a polygon."},
    "alltouched": {"real_option":"allTouched","documentation":"whether to enable the ALL_TOUCHED rasterization option so that all pixels touched by lines or polygons will be updated, not just those on the line render path, or whose center point is within the polygon."},
    "burnvalues": {"real_option":"burnValues","documentation":"list of fixed values to burn into each band for all objects. Excusive with attribute."},
    "attribute": {"real_option":"attribute","documentation":"identifies an attribute field on the features to be used for a burn-in value. The value will be burned into all output bands. Excusive with burnValues."},
    "usez": {"real_option":"useZ","documentation":"whether to indicate that a burn value should be extracted from the “Z” values of the feature. These values are added to the burn value given by burnValues or attribute if provided. As of now, only points and lines are drawn in 3D."},
    "layers": {"real_option":"layers","documentation":"list of layers from the datasource that will be used for input features."},
    "sqlstatement": {"real_option":"SQLStatement","documentation":"SQL statement to apply to the source dataset"},
    "sqldialect": {"real_option":"SQLDialect","documentation":"SQL dialect (‘OGRSQL’, ‘SQLITE’, …)"},
    "where": {"real_option":"where","documentation":"WHERE clause to apply to source layer(s)"},
    "optim": {"real_option":"optim","documentation":"optimization mode (‘RASTER’, ‘VECTOR’)"},
    "scale01": {"real_option":"scale01","documentation":"scale integer to range 0 1 while reading int8 or int16"},
    "scale11": {"real_option":"scale11","documentation":"scale integer to range -1 1 while reading int8 or int16"},
    "defaultvalue": {"real_option":"default_value"},
    "fill": {"real_option":"fill"},
},
'usgs' : {},
'sentinel2' : {},
}

str_to_resampling = dotdict(
average = Resampling.average,
bilinear = Resampling.bilinear,
cubic = Resampling.cubic,
cubic_spline = Resampling.cubic_spline,
gauss = Resampling.gauss,
lanczos = Resampling.lanczos,
max = Resampling.max,
med =  Resampling.med,
min = Resampling.min,
mode = Resampling.mode,
nearest = Resampling.nearest,
q1 =  Resampling.q1,
q3 =  Resampling.q3,
rms =  Resampling.rms,
sum =  Resampling.sum,
)

str_to_byte_size = {
    'k' : 1000,
    'm' : 1000_000,
    'g' : 1000_000_000,
    't' : 1000_000_000_000,
}

str_to_arcsec = {
    'd' : 3600,
    'm' : 60,
    's' : 1,
}

def decodeBands(bands):
    def parseBand(band):
        retval = int(band)
        if retval <= 0:
            raise Exception('Invalid band {band}')
        return retval
    return list(map(parseBand,bands.split(',')))

def getOptionDictionary(s):
    return valid_options[s]

def getValid(l):
    return ", ".join(l.keys())

def getValidDtypes():
    return getValid(str_to_dtype)

def getValidResampling():
    return getValid(str_to_resampling)

def getValidOptions(s):
    return getValid(getOptionDictionary(s))

def str2Dtype(dtype):
    if dtype not in str_to_dtype:
        raise Exception(f"invalid dtype {dtype}. Valid dtypes are : {getValidDtypes()}")
    return str_to_dtype[dtype]

def str2Resampling(resampling):
    if resampling not in str_to_resampling:
        raise Exception(f"invalid resampling {resampling}. Valid dtypes are : {getValidResampling()}")
    return str_to_resampling[resampling]

def dictFromOptions(options):
    retval = dotdict()
    for arg in options:
        if '=' not in arg:
            key, value = arg, None
        else:
            key, value = arg.split('=', 1)
        key = key.lower().replace('_','')
        retval[key] = value
    return retval
    
def capitalizeOptions(options, s):
    l = getOptionDictionary(s)
    retval = dotdict()
    for key, value in options.items():
        if key not in l:
            raise Exception(f"{key} is not a valid option. Valid options are : {getValid(l)}")
        retval[l[key]['real_option']] = value
    return retval

def str2IntMultiplier(arg, l):
    r = re.compile(r"([\d'_]+(?:\.\d+)?)(.*)")
    result = r.fullmatch(arg)
    if not result:
        raise Exception(f"Invalid value : '{arg}'")

    mantissa, exponent = result.groups()

    mantissa = float(mantissa.replace("'",'').replace('_',''))
    exponent_str = result.group(2)

    if exponent_str:
        if exponent_str.lower() not in l:
            raise Exception(f"Unknown multiplier : '{exponent_str}'. Valid multipliers are : {getValid(l)}")
        mantissa = mantissa * l[exponent_str.lower()]
    return mantissa

def ceildivide(a,b):
    return -(a//-b)

def str2ByteSize(arg):
    return int(str2IntMultiplier(arg, str_to_byte_size))

def str2Arcsec(arg):
    return str2IntMultiplier(arg, str_to_arcsec)

def bytes2StrYield(stream):
    for a in stream:
        yield a.decode('utf-8')

def computeWidthHeight(size, bounds):
    dx = abs(bounds[2]-bounds[0])
    dy = abs(bounds[3]-bounds[1])
    ratio = dy/dx
    return int(math.sqrt(size / ratio)), int(math.sqrt(size * ratio))


def ffill(map, no_data):
    mask = (map == -32000)
    idx = np.where(~mask,np.arange(mask.shape[1]),0)
    np.maximum.accumulate(idx,axis=1, out=idx)
    out = map[np.arange(idx.shape[0])[:,None], idx]
    return out

def fill(map, no_data):
    out = ffill(map, no_data)
    out = ffill(out[:, ::-1], no_data)[:, ::-1]
    out = np.transpose(out)
    out = ffill(out, no_data)
    out = ffill(out[:, ::-1], no_data)[:, ::-1]
    out = np.transpose(out)
    return out

def getFirstOrNone(l, default=None):
    return next(iter(l), default)

def createHierachy(path, is_file=False):
    if is_file:
        path = os.path.dirname(path)
    Path(path).mkdir(parents=True, exist_ok=True)

def getMD5FromPath(path):
    with open(path, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)
        return file_hash.digest()

