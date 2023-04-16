import urllib
import drivers.driver as driver
import utils.misc as misc
from utils.rasterio import warpmerge, temporarydataset, LOSSLESS_JPEG2000
import re
import math
from collections import defaultdict
import os
import utils.lru as lru
from rasterio.crs import CRS
from rasterio.warp import Resampling
import rasterio as rio
import warnings
from tempfile import NamedTemporaryFile
from utils.grid import GmtGrid
from shapely import geometry, ops
from utils.process import gdal_translate
import utils.net
import pprint
import dotenv

INDEX_FILE_NAME = "index.html"
DATASET_EXTENSION = "jp2"
GMT_DATABASE = "gmt_data_server.txt"
class Gmt2Raster(driver.RasterDriver):
    def __init__(self):
        driver.RasterDriver.__init__(self, 'raster_layer')
        dotenv.load_dotenv()
        self.lru = lru.get('gmt')

        print(f"-- get equigridmanager")
#        self.equigrid = utils.grid.EquiGridManager(1)
        self.equigrid = GmtGrid()
        print(f"-- done")

        self.overide_crs = CRS.from_epsg(4326)
        self.force_retrieve_index_once = True
        self.root = os.environ.get('GMT_SERVER', utils.net.getBestGMTServer())
        print(f'--  fetching dataset {self.root}')
        self.database = self.getDatabase(f"{self.root}/{GMT_DATABASE}")

    def detailcheckcall(self, key, *_):
        if key not in self.database:
            raise Exception(f'invalid gmt_layer:{key} on server {self.root}; '
                            f'valid gmt_layer are : {misc.getValid(self.database)}')

    def getStep2Dataset(self, gmt_layer):
        return sorted(list(self.database[gmt_layer].items()))

    @staticmethod
    def gmtDbDecode(stream):
        try:
            next(stream)
        except StopIteration:
            raise Exception('Impossible to analyse gmt database')
        for row in stream:
            if not row or row[0].startswith('#'):
                continue 
            yield row.replace('\t\t','\t').split('\t')

    def getDatabase(self, gmt_server):
        columns = ('dir', 'name', 'inc', 'reg', 'scl', 'off', 'size', 'tile', 'date', 'coverage', 'filler', 'cpt', 'remark')
        re_get_collection = re.compile('([^/]+)/$')
        retval = defaultdict(dict)
        with open(self.getFile(gmt_server, force_retrieve=self.force_retrieve_index_once)) as gmt_description:
            for row in self.gmtDbDecode(gmt_description):
                row_dict = {k:v for k,v in zip(columns, row)}
                base = row_dict['dir']
                inc = int(misc.str2Arcsec(row_dict['inc']))
                collection = re_get_collection.search(base).group(1)
                retval[collection][inc] = row_dict
        self.force_retrieve_index_once = False
        return retval

    def retrieveFile(self, path, url, suffix, shall_fix, retrial=1):
        print(f"Fetching {url}")
        try:
            if not shall_fix:
                utils.net.fetchFromInternet(path=path, url=url)
                return True
            with NamedTemporaryFile(suffix=suffix) as src:
                utils.net.fetchFromInternet(path=src.name, url=url, desc=os.path.basename(path))
                gdal_translate(src.name, path, '-a_srs', self.overide_crs.to_string())
                return True
        except urllib.error.HTTPError as e:
            if retrial <= 0:
                return False
            is_g_dataset_re = r"_g/$"
            is_p_dataset_re = r"_p/$"
            if re.search(is_g_dataset_re, url):
                return self.retrieveFile(path, re.sub(is_g_dataset_re, '_p/', url), suffix, shall_fix, retrial - 1)
            if re.search(is_p_dataset_re, url):
                return self.retrieveFile(path, re.sub(is_p_dataset_re, '_g/', url), suffix, shall_fix, retrial - 1)
            return self.retrieveFile(path, url, suffix, shall_fix, retrial - 1)

    def getFile(self, url, shall_open=None, force_retrieve=False):
        url_parsed = urllib.parse.urlsplit(url)
        url_path = url_parsed.path
        *folder, filename = url_path.split('/')
        if folder and not folder[0]:
            folder = folder[1:]
        if not filename:
            filename = INDEX_FILE_NAME

        *basefilename, suffix = filename.split('.')
        shall_fix = suffix in {'grd', 'jp2', 'tif'}
        suffix = f'.{suffix}'

        if shall_fix:
            filename = '.'.join(basefilename + [DATASET_EXTENSION])

        fn = os.path.join(*folder, filename)
        return self.lru.retrieve(
            fn,
            retrieve_func=(
                lambda path, fn:
                    self.retrieveFile(path=path, url=url, suffix=suffix, shall_fix=shall_fix)),
            force_retrieve_func=(
                lambda path, fn:
                    force_retrieve))

    def coordinates2Str(self, lon, lat):
        lon = round(lon)
        lat = round(lat)
        ns = 'N' if lat >= 0 else 'S'
        ns_lat = abs(lat)
        we = 'E' if lon >= 0 else 'W'
        we_lon = abs(lon)
        return f"{ns}{ns_lat:02}{we}{we_lon:03}"

    def getCoordinates2Filename(self, path):
        print(f'--       analysing structure of {path}')
        linux_path = self.getFile(path)
        if linux_path is None:
            return None
        with open(linux_path, 'r') as index_html: 
            regex_coordinates = re.compile(r'href="([NS]\d+[EW]\d+)([^"]*)')
            retval = {}
            for line in index_html:
                for match in regex_coordinates.finditer(line):
                    retval[match.group(1)] = ''.join(match.groups())
            print(f'--         found {len(retval)} files')
        return retval

    def retrieveDataset(self, dataset, obj):
        tiling = int(dataset['tile'])
        dataset_url = self.getUrlDataset(dataset)
        if not tiling:
            print("--     Found an entire map. Stopping here")
            url = dataset_url
            path = self.getFile(url)
            if not path:
                raise Exception('Inconsistent GMT database. Please try different mirror')
            return [path], geometry.Polygon()

        success = []
        failed = []
        coordinates_to_filename = self.getCoordinates2Filename(dataset_url)
        if coordinates_to_filename is None:
            return [], obj
        for bbox, key in self.equigrid.getexactoverlap(
                obj,
                table=f'resolution{tiling:03}',
                getgeometry=(),
                getattribute='name'):
            print(bbox)
            minlon, minlat, _, _ = bbox.bounds
            if key in coordinates_to_filename: 
                fn = coordinates_to_filename[key]
                url = urllib.parse.urljoin(dataset_url, fn)
                path = self.getFile(url)
                if not path:
                    raise Exception('Inconsistent GMT database. Please try different mirror')
                success.append(path)
            else:
                failed.append(bbox)
        failed_shape = obj.intersection(ops.unary_union(failed))
        return success, failed_shape

    def getUrlDataset(self, dataset):
        dataset_base = urllib.parse.urljoin(self.root, dataset['dir'])
        dataset_url = urllib.parse.urljoin(dataset_base, dataset['name'])
        return dataset_url

    def renderToRaster(
        self,
        dst_ds,
        bands,
        shape_bounds,
        gps_bounds,
        key,
        options
    ):
        shape_bounds = gps_bounds
        minx, miny, maxx, maxy = dst_ds.bounds
        
        warnings.warn('projection')
        if maxx < minx:
            maxx += 360
        resolution = ((maxx-minx)%360)/dst_ds.width*3600
        print(f'--   Requested resolution is {resolution:.1f} arcsec')

        successful = []

        step2dataset = self.getStep2Dataset(key)

        stepidx = len(step2dataset) - 1
        for i in range(len(step2dataset) - 1):
            if step2dataset[i + 1][0] > resolution:
                stepidx = i
                break

        while stepidx < len(step2dataset) and not shape_bounds.is_empty:
            step, dataset = step2dataset[stepidx]
            print(f"--   Trying with resolution {dataset['inc']}")
            s,shape_bounds = self.retrieveDataset(
                dataset=dataset,
                obj=shape_bounds,
            )
            successful += s
            stepidx += 1
        print(f'--   Warping')

        warnings.warn('deal with empty merge')
        return warpmerge(
            successful,
            dst_ds=dst_ds,
            bands=bands,
            **options,
            )
