from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import sentinel2.utils as s2utils
import utils.misc as misc
import os
import dotenv
import glob
import utils.geo
from zipfile import ZipFile

API_USER_NAME = "DHUS_USER"
API_PASSWORD_NAME = "DHUS_PASSWORD"
MANDATORY_ENV_DHUS = (API_USER_NAME, API_PASSWORD_NAME)
SENTINEL_VERSION = 2
PLATFORM_NAME = f"Sentinel-{SENTINEL_VERSION}"
SCIHUB_DL_LIMIT = 2
ZIP_EXTENSION = '.zip'

class ScihubApi():
    api = None
    def __init__(self):
        dotenv.load_dotenv()
        if self.__class__.api is None:
            if not all(varname in os.environ for varname in MANDATORY_ENV_DHUS):
                mandatory = "'" + "','".join(MANDATORY_ENV_DHUS) + "'"
                raise Exception(f"{mandatory} variables *has* to be provided, either through environment variables"
                                " or .env")
            self.__class__.api = SentinelAPI(os.environ[API_USER_NAME], os.environ[API_PASSWORD_NAME])

    @staticmethod
    def get():
        return ScihubApi().api

class ScihubSentinel2Downloader(object):
    def __init__(self):
        self.api = ScihubApi.get()

    def getSuffixes(self, bands_resolutions, dataset):
        return {band_resolution
                :s2misc.getSuffix(*band_resolution, dataset=dataset)
                for band_resolution in bands_resolutions}

    def findFile(self, suffix, base):
        return misc.getFirstOrNone(glob.glob(f'**/*{suffix}', root_dir=base, recursive=True))

    def checkIfAdditionalFilesAlreadyDownloaded(self, base, scene):
        return {f:self.findFile(f, base)
                for f in s2misc.additionalFilesFromScene(scene)}

    def checkIfBandsAlreadyDownloaded(self, base, bands_resolutions, dataset):
        return {(band, resolution):self.findFile(suffix, base)
                for (band, resolution), suffix
                in self.getSuffixes(bands_resolutions,dataset).items()}

    def extractBandsToKeep(self, zipfile, bands_resolutions, scene, additional_files):
        zippath = zipfile['path']
        base = os.path.dirname(zippath)
        print(f"--       extracting {zippath}")
        dataset = s2misc.datasetFromScene(scene)
        band_resolution_to_path = {}
        additional_file_to_path = {}
        band_resolution_to_suffix = self.getSuffixes(
            bands_resolutions,
            dataset)
        with ZipFile(zippath, "r") as inzipfile:
            namelist = inzipfile.namelist()
            to_extract = []
            for band_resolution, fn in band_resolution_to_suffix.items():
                path = misc.getFirstOrNone(a for a in namelist if a.endswith(fn))
                if path:
                    file_path = os.path.join(base, path)
                    band_resolution_to_path[band_resolution] = file_path
                    to_extract.append(path)
                else:
                    band_resolution_to_path[band_resolution] = None

            for fn in additional_files:
                path = misc.getFirstOrNone(a for a in namelist if a.endswith(fn))
                if path:
                    file_path = os.path.join(base, path)
                    additional_file_to_path[fn] = file_path
                    to_extract.append(path)
                else:
                    additional_file_to_path[fn] = None

            inzipfile.extractall(path=base, members=to_extract)
        for file_path in band_resolution_to_path.values():
            if file_path is not None:
                s2misc.getLRU(dataset).addFile(path=file_path)
        for file_path in additional_file_to_path.values():
            if file_path is not None:
                s2misc.getLRU(dataset).addFile(path=file_path)

        return band_resolution_to_path, additional_file_to_path

    def downloadScene(self, scene):
        uuid = scene['uuid']
        dataset = s2misc.datasetFromScene(scene)
        base = os.path.join(s2misc.getLRUPath(dataset),
                            s2misc.identifier2Basedir(scene['identifier'],
                            dataset=dataset))
        download = self.api.download_all([uuid], base, n_concurrent_dl=SCIHUB_DL_LIMIT)
        return download.downloaded, download.failed

    def downloadSceneExtractDiscard(self, scene, bands_resolutions, additional_files):
        success, failed = self.downloadScene(scene)
        dataset = s2misc.datasetFromScene(scene)
        if success:
            zipfile = success.popitem()[1]
            retval = self.extractBandsToKeep(zipfile, bands_resolutions, scene, additional_files)[0]
            print(f"--       removing {zipfile['path']}")
            try:
                s2misc.getLRU(dataset).remove(path=zipfile['path'])
            except Exception:
                pass
            os.remove(zipfile['path'])
        else:
            retval = {}
        return retval

    def retrieveScene(self, scene, bands_resolutions):
        identifier = scene['identifier']
        print(f"--       Retrieving {identifier} from scihub")
        dataset = s2misc.datasetFromScene(scene)

        base = os.path.join(
                    s2misc.getLRU(dataset),
                    s2misc.identifier2Basedir(identifier, dataset=dataset, add_safe=True))
        band_resolution_to_path = self.checkIfBandsAlreadyDownloaded(base, bands_resolutions,dataset)
        additional_files_to_path = self.checkIfAdditionalFilesAlreadyDownloaded(base, scene)
        if not all(band_resolution_to_path.values()) or not all(additional_files_to_path.values()):
            return self.downloadSceneExtractDiscard(
                scene,
                bands_resolutions,
                s2misc.additionalFilesFromScene(scene))
        else:
            return band_resolution_to_path

    def __call__(self, *args, **kwargs):
        return self.retrieveScene(*args, **kwargs)

class ScihubSentinel2Description(object):
    def __init__(self):
        self.api = ScihubApi.get()
        self.platformname = PLATFORM_NAME

    def getScenesDescription(self, bbox=None, dates=None, limit=100, months=None, **kwargs):
        print(f'--   Fetching scene description overlapping {bbox}')
        
        months = list(range(1,13)) if months is None else months
        kwargs = kwargs | dict(
            platformname=self.platformname,
            limit=limit * 12 // len(months),
        )

        if bbox is not None:
            kwargs['area'] = utils.geo.boxToWkt(bbox)

        if dates is not None:
            kwargs['date'] = tuple(date.strftime('%Y%m%d') for date in dates)

        query_result = self.api.query(**kwargs)
        print(f'--     Retrieved {len(query_result)} scenes before filtering per month')
        retval = [
            u
            for u in query_result.values()
            if u['beginposition'].month in months]
        print(f'--     Retrieved {len(retval)} scenes after filtering per month')
        return retval

    def __call__(self, *args, **kwargs):
        self.getScenesDescription(*args, **kwargs)
