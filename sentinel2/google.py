from google.cloud import storage
import dotenv
import os
import utils.misc as misc
import sentinel2.utils as s2utils
from tqdm.std import tqdm
import base64

dotenv.load_dotenv()

class GoogleStorageClient():
    client = None
    def __init__(self):
        if self.__class__.client is None:
            self.__class__.client = storage.Client(project=None)
    def __call__(self):
        return self.__class__.client

class RetrievalError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class GoogleStorage(object):
    def __init__(self, bucket_name):
        self.client = GoogleStorageClient()
        self.bucket_name = bucket_name
        self.bucket = self.client().get_bucket(bucket_name)

# from https://github.com/googleapis/google-cloud-python/issues/920#issuecomment-653823847
    def ls(self, prefix):
        iterator = self.client().list_blobs(self.bucket, prefix=prefix, delimiter='/')
        prefixes = set()
        for page in iterator.pages:
            prefixes.update(page.prefixes)
        return prefixes

    def checkcorrectnessorraise(self, blob_path, linux_path, retry):
        if self.isblobdownloaded(blob_path=blob_path, linux_path=linux_path):
            return

        print(f"Error : blob {blob_path} has been modified. Removing")
        os.remove(linux_path)
        if retry <= 0:
            raise Exception(f"Cannot download blob {blob_path}")
        print(f"downloading again")
        self.get(blob_path=blob_path, linux_path=linux_path, retry=retry - 1)

    def get(self, blob_path, linux_path, retry=1):
        blob = self.bucket.get_blob(blob_path)
        try:
            start = os.path.getsize(linux_path)
            if start >= blob.size:
                return self.checkcorrectnessorraise(
                    blob_path=blob_path,
                    linux_path=linux_path,
                    retry=retry)
                
        except OSError:
            start = 0
        with open(linux_path, 'ab') as f:
            with tqdm.wrapattr(
                    f,
                    "write",
                    total=blob.size,
                    initial=start,
                    desc=os.path.basename(linux_path)
                ) as file_obj:
                self.client().download_blob_to_file(
                    blob,
                    file_obj,
                    start=start,
                    )
        return self.checkcorrectnessorraise(
            blob_path=blob_path,
            linux_path=linux_path,
            retry=retry)

    def isblobdownloaded(self, blob_path, linux_path):
        try:
            blob = self.bucket.get_blob(blob_path)
            blob_size = blob.size
            blob_md5 = base64.b64decode(blob.md5_hash)
            linux_size = os.path.getsize(linux_path)
            linux_md5 = misc.getMD5FromPath(linux_path)
            return blob_size == linux_size and blob_md5 == linux_md5 
        except OSError:
            return False

class GoogleStorageSentinel2(object):
    def __init__(self):
        self.googlestorage = GoogleStorage(bucket_name=os.environ['SENTINEL2_BUCKET'])

    def getGoogleStorage(self,blob_path,linux_path):
        misc.createHierachy(linux_path, is_file=True)
        self.googlestorage.get(blob_path=blob_path, linux_path=linux_path)
        return True

    def get(self, blob_path, dataset):
        try:
            return s2misc.getLRU(dataset).retrieve(
                blob_path,
                retrieve_func=lambda linux_path, blob_path: self.getGoogleStorage(linux_path=linux_path, blob_path=blob_path),
                force_retrieve_func=lambda linux_path, blob_path: not self.googlestorage.isblobdownloaded(linux_path=linux_path, blob_path=blob_path),
                )
        except Exception as e:
            raise RetrievalError(f"Impossible to retrieve {blob_path} : {e}") from e

    def getBandPath(self, scene, suffix):
        dataset = s2misc.datasetFromScene(scene)
        tree = s2misc.xmlTreeMTDFromScene(scene, s2misc.getLRUPath(dataset))
        extension = suffix.split('.')[-1]
        addextension = lambda blob_path, extension=extension: blob_path if blob_path.endswith(extension) else f"{blob_path}.{extension}"
        result = misc.getFirstOrNone((
                    addextension(v.text)
                    for v in tree.findall(".//IMAGE_FILE")
                    if addextension(v.text).endswith(suffix)))
        return result

    def retrieveScene(self, scene, bands_resolutions):
        identifier = scene['identifier']
        print(f"--      Retrieving {identifier} from cloud")
        dataset = s2misc.datasetFromScene(scene)
        path = s2misc.identifier2Safe(identifier, dataset)
        for f in s2misc.additionalFilesFromScene(scene):
            self.get(os.path.join(path,f), dataset=dataset)
        retval = {}
        for band_resolution in bands_resolutions:
            ds_path = self.getBandPath(scene, s2misc.getSuffix(*band_resolution, dataset=dataset))
            if ds_path is None:
                retval[band_resolution] = None
            else:
                retval[band_resolution] = self.get(f"{path}/{ds_path}", dataset=dataset)
        return retval

    def __call__(self, *args, **kwargs):
        return self.retrieveScene(*args, **kwargs)
