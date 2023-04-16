import dotenv
from pathlib import Path
import os
from collections import OrderedDict
import utils.misc as misc
import time

class LRUCache(object):
    def __init__(self, cache_dir, max_cache_size, retrieve_func=None, retrieve_args=[], retrieve_kwargs={}):
        self.cache_dir = cache_dir
        self.max_cache_size = max_cache_size
        self.retrieve_func = retrieve_func
        self.retrieve_args = retrieve_args
        self.retrieve_kwargs = retrieve_kwargs
        self.current_cache_size = 0
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
        self.lru = OrderedDict()
        self.idx = 0
        self.fillLru()

    def getPath(self):
        return self.cache_dir

    def addFile(self, fn=None, path=None):
        if fn is None and path is None:
            raise Exception("At least fn or path should be provided")
        if path is None:
            path = os.path.join(self.cache_dir, fn)
        if fn is None:
            fn = os.path.relpath(path, self.cache_dir)
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)
        self.lru[(mtime, self.idx)] = (fn, path, size)
        self.current_cache_size += size
        self.idx += 1

    def removeFile(self):
        _,(_, path, size) = self.lru.popitem(last=False)
        os.remove(path)
        self.current_cache_size -= size

    def fillLru(self):
        for path, subdirs, files in os.walk(self.cache_dir):
            for name in files:
                self.addFile(path=os.path.join(path, name))

    def cleancache(self):
        while self.current_cache_size > self.max_cache_size and self.lru:
            self.removeFile()

    def setRetrieve(self, func=None, args=None, kwargs=None):
        if func is not None:
            self.retrieve_func = func
        if args is not None:
            if not isinstance(args, list):
                raise Exception(f"{args} is not a list")
            self.retrieve_args = args
        if kwargs is not None:
            if not isinstance(args, dict):
                raise Exception(f"{args} is not a dict")
            self.retrieve_kwargs = kwargs

    def retrieve(self, fn, retrieve_func=None, force_retrieve_func=None, args=[], kwargs={}):
        #lookup for element. Slow but who cares
        lookup = misc.getFirstOrNone(
            (key
            for key,(f,_,_) in self.lru.items()
            if f == fn))
        args = self.retrieve_args + args
        kwargs |= self.retrieve_kwargs
 
        if lookup is not None:
            (fn,path,size) = self.lru.pop(lookup)
            self.current_cache_size -= size
            now = time.time()
            if (force_retrieve_func is None
                 or not force_retrieve_func(path, fn, *args, **kwargs)):
                os.utime(path, (now, now))
                self.addFile(path=path, fn=fn)
                return path
        path = os.path.join(self.cache_dir, fn)
        self.cleancache()
        misc.createHierachy(path, is_file=True)
        try:
            success = (retrieve_func or self.retrieve_func)(path, fn, *args, **kwargs)
        except:
            return None
        if not success:
            return None
        self.addFile(path=path, fn=fn)
        return path

bdd = {}
def fillLRUDb():
    global bdd
    dotenv.load_dotenv()
    base = os.environ.get('CACHE_DIR', 'cache')
    data = (
        ('sentinel2L2A', 'SENT2_L2A_CACHE_DIR', 'sentinel2-l2a-cache', 'SENT2_L2A_CACHE_SIZE', '256G'),
        ('sentinel2L1C', 'SENT2_L1C_CACHE_DIR', 'sentinel2-l1c-cache', 'SENT2_L1C_CACHE_SIZE', '64G'),
        ('gmt',          'GMT_CACHE_DIR',       'gmt-cache',           'GMT_CACHE_SIZE',       '16G'),
        ('sentinel2CLD', 'SENT2_CLD_CACHE_DIR', 'sentinel2-cld-cache', 'SENT2_CLD_CACHE_SIZE', '32G'),
    )
    for key, cache_dir, cache_dir_default, cache_size, cache_size_default in data:
        bdd[key] = LRUCache(
            cache_dir=os.path.join(base, os.environ.get(cache_dir, cache_dir_default)),
            max_cache_size=misc.str2ByteSize(os.environ.get(cache_size, cache_size_default)),
        )

def get(lru_name):
    global bdd
    return bdd[lru_name]

fillLRUDb()
