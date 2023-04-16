import os
import subprocess

def execute(*args, env={}, **kwargs):
    print('gdalwarp', *args)
    env = os.environ | env | {
        'LD_LIBRARY_PATH':f'/home/elie/.local/lib:{os.environ.get("LD_LIBRARY_PATH")}',
        'PATH':f'/home/elie/.local/bin:{os.environ.get("PATH", None)}'}
    return subprocess.run(*args,env=env,**kwargs)

def gdal_translate(*args, **kwargs):
    execute(['gdal_translate',*args], stdout=subprocess.PIPE, **kwargs)

def gdal_buildvrt(*args, **kwargs):
    execute(['gdalbuildvrt',*args], stdout=subprocess.PIPE, **kwargs)

def gdal_warp(*args, **kwargs):
    execute(['gdalwarp',*args], stdout=subprocess.PIPE, **kwargs)
