from shapely import geometry, from_wkt, box
import pprint
import utils.misc as misc
import spatialite
import math
import os
import timeit
import dotenv
import numpy as np
from shapely.prepared import prep

def equigrid(env_left_bottom,
             env_right_top,
             tile_count,
             tile_size,
             mapping=lambda *args:args,
             shape_bounds=None):
    if shape_bounds is not None:
        shape_bounds = prep(shape_bounds)
    start = env_left_bottom
    end = env_right_top - tile_size
    return (mapping((idx, idy), (x, y), (x + tile_size[0], y + tile_size[1]))
        for     idx,x in enumerate(np.linspace(start[0], end[0], tile_count[0]))
            for idy,y in enumerate(np.linspace(start[1], end[1], tile_count[1]))
                if shape_bounds is None
                or shape_bounds.intersects(box(x,y,x+tile_size[0],y+tile_size[1]))
    )

def tileFixedTiles(bounds, tile_size, minimum_absolute_overlap=0):
    bounds = np.array(bounds).reshape(2,2)
    tile_size = np.resize(tile_size,2)
    minimum_absolute_overlap = np.resize(minimum_absolute_overlap,2)
    env_left_bottom = np.array(bounds[0])
    env_right_top = np.array(bounds[1])
    env_breadth = env_right_top - env_left_bottom
    if np.all(tile_size == env_breadth):
        return env_left_bottom, env_right_top, np.array([1,1]), tile_size
    if np.any(tile_size > env_breadth):
        raise AttributeError('Impossible to tile, tile are too big')
    grid_min_breadth = env_breadth + minimum_absolute_overlap
    tile_count = misc.ceildivide(grid_min_breadth, tile_size - minimum_absolute_overlap)
    return env_left_bottom, env_right_top, tile_count, tile_size

def tileFixedCount(bounds, tile_count, overlap=0, is_overlap_relative=True, square=False):
    bounds = np.array(bounds).reshape(2,2)
    overlap = np.resize(overlap,2)
    env_left_bottom = np.array(bounds[0])
    env_right_top = np.array(bounds[1])
    env_breadth = env_right_top - env_left_bottom
    if np.any(overlap > env_breadth) and not is_overlap_relative:
        raise AttributeError(f"overlap is bigger than bounds")

    if is_overlap_relative:
        relative_overlap = overlap
        unitary_cell_stride = 1 - relative_overlap
        unitary_cell_breadth = (tile_count - 1) * unitary_cell_stride + 1
        overlap = env_breadth / unitary_cell_breadth * overlap
    
    tile_size = (env_breadth + overlap) / tile_count
    if square:
        tile_size = np.max(tile_size)

    return (env_left_bottom, env_right_top, tile_count, tile_size)

class EquiGridManager(object):
    def __init__(self, min_tiling):
        self.grids = {
            tiling:
            EquiGrid(tiling)
            for tiling in range(min(1,min_tiling),360,min_tiling)
            if WIDTH % tiling == 0
        }

    def getGrid(self, tiling):
        return self.grids.get(tiling,None)

# https://www.gaia-gis.it/gaia-sins/spatialite-sql-5.0.0.html
class GridFromFile(object):
    def __init__(self, spatialite_file, table=None, crs=None):
        print(f"Loading {spatialite_file} into memory")
        self.db = spatialite.connect(spatialite_file)
        table_deducted = os.path.splitext(os.path.split(spatialite_file)[1])[0]
        self.table = table_deducted if table is None else table
        print("-- done")

    def __enter__(self):
        return self

    def __exit__(self):
        self.db.close()

    def getexactoverlap(self, obj, table=None, getgeometry=None, getattribute=None):
        table = self.table if table is None else table
        if getgeometry is None and getattribute is None:
            raise AttributeError('at least getgeometry or getattribute should be provided')
        if getattribute is None:
            for wkt,_ in self.db.execute(
                    f"SELECT AsText(geometry),'0' FROM {table} "
                    f"WHERE ST_INTERSECTS(geometry, GeomFromWKB(?))",
                    (obj.wkb,)):
                yield from_wkt(wkt)
        elif getgeometry is None:
            for attr,_ in self.db.execute(
                    f"SELECT {getattribute},'0' FROM {table} "
                    f"WHERE ST_INTERSECTS(geometry, GeomFromWKB(?))",
                    (obj.wkb,)):
                yield from_wkt(attr)
        else:
            for wkt, attr in self.db.execute(
                    f"SELECT AsText(geometry), {getattribute} FROM {table} "
                    f"WHERE ST_INTERSECTS(geometry, GeomFromWKB(?))",
                    (obj.wkb,)):
                yield (from_wkt(wkt), attr)

dotenv.load_dotenv()
grids = {}

def GmtGrid():
    if 'gmt' in grids:
        return grids['gmt']
    grid = GridFromFile(os.environ.get('GMT_TILES','grids/gmt_tiles.sqlite'))
    grids['gmt'] = grid
    return grid

def Sent2Grid():
    if 'sent2' in grids:
        return grids['sent2']
    grid = GridFromFile(os.environ.get('SENT2_TILES','grids/sent2_tiles.sqlite'))
    grids['sent2'] = grid
    return grid
