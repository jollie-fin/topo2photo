from osgeo import gdal
import drivers.driver as driver
import utils.misc as misc
from landsatxplore.api import API
import dotenv
import os
import time
import pprint
import shapely.geometry
import shapely.ops
import traceback
from collections import defaultdict
from landsatxplore.earthexplorer import EarthExplorer

import matplotlib.pyplot as plt
import geopandas as gpd
def showshapely(form):
    p = gpd.GeoSeries(form)
    p.plot()
    plt.show()

API_KEY_NAME = "USGS_API_KEY_TOKEN"
API_USER_NAME = "LANDSATXPLORE_USERNAME"
API_PASSWORD_NAME = "LANDSATXPLORE_PASSWORD"
MANDATORY_ENV = (API_KEY_NAME, API_USER_NAME, API_PASSWORD_NAME)

class PolygoneCover(object):
    def __init__(self, world, polygon_list):
        print(f'--     computing coverage')
        print('--       splitting plane')
        self.coverage = self.splitForCoverage(world, polygon_list)
        print(f'--         found {len(self.coverage)} sections')
        if not self.coverage[-1]:
            print('Impossible to cover')
        print('--       building graph')
        self.scene_to_section = defaultdict(set)
        self.section_to_scene = defaultdict(set)
        for section_idx, owners in enumerate(self.coverage):
            for o in owners:
                self.scene_to_section[o].add(section_idx)
                self.section_to_scene[section_idx].add(o)

    def removeLink(self, scene):
        for section in list(self.scene_to_section[scene]):
            for sc in list(self.section_to_scene.get(section,{})):
                self.section_to_scene.get(section,set()).discard(sc)
                self.scene_to_section.get(section,set()).discard(section)
        for scene, sections in list(self.scene_to_section.items()):
            if not sections:
                self.scene_to_section.pop(scene, None)
        self.scene_to_section.pop(scene, None)

    def splitForCoverage(self, world, polygon_list):
        current_coverage = [(world,[])]
        for i, polygon in enumerate(polygon_list):
            next_coverage = []
            for form, owners in current_coverage:
                w, wo = form & polygon, form - polygon
                if not w.area < 1/3600**2:
                    next_coverage.append((w,owners+[i]))
                if not wo.area < 1/3600**2:
                    next_coverage.append((wo,owners))
            current_coverage = next_coverage
        return [owners for form, owners in current_coverage]

    def getCoverage(self, scene):
        return len(self.scene_to_section.get(scene,()))

class USGS2Raster(driver.RasterDriver):
    def __init__(self, landsat_ds, cache_dir, bands, layer, dtype, *options):
        driver.RasterDriver.__init__(self, dtype, landsat_ds, layer, 'usgs', options)

        dotenv.load_dotenv()
        if not all(varname in os.environ for varname in MANDATORY_ENV):
            mandatory = "'" + "','".join(MANDATORY_ENV) + "'"
            raise Exception(f"{mandatory} variables *has* to be provided, either through environment variables"
                            " or .env")
        self.api_key = os.environ[API_KEY_NAME]
        self.username = os.environ[API_USER_NAME]
        self.password = os.environ[API_PASSWORD_NAME]
        self.api = API(self.username, self.password)
        self.ee = EarthExplorer(self.username, self.password)
        self.cache_dir = cache_dir
        self.landsat_ds = landsat_ds

    def __del__(self):
        try:
            print("cleaning up api")
            self.api.logout()
        except AttributeError:
            pass

        try:
            print("cleaning up earth explorer")
            self.ee.logout()
        except AttributeError:
            pass

    def getCoveragePolygonFromScene(self, scene):
        order = ('lower_left', 'upper_left', 'upper_right', 'lower_right')
        axis = ('longitude','latitude')
        return shapely.geometry.Polygon([[scene[f"corner_{c}_{a}"] for a in axis] for c in order])

    def getPolygonFromBBox(self, bbox):
        x_order = (0,0,2,2)
        y_order = (1,3,3,1)
        return shapely.geometry.Polygon([[bbox[x],bbox[y]] for x,y in zip(x_order, y_order)])

    def chooseScene(self, scenes, remaining_scenes, key):
        return max(remaining_scenes, key=lambda arg:key(*scenes[arg]))

    def doesScenesCoverBbox(self, bbox, scenes):
        world = self.getPolygonFromBBox(bbox)
        coverage = shapely.ops.unary_union([self.getCoveragePolygonFromScene(scene) for scene in scenes])
        return (world - coverage).is_empty
    def getScenes(self, bbox):
        print(f'--   getting scenes from usgs')
        max_results = 10000

        scenes = []
        max_cloud_cover = 1
        while not self.doesScenesCoverBbox(bbox, scenes) and max_cloud_cover < 100:
            print(f'--     trying with cloud cover = {max_cloud_cover}')
            scenes = self.api.search(
                dataset=self.landsat_ds,
                bbox=bbox,
                months=[2],
                max_cloud_cover=max_cloud_cover,
                max_results = 10000,
            )
            if len(scenes) < max_results:
                print(f'--       found only {len(scenes)} scenes instead of {max_results}')
                max_cloud_cover *= 2

        scenes_list = [
            (scene, self.getCoveragePolygonFromScene(scene), i)
            for i, scene in enumerate(scenes)]

        remaining_coverage = self.getPolygonFromBBox(bbox)
        remaining_scenes_set = {i for i, _ in enumerate(scenes)}
        
#        polygone_cover = PolygoneCover(remaining_coverage, list(cover for _,cover,_ in scenes_list))

        selected_scenes = []
        selected_idx = set()

        def coverage_evaluator(scene, coverage, *_):
            nonlocal remaining_coverage
            return (coverage & remaining_coverage).area, -scene['cloud_cover']
        def cloud_evaluator(scene, *_):
            return -scene['cloud_cover']
        def minset_evaluator(scene, coverage, idx):
            return polygone_cover.getCoverage(idx), -scene['cloud_cover']

        while remaining_scenes_set and not remaining_coverage.is_empty:
#            showshapely(remaining_coverage)
            if remaining_coverage.is_empty:
                break
#            candidate = self.chooseScene(scenes_list, remaining_scenes_set, minset_evaluator)
            candidate = self.chooseScene(scenes_list, remaining_scenes_set, coverage_evaluator)
#            print(f"choosing {candidate}")

#            polygone_cover.removeLink(candidate)
            remaining_scenes_set.remove(candidate)
            scene, coverage, _ = scenes_list[candidate]
            remaining_coverage -= coverage
            selected_scenes.append(scene)
            selected_idx.add(candidate)
        self.getImageFromScene(selected_scenes[0])
        return selected_scenes

    def getImageFromScene(self, scene):
        self.ee.download(scene['entity_id'], output_dir=self.cache_dir)


    def getRaster(self, args):
        return None

import sys
import random
def chooseScene(remaining_scenes, key):
    if not remaining_scenes:
        return None
    return max((remaining_scenes.items()), key=lambda arg:key(*arg[1]))[0]

def mock():
    count = 10
    remaining_scenes = {i:('',{j for j in range(count) if not random.randint(0,3)},i) for i in range(count)}
    for _, cover1, i in list(remaining_scenes.values()):
        for k in list(cover1):
            remaining_scenes[i][1].add(k)
            remaining_scenes[k][1].add(i)

    pprint.pprint(remaining_scenes)
    graph = {i:{j
                for _,cover2,j in remaining_scenes.values()
                if i in cover2}
                for _,cover1,i in remaining_scenes.values()}
    pprint.pprint(graph)
    selected_scenes = []

    remaining_coverage = set(graph.keys())
    candidates_idx = set()

    def minset_evaluator(scene, coverage, idx):
        nonlocal candidates_idx
        nonlocal graph
        retval = len(graph[idx].difference(candidates_idx))
        return retval

    while remaining_scenes:
        if not remaining_coverage:
            break
        candidate = chooseScene(remaining_scenes, minset_evaluator)
        scene, coverage, _ = remaining_scenes.pop(candidate)
        remaining_coverage -= coverage
#        candidates.append(scene)
        candidates_idx.add(candidate)
        # 'day-night_indicator': 'DAY',

        # pprint.pprint(scenes[0])
        # pprint.pprint([scene['spatial_bounds'] for scene in scenes])
        # pprint.pprint([scene['acquisition_date'] for scene in scenes])
    pprint.pprint(candidates_idx)

def main(argv):
    try:
        usgs = USGS2Raster(landsat_ds='landsat_ot_c2_l2', bands='1,2,3', cache_dir='usgs-cache', layer='satview', dtype='uint8')
        usgs.getScenes(bbox = (31,33,41,43))
        del usgs
    except Exception as e:
        del usgs
        traceback.print_exception(e)
        time.sleep(1)
    time.sleep(1)
