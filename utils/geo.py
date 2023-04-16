from rasterio.crs import CRS
from rasterio import features, warp
from shapely import geometry, ops, prepared, box
import fiona
import pprint
import warnings

WGS84 = CRS.from_epsg(4326)

def getCrsFromInput(input):
    return CRS.from_user_input(input)

def projectBounds(bounds, dst_crs, src_crs=WGS84):
    bounds = warp.transform_bounds(src_crs, dst_crs, *bounds)
    return box(*bounds)

def boxToWkt(box):
    return box(*box).wkt

def getBoxFromDataset(ds):
    pass
def checkIfDsIntersect(test_ds, ref_ds):
    box1 = box(*ref_ds.bounds)
    box2 = box(
        *warp.transform_bounds(test_ds.crs, ref_ds.crs, *test_ds.bounds))
    return box1.intersects(box2)

def readFeaturesFromShapeFile(shape_file, bounds=None, where=None, crs=None, getattribute=None):
    source = fiona.open(shape_file, "r")
    crs = source.crs if crs is None else crs
    src_crs = source.crs
    if bounds is not None:
        bounds = warp.transform_bounds(crs, src_crs, *bounds)

    def featureToElement(feature):
        shape = geometry.shape(
            warp.transform_geom(
                src_crs,
                crs,
                geometry.shape(
                    feature.geometry)))
        return (shape if getattribute is None
                    else (shape, feature.properties.get(getattribute, None)))

    return (
        featureToElement(feature)
        for feature in source.filter(where=where,bbox=bounds))

def deduceBoundsFromShapeFile(shape_file, where, crs):
    print(f"-- deducing bounds from shapefile {shape_file} where {where}")
    warnings.warn("be smarter")
    obj = ops.unary_union(list(readFeaturesFromShapeFile(shape_file, where=where, crs=crs)))
    return box(*obj.bounds)

def deduceBoundsFromArgs(args, projection):
    if args.orig_bounds:
        if args.deduce_bounds:
            raise Exception(f"--area and --deduce-area are mutually exclusive")
        print(f"-- deducing bounds by projecting coordinates")
        gps_bounds = box(*args.orig_bounds)
        return projectBounds(args.orig_bounds, dst_crs=projection), gps_bounds
    elif args.deduce_bounds:
        gps_bounds = deduceBoundsFromShapeFile(*args.deduce_bounds, WGS84)
        warnings.warn('beuuuh')
        return deduceBoundsFromShapeFile(*args.deduce_bounds, projection),gps_bounds
    else:
        raise Exception(f"At least --area or --deduce-area should be provided")

