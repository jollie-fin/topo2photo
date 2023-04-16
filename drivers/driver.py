from abc import ABC, abstractmethod
import utils.misc as misc

class RasterDriver(ABC):
    def __init__(self, option_type):
        print(f'-- Initializing driver {self.name()}')
        self.option_type = option_type

    def name(self):
        return self.__class__.__name__

    def checkcall(self, dst_ds, bands, shape_bounds, gps_bounds, key, layer, options):
        options = misc.capitalizeOptions(options, self.option_type)
        self.detailcheckcall(dst_ds=dst_ds, bands=bands, shape_bounds=shape_bounds, key=key, layer=layer, options=options)

    def detailcheckcall(self, *_):
        pass       

    def __call__(self, dst_ds, bands, shape_bounds, gps_bounds, key, layer, options):
        print(f'-- Rasterizing layer {layer} with {self.name()}')
        print(f'--   from {key}')
        print(f'--   on bands {bands}')
        options = misc.capitalizeOptions(options, self.option_type)
        print(f'--   with options {options}')
        self.renderToRaster(
            dst_ds = dst_ds,
            bands = bands,
            shape_bounds = shape_bounds,
            gps_bounds = gps_bounds,
            key = key,
            options = options)
        print(f'--   done')

    @abstractmethod
    def renderToRaster(self, *_):
        pass

