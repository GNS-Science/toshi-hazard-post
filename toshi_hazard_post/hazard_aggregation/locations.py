from typing import Dict, List, Tuple

from nzshm_common.grids.region_grid import load_grid
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID, LOCATIONS_SRWG214_BY_ID

from toshi_hazard_post.hazard_aggregation.aggregation_config import AggregationConfig

def stat_test_missing():

    locations = [
        # (-38.100, 176.800),
        (-42.400, 171.200),
        # (-42.100, 171.900),
    ]

    return [(round(loc[0], 1), round(loc[1], 1)) for loc in locations]


def stat_test_64():

    locations = [
        (-42.117, 171.86),
        (-38.114, 176.817),
        (-39.717, 175.138),
        (-41.802, 172.318),
        (-42.449, 171.211),
        (-42.718, 170.964),
        (-42.949, 171.568),
        (-43.274, 172.596),
        (-43.483, 172.53),
        (-42.813, 173.275),
        (-43.566, 172.624),
        (-40.899, 176.221),
        (-43.227, 171.724),
        (-43.312, 172.381),
        (-43.49, 172.102),
        (-45.902, 170.493),
        (-43.667, 172.198),
        (-40.649, 175.709),
        (-40.214, 175.573),
        (-44.099, 170.829),
        (-38.666, 178.023),
        (-43.883, 169.044),
        (-43.54, 171.96),
        (-42.523, 172.83),
        (-41.077, 175.23),
        (-41.197, 174.892),
        (-43.715, 169.423),
        (-45.521, 167.278),
        (-41.211, 175.461),
        (-41.508, 173.944),
        (-41.265, 174.706),
        (-42.088, 173.257),
        (-43.706, 172.654),
        (-41.271, 173.284),
        (-39.507, 176.897),
        (-43.996, 168.661),
        (-43.445, 172.661),
        (-43.326, 172.038),
        (-40.914, 175.005),
        (-43.924, 171.234),
        (-40.302, 176.612),
        (-45.032, 168.663),
        (-43.751, 172.023),
        (-46.147, 167.473),
        (-43.809, 172.252),
        (-43.675, 172.318),
        (-43.607, 172.645),
        (-43.369, 172.495),
        (-46.537, 169.139),
        (-45.417, 167.719),
        (-38.373, 178.301),
        (-41.21, 174.276),
        (-37.633, 178.365),
        (-40.849, 172.821),
        (-38.807, 177.15),
        (-41.28, 174.778),
        (-41.231, 174.931),
        (-41.756, 171.6),
        (-41.827, 174.138),
        (-40.338, 175.87),
        (-41.284, 174.768),
        (-42.701, 172.8),
        (-39.944, 176.584),
        (-41.62, 173.351),
    ]

    return [(round(loc[0], 1), round(loc[1], 1)) for loc in locations]


def get_locations(config: AggregationConfig) -> List[Tuple[float]]:

    locations = []
    for location_spec in config.locations:
        if LOCATIONS_BY_ID.get(location_spec):
            locations.append((LOCATIONS_BY_ID[location_spec]['latitude'], LOCATIONS_BY_ID[location_spec]['longitude']))
        elif '~' in location_spec:
            locations.append(tuple(map(float, location_spec.split('~'))))
        elif location_spec == "NZ_34":
            locations += [(loc['latitude'], loc['longitude']) for loc in LOCATIONS_BY_ID.values()]
        elif location_spec == "STAT_TEST_64":
            locations += stat_test_64()
        elif location_spec == 'STAT_TEST_MISSING':
            locations += stat_test_missing()
        elif location_spec == "SRWG214":
            locations += [(loc['latitude'], loc['longitude']) for loc in LOCATIONS_SRWG214_BY_ID.values()]
        else:
            locs = (
                load_grid(location_spec)
                if not config.location_limit
                else load_grid(config.locations)[: config.location_limit]
            )
            if location_spec == 'NZ_0_1_NB_1_1':  # TODO: hacky fix to a missing point in the oq calculation grid
                l = locs.index((-34.7, 172.7))
                locs = locs[0:l] + locs[l + 1 :]
            locations += locs
    if config.location_limit:
        locs = locs[: config.location_limit]

    return locations


def locations_by_degree(
    grid_points: List[Tuple[float, float]], grid_res: float, point_res: float
) -> Dict[str, List[CodedLocation]]:
    """Produce a dict of key_location:"""

    binned: Dict[str, CodedLocation] = dict()
    for pt in grid_points:
        bc = CodedLocation(*pt, point_res).downsample(grid_res).code
        if not binned.get(bc):
            binned[bc] = []
        binned[bc].append(CodedLocation(*pt, point_res).downsample(point_res))
    return binned


def locations_by_chunk(
    grid_points: List[Tuple[float, float]], point_res: float, chunk_size: int
) -> Dict[int, List[CodedLocation]]:
    chunked = dict()
    # chunk_size=25
    # for n in range(int(len(grid_points)/chunk_size) +2):
    #     chunk = grid_points[n * chunk_size: max(len(grid_points) - n+(n*chunk_size) , n+(n*chunk_size))]
    #     print(n, chunk)
    #     chunked[n] = [CodedLocation(*pt).downsample(point_res).code for pt in chunk]

    for ni in range(int((len(grid_points) - 1) / chunk_size) + 1):
        pts = grid_points[ni * chunk_size : ni * chunk_size + chunk_size]
        coded_pts = []
        if pts:
            for pt in pts:
                coded_pts.append(CodedLocation(*pt, point_res).downsample(point_res))
        chunked[ni] = coded_pts
    return chunked


def locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001):

    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    nz_0_2 = load_grid("NZ_0_2_NB_1_1")
    nz34 = [(o['latitude'], o['longitude']) for o in LOCATIONS_BY_ID.values()]

    grid_points = nz34 + nz_0_2
    return locations_by_degree(grid_points, grid_res, point_res)


def locations_nzpt2_chunked(grid_res=1.0, point_res=0.001, range=None):

    chunk_size = 25
    grid_points = load_grid("NZ_0_2_NB_1_1")
    lbc = {}
    if range:
        for i, (k, v) in enumerate(locations_by_chunk(grid_points, point_res, chunk_size).items()):
            if (i >= range[0]) & (i <= range[1]):
                lbc[k] = v
    else:
        lbc = locations_by_chunk(grid_points, point_res, chunk_size)
    return lbc


def locations_nzpt2_and_nz34_chunked(grid_res=1.0, point_res=0.001):

    chunk_size = 25
    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    nz_0_2 = load_grid("NZ_0_2_NB_1_1")
    nz34 = [(o['latitude'], o['longitude']) for o in LOCATIONS_BY_ID.values()]
    grid_points = nz34 + nz_0_2
    return locations_by_chunk(grid_points, point_res, chunk_size)


def locations_nz34_chunked(grid_res=1.0, point_res=0.001):

    chunk_size = 2
    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    nz34 = [(o['latitude'], o['longitude']) for o in LOCATIONS_BY_ID.values()]
    grid_points = nz34
    return locations_by_chunk(grid_points, point_res, chunk_size)


def locations_nz2_chunked(grid_res=1.0, point_res=0.001):
    '''used for testing'''

    chunk_size = 1
    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    cities = ['WLG', 'CHC', 'KBZ']
    nz34 = [(o['latitude'], o['longitude']) for o in LOCATIONS_BY_ID.values() if o['id'] in cities]
    grid_points = nz34
    return locations_by_chunk(grid_points, point_res, chunk_size)


if __name__ == "__main__":

    # For NZ_0_2: binning 1.0 =>  66 bins max 25 pts
    # For NZ 0_2: binning 0.5 => 202 bins max 9 pts

    # Settings for the THS rlz_query
    for key, locs in locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001):
        print(f"{key} => {locs}")
