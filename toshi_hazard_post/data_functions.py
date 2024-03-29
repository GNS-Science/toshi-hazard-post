import logging
import time
from collections import namedtuple
from typing import Any, Dict, List, Set

import numpy as np
import numpy.typing as npt
import toshi_hazard_store

# from toshi_hazard_post.logic_tree.branch_combinator import SourceBranchGroup
from toshi_hazard_post.calculators import prob_to_rate
from toshi_hazard_post.local_config import WORK_PATH
from toshi_hazard_post.logic_tree.logic_tree import HazardLogicTree
from toshi_hazard_post.util.file_utils import get_disagg
from toshi_hazard_post.util.toshi_client import download_csv

INV_TIME = 1.0
log = logging.getLogger(__name__)


# TODO: split rlz number from id rather than joining in key, could keep interface the same and then transition to
# no longer using id:rlz keys later
class ValueStore:
    """storage class for individual oq data from THS. This results in ~15-20% performance hit when calculating
    aggregations on a 44 element array, but the interface is far superior to 3x nested dicts"""

    DictKey = namedtuple("DictKey", "key loc imt")

    def __init__(self) -> None:
        self._values: Dict[ValueStore.DictKey, npt.NDArray] = {}

    def set_values(self, *, value: npt.NDArray, key: str, loc: str, imt: str) -> None:
        self._values[ValueStore.DictKey(key=key, loc=loc, imt=imt)] = value

    def values(self, *, key: str, loc: str, imt: str) -> npt.NDArray:
        return self._values[ValueStore.DictKey(key=key, loc=loc, imt=imt)]

    @property
    def len_rate(self) -> int:
        return len(next(iter(self._values.values())))

    @property
    def toshi_hazard_ids(self) -> Set[str]:
        ids = []
        for k in self._values.keys():
            ids.append(k.key.split(':')[0])
        return set(ids)

    def locs(self, toshi_hazard_id: str) -> Set[str]:
        lcs = []
        for k in self._values.keys():
            if k.key.split(':')[0] == toshi_hazard_id:
                lcs.append(k.loc)
        return set(lcs)


def get_levels(logic_tree: HazardLogicTree, locs: List[str], vs30: int, imts: List[str]) -> Any:
    """Get the values of the levels (shaking levels) for the hazard curve from Toshi-Hazard-Store

    Parameters
    ----------
    source_branches : list
        complete logic tree with Openquake Hazard Solutions Toshi IDs
    locs : List[str]
        coded locations
    vs30 : int
        vs30

    Returns
    -------
    levels : List[float]
        shaking levels of hazard curve"""

    id = logic_tree.hazard_ids[0]

    log.info(f"get_levels locs[0]: {locs[0]} vs30: {vs30}, id {id}")
    tic = time.perf_counter()
    # TODO: currently the max realizations is hardcoded as 21
    hazard = next(toshi_hazard_store.query_v3.get_rlz_curves_v3([locs[0]], [vs30], list(range(21)), [id], imts))
    toc = time.perf_counter()
    log.debug(f'time to get levels from THS: {toc-tic:.1f} seconds')

    return hazard.values[0].lvls


def get_imts(logic_tree: HazardLogicTree, vs30: int) -> list:
    """Get the intensity measure types (IMTs) for the hazard curve from Toshi-Hazard-Store

    Parameters
    ----------
    source_branches : list
        complete logic tree with Openquake Hazard Solutions Toshi IDs
    locs : List[str]
        coded locations
    vs30 : int
        vs30

    Returns
    -------
    levels : List[str]
        IMTs of hazard curve"""

    tic = time.perf_counter()
    meta = next(toshi_hazard_store.query_v3.get_hazard_metadata_v3(logic_tree.hazard_ids, [vs30]))
    toc = time.perf_counter()
    log.debug(f'time to get imts from THS: {toc-tic:.1f} seconds')
    imts = list(meta.imts)
    imts.sort()

    return imts


def check_values(values: ValueStore, toshi_hazard_ids: List[str], locs: List[str]) -> None:
    """check that the correct number of ids and locations are stored in values

    Parameters
    ----------
    values
        keys to be checked
    toshi_hazard_ids
        ids that should be present
    locs
        locations that should be present
    """

    diff_ids = set(toshi_hazard_ids) - values.toshi_hazard_ids
    if diff_ids:
        log.warn('missing ids: %s' % diff_ids)

    for id in toshi_hazard_ids:
        diff_locs = set(locs) - values.locs(id)
        if diff_locs:
            log.warn('missing locations: %s for id %s' % (diff_locs, id))


def get_site_vs30(toshi_ids: List[str], loc: str) -> float:
    # TODO fix for new version of THS

    vs30 = 0
    tic = time.perf_counter()
    for res in toshi_hazard_store.query_v3.get_rlz_curves_v3([loc], [0], [], toshi_ids, []):
        if not (vs30):
            vs30 = res.site_vs30
        elif res.site_vs30 != vs30:
            raise Exception(f'not all Hazard Solutions have teh samve site_vs30. HazardSolution IDs: {toshi_ids}')

    toc = time.perf_counter()
    log.debug(f'time to get site vs30 from THS: {toc-tic:.1f} seconds')

    return res.site_vs30


def load_realization_values(toshi_ids: List[str], locs: List[str], vs30s: List[int], imts: List[str]) -> ValueStore:
    """Load hazard curves from Toshi-Hazard-Store.

    Parameters
    ----------
    toshi_ids
        Openquake Hazard Solutions Toshi IDs
    locs
        coded location strings
    vs30s
        vs30s

    Returns
    values
        hazard curve values (probabilities) keyed by Toshi ID and gsim realization number
    """

    log.info('loading %s hazard IDs ... ' % len(toshi_ids))
    values = ValueStore()
    tic = time.perf_counter()
    try:
        for res in toshi_hazard_store.query_v3.get_rlz_curves_v3(locs, vs30s, list(range(21)), toshi_ids, imts):
            key = ':'.join((res.hazard_solution_id, str(res.rlz)))
            for val in res.values:
                values.set_values(
                    value=prob_to_rate(np.array(val.vals), INV_TIME), key=key, loc=res.nloc_001, imt=val.imt
                )
    except Exception as err:
        logging.warning(
            'load_realization_values() got exception %s with toshi_ids: %s , locs: %s vs30s: %s'
            % (err, toshi_ids, locs, vs30s)
        )
        raise

    toc = time.perf_counter()
    log.debug(f'time to load realizations from THS: {toc-tic:.1f} seconds')

    # check that the correct number of records came back
    check_values(values, toshi_ids, locs)

    return values


def load_realization_values_deagg(toshi_ids, locs, vs30s, deagg_dimensions):
    """Load deagg matricies from oq-engine csv archives. Temporoary until deaggs are stored in THS.

    Parameters
    ----------
    toshi_ids : List[str]
        Openquake Hazard Solutions Toshi IDs
    locs : List[str]
        coded locations
    vs30s : List[int]
        not used

    Returns
    values : dict
        hazard curve values (probabilities) keyed by Toshi ID and gsim realization number
    bins : dict
        bin centers for each deagg dimension
    """

    tic = time.perf_counter()
    log.info('loading %s hazard IDs ... ' % len(toshi_ids))

    values = ValueStore()

    # download csv archives
    downloads = download_csv(toshi_ids, WORK_PATH)
    log.info('finished downloading csv archives')
    for i, download in enumerate(downloads.values()):
        csv_archive = download['filepath']
        hazard_solution_id = download['hazard_id']
        disaggs, bins, location, imt = get_disagg(csv_archive, deagg_dimensions)
        log.info(f'finished loading data from csv archive {i+1} of {len(downloads)}')
        for rlz in disaggs.keys():
            key = ':'.join((hazard_solution_id, rlz))
            values.set_values(value=prob_to_rate(np.array(disaggs[rlz]), INV_TIME), key=key, loc=location, imt=imt)

    # check that the correct number of records came back
    check_values(values, toshi_ids, locs)

    toc = time.perf_counter()
    print(f'time to load realizations: {toc-tic:.1f} seconds')

    return values, bins
