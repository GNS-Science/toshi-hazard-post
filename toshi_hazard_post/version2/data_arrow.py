import logging
import time
from typing import TYPE_CHECKING, List

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow import fs

from toshi_hazard_post.version2.local_config import ARROW_DIR

# from toshi_hazard_post.version2.calculators import rate_to_prob, prob_to_rate


if TYPE_CHECKING:
    from nzshm_common.location.code_location import CodedLocation

    from toshi_hazard_post.version2.logic_tree import HazardLogicTree

log = logging.getLogger(__name__)


def load_realizations(
    logic_tree: 'HazardLogicTree',
    imts: List[str],
    location: 'CodedLocation',
    vs30: int,
    compatibility_key: str,
) -> pa.table:
    """
    Load component realizations from the database.

    Parameters:
        logic_tree: the full (srm + gmcm) logic tree
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        location: the site location
        vs30: the site vs30
        compatibility_key: the compatibility key used to lookup the correct realizations in the database

    Returns:
        values: the component realizations rates (not probabilities)
    """
    filesystem = fs.LocalFileSystem()
    root = str(ARROW_DIR)

    partition = f"nloc_0={location.downsample(1).code}"
    t0 = time.monotonic()
    dataset = ds.dataset(f'{root}/{partition}', format='parquet', filesystem=filesystem)
    t1 = time.monotonic()

    flt0 = (
        (pc.field('nloc_001') == pc.scalar(location.downsample(0.001).code))
        # & (pc.field('imt') == pc.scalar(imt))
        & (pc.is_in(pc.field('imt'), pa.array(imts)))
        & (pc.field('vs30') == pc.scalar(vs30))
        & (pc.field('compatible_calc_fk') == pc.scalar(compatibility_key))
    )
    columns = ['sources_digest', 'gmms_digest', 'values', 'imt']
    arrow_scanner = ds.Scanner.from_dataset(dataset, filter=flt0, columns=columns)
    t2 = time.monotonic()

    ## NB was trying to figure out if ducksb was causing the numpy array issues
    # con = duckdb.connect()
    # results = con.execute(f"SELECT sources_digest, gmms_digest, values from arrow_scanner;")
    # rlz_table = results.arrow()

    # colvals = rlz_table.column(2).combine_chunks()
    # # print(np.array(colvals))
    # assert np.all(rlz_table.column(2).to_numpy()[0]) == np.all(np.array(colvals)[0])
    # assert 0
    # print(rlz_table.column(2).to_numpy().shape)
    # print(rlz_table.column(2).to_pandas().shape)

    t3 = time.monotonic()
    rlz_table = arrow_scanner.to_table()
    t4 = time.monotonic()

    log.info(
        f"load ds: {round(t1-t0, 6)}, scanner:{round(t2-t1, 6)} duck_sql:{round(t3-t2, 6)}: to_arrow {round(t4-t3, 6)}"
    )
    log.info("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
    log.info("loaded %s realizations in arrow", rlz_table.shape[0])
    return rlz_table


# def save_aggregations(
#     hazard: 'npt.NDArray',
#     location: 'CodedLocation',
#     vs30: int,
#     imt: str,
#     agg_types: List[str],
#     hazard_model_id: str,
# ) -> None:
#     """
#     Save the aggregated hazard to the database. Converts hazard as rates to proabilities before saving.

#     Parameters:
#         hazard: the aggregate hazard rates (not proabilities)
#         location: the site location
#         vs30: the site vs30
#         imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
#         agg_types: the statistical aggregate types (e.g. "mean", "0.5")
#         hazard_model_id: the model id for storing in the database
#     """
#     hazard = rate_to_prob(hazard, 1.0)
#     write_aggs_to_ths(hazard, location, vs30, imt, agg_types, hazard_model_id)
