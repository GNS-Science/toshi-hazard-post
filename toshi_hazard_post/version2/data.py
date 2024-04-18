import logging
from typing import TYPE_CHECKING, Dict, List

import numpy as np

from toshi_hazard_post.version2.calculators import prob_to_rate, rate_to_prob
from toshi_hazard_post.version2.logic_tree import HazardComponentBranch
from toshi_hazard_post.version2.ths_mock import query_realizations, write_aggs_to_ths

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.code_location import CodedLocation

    from toshi_hazard_post.version2.logic_tree import HazardLogicTree

log = logging.getLogger(__name__)


class ValueStore:
    """
    key arguments: source (contact of source IDs), gsim (with args and arg values),
    """

    def __init__(self) -> None:
        self._values: Dict[str, npt.NDArray] = {}

    def _key(self, branch: HazardComponentBranch) -> str:
        return branch.registry_identity

    def get_values(self, branch: HazardComponentBranch) -> 'npt.NDArray':
        return self._values[self._key(branch)]

    def set_values(self, values: 'npt.NDArray', branch: HazardComponentBranch) -> None:
        self._values[self._key(branch)] = values

    def __len__(self):
        return len(self._values)


def load_realizations(
    logic_tree: 'HazardLogicTree',
    imt: str,
    location: 'CodedLocation',
    vs30: int,
    compatibility_key: str,
) -> ValueStore:
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
    value_store = ValueStore()
    for i, res in enumerate(
        query_realizations(
            location,
            vs30,
            imt,
            logic_tree.component_branches,
            compatibility_key,
        )
    ):
        component_branch = HazardComponentBranch(res.source, tuple(res.gsims))
        values = prob_to_rate(np.array(res.values), 1.0)
        value_store.set_values(values, component_branch)
    log.info(
        f"loaded {i+1} realizations and {len(value_store)} entries",
    )
    return value_store


def save_aggregations(
    hazard: 'npt.NDArray',
    location: 'CodedLocation',
    vs30: int,
    imt: str,
    agg_types: List[str],
    hazard_model_id: str,
) -> None:
    """
    Save the aggregated hazard to the database. Converts hazard as rates to proabilities before saving.

    Parameters:
        hazard: the aggregate hazard rates (not proabilities)
        location: the site location
        vs30: the site vs30
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the statistical aggregate types (e.g. "mean", "0.5")
        hazard_model_id: the model id for storing in the database
    """
    hazard = rate_to_prob(hazard, 1.0)
    write_aggs_to_ths(hazard, location, vs30, imt, agg_types, hazard_model_id)


# if __name__ == "__main__":

#     import string
#     import random
#     import numpy as np
#     from toshi_hazard_post.version2.logic_tree import HazardComponentBranch
#     class FastValueStore:
#         """For baseline performance testing"""

#         def __init__(self):
#             self._values = {}


#         def get_values(self, *, key: str) -> 'npt.NDArray':
#             return self._values[key]

#         def set_values(self, *, values: 'npt.NDArray', key: str) -> None:
#             self._values[key] = values


#     def generate_branches(source_names, gmcm_names):
#         print("generating branches")
#         branches = []
#         for sname, gname in zip(source_names, gmcm_names):
#             sb = SourceBranch(sname)
#             gb = GMCMBranch(gname)
#             branches.append(HazardComponentBranch(sb, gb))
#         return branches


#     def generate_keys():
#         print("generating keys")
#         def id_generator(size=20, chars=string.ascii_uppercase + string.digits):
#             return ''.join(random.choice(chars) for _ in range(size))
#         keys = set()
#         while len(keys) < 100:
#             keys.add(id_generator())
#         return keys

#     def generate_fast_values(keys):
#         print("generating fast values")
#         values = FastValueStore()

#         for key in keys:
#             values.set_values(values=np.random.rand(10), key=key)

#         return values

#     def generate_values(branches):
#         print("generating values")
#         values = ValueStore()
#         for branch in branches:
#             values.set_values(values=np.random.rand(10), branch=branch)
#         return values

#     def generate_values_dict(keys):
#         d = {}
#         for key in keys:
#             d[key] = np.random.rand(10)
#         return d

#     def generate_values_slow_dict(branches):
#         d = {}
#         for branch in branches:
#             key = repr(branch)
#             d[key] = np.random.rand(10)
#         return d


#     keys = generate_keys()
#     snames = generate_keys()
#     gnames = generate_keys()
#     branches = generate_branches(snames, gnames)
#     values_fast = generate_fast_values(keys)
#     values_slow = generate_values(branches)
#     values_dict = generate_values_dict(keys)
#     values_slow_dict = generate_values_slow_dict(branches)

#     def time_get_fast_values():
#         for key in keys:
#             a = values_fast.get_values(key=key)


#     def time_get_values():
#         for branch in branches:
#             a = values_slow.get_values(branch)

#     def time_get_dict_values():
#         for key in keys:
#             a = values_dict[key]

#     def time_get_dict_slow_values():
#         for branch in branches:
#             key =  repr(branch)
#             a = values_slow_dict[key]

#     import timeit
#     number = 10000

#     t_dict = timeit.timeit(time_get_dict_values, globals=globals(), number=number)
#     print(f"time to  ValueStoreDict {number} times: {t_dict} seconds")

#     t_fast = timeit.timeit(time_get_fast_values, globals=globals(), number=number)
#     print(f"time to  ValueStoreFast {number} times: {t_fast} seconds")

#     t_slow = timeit.timeit(time_get_fast_values, globals=globals(), number=number)
#     print(f"time to  ValueStore {number} times: {t_slow} seconds")
