import ast
import itertools
import logging
import math
import time
from functools import reduce
from operator import mul

import numpy as np
import pandas as pd
from toshi_hazard_store.query_v3 import get_hazard_metadata_v3, get_rlz_curves_v3

# from toshi_hazard_store.branch_combinator.SLT_37_GT_VS400_DATA import data as gtdata
# from toshi_hazard_store.branch_combinator.SLT_37_GT_VS400_gsim_DATA import data as gtdata
from toshi_hazard_post.data_functions import weighted_quantile

# from toshi_hazard_store.branch_combinator.branch_combinator import get_weighted_branches, grouped_ltbs, merge_ltbs
# from toshi_hazard_store.branch_combinator.SLT_37_GRANULAR_RELEASE_1 import logic_tree_permutations

DTOL = 1.0e-6
inv_time = 1.0
VERBOSE = True

log = logging.getLogger(__name__)


def get_imts(source_branches, vs30):

    ids = source_branches[0]['ids']
    meta = next(get_hazard_metadata_v3(ids, [vs30]))
    imts = list(meta.imts)
    imts.sort()

    return imts


def load_realization_values(toshi_ids, locs, vs30s):

    tic = time.perf_counter()
    # unique_ids = []
    # for branch in source_branches:
    #     unique_ids += branch['ids']
    # unique_ids = list(set(unique_ids))
    log.info('loading %s hazard IDs ... ' % len(toshi_ids))

    values = {}
    try:
        for res in get_rlz_curves_v3(locs, vs30s, None, toshi_ids, None):
            key = ':'.join((res.hazard_solution_id, str(res.rlz)))
            if key not in values:
                values[key] = {}
            values[key][res.nloc_001] = {}
            for val in res.values:
                values[key][res.nloc_001][val.imt] = np.array(val.vals)
    except Exception as err:
        logging.warning(
            'load_realization_values() got exception %s with toshi_ids: %s , locs: %s vs30s: %s'
            % (err, toshi_ids, locs, vs30s)
        )
        raise

    # check that the correct number of records came back
    ids_ret = []
    for k1, v1 in values.items():
        nlocs_ret = len(v1.keys())
        if not nlocs_ret == len(locs):
            log.warn('location %s missing %s locations.' % (k1, len(locs) - nlocs_ret))
        ids_ret += [k1.split(':')[0]]
    ids_ret = set(ids_ret)
    if len(ids_ret) != len(toshi_ids):
        log.warn('Missing %s toshi IDs' % (len(toshi_ids) - len(ids_ret)))
        log.warn('location %s missing %s locations.' % (k1, len(locs) - nlocs_ret))
        toshi_ids = set(toshi_ids)
        print('Missing ids: %s' % (toshi_ids - ids_ret))

    toc = time.perf_counter()
    print(f'time to load realizations: {toc-tic:.1f} seconds')

    return values


# TODO: Refactor to reduce McCabe complexity score ( and intelligibility )
def build_rlz_table(branch, vs30, correlations=None):  # noqa: C901
    """
    build the table of ground motion combinations and weights for a single source branch
    assumes only one source per run and the same gsim weights in every run
    """

    if correlations:
        correlation_master = [corr[0] for corr in correlations]
        correlation_puppet = [corr[1] for corr in correlations]
    else:
        correlation_master = []
        correlation_puppet = []

    tic = time.perf_counter()

    ids = branch['ids']
    rlz_sets = {}
    weight_sets = {}
    for meta in get_hazard_metadata_v3(ids, [vs30]):
        gsim_lt = ast.literal_eval(meta.gsim_lt)
        trts = list(set(gsim_lt['trt'].values()))
        trts.sort()
        for trt in trts:
            rlz_sets[trt] = {}
            weight_sets[trt] = {}

    for meta in get_hazard_metadata_v3(ids, [vs30]):
        rlz_lt = ast.literal_eval(meta.rlz_lt)
        gsim_lt = ast.literal_eval(meta.gsim_lt)
        for trt in rlz_sets.keys():
            if trt in rlz_lt:
                # gsims = list(set(rlz_lt[trt].values()))
                gsims = list(set(gsim_lt['uncertainty'].values()))
                gsims.sort()
                for gsim in gsims:
                    rlz_sets[trt][gsim] = []

    for meta in get_hazard_metadata_v3(ids, [vs30]):
        rlz_lt = ast.literal_eval(meta.rlz_lt)
        gsim_lt = ast.literal_eval(meta.gsim_lt)
        hazard_id = meta.hazard_solution_id
        trts = list(set(gsim_lt['trt'].values()))
        trts.sort()
        for trt in trts:
            # for rlz, gsim in rlz_lt[trt].items():
            for rlz, gsim in gsim_lt['uncertainty'].items():
                rlz_key = ':'.join((hazard_id, rlz))
                rlz_sets[trt][gsim].append(rlz_key)
                weight_sets[trt][gsim] = 1 if gsim in correlation_puppet else gsim_lt['weight'][rlz]

    # find correlated gsims and mappings between gsim name and rlz_key
    if correlations:
        all_rlz = [(gsim, rlz) for rlz_set in rlz_sets.values() for gsim, rlz in rlz_set.items()]
        correlation_list = []
        all_rlz_copy = all_rlz.copy()
        for rlzm in all_rlz:
            for i, cm in enumerate(correlation_master):
                if cm == rlzm[0]:
                    correlation_list.append(rlzm[1].copy())
                    for rlzp in all_rlz_copy:
                        if correlation_puppet[i] == rlzp[0]:
                            correlation_list[-1] += rlzp[1]

    rlz_sets_tmp = rlz_sets.copy()
    weight_sets_tmp = weight_sets.copy()
    for k, v in rlz_sets.items():
        rlz_sets_tmp[k] = []
        weight_sets_tmp[k] = []
        for gsim in v.keys():
            rlz_sets_tmp[k].append(v[gsim])
            weight_sets_tmp[k].append(weight_sets[k][gsim])

    rlz_lists = list(rlz_sets_tmp.values())
    weight_lists = list(weight_sets_tmp.values())

    # TODO: fix rlz from the same ID grouped together
    # TODO: I sure hope itertools.product produces the same order every time
    rlz_iter = itertools.product(*rlz_lists)
    weight_iter = itertools.product(*weight_lists)
    rlz_combs = []
    weight_combs = []

    for src_group, weight_group in zip(rlz_iter, weight_iter):
        if correlations:
            foo = [s for src in src_group for s in src]
            if any([len(set(foo).intersection(set(cl))) == len(cl) for cl in correlation_list]):
                rlz_combs.append(foo)
                weight_combs.append(reduce(mul, weight_group, 1))
        else:
            rlz_combs.append([s for src in src_group for s in src])
            weight_combs.append(reduce(mul, weight_group, 1))

    toc = time.perf_counter()
    if VERBOSE:
        print(f'time to build realization table: {toc-tic:.1f} seconds')

    sum_weight = sum(weight_combs)
    if not ((sum_weight > 1.0 - DTOL) & (sum_weight < 1.0 + DTOL)):
        print(sum_weight)
        raise Exception('weights do not sum to 1')

    return rlz_combs, weight_combs


def get_weights(branch, vs30):

    weights = {}
    ids = branch['ids']
    for meta in get_hazard_metadata_v3(ids, [vs30]):
        rlz_lt = ast.literal_eval(meta.rlz_lt)  # TODO should I be using this or gsim_lt?
        hazard_id = meta.hazard_solution_id

        for rlz, weight in rlz_lt['weight'].items():
            rlz_key = ':'.join((hazard_id, rlz))
            weights[rlz_key] = weight

    return weights


def prob_to_rate(prob):

    return -np.log(1 - prob) / inv_time


def rate_to_prob(rate):

    return 1.0 - np.exp(-inv_time * rate)


def build_source_branch(values, rlz_combs, imt, loc):

    # TODO: there has got to be a better way to do this!
    k1 = next(iter(values.keys()))
    k2 = next(iter(values[k1].keys()))
    k3 = next(iter(values[k1][k2].keys()))
    rate_shape = values[k1][k2][k3].shape

    tic = time.perf_counter()
    for i, rlz_comb in enumerate(rlz_combs):
        rate = np.zeros(rate_shape)
        for rlz in rlz_comb:
            rate += prob_to_rate(values[rlz][loc][imt])
        prob = rate_to_prob(rate)
        if i == 0:
            prob_table = np.array(prob)
        else:
            prob_table = np.vstack((prob_table, np.array(prob)))

    toc = time.perf_counter()
    log.debug('build_source_branch took: %s' % (toc - tic))
    return prob_table


def calculate_aggs(branch_probs, aggs, weight_combs):

    tic = time.perf_counter()
    median = np.array([])
    for i in range(branch_probs.shape[1]):
        quantiles = weighted_quantile(branch_probs[:, i], aggs, sample_weight=weight_combs)
        if i == 0:
            median = np.array(quantiles)
        else:
            median = np.vstack((median, quantiles))
    toc = time.perf_counter()
    log.debug('calculate_aggs took: %s' % (toc - tic))
    return median


def build_branches(source_branches, values, imt, loc, vs30):
    '''for each source branch, assemble the gsim realization combinations'''

    tic = time.perf_counter()

    weights = np.array([])
    for i, branch in enumerate(source_branches):

        # rlz_combs, weight_combs = build_rlz_table(branch, vs30)
        rlz_combs = branch['rlz_combs']
        weight_combs = branch['weight_combs']

        w = np.array(weight_combs) * branch['weight']
        weights = np.hstack((weights, w))

        # set of realization probabilties for a single complete source branch
        # these can then be aggrigated in prob space (+/- impact of NB) to create a hazard curve
        if i == 0:
            branch_probs = build_source_branch(values, rlz_combs, imt, loc)
        else:
            branch_probs = np.vstack((branch_probs, build_source_branch(values, rlz_combs, imt, loc)))

    toc = time.perf_counter()
    log.debug('build_branches took: %s ' % (toc - tic))

    return weights, branch_probs


def load_source_branches():

    source_branches = [
        dict(name='test', ids=['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2MDE0', 'A'], weight=1.0)
        # dict(name='A', ids=['A_CRU', 'A_HIK', 'A_PUY'], weight=0.25),
        # dict(name='B', ids=['B_CRU', 'B_HIK', 'B_PUY'], weight=0.75),
    ]

    return source_branches


def get_levels(source_branches, locs, vs30):

    id = source_branches[0]['ids'][0]

    log.info(f"{locs[0]} {vs30}, {id}")
    hazard = next(get_rlz_curves_v3([locs[0]], [vs30], None, [id], None))

    return hazard.values[0].lvls


def concat_df_files(df_file_names):
    columns = ['lat', 'lon', 'imt', 'agg', 'level', 'hazard']

    hazard_curves = pd.DataFrame(columns=columns)

    dtype = {'lat': str, 'lon': str}

    for df_file_name in df_file_names:
        binned_hazard_curves = pd.read_json(df_file_name, dtype=dtype)
        hazard_curves = pd.concat([hazard_curves, binned_hazard_curves], ignore_index=True)

    return hazard_curves


def compute_hazard_at_poe(levels, values, poe, inv_time):

    rp = -inv_time / np.log(1 - poe)
    haz = np.exp(np.interp(np.log(1 / rp), np.flip(np.log(values)), np.flip(np.log(levels))))
    return haz


def process_disagg_location_list(hazard_curves, source_branches, toshi_ids, poes, inv_time, vs30, locs, aggs, imts):

    values = load_realization_values(toshi_ids, locs, [vs30])
    k1 = next(iter(values.keys()))
    k2 = next(iter(values[k1].keys()))
    k3 = next(iter(values[k1][k2].keys()))
    rate_shape = values[k1][k2][k3].shape

    disagg_rlzs = []
    for loc in locs:
        lat, lon = loc.split('~')
        for poe in poes:
            for agg in aggs:
                for imt in imts:
                    # disagg_key = ':'.join((loc, str(poe), str(agg), imt))

                    # get target level of shaking
                    hc = hazard_curves.loc[
                        (hazard_curves['agg'] == agg)
                        & (hazard_curves['imt'] == imt)
                        & (hazard_curves['lat'] == lat)
                        & (hazard_curves['lon'] == lon)
                    ]
                    levels = hc['level'].to_numpy()
                    hazard_vals = hc['hazard'].to_numpy()
                    target_level = compute_hazard_at_poe(levels, hazard_vals, poe, inv_time)
                    min_dist = math.inf

                    # find realization with nearest level of shaking
                    # TODO: repeating a lot of code here. Unify with agg processing code when done
                    for i, branch in enumerate(source_branches):
                        rlz_combs = branch['rlz_combs']

                        for i, rlz_comb in enumerate(rlz_combs):
                            rate = np.zeros(rate_shape)
                            for rlz in rlz_comb:
                                rate += prob_to_rate(values[rlz][loc][imt])
                            prob = rate_to_prob(rate)
                            rlz_level = compute_hazard_at_poe(levels, prob, poe, inv_time)
                            dist = abs(rlz_level - target_level)
                            if dist < min_dist:
                                nearest_rlz = rlz_comb
                                min_dist = dist
                                nearest_level = rlz_level

                    hazard_ids = [id.split(':')[0] for id in nearest_rlz]
                    source_ids, gsims = get_source_and_gsim(nearest_rlz, vs30)

                    disagg_rlzs.append(
                        dict(
                            vs30=vs30,
                            source_ids=source_ids,
                            inv_time=inv_time,
                            imt=imt,
                            agg=agg,
                            poe=poe,
                            level=nearest_level,
                            location=loc,
                            gsims=gsims,
                            dist=min_dist,
                            nearest_rlz=nearest_rlz,
                            target_level=target_level,
                            hazard_ids=hazard_ids,
                        )
                    )
    return disagg_rlzs


def get_source_and_gsim(rlz, vs30):

    gsims = {}
    source_ids = []
    for rlz_key in rlz:
        id, gsim_rlz = rlz_key.split(':')
        meta = next(get_hazard_metadata_v3([id], [vs30]))
        gsim_lt = ast.literal_eval(meta.gsim_lt)
        rlz_lt = ast.literal_eval(meta.rlz_lt)
        trt = gsim_lt['trt']['0']
        if gsims.get(trt):
            if not gsims[trt] == rlz_lt[trt][gsim_rlz]:
                raise Exception(f'single branch has more than one gsim for trt {trt}')
        gsims[trt] = rlz_lt[trt][gsim_rlz]
        source_ids += (rlz_lt['source combination'][gsim_rlz]).split('|')

    source_ids = [sid for sid in source_ids if sid]  # remove empty strings

    return source_ids, gsims


if __name__ == "__main__":
    pass
    """
    tic_total = time.perf_counter()

    # TODO: I'm making assumptions that the levels array is the same for every realization, imt, run, etc.
    # If that were not the case, I would have to add some interpolation

    binned_locs = locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001)

    vs30 = 400
    aggs = ['mean', 0.05, 0.1, 0.2, 0.5, 0.8, 0.9, 0.95]

    # source_branches = load_source_branches()
    # omit = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2MDEy']  # this is the failed/clonded job
    omit: List[str] = []
    toshi_ids = [b.hazard_solution_id for b in merge_ltbs(logic_tree_permutations, gtdata=gtdata, omit=omit)]

    grouped = grouped_ltbs(merge_ltbs(logic_tree_permutations, gtdata=gtdata, omit=omit))
    source_branches = get_weighted_branches(grouped)

    # imts = get_imts(source_branches, vs30)
    binned_locs = locations_nzpt2_and_nz34_chunked(grid_res=1.0, point_res=0.001)
    levels = get_levels(source_branches, list(binned_locs.values())[0], vs30)  # TODO: get seperate levels for every IMT

    # print(source_branches)
    imts = ['PGA']
    # imts = get_imts(source_branches, vs30)
    levels = get_levels(source_branches, list(binned_locs.values())[0], vs30)  # TODO: get seperate levels for every IMT

    columns = ['lat', 'lon', 'imt', 'agg', 'level', 'hazard']
    # index = range(len(locs)*len(imts)*len(aggs)*len(levels))
    hazard_curves = pd.DataFrame(columns=columns)

    tic = time.perf_counter()
    for i in range(len(source_branches)):
        rlz_combs, weight_combs = build_rlz_table(source_branches[i], vs30)
        source_branches[i]['rlz_combs'] = rlz_combs
        source_branches[i]['weight_combs'] = weight_combs
    toc = time.perf_counter()
    print(f'time to build all realization tables {toc-tic:.1f} seconds')

    for key, locs in locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001).items():
        binned_hazard_curves = process_location_list(locs, toshi_ids, source_branches, aggs, imts, levels, vs30)
        binned_hazard_curves.to_json(f"./df_{key}_aggregates.json")
        hazard_curves = pd.concat([hazard_curves, binned_hazard_curves])

    toc = time.perf_counter()
    print(f'agg time: {toc-tic:.1f} seconds')
    print(f'total imts: {len(imts)}')
    print(f'total locations: {len(locs)}')
    print(f'total aggregations: {len(aggs)}')
    print(f'total levels: {len(levels)}')
    print(f'total time: {toc-tic_total:.1f} seconds')

    print(hazard_curves)
    """
