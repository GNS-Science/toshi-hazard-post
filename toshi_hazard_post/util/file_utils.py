import csv
import io
import json
import logging
import os
from collections import namedtuple
from dataclasses import asdict
from enum import Enum
from functools import reduce
from operator import mul
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import numpy.typing as npt
import pandas as pd
from nzshm_common.location.code_location import CodedLocation

# from toshi_hazard_post.logic_tree.branch_combinator import SourceBranchGroup
from toshi_hazard_post.logic_tree.logic_tree import HazardLogicTree

log = logging.getLogger(__name__)


class Dimension(Enum):
    trt = 'TRT'
    mag = 'Mag'
    dist = 'Dist'
    eps = 'Eps'


def disagg_df(rlz_names, dimensions, bin_widths):

    dimensions = list(map(str.lower, dimensions))

    mags = np.arange(0, 10, bin_widths['mag']) + bin_widths['mag'] / 2.0
    mags = mags[(mags >= 5) & (mags <= 10)]
    epss = np.arange(-4, 4, bin_widths['eps']) + bin_widths['eps'] / 2.0
    dists = np.array(bin_widths['dist'])  # distance bins are defined explicitly

    trts = ['Active Shallow Crust', 'Subduction Interface', 'Subduction Intraslab']

    bins = dict(
        mag=list(map('{:0.3f}'.format, mags)),
        dist=list(map('{:0.3f}'.format, dists)),
        trt=trts,
        eps=list(map('{:0.3f}'.format, epss)),
    )

    bin_centers = dict(
        mag=mags,
        dist=dists,
        trt=trts,
        eps=epss,
    )
    bin_centers = {k: v for k, v in bin_centers.items() if k in dimensions}

    bins = {k: v for k, v in bins.items() if k in dimensions}
    array_lens = [len(v) for v in bins.values()]
    total_length = reduce(mul, array_lens)

    log.info('bin centers %s' % bins)
    log.info('total array length: %s' % total_length)

    for rlz_name in rlz_names:
        bins[rlz_name] = [0]

    return pd.MultiIndex.from_product(bins.values(), names=bins.keys()).to_frame(index=False), bin_centers


def get_location(header):
    """
    get the location from the disagg csv
    """

    info = header[-1]
    start_lon = info.index('lon=') + 4
    tail = info[start_lon:]
    try:
        end_lon = tail.index(',')
        lon = tail[:end_lon]
    except ValueError:
        lon = tail[:]

    start_lat = info.index('lat=') + 4
    tail = info[start_lat:]
    try:
        end_lat = tail.index(',')
        lat = tail[:end_lat]
    except ValueError:
        lat = tail[:]

    location = CodedLocation(float(lat), float(lon), 0.001).code

    return location


def match_index(disaggs, values):

    ind = pd.Series(data=True, index=range(len(disaggs)))
    for name, value in values.items():
        ind = ind & disaggs[name].isin([value])
    return ind


def get_values_from_csv(disagg_data):

    values = {}
    for field in disagg_data._fields:
        if field not in ['mag', 'dist', 'trt', 'eps']:
            continue
        if field == 'trt':
            values['trt'] = disagg_data.trt
        else:
            values[field] = f'{float(getattr(disagg_data,field)):0.3f}'
    return values


def get_bin_widths(header):

    info = header[-1]
    dimensions = ['mag', 'dist', 'eps']
    bin_widths = {}

    for d in dimensions:
        s = d + '_bin_edges=['
        tail = info[info.index(s) + len(s) :]
        bin_edges = list(map(float, tail[: tail.index(']')].split(', ')))
        if d == 'dist':  # distance bins are defined explicitly
            bin_widths[d] = [(bin_edges[i + 1] + bin_edges[i]) / 2.0 for i in range(len(bin_edges) - 1)]
        else:
            bin_widths[d] = bin_edges[1] - bin_edges[0]

    return bin_widths


def load_file(disagg_file, deagg_dimensions, csv_archive):

    disagg_reader = csv.reader(disagg_file)
    header0 = next(disagg_reader)
    location = get_location(header0)
    bin_widths = get_bin_widths(header0)

    header = next(disagg_reader)
    DisaggData = namedtuple("DisaggData", header, rename=True)
    rlz_names = [col for col in header if 'rlz' in col]
    disaggs, bins = disagg_df(rlz_names, deagg_dimensions, bin_widths)

    ind_rlz = len(deagg_dimensions)
    ind_rlz_csv = ['rlz' in col for col in header].index(True)
    for row in disagg_reader:
        disagg_data = DisaggData(*row)
        values = get_values_from_csv(disagg_data)
        imt = disagg_data.imt
        ind = match_index(disaggs, values)
        if not any(ind):
            exc_text = f'no index found for {csv_archive} row: {row}'
            exc_text += f'\nvalues: {values}'
            raise Exception(exc_text)
        disaggs.iloc[ind, ind_rlz:] = list(map(float, row[ind_rlz_csv:]))

    return disaggs, bins, location, imt, rlz_names


def get_disagg(csv_archive, deagg_dimensions):
    """
    get the disagg data from the csv archive
    this is terrible and hacky and only temporory until disagg data is stored by THS
    assuming only 1 location and 1 imt.

    If the requested file is missing, assume that the poe is zero (oq does not write non TRT disaggs if poe==0)
    """

    deagg_dimensions = list(map(str.lower, deagg_dimensions))
    filename = '_'.join([d.value for d in Dimension if d.name in deagg_dimensions]) + '-0_1.csv'

    with ZipFile(csv_archive) as zipf:
        if filename in zipf.namelist():
            with io.TextIOWrapper(zipf.open(filename), encoding="utf-8") as disagg_file:
                disaggs, bins, location, imt, rlz_names = load_file(disagg_file, deagg_dimensions, csv_archive)
        else:
            ddims = ['mag', 'dist', 'eps', 'trt']
            filename_fallback = '_'.join([d.value for d in Dimension]) + '-0_1.csv'
            with io.TextIOWrapper(zipf.open(filename_fallback), encoding="utf-8") as disagg_file:
                log.info(
                    f'file requested {filename} missing from archive {csv_archive} '
                    f'falling back to loading {filename_fallback}'
                )
                disaggs, bins, location, imt, rlz_names = load_file(disagg_file, ddims, csv_archive)
                if not (disaggs[rlz_names] == 0).all().all():
                    raise Exception(f'{deagg_dimensions} file missing, but not all values are 0 in file {csv_archive}')
                disaggs = disaggs.drop(labels=set(ddims).difference(set(deagg_dimensions)), axis=1).drop_duplicates(
                    subset=deagg_dimensions
                )
                bins = {k: v for k, v in bins.items() if k in deagg_dimensions}

    disaggs_dict = {}
    for rlz in rlz_names:
        disaggs_dict[rlz[3:]] = disaggs[rlz].to_numpy(dtype='float64')

    return disaggs_dict, bins, location, imt


def save_deaggs(deagg_data, bins, loc, imt, imtl, poe, vs30, model_id, deagg_dimensions):

    shape = [len(v) for v in bins.values()]
    deagg_data = deagg_data.reshape(shape)

    bins['imtl'] = imtl  # we're going to stash the IMTL of the disagg in the bins array
    bins_array = np.array(list(bins.values()), dtype=object)

    working_dir = Path(os.getenv('NZSHM22_SCRIPT_WORK_PATH'))
    dim = '-'.join(deagg_dimensions)
    deagg_filename = f'deagg_{model_id}_{loc}_{vs30}_{imt}_{int(poe*100)}_{dim}.npy'
    bins_filename = f'bins_{model_id}_{loc}_{vs30}_{imt}_{int(poe*100)}_{dim}.npy'
    deagg_dir = Path(working_dir, 'deaggs-test')
    if not deagg_dir.exists():
        deagg_dir.mkdir()
    deagg_filepath = Path(deagg_dir, deagg_filename)
    bins_filepath = Path(deagg_dir, bins_filename)
    np.save(deagg_filepath, deagg_data)
    np.save(bins_filepath, bins_array)
    log.info(f'saved deagg results to {deagg_filename}')


def save_realizations(
    imt: str, loc: str, vs30: int, branch_probs: npt.NDArray, weights: npt.NDArray, logic_tree: HazardLogicTree
) -> None:
    """Save realization arrays to disk. Should be replaced with write to THS when THS supports saving full realizations.

    Parameters
    ----------
    imt
        intensity measure type
    loc
        location code string
    vs30
        site condition
    branch_probs
        2D array of probabilities (realizations)
    weights
        array of weights
    source_branches
        logic tree definition
    """

    save_dir = '/work/chrisdc/NZSHM-WORKING/PROD/branch_rlz/SRWG/'
    branches_filepath = save_dir + f'branches_{imt}-{loc}-{vs30}'
    weights_filepath = save_dir + f'weights_{imt}-{loc}-{vs30}'
    logic_tree_filepath = save_dir + f'source_branches_{imt}-{loc}-{vs30}.json'
    np.save(branches_filepath, branch_probs)
    np.save(weights_filepath, weights)
    # TODO: this should not be done every time, the logic_tree is the same
    with open(logic_tree_filepath, 'w') as jsonfile:
        json.dump(asdict(logic_tree), jsonfile)
