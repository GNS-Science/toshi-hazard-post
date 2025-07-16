import csv
from collections import namedtuple
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Generator, Iterable, Optional, Union

from nzshm_common.location.coded_location import CodedLocation
from nzshm_common.location.location import get_locations
from nzshm_model import get_model_version
from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree


@dataclass
class Site:
    location: CodedLocation
    vs30: int

    def __repr__(self):
        return f"{self.location.lat}, {self.location.lon}, vs30={self.vs30}"


def get_vs30s(site_filepath: Union[str, Path]) -> Generator[int, None, None]:
    with Path(site_filepath).open() as site_file:
        reader = csv.reader(site_file)
        SiteCSV = namedtuple("SiteCSV", next(reader), rename=True)  # type:ignore
        for row in reader:
            site = SiteCSV(*row)
            yield int(site.vs30)  # type:ignore


def get_logic_trees(
    nshm_model_version: Optional[str] = None,
    srm_logic_tree_filepath: Optional[Union[str, Path]] = None,
    gmcm_logic_tree_filepath: Optional[Union[str, Path]] = None,
) -> tuple[SourceLogicTree, GMCMLogicTree]:
    """Get a source and ground motion logic tree given a NZ NSHM model version availble from nzhsm-model
    and/or filepaths to logic trees. Any logic tree files passed will take precidence over the logic
    trees from the model version (i.e. if nshm_model_version an srm_logic_tree_filepath are both passed,
    the ground motion logic tree will come from the nshm_model_version but the source logic tree will
    come from the file).

    Parameters:
        nshm_model_version: model version from nzshm-model package
        srm_logic_tree_filepath: path to a json file defining a SourceLogicTree object
        gmcm_logic_tree_filepath: path to a json file defining a GMCMLogicTree object

    Returns:
        a tuple of source logic tree and ground motion logic tree
    """

    if nshm_model_version:
        model = get_model_version(nshm_model_version)
        srm_logic_tree = model.source_logic_tree
        gmcm_logic_tree = model.gmm_logic_tree

    if srm_logic_tree_filepath:
        srm_logic_tree = SourceLogicTree.from_json(srm_logic_tree_filepath)
    if gmcm_logic_tree_filepath:
        gmcm_logic_tree = GMCMLogicTree.from_json(gmcm_logic_tree_filepath)

    return srm_logic_tree, gmcm_logic_tree


def get_sites(
    locations_file: Optional[Path] = None,
    locations: Optional[Iterable[str]] = None,
    vs30s: Optional[Iterable[int]] = None,
) -> list[Site]:
    """
    Get the sites (combined location and vs30) at which to calculate hazard. Either a locations_file
    or locations can be passed, but not both. If the locations_file contains vs30 values, they
    will be used. If the vs30s argument is passed they will be iterated over as uniform vs30
    values (e.g. for 4 locations and 2 vs30s, 8 location-vs30 pairs will be returned).

    Parameters:
        locations_file: file path to a csv file of site lat,lon, and optionally vs30 values
        locations: location identifiers. Identifiers can be anything accepted
        by nzshm_common.location.location.get_locations
        vs30s: the vs30s. If empty use the vs30s from the site files

    Returns:
        location_vs30s: Location, vs30 pairs

    Raises:
        ValueError: If both locations and locations_file passed
        ValueError: If vs30s not passed and both locations_file does not have a vs30 row
        ValueError: If locations but not vs30s passed
        ValueError: If neither locations or locations_file passed
    """

    if locations_file and locations:
        raise ValueError("cannot provide both locations and locations_file")

    if locations:
        coded_locations = get_locations(locations, resolution=0.001)
    elif locations_file:
        coded_locations = get_locations([locations_file], resolution=0.001)

    if vs30s:
        sites = [Site(location, vs30) for location, vs30 in product(coded_locations, vs30s)]
    elif locations_file:
        vs30s = list(get_vs30s(locations_file))
        sites = list(map(Site, coded_locations, vs30s))
    else:
        raise ValueError(
            """must provide one of:
        a) locations and vs30s
        b) locations_file and vs30s
        c) locations_file with vs30 row"""
        )

    return sites
