"""Benchmark building composite branch rates (GitHub issue #93).

Times the legacy implementation (a Python loop over composite branches with dict lookups keyed by digest
strings, frozen in this script as the reference) against the implementation currently in toshi_hazard_post
(integer index table + numba kernel) and checks that both produce the same rates.

Run it before and after a change to the kernel:

    uv run python scripts/bench_composite_rates.py --save before.npy
    uv run python scripts/bench_composite_rates.py --compare before.npy

By default the logic tree is synthetic but has the shape of NSHM_v1.0.4 (979,776 composite branches of 4
component branches each, 912 unique component branches). Use --bht to time against a real branch hash table,
i.e. a .npy file holding np.array(HazardLogicTree.branch_hash_table).
"""

import argparse
import statistics
import timeit
from collections.abc import Callable
from pathlib import Path

import numpy as np
import numpy.typing as npt

import toshi_hazard_post.aggregation_calc as aggregation_calc
import toshi_hazard_post.logic_tree as logic_tree

# number of unique component branches in each column of the NSHM_v1.0.4 branch hash table
NSHM_COLUMN_SIZES = (36, 108, 756, 12)
AGG_TYPES = ["mean", "cov", "std", "0.005", "0.01", "0.025", "0.5", "0.2", "0.8", "0.1", "0.9"]
DIGEST_LEN = 24


def legacy_build_branch_rates(branch_hash_table: npt.NDArray, component_rates: dict[str, npt.NDArray]) -> npt.NDArray:
    """The implementation before issue #93. Returns rates with shape (n composite branches, n levels)."""
    nlevels = len(next(iter(component_rates.values())))

    def calc_composite_rates(branch_hashes: npt.NDArray) -> npt.NDArray:
        rates = np.zeros((nlevels,))
        for branch_hash in branch_hashes:
            rates += component_rates[branch_hash]
        return rates

    return np.array([calc_composite_rates(branch) for branch in branch_hash_table])


def synthetic_hash_table(n_composite: int, rng: np.random.Generator) -> npt.NDArray:
    """A branch hash table of random digests with the same column cardinalities as NSHM_v1.0.4."""
    alphabet = np.array(list("0123456789abcdef"))
    columns = []
    for size in NSHM_COLUMN_SIZES:
        digests = np.array(["".join(rng.choice(alphabet, DIGEST_LEN)) for _ in range(size)])
        columns.append(digests[rng.integers(0, size, n_composite)])
    return np.stack(columns, axis=1)


def synthetic_component_rates(n_components: int, n_levels: int, rng: np.random.Generator) -> npt.NDArray:
    """Hazard curve like rates with shape (n components, n levels): decreasing with level, varying by branch."""
    scale = np.exp(rng.normal(0.0, 1.0, (n_components, 1)))
    noise = np.exp(rng.normal(0.0, 0.3, (n_components, n_levels)))
    return scale * np.logspace(-1, -7, n_levels)[None, :] * noise


def time_it(label: str, func: Callable[[], object], repeat: int) -> float:
    """Print the best and median wall time of func over repeat runs and return the best."""
    times = timeit.repeat(func, number=1, repeat=repeat)
    best = min(times)
    print(f"{label:<58s} best {best:8.4f} s   median {statistics.median(times):8.4f} s   (n={repeat})")
    return best


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bht", type=Path, help="path to a .npy branch hash table of digest strings")
    parser.add_argument("--n-composite", type=int, default=979_776, help="composite branches if --bht is not given")
    parser.add_argument("--n-levels", type=int, default=44, help="number of IMT levels")
    parser.add_argument("--repeat", type=int, default=3, help="number of timing repeats")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--save", type=Path, help="save the calculated aggregates to this .npy file")
    parser.add_argument("--compare", type=Path, help="compare the calculated aggregates to this .npy file")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    if args.bht:
        branch_hash_table = np.load(args.bht)
    else:
        branch_hash_table = synthetic_hash_table(args.n_composite, rng)
    component_digests = [str(digest) for digest in np.unique(branch_hash_table)]
    component_rates = synthetic_component_rates(len(component_digests), args.n_levels, rng)
    weights = rng.random(len(branch_hash_table))
    weights /= weights.sum()

    print(
        f"{branch_hash_table.shape[0]} composite branches x {branch_hash_table.shape[1]} component branches, "
        f"{len(component_digests)} unique component branches, {args.n_levels} levels\n"
    )

    print("--- legacy: digest strings + Python loop ---")
    print(f"{'shared table size':<58s} {branch_hash_table.nbytes / 2**20:8.1f} MB")
    component_rates_dict = dict(zip(component_digests, component_rates, strict=True))
    rates_legacy = legacy_build_branch_rates(branch_hash_table, component_rates_dict)
    t_legacy = time_it(
        "build composite rates (per task)",
        lambda: legacy_build_branch_rates(branch_hash_table, component_rates_dict),
        args.repeat,
    )

    print("\n--- toshi_hazard_post: integer index table ---")
    branch_index_table = logic_tree.build_branch_index_table(branch_hash_table, component_digests)
    print(f"{'shared table size':<58s} {branch_index_table.nbytes / 2**20:8.1f} MB ({branch_index_table.dtype})")
    time_it(
        "build index table (once per run, in the parent)",
        lambda: logic_tree.build_branch_index_table(branch_hash_table, component_digests),
        args.repeat,
    )
    # the first call includes numba compilation, which happens once per worker process
    t_first = timeit.timeit(lambda: aggregation_calc.build_branch_rates(branch_index_table, component_rates), number=1)
    print(f"{'build composite rates, first call (includes JIT compile)':<58s} {t_first:13.4f} s")
    rates = aggregation_calc.build_branch_rates(branch_index_table, component_rates)
    t_new = time_it(
        "build composite rates (per task)",
        lambda: aggregation_calc.build_branch_rates(branch_index_table, component_rates),
        args.repeat,
    )
    print(f"\nspeedup of build composite rates: {t_legacy / t_new:.1f}x")
    print(f"rates identical to legacy (level-major vs branch-major): {np.array_equal(rates.T, rates_legacy)}")

    print("\n--- downstream: calculate_aggs() on the rates produced above ---")
    aggs = aggregation_calc.calculate_aggs(rates, weights, AGG_TYPES)
    time_it(
        f"calculate_aggs, rates shape {rates.shape}",
        lambda: aggregation_calc.calculate_aggs(rates, weights, AGG_TYPES),
        args.repeat,
    )

    if args.save:
        np.save(args.save, aggs)
        print(f"\nsaved aggregates to {args.save}")
    if args.compare:
        expected = np.load(args.compare)
        with np.errstate(divide='ignore', invalid='ignore'):
            rel_diff = np.nanmax(np.abs(aggs - expected) / np.abs(expected))
        print(f"\naggregates identical to {args.compare}: {np.array_equal(aggs, expected)}")
        print(f"maximum relative difference: {rel_diff:.3e}")


if __name__ == "__main__":
    main()
