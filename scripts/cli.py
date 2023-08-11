"""Console script for toshi_hazard_post."""

import logging
import os
import sys
import toml

import click
from toshi_hazard_store.model import migrate as migrate_ths

# from toshi_hazard_post.hazard_aggregation import AggregationConfig, process_aggregation, process_deaggregation
from toshi_hazard_post.hazard_aggregation.aggregation import process_aggregation
from toshi_hazard_post.hazard_aggregation.aggregation_config import AggregationConfig
from toshi_hazard_post.hazard_aggregation.deaggregation import process_deaggregation
from toshi_hazard_post.hazard_aggregation.aws_aggregation import distribute_aggregation, push_test_message
from toshi_hazard_post.hazard_aggregation.aws_deaggregation import distribute_deaggregation
from toshi_hazard_post.hazard_grid.misc import get_site_lists, migrate, get_filter_locations
from toshi_hazard_post.hazard_grid.gridded_hazard import calc_gridded_hazard
from toshi_hazard_post.hazard_grid.aws_gridded_hazard import distribute_gridded_hazard

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)
logging.getLogger('nshm_toshi_client.toshi_client_base').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_post').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)
logging.getLogger('gql.transport.requests').setLevel(logging.WARN)

formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
log.addHandler(screen_handler)


@click.group()
def thp():
    pass


@thp.command(name='build-grid', help='calculate hazard grids used to create maps')
@click.option('-H', '--hazard_model_ids', help='comma-delimted list of hazard model ids.')
@click.option('-L', '--site-list', help='A site list ENUM.')
@click.option('-I', '--imts', help='comma-delimited list of imts.')
@click.option('-A', '--aggs', help='comma-delimited list of aggs.')
@click.option('-V', '--vs30s', help='comma-delimited list of vs30s.')
@click.option('-P', '--poes', help='comma-delimited list of poe_levels.')
@click.option('-c', '--config', type=click.Path(exists=True))  # help="path to a valid THU configuration file."
@click.option('-lsl', '--list-site-lists', help='print the list of sites list ENUMs and exit', is_flag=True)
@click.option('-v', '--verbose', is_flag=True)
@click.option('-d', '--dry-run', is_flag=True)
@click.option('-m', '--migrate-tables', is_flag=True)
@click.option('-f', '--force', is_flag=True)
@click.option(
    '--mode',
    '-m',
    default='LOCAL',
    type=click.Choice(['AWS_BATCH', 'LOCAL'], case_sensitive=True),
)
@click.option(
    '--iter-method',
    '-i',
    default='product',
    type=click.Choice(['product', 'zip'], case_sensitive=True),
)
@click.option('-w', '--num-workers', default=4, show_default=True)
def cli_gridded_hazard(
    hazard_model_ids,
    site_list,
    imts,
    aggs,
    vs30s,
    poes,
    config,
    list_site_lists,
    verbose,
    dry_run,
    migrate_tables,
    force,
    mode,
    iter_method,
    num_workers,
):
    """Process gridded hazard for a given set of arguments."""

    if list_site_lists:
        click.echo("ENUM name\tDetails")
        click.echo("===============\t======================================================================")
        for rg in get_site_lists():
            click.echo(f"{rg.name}\t{rg.value}")
        return

    # site_lists = site_lists.split(',') if site_lists else None

    hazard_model_ids = hazard_model_ids.split(',') if hazard_model_ids else None
    vs30s = vs30s.split(',') if vs30s else None
    imts = imts.split(',') if imts else None
    aggs = aggs.split(',') if aggs else None
    poes = poes.split(',') if poes else None
    filter_sites = None

    if config:
        conf = toml.load(config)
        if verbose:
            click.echo(f"using settings in {config} for export")

        site_list = site_list or conf.get('site_list')
        hazard_model_ids = hazard_model_ids or conf.get('hazard_model_ids')
        imts = imts or conf.get('imts')
        vs30s = vs30s or conf.get('vs30s')
        aggs = aggs or conf.get('aggs')
        poes = poes or conf.get('poes')
        filter_sites = filter_sites or conf.get('filter_sites')

    if verbose:
        click.echo(f"{hazard_model_ids} {imts} {vs30s}")

    if dry_run:
        click.echo(f"dry-run {site_list} {hazard_model_ids} {imts} {vs30s}")
        return

    if migrate_tables:
        click.echo("Ensuring that dynamodb tables are available in target region & stage.")
        migrate()

    if mode == "LOCAL":
        try:
            click.echo(filter_sites)
            filter_locations = get_filter_locations(filter_sites)
            click.echo(filter_locations)
            calc_gridded_hazard(
                location_grid_id=site_list,
                poe_levels=poes,
                hazard_model_ids=hazard_model_ids,
                vs30s=vs30s,
                imts=imts,
                aggs=aggs,
                num_workers=num_workers,
                force=force,
                filter_locations=filter_locations,
                iter_method=iter_method,
            )
        except Exception as err:
            click.echo(err)
            raise click.UsageError('An error occurred, pls check usage.')

        # haggs = query_v3.get_hazard_curves(locations, vs30s, hazard_model_ids, imts=imts, aggs=aggs)
        click.echo('Done!')
    elif mode == "AWS_BATCH":
        try:
            click.echo(filter_sites)
            filter_locations = get_filter_locations(filter_sites)
            click.echo(filter_locations)
            distribute_gridded_hazard(
                location_grid_id=site_list,
                poe_levels=poes,
                hazard_model_ids=hazard_model_ids,
                vs30s=vs30s,
                imts=imts,
                aggs=aggs,
                force=force,
                filter_locations=filter_locations,
                iter_method=iter_method,
            )
        except Exception as err:
            click.echo(err)
            raise click.UsageError('An error occurred, pls check usage.')


@thp.command(name='aggregate', help='aggregate hazard curves or disaggregations')
@click.option(
    '--mode',
    '-m',
    default=lambda: os.environ.get("NZSHM22_THP_MODE", 'LOCAL'),
    type=click.Choice(['AWS_BATCH', 'LOCAL'], case_sensitive=True),
)
@click.option('--deagg', '-d', is_flag=True)
@click.option('--push-sns-test', '-pt', is_flag=True)
@click.option('--migrate-tables', '-M', is_flag=True)
@click.argument('config', type=click.Path(exists=True))  # help="path to a valid THP configuration file."
def aggregate(config, mode, deagg, push_sns_test, migrate_tables):
    """Main entrypoint."""
    click.echo("Hazard post-processing pipeline as serverless AWS infrastructure.")
    click.echo(f"mode: {mode}")

    agconf = AggregationConfig(config)
    # click.echo(agconf)
    # log.info("Doit")

    if push_sns_test:
        push_test_message()
        return

    if mode == 'LOCAL':
        if deagg:
            process_deaggregation(agconf)
        else:
            process_aggregation(agconf)
        return
    if 'AWS_BATCH' in mode:  # TODO: multiple vs30s
        if migrate_tables:
            click.echo("Ensuring that dynamodb tables are available in target region & stage.")
            migrate_ths()
        if deagg:
            distribute_deaggregation(agconf, mode)
        else:
            distribute_aggregation(agconf, mode)
        return


if __name__ == "__main__":
    thp()  # pragma: no cover
