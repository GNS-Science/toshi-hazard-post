"""ToshiAPI interface utility & helpers."""

import json
import logging
from collections import namedtuple
from pathlib import Path, PurePath
from typing import Union

from nshm_toshi_client.toshi_client_base import ToshiClientBase
from nshm_toshi_client.toshi_file import ToshiFile

from toshi_hazard_post.local_config import API_KEY, API_URL, S3_URL, WORK_PATH
from toshi_hazard_post.util import archive

log = logging.getLogger(__name__)


def create_archive(filename: str, working_path: Union[str, PurePath]) -> str:
    """Verify source and if OK return the path to the zipped contents."""
    log.info(f"create_archive {filename}.zip in working_path={working_path}")
    if Path(filename).exists():
        return archive(filename, Path(working_path, f"{Path(filename).name}.zip"))
    raise Exception("file does not exist.")


def save_sources_to_toshi(filepath: str, tag: str = None) -> str:
    """Archive and upload one file."""
    log.info(f"Processing */{Path(filepath).name} :: {tag}")

    archive_path = create_archive(filepath, WORK_PATH)
    log.info(f'archived {filepath} in {archive_path}.')

    headers = {"x-api-key": API_KEY}
    toshi_api = ToshiFile(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    filename = Path(filepath).name

    meta = dict(filename=filename)
    if tag:
        meta['tag'] = str(tag)

    archive_file_id, post_url = toshi_api.create_file(Path(archive_path), meta=meta)
    toshi_api.upload_content(post_url, archive_path)
    log.info(f"pushed {archive_path} to ToshiAPI {API_URL} with id {archive_file_id}")
    return archive_file_id


class DisaggDetails(ToshiClientBase):
    def __init__(self, url, s3_url, auth_token, with_schema_validation=False, headers=None):
        super().__init__(url, auth_token, with_schema_validation, headers)
        self._s3_url = s3_url

    def get_dissag_detail(self, general_task_id):
        qry = '''
        query disagg_gt ($general_task_id:ID!) {
            node1: node(id: $general_task_id) {
            id
            ... on GeneralTask {
              subtask_count
              children {
                total_count
                edges {
                  node {
                    child {
                      ... on OpenquakeHazardTask {
                        arguments {k v}
                        result
                        hazard_solution {
                          id
                          csv_archive { id }
                          hdf5_archive { id }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        '''

        print(qry)
        input_variables = dict(general_task_id=general_task_id)
        executed = self.run_query(qry, input_variables)
        return executed


def get_deagg_config(data):

    DeaggConfig = namedtuple("DeaggConfig", "vs30 imt location poe inv_time")
    node = data['data']['node1']['children']['edges'][0]
    args = node['node']['child']['arguments']
    for arg in args:
        if arg['k'] == "disagg_config":
            disagg_args = json.loads(arg['v'].replace("'", '"'))

    location = disagg_args['location']
    vs30 = int(disagg_args['vs30'])
    imt = disagg_args['imt']
    poe = float(disagg_args['poe'])
    inv_time = int(disagg_args['inv_time'])
    return DeaggConfig(vs30, imt, location, poe, inv_time)


def get_gtdata(gtid):

    headers = {"x-api-key": API_KEY}
    disagg_api = DisaggDetails(API_URL, None, None, with_schema_validation=False, headers=headers)
    return {'data': disagg_api.get_dissag_detail(gtid)}


def get_imtl(gtdata):

    for arg in gtdata['data']['node1']['children']['edges'][0]['node']['child']['arguments']:
        if arg['k'] == 'disagg_config':
            return json.loads(arg['v'].replace("'", '"'))['level']
