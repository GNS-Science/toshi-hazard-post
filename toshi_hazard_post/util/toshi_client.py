import logging
import os
from pathlib import Path, PurePath

import requests
from nshm_toshi_client.toshi_client_base import ToshiClientBase

log = logging.getLogger(__name__)


def get_archive_info(hazard_soln_id, archive_type):
    """
    archive_type: str {'csv','hdf5'}
    """

    API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY')
    API_URL = os.environ.get('NZSHM22_TOSHI_API_URL')
    headers = {"x-api-key": API_KEY}
    api = ToshiClientBase(API_URL, None, True, headers)

    qry = '''
    query oqhazsoln ($id:ID!) {  
        node (id: $id) {
        ... on OpenquakeHazardSolution {
                ###archive_type###_archive {
                id
                file_name
                file_size
                file_url
                }
            }
        }
    }'''
    qry = qry.replace('###archive_type###', archive_type)
    input_variables = dict(id=hazard_soln_id)
    executed = api.run_query(qry, input_variables)

    archive_info = executed['node'][f'{archive_type}_archive']

    return archive_info


def download_csv(hazard_soln_ids, dest_folder, overwrite=False):

    downloads = dict()

    for hazard_soln_id in hazard_soln_ids:

        file_info = get_archive_info(hazard_soln_id, 'csv')
        folder = Path(dest_folder, 'downloads', hazard_soln_id)
        folder.mkdir(parents=True, exist_ok=True)
        file_path = PurePath(folder, file_info['file_name'])

        downloads[file_info['id']] = dict(
            id=file_info['id'], filepath=str(file_path), info=file_info, hazard_id=hazard_soln_id
        )

        if not overwrite and os.path.isfile(file_path):
            log.info(f"Skip DL for existing file: {file_path}")
            continue

        r1 = requests.get(file_info['file_url'])
        with open(str(file_path), 'wb') as f:
            f.write(r1.content)
            log.info(f"downloaded input file: {file_path} {f}")
            os.path.getsize(file_path) == file_info['file_size']

    return downloads


def download_hdf(self, hazard_soln_ids, dest_folder, overwrite=False):

    downloads = dict()

    for hazard_soln_id in hazard_soln_ids:

        file_info = self.get_archive_info(hazard_soln_id, 'hdf5')

        folder = Path(dest_folder, 'downloads', hazard_soln_id)
        folder.mkdir(parents=True, exist_ok=True)
        file_path = PurePath(folder, file_info['file_name'])

        downloads[file_info['id']] = dict(
            id=file_info['id'], filepath=str(file_path), info=file_info, hazard_id=hazard_soln_id
        )

        if not overwrite and os.path.isfile(file_path):
            log.info(f"Skip DL for existing file: {file_path}")
            continue

        r1 = requests.get(file_info['file_url'])
        with open(str(file_path), 'wb') as f:
            f.write(r1.content)
            log.info("downloaded input file:", file_path, f)
            os.path.getsize(file_path) == file_info['file_size']

    return downloads
