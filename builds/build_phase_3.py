#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import click
import fnmatch
import glob
import logging.config
import networkx  # type: ignore
import os
import ray
import re
import shutil
import subprocess
import traceback

from collections import Counter
from datetime import datetime
from google.cloud import storage  # type: ignore

from builds.build_utilities import *  # type: ignore
from builds.phase3_log_daemon import PKTLogUploader  # type: ignore
from pkt_kg.__version__ import __version__  # type: ignore
from pkt_kg.utils import *  # type: ignore

# set environment variables
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'resources/project_keys/pheknowlator-6cc612b4cbee.json'
# logging
log_dir, log, log_config = 'builds/logs', 'pkt_build_log.log', glob.glob('**/logging.ini', recursive=True)
if os.path.exists(log_dir): shutil.rmtree(log_dir)
os.mkdir(log_dir); logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


def uploads_build_data(bucket, gcs_location) -> None:
    """Moves data from docker container to the dedicated Google Cloud Storage Bucket directory.

    Args:
        bucket: A storage bucket object specifying a Google Cloud Storage bucket.
        gcs_location: A string containing a Google Cloud Storage Bucket location.

    Returns:
        None.
    """

    resources_loc = 'resources/'; kg_loc = resources_loc + 'knowledge_graphs/'
    metadata_loc = resources_loc + 'node_data/'; construct_app = resources_loc + 'construction_approach/'
    data_type = '_OWLNETS' if gcs_location.endswith('/owlnets/') else '_OWL'

    # move knowledge graph data
    for kg_file in [x for x in glob.glob(kg_loc + '*') if 'README.md' not in x]:
        if data_type + '_' in kg_file or data_type + '.' in kg_file:
            uploads_data_to_gcs_bucket(bucket, gcs_location, kg_loc, kg_file.split('/')[-1])
    uploads_data_to_gcs_bucket(bucket, gcs_location, kg_loc, 'PheKnowLator_MergedOntologies.owl')
    uploads_data_to_gcs_bucket(bucket, gcs_location, resources_loc, 'Master_Edge_List_Dict.json')
    uploads_data_to_gcs_bucket(bucket, gcs_location, resources_loc + 'edge_data/', 'edge_source_metadata.txt')
    uploads_data_to_gcs_bucket(bucket, gcs_location, resources_loc + 'ontologies/', 'ontology_source_metadata.txt')
    uploads_data_to_gcs_bucket(bucket, gcs_location, metadata_loc, 'node_metadata_dict.pkl')
    uploads_data_to_gcs_bucket(bucket, gcs_location, construct_app, 'subclass_map_log.json')

    return None


@click.command()
@click.option('--app', prompt='construction approach to use (i.e. instance or subclass)')
@click.option('--rel', prompt='yes/no - adding inverse relations to knowledge graph')
@click.option('--owl', prompt='yes/no - removing OWL Semantics from knowledge graph')
def main(app, rel, owl):

    log_str = 'BUILD PHASE 3: DATA PRE-PROCESSING'
    print('#' * 35 + '\n' + log_str + '\n' + '#' * 35); logger.info('#' * 5 + '\n' + log_str + '\n' + '#' * 5)
    start_time = datetime.now()

    #############################################################################
    # STEP 1 - INITIALIZE GOOGLE STORAGE BUCKET OBJECTS
    logs = 'STEP 1: INITIALIZE GCS BUCKET AND REFORMAT INPUT ARGUMENTS'; print('\n' + logs); logger.info(logs)

    # set GCS bucket information and find name of current GCS archived_builds directory
    release = 'release_v' + __version__; bucket = storage.Client().get_bucket('pheknowlator')
    bucket_files = [file.name.split('/')[2] for file in bucket.list_blobs(prefix='archived_builds/' + release + '/')]
    builds = [x[0] for x in [re.findall(r'(?<=_)\d.*', x) for x in bucket_files] if len(x) > 0]
    sorted_dates = sorted([datetime.strftime(datetime.strptime(str(x), '%d%b%Y'), '%Y-%m-%d').upper() for x in builds])
    build = 'build_' + datetime.strftime(datetime.strptime(sorted_dates[-1], '%Y-%m-%d'), '%d%b%Y').upper()

    # reformat input build arguments and create needed GCS directory variables
    build_app = 'instance_builds' if app == 'instance' else 'subclass_builds'
    rel_type = 'relations_only' if rel == 'no' else 'inverse_relations'
    # owl_decoding = 'owl' if owl == 'no' else 'owlnets'
    arch_string = 'archived_builds/{}/{}/knowledge_graphs/{}/{}/{}/'
    gcs_current_root = 'current_build/'; gcs_log_root = 'temp_build_inprogress/'
    gcs_log_location = '{}knowledge_graphs/{}/{}/{}/'.format(gcs_log_root, build_app, rel_type, 'owl')
    # full owl build
    gcs_archive_loc_owl = arch_string.format(release, build, build_app, rel_type, 'owl')
    gcs_current_loc_owl = '{}knowledge_graphs/{}/{}/{}/'.format(gcs_current_root, build_app, rel_type, 'owl')
    # owl-nets build
    gcs_archive_loc_owlnets = arch_string.format(release, build, build_app, rel_type, 'owlnets')
    gcs_current_loc_owlnets = '{}knowledge_graphs/{}/{}/{}/'.format(gcs_current_root, build_app, rel_type, 'owlnets')

    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)  # uploads log to gcs bucket

    #############################################################################
    # STEP 2 - CONSTRUCT KNOWLEDGE GRAPH
    log_str1 = 'STEP 2: CONSTRUCT KNOWLEDGE GRAPH'; log_str2 = 'KG Build: {} + {}.txt'.format(app, rel_type.lower())
    print('\n' + log_str1); logger.info(log_str1); print(log_str2); logger.info(log_str2)
    ray.init()  # start daemon to continuously upload logs while the pkt main knowledge graph function runs
    background_task = PKTLogUploader.remote('pheknowlator', gcs_log_location, log_dir, 90)
    # run the pkt_kg main method
    command = 'python Main.py --onts resources/ontology_source_list.txt --edg resources/edge_source_list.txt ' \
              '--res resources/resource_info.txt --out ./resources/knowledge_graphs --nde yes --kg full ' \
              '--app {} --rel {} --owl {}'
    try:
        return_code = os.system(command.format(app, rel, owl))
        if return_code != 0:
            logger.error('ERROR: Program Finished with Errors: {}'.format(return_code))
            raise Exception('ERROR: Program Finished with Errors: {}'.format(return_code))
    except: logger.error('ERROR: Uncaught Exception: {}'.format(traceback.format_exc()))
    background_task.__ray_terminate__.remote()  # kills the process with an `exit(0)`
    ray.shutdown()

    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)  # uploads log to gcs bucket

    #############################################################################
    # STEP 3 - UPLOAD BUILD DATA TO GOOGLE CLOUD STORAGE
    logs = 'STEP 3: UPLOAD KNOWLEDGE GRAPH DATA TO GOOGLE CLOUD STORAGE'; print('\n' + logs); logger.info(logs)

    # remove GCS current_build directory data before writing new data
    logs = 'Removing Existing Data from Current Build Directory on Google Cloud Storage'; print(logs); logger.info(logs)
    deletes_bucket_files(bucket, gcs_current_root + 'data/')  # data directories
    bucket.blob(gcs_current_root + 'data/original_data/').upload_from_string('')
    bucket.blob(gcs_current_root + 'data/processed_data/').upload_from_string('')
    deletes_bucket_files(bucket, gcs_current_loc_owl); bucket.blob(gcs_current_loc_owl).upload_from_string('')
    deletes_bucket_files(bucket, gcs_current_loc_owlnets); bucket.blob(gcs_current_loc_owlnets).upload_from_string('')

    # copy GCS archived_data/data --> GCS current_build/data
    source_dir, destination_dir = 'archived_builds/{}/{}/data/'.format(release, build), gcs_current_root + 'data/'
    source_data = ['/'.join(_.name.split('/')[-2:]) for _ in bucket.list_blobs(prefix=source_dir)]
    logs = 'Copying Data FROM: {} TO: {}'.format(source_dir, destination_dir); print(logs); logger.info(logs)
    copies_data_between_gcs_bucket_directories(bucket, source_dir, destination_dir, source_data)
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    # move Docker pkt output --> GCS archived_builds
    logs = 'Uploading Knowledge Graph Data from Docker to the archived_builds Directory'; print(logs); logger.info(logs)
    uploads_build_data(bucket, gcs_archive_loc_owl); uploads_build_data(bucket, gcs_archive_loc_owlnets)
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)  # uploads log to gcs bucket

    # copy GCS archived_builds --> GCS current_build
    logs = 'Copying Graph Data from archived_builds to current_builds'; print(logs); logger.info(logs)
    # full owl build
    source_data = [_.name.split('/')[-1] for _ in bucket.list_blobs(prefix=gcs_archive_loc_owl)]
    copies_data_between_gcs_bucket_directories(bucket, gcs_archive_loc_owl, gcs_current_loc_owl, source_data)
    # owl-nets build
    source_data = [_.name.split('/')[-1] for _ in bucket.list_blobs(prefix=gcs_archive_loc_owlnets)]
    copies_data_between_gcs_bucket_directories(bucket, gcs_archive_loc_owlnets, gcs_current_loc_owlnets, source_data)

    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)  # uploads log to gcs bucket

    #############################################################################
    # STEP 4 - CLEAN UP BUILD ENVIRONMENT + LOG EXIT STATUS TO FINISH RUN
    print('\nSTEP 4: BUILD CLEAN-UP'); logger.info('STEP 4: BUILD CLEAN-UP')
    runtime = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\n\n' + '*' * 5 + ' COMPLETED BUILD PHASE 3: {} MINUTES '.format(runtime) + '*' * 5)
    logger.info('COMPLETED BUILD PHASE 3: {} MINUTES'.format(runtime)); logger.info('EXIT BUILD PHASE 3')
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    # copy build logs from GCS temp_build_inprogress --> GCS archived_builds and GCS current_build
    logs = 'Copying Logs to the current_build and archived_builds Directories'; print(logs); logger.info(logs)
    log_1 = [x for x in [_.name for _ in bucket.list_blobs(prefix=gcs_log_root)] if x.endswith('phases12_log.log')]
    # owl build
    copies_data_between_gcs_bucket_directories(bucket, gcs_log_root, gcs_archive_loc_owl, [log_1[0].split('/')[-1]])
    copies_data_between_gcs_bucket_directories(bucket, gcs_log_root, gcs_current_loc_owl, [log_1[0].split('/')[-1]])
    copies_data_between_gcs_bucket_directories(bucket, gcs_log_location, gcs_archive_loc_owlnets, [log])
    copies_data_between_gcs_bucket_directories(bucket, gcs_log_location, gcs_current_loc_owl, [log])
    # owl-nets build
    copies_data_between_gcs_bucket_directories(bucket, gcs_log_root, gcs_archive_loc_owlnets, [log_1[0].split('/')[-1]])
    copies_data_between_gcs_bucket_directories(bucket, gcs_log_root, gcs_current_loc_owlnets, [log_1[0].split('/')[-1]])
    copies_data_between_gcs_bucket_directories(bucket, gcs_log_location, gcs_archive_loc_owlnets, [log])
    copies_data_between_gcs_bucket_directories(bucket, gcs_log_location, gcs_current_loc_owlnets, [log])

    # exit build
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)  # uploads log to gcs bucket

    return None


if __name__ == '__main__':
    main()
