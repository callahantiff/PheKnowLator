#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import click
import fnmatch
import glob
import logging.config
import os
import ray
import re
import shutil
import subprocess
import traceback

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
os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


def uploads_build_data(bucket, gcs_location):
    """Methods moves data generated by the knowledge graph construction process from the docker container to the
    dedicated Google Cloud Storage Bucket for the current build.

    Args:
        bucket: A storage bucket object specifying a Google Cloud Storage bucket.
        gcs_location: A string containing the location for the archive build in the current release directory
            within the dedicated project Google Cloud Storage Bucket.

    Returns:
        None.
    """

    # create variables to store source directory locations
    resources_loc = 'resources/'
    kg_loc = resources_loc + 'knowledge_graphs/'
    metadata_loc = resources_loc + 'node_data/'
    construct_app = resources_loc + 'construction_approach/'

    # move knowledge graph data
    for kg_file in [x for x in glob.glob(kg_loc + '*') if 'README.md' not in x]:
        uploads_data_to_gcs_bucket(bucket, gcs_location, kg_loc, kg_file.split('/')[-1])
    # move master edge list
    uploads_data_to_gcs_bucket(bucket, gcs_location, resources_loc, 'Master_Edge_List_Dict.json')
    # move metadata files
    uploads_data_to_gcs_bucket(bucket, gcs_location, resources_loc + 'edge_data/', 'edge_source_metadata.txt')
    uploads_data_to_gcs_bucket(bucket, gcs_location, resources_loc + 'ontologies/', 'ontology_source_metadata.txt')
    # node metadata dict
    uploads_data_to_gcs_bucket(bucket, gcs_location, metadata_loc, 'node_metadata_dict.pkl')
    # construction approach logs
    uploads_data_to_gcs_bucket(bucket, gcs_location, construct_app, 'subclass_map_missing_node_log.json')

    return None


@click.command()
@click.option('--app', prompt='construction approach to use (i.e. instance or subclass)')
@click.option('--rel', prompt='yes/no - adding inverse relations to knowledge graph')
@click.option('--owl', prompt='yes/no - removing OWL Semantics from knowledge graph')
def main(app, rel, owl):

    print('#' * 35 + '\nBUILD PHASE 3: DATA PRE-PROCESSING\n' + '#' * 35)
    logger.info('#' * 5 + 'BUILD PHASE 3: DATA PRE-PROCESSING' + '#' * 5)
    start_time = datetime.now()

    #############################################################################
    # STEP 1 - INITIALIZE GOOGLE STORAGE BUCKET OBJECTS
    print('\nSTEP 1: INITIALIZE GOOGLE STORAGE BUCKET AND REFORMAT INPUT ARGUMENTS')
    logger.info('STEP 1: INITIALIZE GOOGLE STORAGE BUCKET AND REFORMAT INPUT ARGUMENTS')

    # set bucket information and find current archived build directory
    release = 'release_v' + __version__
    bucket = storage.Client().get_bucket('pheknowlator')
    bucket_files = [file.name.split('/')[2] for file in bucket.list_blobs(prefix='archived_builds/' + release + '/')]
    builds = [x[0] for x in [re.findall(r'(?<=_)\d.*', x) for x in bucket_files] if len(x) > 0]
    sorted_dates = sorted([datetime.strftime(datetime.strptime(str(x), '%d%b%Y'), '%Y-%m-%d').upper() for x in builds])
    build = 'build_' + datetime.strftime(datetime.strptime(sorted_dates[-1], '%Y-%m-%d'), '%d%b%Y').upper()

    # reformat input arguments and create gcs directory variables
    build_app = 'instance_builds' if app == 'instance' else 'subclass_builds'
    rel_type = 'relations_only' if rel == 'no' else 'inverse_relations'
    owl_decoding = 'owl' if owl == 'no' else 'owlnets'
    arch_string = 'archived_builds/{}/{}/knowledge_graphs/{}/{}/{}/'
    gcs_archive_loc = arch_string.format(release, build, build_app, rel_type, owl_decoding)
    gcs_current_loc = 'current_build/knowledge_graphs/{}/{}/{}/'.format(build_app, rel_type, owl_decoding)

    # clean out directory before writing new data
    print('Removing Existing Data from Build Directory on Google Cloud Storage')
    logging.info('Removing Existing Data from Build Directory on Google Cloud Storage')
    deletes_bucket_files(bucket, gcs_current_loc)
    bucket.blob(gcs_current_loc).upload_from_string('')

    uploads_data_to_gcs_bucket(bucket, gcs_current_loc, log_dir, log)  # uploads log to gcs bucket

    #############################################################################
    # STEP 2 - CONSTRUCT KNOWLEDGE GRAPH
    print('\nSTEP 2: CONSTRUCT KNOWLEDGE GRAPH')
    print('Knowledge Graph Build: {} + {} + {}.txt'.format(app, rel_type.lower(), owl_decoding.lower()))
    logger.info('STEP 2: CONSTRUCT KNOWLEDGE GRAPH')
    logger.info('Knowledge Graph Build: {} + {} + {}.txt'.format(app, rel_type.lower(), owl_decoding.lower()))
    # start background process to upload logs while the pkt main knowledge graph function runs
    ray.init()
    background_task = PKTLogUploader.remote('pheknowlator', gcs_current_loc, log_dir, 90)
    logger.info('RAN THE CONSTRUCT KNOWLEDGE GRAPH CODE')
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

    uploads_data_to_gcs_bucket(bucket, gcs_current_loc, log_dir, log)  # uploads log to gcs bucket

    #############################################################################
    # STEP 3 - UPLOAD BUILD DATA TO GOOGLE CLOUD STORAGE
    print('\nSTEP 3: UPLOAD KNOWLEDGE GRAPH DATA TO GOOGLE CLOUD STORAGE')
    logger.info('STEP 3: UPLOAD KNOWLEDGE GRAPH DATA TO GOOGLE CLOUD STORAGE')

    # upload data from Docker to archived_builds Google Cloud Storage Bucket
    print('Uploading Knowledge Graph Data to the current_build Directory')
    logger.info('Uploading Knowledge Graph Data to the current_build Directory')
    uploads_build_data(bucket, gcs_current_loc)
    uploads_data_to_gcs_bucket(bucket, gcs_current_loc, log_dir, log)  # uploads log to gcs bucket

    # upload data from Docker to archived_builds Google Cloud Storage Bucket
    print('Copying Knowledge Graph Data from the current_build Directory to the archived_builds Directory')
    logger.info('Copying Knowledge Graph Data from the current_build Directory to the archived_builds Directory')
    source_data = [_.name.split('/')[-1] for _ in bucket.list_blobs(prefix=gcs_current_loc)]
    copies_data_between_gcs_bucket_directories(bucket, gcs_current_loc, gcs_archive_loc, source_data)
    uploads_data_to_gcs_bucket(bucket, gcs_current_loc, log_dir, log)  # uploads log to gcs bucket

    #############################################################################
    # STEP 4 - CLEAN UP BUILD ENVIRONMENT + LOG EXIT STATUS TO FINISH RUN
    print('\nSTEP 4: BUILD CLEAN-UP')
    logger.info('STEP 4: BUILD CLEAN-UP')
    runtime = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\n\n' + '*' * 5 + ' COMPLETED BUILD PHASE 3: {} MINUTES '.format(runtime) + '*' * 5)
    logger.info('COMPLETED BUILD PHASE 3: {} MINUTES'.format(runtime))  # don't delete needed for build monitoring
    logger.info('EXIT BUILD PHASE 3')  # don't delete needed for build monitoring

    # copy build logs
    print('Copying Logs from the current_build Directory to the archived_builds Directory')
    logger.info('Copying Logs from the current_build Directory to the archived_builds Directory')
    log_1 = [x for x in [_.name for _ in bucket.list_blobs(prefix='current_build/')] if x.endswith('phases12_log.log')]
    copies_data_between_gcs_bucket_directories(bucket, 'current_build/', gcs_archive_loc, [log_1[0].split('/')[-1]])
    copies_data_between_gcs_bucket_directories(bucket, gcs_current_loc, gcs_archive_loc, [log])

    # exit build
    uploads_data_to_gcs_bucket(bucket, gcs_current_loc, log_dir, log)  # uploads log to gcs bucket

    return None


if __name__ == '__main__':
    main()
