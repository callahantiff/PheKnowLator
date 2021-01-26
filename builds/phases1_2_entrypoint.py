#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import logging.config
import os
import shutil
import traceback

from datetime import datetime
from google.api_core.exceptions import NotFound  # type: ignore
from google.cloud import storage  # type: ignore

from builds.build_phase_1 import *  # type: ignore
from builds.build_phase_2 import *  # type: ignore
from builds.build_utilities import deletes_single_file, uploads_data_to_gcs_bucket  # type: ignore

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'resources/project_keys/pheknowlator-6cc612b4cbee.json'

# set environment variables
log_dir, log, log_config = 'builds/logs', 'pkt_builder_phases12_log.log', glob.glob('**/logging.ini', recursive=True)
if os.path.exists(log_dir): shutil.rmtree(log_dir)
os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


def main():

    start_time = datetime.now()

    # initialize Google Cloud Storage Bucket object and delete prior logs (if present) from current_build directory
    bucket = storage.Client().get_bucket('pheknowlator')
    gcs_current_build = 'current_build/'
    try: deletes_single_file(bucket, gcs_current_build + '{}'.format(log))
    except NotFound: pass

    # start logger and configure Google Cloud Storage settings
    print('\n\n' + '*' * 10 + ' STARTING PHEKNOWLATOR KNOWLEDGE GRAPH BUILD ' + '*' * 10)
    logger.info('*' * 10 + ' STARTING PHEKNOWLATOR KNOWLEDGE GRAPH BUILD ' + '*' * 10)
    uploads_data_to_gcs_bucket(bucket, gcs_current_build, log_dir, log)

    # run phase 1 of build
    print('#' * 35 + '\nBUILD PHASE 1: DOWNLOADING BUILD DATA\n' + '#' * 35)
    logger.info('#' * 5 + 'BUILD PHASE 1: DOWNLOADING BUILD DATA' + '#' * 5)
    # try: run_phase_1()
    # except: logger.error('ERROR: Uncaught Exception: {}'.format(traceback.format_exc()))
    uploads_data_to_gcs_bucket(bucket, gcs_current_build, log_dir, log)

    # run phase 2 build
    print('#' * 35 + '\nBUILD PHASE 2: DATA PRE-PROCESSING\n' + '#' * 35)
    logger.info('#' * 5 + 'BUILD PHASE 2: DATA PRE-PROCESSING' + '#' * 5)
    try: run_phase_2()
    except: logger.error('ERROR: Uncaught Exception: {}'.format(traceback.format_exc()))
    uploads_data_to_gcs_bucket(bucket, gcs_current_build, log_dir, log)

    # print build statistics and upload logging for data preprocessing and ontology cleaning
    runtime = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\n\n' + '*' * 10 + ' COMPLETED BUILD PHASES 1-2: {} MINUTES '.format(runtime) + '*' * 10)
    logger.info('COMPLETED BUILD PHASES 1-2: {} MINUTES'.format(runtime))  # don't delete needed for build monitoring
    logger.info('EXIT BUILD PHASES 1-2')  # don't delete needed for build monitoring
    uploads_data_to_gcs_bucket(bucket, gcs_current_build, log_dir, log)


if __name__ == '__main__':
    main()
