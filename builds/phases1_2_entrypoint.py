#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import logging.config
import os
import traceback

from datetime import datetime
from google.cloud import storage  # type: ignore

from build_phase_1 import *  # type: ignore
from build_phase_2 import *  # type: ignore


# set-up logging
log_dir, log, log_config = 'logs', 'pkt_builder_logs.log', glob.glob('**/logging.ini', recursive=True)
if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


def main():

    # start logger and configure Google Cloud Storage settings
    start_time = datetime.now()
    print('\n\n' + '*' * 10 + ' STARTING PHEKNOWLATOR KNOWLEDGE GRAPH BUILD ' + '*' * 10)
    logger.info('STARTING PHEKNOWLATOR KNOWLEDGE GRAPH BUILD')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('pheknowlator')

    # run phase 1 of build
    print('#' * 35 + '\nBUILD PHASE 1: DOWNLOADING BUILD DATA\n' + '#' * 35)
    logger.info('BUILD PHASE 1: DOWNLOADING BUILD DATA')
    # try: run_phase_1()
    # except: logger.error('Uncaught Exception: {}'.format(traceback.format_exc()))

    # run phase 2 build
    print('#' * 35 + '\nBUILD PHASE 2: DATA PRE-PROCESSING\n' + '#' * 35)
    logger.info('BUILD PHASE 2: DATA PRE-PROCESSING')
    # try: run_phase_2()
    # except: logger.error('Uncaught Exception: {}'.format(traceback.format_exc()))

    # print build statistics and upload logging for data preprocessing and ontology cleaning
    runtime = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\n\n' + '*' * 5 + ' COMPLETED BUILD PHASES 1-2: {} MINUTES '.format(runtime) + '*' * 5)
    logger.info('COMPLETED BUILD PHASES 1-2: {} MINUTES'.format(runtime))  # don't delete needed for build monitoring
    blob = bucket.blob('{}/current_build/'.format('release_v' + __version__) + log)
    blob.upload_from_filename(log_dir + '/' + log)


if __name__ == '__main__':
    main()
