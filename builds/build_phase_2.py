#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import fnmatch
import glob
import logging.config
import os
import shutil
import re

from datetime import datetime
from google.cloud import storage  # type: ignore
from tqdm import tqdm  # type: ignore

from builds.build_utilities import *
from builds.data_preprocessing import DataPreprocessing
from builds.ontology_cleaning import OntologyCleaner
from pkt_kg.__version__ import __version__
from pkt_kg.utils import data_downloader

# set environment variables
# logging
log_dir, log, log_config = 'builds/logs', 'pkt_builder_phases12_log.log', glob.glob('**/logging.ini', recursive=True)
if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


def get_file_metadata(url, file_location, gcs_url):
    """Takes a file path and the original URL used to download the data and retrieves metadata on the file.

    Args:
        url: A string containing the original url used to download the data file.
        file_location: A string pointing to a file saved locally.
        gcs_url: A string containing a URL pointing to the processed_data directory in the Google Cloud Storage bucket
            for the current build.

    Returns:
        metadata: A list containing metadata information on the file_location input object.
    """

    metadata = [
        'DATA INFO\n  - DOWNLOAD_URL = {}'.format(str(url)),
        '  - DOWNLOAD_DATE = {}'.format(str(datetime.now().strftime('%m/%d/%Y'))),
        '  - FILE_SIZE_IN_BYTES = {}'.format(str(os.stat(file_location).st_size)),
        '  - GOOGLE_CLOUD_STORAGE_URL = {}'.format(str(gcs_url + file_location.split('/')[-1]))
    ]

    return metadata


def writes_metadata(bucket, metadata, temp_directory, gcs_processed_location):
    """Writes the downloaded URL metadata information locally and to the original_data directory in Google Cloud
    Storage bucket for the current build.

    Args:
        bucket: A storage bucket object specifying a Google Cloud Storage bucket.
        metadata: A nested list containing metadata information on each downloaded url.
        temp_directory: A local directory where preprocessed data is stored.
        gcs_processed_location: A string containing a path to a directory in the Google Cloud Storage Bucket for the
            current build.

    Returns:
        None.
    """

    filename = 'preprocessed_build_metadata.txt'
    with open(temp_directory + '/' + filename, 'w') as out:
        date_info = str(datetime.utcnow().strftime('%a %b %d %X UTC %Y'))
        out.write('=' * 35 + '\n{}\n'.format(date_info) + '=' * 35 + '\n\n')
        for row in metadata:
            for i in range(4):
                out.write(str(row[i]) + '\n')

    uploads_data_to_gcs_bucket(bucket, gcs_processed_location, temp_directory, filename)

    return None


def updates_dependency_documents(gcs_url, file_url, bucket, temp_directory):
    """Takes a dependency file url and downloads the file it points to. That file is then iterated over and all urls
    are updated to point their current Google Cloud Storage bucket. The updated file is saved locally and pushed to
    the processed_data directory in the archived_build directory of the current build.

    Args:
        gcs_url: A string containing a URL to the current Google Cloud Storage bucket.
        file_url: A string containing a URL to a build dependency document.
        bucket: A storage bucket object specifying a Google Cloud Storage bucket.
        temp_directory: A local directory where preprocessed data is stored.

    Returns:
        None.
    """

    # set bucket information
    bucket_path = '/'.join(gcs_url.split('/')[4:])
    gcs_processed = [_.name.split('/')[-1] for _ in bucket.list_blobs(prefix=bucket_path + 'processed_data/')]
    gcs_original = [_.name.split('/')[-1] for _ in bucket.list_blobs(prefix=bucket_path + 'original_data/')]
    # download dependency file and read in data
    data_downloader(file_url, temp_directory + '/')
    filename = file_url.split('/')[-1]
    data = open(temp_directory + '/' + filename).readlines()
    # update file with current gcs bucket url
    if filename != 'resource_info.txt':
        with open(temp_directory + '/' + filename, 'w') as out:
            for row in tqdm(data):
                prefix, suffix = row.strip('\n').split(', ')
                if '.owl' not in suffix:
                    suffix = suffix.strip('?dl=1|.gz|.zip')
                    d_file = suffix.split('/')[-1].strip('?dl=1|.gz|.zip')
                    org_file = fnmatch.filter(gcs_original, suffix.split('/')[-1].replace('.', '*.'))
                    prc_file = fnmatch.filter(gcs_processed, suffix.split('/')[-1].replace('.', '*.'))
                    if d_file in gcs_processed: out.write(prefix + ', ' + gcs_url + 'processed_data/' + d_file + '\n')
                    elif d_file in gcs_original: out.write(prefix + ', ' + gcs_url + 'original_data/' + d_file + '\n')
                    elif len(prc_file) > 0: out.write(prefix + ', ' + gcs_url + 'processed_data/' + prc_file[0] + '\n')
                    else: out.write(prefix + ', ' + gcs_url + 'original_data/' + org_file[0] + '\n')
                if '.owl' in suffix:
                    d_file = fnmatch.filter(gcs_processed, suffix.split('/')[-1].replace('.', '*.'))
                    if len(d_file) > 0: out.write(prefix + ', ' + gcs_url + 'processed_data/' + d_file[0] + '\n')

    uploads_data_to_gcs_bucket(bucket, bucket_path + 'processed_data/', temp_directory, filename)

    return None


def run_phase_2():

    # set temp directory to use locally for writing data to
    temp_dir = 'builds/temp'
    if not os.path.exists(temp_dir): os.mkdir(temp_dir)

    #####################################################
    # STEP 1 - INITIALIZE GOOGLE STORAGE BUCKET OBJECTS
    # define write path to Google Cloud Storage bucket
    bucket = storage.Client().get_bucket('pheknowlator')
    release = 'release_v' + __version__
    bucket_files = [file.name.split('/')[2] for file in bucket.list_blobs(prefix='archived_builds/' + release + '/')]
    # find current archived build directory
    builds = [x[0] for x in [re.findall(r'(?<=_)\d.*', x) for x in bucket_files] if len(x) > 0]
    sorted_dates = sorted([datetime.strftime(datetime.strptime(str(x), '%d%b%Y'), '%Y-%m-%d').upper() for x in builds])
    build = 'build_' + datetime.strftime(datetime.strptime(sorted_dates[-1], '%Y-%m-%d'), '%d%b%Y').upper()

    # set gcs bucket variables
    base_folder_path = 'archived_builds/{}/{}/data/'.format(release, build)
    gcs_original_data = base_folder_path + '{}'.format('original_data/')
    gcs_processed_data = base_folder_path + '{}'.format('processed_data/')
    gcs_log_location = 'temp_build_inprogress/'
    gcs_url = 'https://storage.googleapis.com/pheknowlator/{}'.format(base_folder_path)

    #####################################################
    # STEP 2 - PREPROCESS BUILD DATA
    lod_data = DataPreprocessing(bucket, gcs_original_data, gcs_processed_data, temp_dir)
    lod_data.preprocesses_build_data()
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    #####################################################
    # STEP 3 - CLEAN ONTOLOGY DATA
    ont_data = OntologyCleaner(bucket, gcs_original_data, gcs_processed_data, temp_dir)
    ont_data.cleans_ontology_data()
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    #####################################################
    # STEP 4 - GENERATE METADATA DOCUMENTATION
    logger.info('Generating and Writing Preprocessed Data Metadata')
    metadata, processed_data_files, processed_data = [], glob.glob(temp_dir + '/*'), []
    gcs_processed_files = [f.name for f in bucket.list_blobs(prefix=gcs_processed_data)][1:]
    if len(processed_data_files) != len(gcs_processed_files):
        for _ in tqdm(gcs_processed_files):
            bucket.blob(_).download_to_filename(temp_dir + '/' + _.split('/')[-1])
            processed_data.append(temp_dir + '/' + _.split('/')[-1])
    else: processed_data = processed_data_files
    for data_file in tqdm(processed_data):
        url = gcs_url + 'original_data/' + data_file.replace(temp_dir + '/', '')
        metadata += [get_file_metadata(url, data_file, gcs_url + 'processed_data/')]
    writes_metadata(bucket, metadata, temp_dir, gcs_processed_data)
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    #####################################################
    # STEP 5 - UPDATE INPUT DEPENDENCY DOCUMENTS
    logger.info('Updating Input Dependency Documents')
    # edge source list
    edge_src_list = 'https://raw.githubusercontent.com/callahantiff/PheKnowLator/master/resources/edge_source_list.txt'
    updates_dependency_documents(gcs_url, edge_src_list, bucket, temp_dir)
    # ontology source list
    ont_list = 'https://raw.githubusercontent.com/callahantiff/PheKnowLator/master/resources/ontology_source_list.txt'
    updates_dependency_documents(gcs_url, ont_list, bucket, temp_dir)
    # resource info
    resources = 'https://raw.githubusercontent.com/callahantiff/PheKnowLator/master/resources/resource_info.txt'
    updates_dependency_documents(gcs_url, resources, bucket, temp_dir)
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    #####################################################
    # STEP 6 - PREPARE FOR PHASE 3
    # copy archived_data/data to temp_build_inprogress/data -- needed for build phase 3 Dockerfile
    logger.info('Uploading Processed Data to the temp_build_inprogress/data Directory')
    source_dir, destination_dir = 'archived_builds/{}/{}/data/'.format(release, build), gcs_log_location + 'data/'
    source_data = ['/'.join(_.name.split('/')[-2:]) for _ in bucket.list_blobs(prefix=source_dir)]
    print('Copying Data FROM: {} TO: {}'.format(source_dir, destination_dir))
    logger.info('Copying Data FROM: {} TO: {}'.format(source_dir, destination_dir))
    copies_data_between_gcs_bucket_directories(bucket, source_dir, destination_dir, source_data)
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    return None
