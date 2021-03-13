#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import fnmatch
import glob
import logging.config
import os
import re
import shutil
import sys
import traceback

from datetime import date, datetime
from google.cloud import storage  # type: ignore

from builds.build_utilities import *  # type: ignore
from pkt_kg.__version__ import __version__
from pkt_kg.utils import data_downloader

# set environment variables
# logging
gcs_log_location = 'temp_build_inprogress/'
log_dir, log, log_config = 'builds/logs', 'pkt_builder_phases12_log.log', glob.glob('**/logging.ini', recursive=True)
if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})
# owl tools location
owltools_location = './builds/owltools'
# owltools_location = './pkt_kg/libs/owltools'


def creates_build_directory_structure(bucket, release, build):
    """Takes a Google Cloud Storage bucket and generates all of the folders needed for a knowledge graph build.

    Args:
        bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        release: A string containing the current pkt software release.
        build: A string containing the name of the current build.

    Returns:
         original_data: A string containing the Google Cloud Storage bucket directory to download data to for the
            current build.
    """

    log_str = 'Creating Build Directory Structure'; print(log_str); logger.info(log_str)

    folder_list = [
        'archived_builds/{}/{}/data/original_data/'.format(release, build),
        'archived_builds/{}/{}/data/processed_data/'.format(release, build),
        'archived_builds/{}/{}/knowledge_graphs/subclass_builds/relations_only/owl/'.format(release, build),
        'archived_builds/{}/{}/knowledge_graphs/subclass_builds/relations_only/owlnets/'.format(release, build),
        'archived_builds/{}/{}/knowledge_graphs/subclass_builds/inverse_relations/owl/'.format(release, build),
        'archived_builds/{}/{}/knowledge_graphs/subclass_builds/inverse_relations/owlnets/'.format(release, build),
        'archived_builds/{}/{}/knowledge_graphs/instance_builds/relations_only/owl/'.format(release, build),
        'archived_builds/{}/{}/knowledge_graphs/instance_builds/relations_only/owlnets/'.format(release, build),
        'archived_builds/{}/{}/knowledge_graphs/instance_builds/inverse_relations/owl/'.format(release, build),
        'archived_builds/{}/{}/knowledge_graphs/instance_builds/inverse_relations/owlnets/'.format(release, build)
    ]

    # add directories to Google Cloud Storage bucket
    for folder in folder_list:
        blob = bucket.blob(folder)
        blob.upload_from_string('')

    return 'archived_builds/{}/{}/data/original_data/'.format(release, build)


def get_file_metadata(url, file_location, gcs_url):
    """Takes a file path and the original URL used to download the data and retrieves metadata on the file.

    Args:
        url: A string containing the original url used to download the data file.
        file_location: A string pointing to a file saved locally.
        gcs_url: A string containing a URL pointing to the original_data directory in the Google Cloud Storage bucket
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


def writes_metadata(metadata, bucket, original_data, temp_directory):
    """Writes the downloaded URL metadata information locally and to the original_data directory in Google Cloud
    Storage bucket for the current build.

    Args:
        bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        original_data: A string specifying the location of the original_data directory for a specific build.
        temp_directory: A local directory where preprocessed data is stored.
        metadata: A nested list containing metadata information on each downloaded url.

    Returns:
        None.
    """

    filename = 'downloaded_build_metadata.txt'
    with open(temp_directory + '/' + filename, 'w') as out:
        out.write('=' * 35 + '\n{}\n'.format(str(datetime.utcnow().strftime('%a %b %d %X UTC %Y'))) + '=' * 35 + '\n\n')
        for row in metadata:
            for i in range(4):
                out.write(str(row[i]) + '\n')

    uploads_data_to_gcs_bucket(bucket, original_data, temp_directory, filename)

    return None


def downloads_build_data(bucket, original_data, gcs_url, temp_directory, file_loc='builds/data_to_download.txt'):
    """Reads in the list of data to download for the current build, downloads each object, and pushes the downloaded
    object up to a Google Cloud Storage bucket. Once all of the data are downloaded, a metadata file object is
    generated and pushed with the downloaded data to the original_data Google Cloud Storage bucket for the current
    build.

    Args:
        bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        original_data: A string specifying the location of the original_data directory for a specific build.
        gcs_url: A string containing a URL pointing to the original_data directory in the Google Cloud Storage bucket
            for the current build.
        temp_directory: A local directory where preprocessed data is stored.
        file_loc: A string containing the filepath and name of the data to download for the build.

    Returns:
        None.
    """

    log_str = 'Downloading Build Data'; print(log_str); logger.info(log_str)

    # check what data has already been downloaded
    gcs_original_path = '/'.join(gcs_url.split('/')[4:-1])
    downloaded_data = [file.name for file in bucket.list_blobs(prefix=gcs_original_path)]
    urls, metadata = [x.strip('\n') for x in open(file_loc, 'r').readlines() if not x.startswith('#') and x != '\n'], []
    for url in urls:
        log_str = 'Downloading {}'.format(url); print(log_str);logger.info(log_str)
        if url.startswith('http://purl.obolibrary.org/obo/'):
            ont_name = url.split('/')[-1][:-4] + '_with_imports.owl'
            file_path = temp_directory + '/' + ont_name
            if len([x for x in downloaded_data if x.endswith(ont_name)]) > 0:
                downloads_data_from_gcs_bucket(bucket, gcs_original_path, None, ont_name, temp_directory)
            else:
                command_string = '{} {} --merge-import-closure -o {}'
                return_code = os.system(command_string.format(owltools_location, url, file_path))
                if return_code != 0:
                    log_str = 'ERROR: Unable to successfully download {}'.format(url)
                    logger.error(log_str + ': {}'.format(return_code)); raise Exception(log_str)
        else:
            filename, url = url.split(', ')
            file_path = temp_directory + '/' + re.sub('.zip|.gz', '', filename)
            if len([x for x in downloaded_data if x.endswith(file_path)]) > 0:
                downloads_data_from_gcs_bucket(bucket, gcs_original_path, None, file_path, temp_directory)
            else: data_downloader(url, temp_directory + '/', filename)

        metadata += [get_file_metadata(url, file_path, gcs_url)]
        f_name = re.sub('.zip|.gz', '', file_path.replace(temp_directory + '/', ''))
        uploads_data_to_gcs_bucket(bucket, original_data, temp_directory, f_name)
        uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    # writes metadata locally and pushes it to a Google Cloud Storage bucket
    writes_metadata(metadata, bucket, original_data, temp_directory)

    return None


def run_phase_1():

    # create temp directory to use locally for writing data GCS data to
    temp_dir = 'builds/temp'
    os.mkdir(temp_dir)

    ###############################################
    # STEP 1 - INITIALIZE GOOGLE STORAGE BUCKET OBJECTS
    bucket = storage.Client().get_bucket('pheknowlator')
    build = 'build_' + datetime.strftime(datetime.strptime(str(date.today()), '%Y-%m-%d'), '%d%b%Y').upper()
    gcs_original_data = creates_build_directory_structure(bucket, 'release_v' + __version__, build)
    uploads_data_to_gcs_bucket(bucket, gcs_log_location, log_dir, log)

    ###############################################
    # STEP 2 - DOWNLOAD BUILD DATA
    gcs_url = 'https://storage.googleapis.com/pheknowlator/{}'.format(gcs_original_data)
    downloads_build_data(bucket, gcs_original_data, gcs_url, temp_dir)

    return None
