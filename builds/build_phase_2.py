#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import fnmatch
import glob
import logging
import os
import shutil
import re

from datetime import datetime
from google.cloud import storage  # type: ignore

from builds.data_preprocessing import DataPreprocessing  # type: ignore
from builds.ontology_cleaning import OntologyCleaner  # type: ignore
from pkt_kg.__version__ import __version__
from pkt_kg.utils import data_downloader

# set environment variable -- this should be replaced with GitHub Secret for build
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'resources/project_keys/pheknowlator-6cc612b4cbee.json'


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


def writes_metadata(metadata, temp_directory):
    """Writes the downloaded URL metadata information locally and to the original_data directory in Google Cloud
    Storage bucket for the current build.

    Args:
        metadata: A nested list containing metadata information on each downloaded url.
        temp_directory: A local directory where preprocessed data is stored.
        metadata: A nested list containing metadata information on each downloaded url.

    Returns:
        None.
    """

    filename = 'preprocessed_build_metadata.txt'
    with open(temp_directory + '/' + filename, 'w') as out:
        out.write('=' * 35 + '\n{}\n'.format(str(datetime.utcnow().strftime('%a %b %d %X UTC %Y'))) +
                  '=' * 35 + '\n\n')
        for row in metadata:
            for i in range(4):
                out.write(str(row[i]) + '\n')

    lod_data.uploads_data_to_gcs_bucket(filename)

    return None


def updates_dependency_documents(gcs_url, file_url, bucket, temp_directory):
    """Takes a dependency file url and downloads the file it points to. That file is then iterated over and all urls
    are updated to point their current Google Cloud Storage bucket. The updated file is saved locally and pushed to
    the processed_data directory of the current build.

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

    # push data to bucket
    lod_data.uploads_data_to_gcs_bucket(filename)

    return None


def run_phase_2():
    print('#' * 35 + '\nBUILD PHASE 2: DATA PRE-PROCESSING\n' + '#' * 35)

    temp_dir = 'temp'
    if not os.path.exists(temp_dir): os.mkdir(temp_dir)

    ###############################################
    # STEP 1 - INITIALIZE GOOGLE STORAGE BUCKET OBJECTS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('pheknowlator')
    # define write path to Google Cloud Storage bucket
    release = 'release_v' + __version__
    bucket_files = [file.name.split('/')[2] for file in bucket.list_blobs(prefix=release + '/archived_builds/')]
    # find current archived build directory
    builds = [x[0] for x in [re.findall(r'(?<=_)\d.*', x) for x in bucket_files] if len(x) > 0]
    sorted_dates = sorted([datetime.strftime(datetime.strptime(str(x), '%d%b%Y'), '%Y-%m-%d').upper() for x in builds])
    build = 'build_' + datetime.strftime(datetime.strptime(sorted_dates[-1], '%Y-%m-%d'), '%d%b%Y')
    # set gcs bucket variables
    gcs_original_data = '{}/archived_builds/{}/data/{}'.format(release, build, 'original_data/')
    gcs_processed_data = '{}/archived_builds/{}/data/{}'.format(release, build, 'processed_data/')
    gcs_url = 'https://storage.googleapis.com/pheknowlator/{}/archived_builds/{}/data/'.format(release, build)

    ###############################################
    # STEP 2 - PREPROCESS BUILD DATA
    lod_data = DataPreprocessing(bucket, gcs_original_data, gcs_processed_data, temp_dir)
    lod_data.preprocesses_build_data()

    ###############################################
    # STEP 3 - CLEAN ONTOLOGY DATA
    ont_data = OntologyCleaner(bucket, gcs_original_data, gcs_processed_data, temp_dir)
    ont_data.cleans_ontology_data()

    ###############################################
    # STEP 4 - GENERATE METADATA DOCUMENTATION
    metadata, processed_data = [], glob.glob(temp_dir + '/*')
    for data_file in tqdm(processed_data):
        url = gcs_url + 'original_data/' + data_file.replace(temp_dir + '/', '')
        metadata += [get_file_metadata(url, data_file, gcs_url + 'processed_data/')]
    writes_metadata(metadata, temp_dir)

    ###############################################
    # STEP 5 - UPDATE INPUT DEPENDENCY DOCUMENTS
    # edge source list
    edge_src_list = 'https://raw.githubusercontent.com/callahantiff/PheKnowLator/master/resources/edge_source_list.txt'
    updates_dependency_documents(gcs_url, edge_src_list, bucket, temp_dir)
    # ontology source list
    ont_list = 'https://raw.githubusercontent.com/callahantiff/PheKnowLator/master/resources/ontology_source_list.txt'
    updates_dependency_documents(gcs_url, ont_list, bucket, temp_dir)
    # resource info
    resources = 'https://raw.githubusercontent.com/callahantiff/PheKnowLator/master/resources/resource_info.txt'
    updates_dependency_documents(gcs_url, resources, bucket, temp_dir)

    # clean up environment after uploading all processed data
    shutil.rmtree(temp_dir)

    return None
