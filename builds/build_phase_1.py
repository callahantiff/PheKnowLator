#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import fnmatch
import logging
import os
import re
import shutil
import sys

from datetime import date, datetime
from google.cloud import storage  # type: ignore

from pkt_kg.__version__ import __version__
from pkt_kg.utils import data_downloader


# set environment variable -- this should be replaced with GitHub Secret for build
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'resources/project_keys/pheknowlator-6cc612b4cbee.json'


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

    print('Creating Build Directory Structure')

    # set-up file names
    folder_list = [
        '{}/archived_builds/{}/data/original_data/'.format(release, build),
        '{}/archived_builds/{}/data/processed_data/'.format(release, build),
        '{}/archived_builds/{}/knowledge_graphs/subclass_builds/relations_only/owl/'.format(release, build),
        '{}/archived_builds/{}/knowledge_graphs/subclass_builds/relations_only/owlnets/'.format(release, build),
        '{}/archived_builds/{}/knowledge_graphs/subclass_builds/inverse_relations/owl/'.format(release, build),
        '{}/archived_builds/{}/knowledge_graphs/subclass_builds/inverse_relations/owlnets/'.format(release, build),
        '{}/archived_builds/{}/knowledge_graphs/instance_builds/relations_only/owl/'.format(release, build),
        '{}/archived_builds/{}/knowledge_graphs/instance_builds/relations_only/owlnets/'.format(release, build),
        '{}/archived_builds/{}/knowledge_graphs/instance_builds/inverse_relations/owl/'.format(release, build),
        '{}/archived_builds/{}/knowledge_graphs/instance_builds/inverse_relations/owlnets/'.format(release, build)
    ]

    # add directories to Google Cloud Storage bucket
    for folder in folder_list:
        blob = bucket.blob(folder)
        blob.upload_from_string('')

    return '{}/archived_builds/{}/data/original_data/'.format(release, build)


def downloads_data_from_gcs_bucket(bucket, original_data, filename, temp_directory):
    """Takes a filename and and downloads the corresponding data to a local temporary directory, if it has not
    already been downloaded.

    Args:
        bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        original_data: A string containing a path to the original data directory in a Google Cloud Storage bucket.
        filename: A string containing the name of file to write to a Google Cloud Storage bucket.
        temp_directory: A local directory where preprocessed data is stored.

    Returns:
        data_file: A string containing the local filepath for a file downloaded from a GSC bucket.

    Raises:
        ValueError: when trying to download a non-existent file from the GCS original_data dir of the current build.
    """

    try:
        _files = [_.name for _ in bucket.list_blobs(prefix=original_data)]
        matched_file = fnmatch.filter(_files, '*/' + filename)[0]  # poor man's glob
        data_file = temp_directory + '/' + matched_file.split('/')[-1]
        if not os.path.exists(data_file):  # only download if file has not yet been downloaded
            bucket.blob(matched_file).download_to_filename(temp_directory + '/' + matched_file.split('/')[-1])
    except IndexError:
        raise ValueError('Cannot find {} in the GCS original_data directory of the current build'.format(filename))

    return data_file


def uploads_data_to_gcs_bucket(bucket, original_data, temp_directory, filename):
    """Takes a file name and pushes the data referenced by the filename object and stored locally in that object to
    a Google Cloud Storage bucket.

    Args:
        bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        original_data: A string specifying the location of the original_data directory for a specific build.
        temp_directory: A local directory where preprocessed data is stored.
        filename: A string containing a filename.

    Returns:
        None.
    """

    print('Uploading {} to GCS bucket: {}'.format(filename, original_data))

    blob = bucket.blob(original_data + filename)
    blob.upload_from_filename(temp_directory + '/' + filename)

    return None


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


def downloads_build_data(bucket, original_data, gcs_url, temp_directory, file_loc='data_to_download.txt'):
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

    print('Downloading Build Data')

    # check what data has already been downloaded
    gcs_original_path = '/'.join(gcs_url.split('/')[4:-1])
    downloaded_data = [file.name for file in bucket.list_blobs(prefix=gcs_original_path)]
    urls, metadata = [x.strip('\n') for x in open(file_loc, 'r').readlines() if not x.startswith('#') and x != '\n'], []
    for url in urls:
        print('Downloading {}'.format(url))
        if url.startswith('http://purl.obolibrary.org/obo/'):
            file_path = url.split('/')[-1][:-4] + '_with_imports.owl'
            if len([x for x in downloaded_data if x.endswith(file_path)]) > 0:
                downloads_data_from_gcs_bucket(bucket, gcs_original_path, file_path, temp_directory)
            else: os.system("./owltools {} --merge-import-closure -o {}".format(url, temp_directory + '/' + file_path))
        else:
            filename, url = url.split(', ')
            file_path = temp_directory + '/' + re.sub('.zip|.gz', '', filename)
            if len([x for x in downloaded_data if x.endswith(file_path)]) > 0:
                downloads_data_from_gcs_bucket(bucket, gcs_original_path, file_path, temp_directory)
            else: data_downloader(url, temp_directory + '/', filename)
        metadata += [get_file_metadata(url, file_path, gcs_url)]
        f_name = re.sub('.zip|.gz', '', file_path.replace(temp_directory + '/', ''))
        uploads_data_to_gcs_bucket(bucket, original_data, temp_directory, f_name)

    # writes metadata locally and pushes it to a Google Cloud Storage bucket
    writes_metadata(metadata, bucket, original_data, temp_directory)

    return None


def run_phase_1():

    # print('#' * 35 + '\nBUILD PHASE 1: DOWNLOADING BUILD DATA\n' + '#' * 35)
    logging.info('#' * 35 + '\nBUILD PHASE 1: DOWNLOADING BUILD DATA\n' + '#' * 35)

    # create temp directory to use locally for writing data GCS data to
    temp_dir = 'temp'
    os.mkdir(temp_dir)

    ###############################################
    # STEP 1 - INITIALIZE GOOGLE STORAGE BUCKET OBJECTS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('pheknowlator')
    release = 'release_v' + __version__
    build = 'build_' + datetime.strftime(datetime.strptime(str(date.today()), '%Y-%m-%d'), '%d%b%Y').upper()

    # create gcs bucket directories
    gcs_original_data = creates_build_directory_structure(bucket, release, build)

    ###############################################
    # STEP 2 - DOWNLOAD BUILD DATA
    gcs_url = 'https://storage.googleapis.com/pheknowlator/{}/archived_builds/{}/data/original_data/'
    downloads_build_data(bucket, gcs_original_data, gcs_url.format(release, build), temp_dir)

    return None
