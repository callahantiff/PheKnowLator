#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import os

from google.cloud import storage

from builds.data_preprocessing import DataPreprocessing
from pkt_kg.__version__ import __version__
from pkt_kg.utils import data_downloader

# set environment variable -- this should be replaced with GitHub Secret for build
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'resources/project_keys/pheknowlator-6cc612b4cbee.json'


def updates_dependency_documents(bucket_url, file_url, bucket, gcs_loc, temp_directory) -> None:
    """Takes a dependency file url and downloads the file it points to. That file is then iterated over and all urls
    are updated to point their current Google Cloud Storage bucket. The updated file is saved locally and pushed to
    the processed_data directory of the current build.

    Args:
        bucket_url: A string containing a URL to the current Google Cloud Storage bucket.
        file_url: A string containing a URL to a build dependency document.
        bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        gcs_loc: A string specifying the location of the original_data directory for a specific build.
        temp_directory: A local directory

    Returns:
        None.
    """

    # set bucket information
    bucket_path = '/'.join(bucket_url.split('/')[4:])
    original_bucket = [_.name.split('/')[-1] for _ in bucket.list_blobs(prefix=bucket_path + 'original_data')]
    processed_bucket = [_.name.split('/')[-1] for _ in bucket.list_blobs(prefix=bucket_path + 'processed_data/')]

    # download dependency file and read in data
    data_downloader(file_url, temporary_directory + '/')
    filename = file_url.split('/')[-1]
    data = open(temp_directory + '/' + filename).readlines()

    # update file with current gcs bucket url
    with open(temp_directory + '/' + filename, 'w') as out:
        for row in data:
            prefix, suffix = row.split(', ')
            data_file = suffix.split('/')[-1].strip('?dl=1')
            if data_file in original_bucket: out.write(prefix + ', ' + bucket_url + 'original_data/' + data_file)
            if data_file in processed_bucket: out.write(prefix + ', ' + bucket_url + 'processed_data/' + data_file)

    # push data to bucket
    blob = bucket.blob(gcs_loc + filename)
    blob.upload_from_filename(temp_directory + '/' + filename)

    return None


def main():

    # create temp directory to use locally for writing data GCS data to
    temp_dir = 'builds/temp'
    os.mkdir(temp_dir)

    ###############################################
    # STEP 1 - INITIALIZE GOOGLE STORAGE BUCKET OBJECTS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('pheknowlator')

    # define write paths to GCS bucket
    release = 'release_v' + __version__
    bucket_files = [file.name for file in bucket.list_blobs(prefix=release)]
    build = 'build_' + sorted([x[0] for x in [re.findall(r'(?<=_)\d.*', x) for x in bucket_files] if len(x) > 0])[-1]
    gcs_original_data = '{}/{}/data/{}'.format(release, build, 'original_data/')
    gcs_processed_data = '{}/{}/data/{}'.format(release, build, 'processed_data/')

    ###############################################
    # STEP 2 - Preprocess Linked Open Data Sources
    lod_data = DataPreprocessing(bucket, gcs_original_data, gcs_processed_data, temp_dir)
    lod_data.preprocess_build_data()

    ###############################################
    # STEP 3 - Ontology Data Sources

    ###############################################
    # STEP 4 - Update Required Input Build Resources
    url = 'https://storage.googleapis.com/pheknowlator/{}/{}/data/'.format(release, build)

    # edge source list
    edge_src_list = ' https://raw.githubusercontent.com/callahantiff/PheKnowLator/master/resources/edge_source_list.txt'
    updates_dependency_documents(url, edge_src_list, bucket, gcs_processed_data, temp_dir)

    # ontology source list
    ont_list = ' https://raw.githubusercontent.com/callahantiff/PheKnowLator/master/resources/ontology_source_list.txt'
    updates_dependency_documents(url, ont_list, bucket, gcs_processed_data, temp_dir)

    # clean up environment after uploading all processed data
    shutil.rmtree(temp_dir)


if __name__ == '__main__':
    main()
