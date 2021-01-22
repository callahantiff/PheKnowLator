#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import click
import datetime
import fnmatch
import glob
import logging
import pickle
import os
import re
import subprocess

from datetime import datetime
from google.cloud import storage  # type: ignore

from pkt_kg.__version__ import __version__

# set environment variable -- this should be replaced with GitHub Secret for build
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'resources/project_keys/pheknowlator-6cc612b4cbee.json'


def uploads_data_to_gcs_bucket(bucket, bucket_location, file_loc):
    """Takes a file name and pushes the corresponding data referenced by the filename object from a local
    temporary directory to a Google Cloud Storage bucket.

    Args:
        bucket: A storage bucket object specifying a Google Cloud Storage bucket.
        bucket_location: A string containing a file path to a directory within a Google Cloud Storage Bucket.
        file_loc: A string containing the name of file to write to a Google Cloud Storage bucket.

    Returns:
        None.
    """

    blob = bucket.blob(bucket_location + file_loc)
    blob.upload_from_filename(self.temp_dir + '/' + file_loc)

    return None


@click.command()
@click.option('--app', prompt='construction approach to use (i.e. instance or subclass)')
@click.option('--rel', prompt='yes/no - adding inverse relations to knowledge graph')
@click.option('--owl', prompt='yes/no - removing OWL Semantics from knowledge graph')
def main(app, rel, owl):

    rel_type = 'RelationsOnly' if rel == 'no' else 'InverseRelations'
    owl_decoding = 'OWL' if owl == 'no' else 'OWL DeCoding'

    # #####################################################
    # # STEP 1 - INITIALIZE GOOGLE STORAGE BUCKET OBJECTS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('pheknowlator')
    # define write path to Google Cloud Storage bucket
    release = 'release_v' + __version__
    bucket_files = [file.name.split('/')[2] for file in bucket.list_blobs(prefix=release + '/archived_builds/')]
    # find current archived build directory
    builds = [x[0] for x in [re.findall(r'(?<=_)\d.*', x) for x in bucket_files] if len(x) > 0]
    sorted_dates = sorted([datetime.strftime(datetime.strptime(str(x), '%d%b%Y'), '%Y-%m-%d').upper() for x in builds])
    build = 'build_' + datetime.strftime(datetime.strptime(sorted_dates[-1], '%Y-%m-%d'), '%d%b%Y').upper()
    # set gcs bucket variables
    gcs_archived_build = '{}/archived_builds/{}/'.format(release, build)
    gcs_current_build = '{}/current_build/'.format(release)

    # call method
    start_time = datetime.now()
    print('\n\n' + '*' * 10 + ' PKT: STARTING PHEKNOWLATOR KNOWLEDGE GRAPH BUILD ' + '*' * 10)

    # configure pkt build args
    # command = 'python Main.py --onts resources/ontology_source_list.txt --edg resources/edge_source_list.txt ' \
    #           '--res resources/resource_info.txt --out ./resources/knowledge_graphs --nde yes --kg full' \
    #           '--app {} --rel {} --owl {}'
    # return_code = os.system(command.format(app, rel, owl))

    # update status
    return_code = 0
    filename = 'program_status_{}_{}_{}.txt'.format(app, rel_type.lower(), owl_decoding.lower())
    with open(filename, 'w') as o:
        if return_code != 0: o.write('FAILED')
        else: o.write('SUCCEEDED')
    # push to GCS current build bucket
    blob = bucket.blob(gcs_current_build + filename)
    blob.upload_from_filename(filename)

    #####################################################
    # STEP 2 - UPLOAD BUILD DATA TO GOOGLE CLOUD STORAGE

    # find which directories to upload to from the file names --> make this a func()

    #####################################################
    # STEP 3 - COPY ARCHIVED BUILD DATA TO CURRENT BUILD

    # print build statistics
    runtime = round((datetime.now() - start_time).total_seconds() / 60, 3)
    command = '\n\n' + '*' * 5 + ' PKT: COMPLETED BUILD PHASE 3 - JOB ({} + {} + {}): {} MINUTES '
    rel_type = 'RelationsOnly' if rel == 'no' else 'InverseRelations'
    owl_decoding = 'OWL' if owl == 'no' else 'OWL DeCoding'
    print(command.format(runtime, app, rel_type, owl_decoding) + '*' * 5)

    return None


if __name__ == '__main__':
    main()
