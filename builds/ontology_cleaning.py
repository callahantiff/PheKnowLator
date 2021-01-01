#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import fnmatch
import itertools
import networkx
import os
import pandas
import pickle

from google.cloud import storage
from owlready2 import subprocess
from rdflib import Graph, URIRef
from reactome2py import content
from tqdm import tqdm
from typing import Dict, Bool, List, Tuple

# import script containing helper functions
from pkt_kg.utils import *  # tests written for called methods in pkt_kg/utils/data_utils.py


class OntologyCleaning(object):
    """Class provides a container for the ontology cleaning methods, original housed in the Ontology_Cleaning.ipynb
    Jupyter Notebook. See notebook (https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb) for
    more detailed descriptions of each processed data source and the rationale behind the different filtering and
    processing approaches.

    Attributes:
        gcs_bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        org_data: A string specifying the location of the original_data directory for a specific build.
        processed_data: A string specifying the location of the original_data directory for a specific build.
        temp_dir: A string specifying a temporary directory to use while processing data locally.

    Raises:
        ValueError: when trying to download a non-existent file from the GCS original_data dir of the current build
    """

    def __init__(self, gcs_bucket: storage.bucket.Bucket, org_data: str, processed_data: str, temp_dir: str) -> None:

        self.bucket: storage.bucket.Bucket = gcs_bucket
        self.original_data: str = org_data
        self.processed_data: str = processed_data
        self.temp_dir = temp_dir

    def uploads_data_to_gcs_bucket(self, file_loc: str, original_data: Bool = False) -> None:
        """Takes a file name and pushes the data referenced by the filename object and stored locally in that object to
        a Google Cloud Storage bucket.

        Args:
            file_loc: A string containing the name of file to write to a Google Cloud Storage bucket.
            original_data: A bool indicating whether the data should be updated to the processed_data and
                original_data buckets on Google Cloud Storage for the current build (default=False).

        Returns:
            None.
        """

        print('Uploading {} to GCS bucket: {}'.format(file_loc, self.processed_data))

        if original_data:
            blob_original = self.bucket.blob(self.original_data + file_loc)
            blob_original.upload_from_filename(self.temp_dir + '/' + file_loc)
            blob_processed = self.bucket.blob(self.processed_data + file_loc)
            blob_processed.upload_from_filename(self.temp_dir + '/' + file_loc)
        else:
            blob = self.bucket.blob(self.processed_data + file_loc)
            blob.upload_from_filename(self.temp_dir + '/' + file_loc)

        return None

    def downloads_data_from_gcs_bucket(self, file_loc: str) -> str:
        """Takes a filename and a data object and writes the data in that object to the Google Cloud Storage bucket
        specified in the filename.

        Args:
            file_loc: A string containing the name of file to write to a Google Cloud Storage bucket.

        Returns:
            data_file: A string containing the local filepath for a file downloaded from a GSC bucket.

        Raises:
            ValueError: when trying to download a non-existent file from the GCS original_data dir of the current build.
        """

        print('Downloading {} from the GCS original_data directory of the current build'.format(file_loc))

        try:
            _files = [_.name for _ in self.bucket.list_blobs(prefix=self.original_data)]
            matched_file = fnmatch.filter(_files, '*/' + file_loc)[0]  # poor man's glob
            self.bucket.blob(matched_file).download_to_filename(self.temp_dir + '/' + matched_file.split('/')[-1])
            data_file = self.temp_dir + '/' + matched_file.split('/')[-1]
        except IndexError:
            raise ValueError('Cannot find {} in the GCS original_data directory of the current build'.format(file_loc))

        return data_file

    def reads_gcs_bucket_data_to_graph(self, file_location: str) -> Graph:
        """Takes a Google Cloud Storage bucket and a filename and downloads to the data to a local temp directory.
        Once downloaded, the file is read into an RDFLib Graph object.

        Args:
            file_location: A string containing the name of file that exists in a Google Cloud Storage bucket.

        Returns:
             graph: An RDFLib graph object containing data read from a Google Cloud Storage bucket.
        """

        dat = self.downloads_data_from_gcs_bucket(file_location)
        graph = Graph().parse(dat, format='xml')

        return graph
