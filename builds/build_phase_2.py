#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import os

from google.cloud import storage

from builds.data_preprocessing import DataPreprocessing
from pkt_kg.__version__ import __version__

# set environment variable -- this should be replaced with GitHub Secret for build
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'resources/project_keys/pheknowlator-6cc612b4cbee.json'


def writes_data_to_gcs_bucket(filename: str) -> None:
    """Takes a filename and a data object and writes the data in that object to the Google Cloud Storage bucket
    specified in the filename.

    Args:
        filename: A string containing the name of file to write to a Google Cloud Storage bucket.

    Returns:
        None.
    """

    blob = self.bucket.blob(self.processed_data + filename)
    blob.upload_from_filename(self.temp_dir + '/' + filename)
    print('Uploaded {} to GCS bucket: {}'.format(filename, self.processed_data))

    return None


def reads_gcs_bucket_data_to_df(self, file_loc: str, delm: str, skip: int = 0,
                                header: Optional[int, List] = None) -> pd.DataFrame:
    """Takes a Google Cloud Storage bucket and a filename and downloads to the data to a local temp directory.
    Once downloaded, the file is read into a Pandas DataFrame.

    Args:
        file_loc: A string containing the name of file that exists in a Google Cloud Storage bucket.
        delm: A string specifying a file delimiter.
        skip: An integer specifying the number of rows to skip when reading in the data.
        header: An integer specifying the header row, None for no header or a list of header names.

    Returns:
         data: A Pandas DataFrame object containing data read from a Google Cloud Storage bucket.
    """

    try:
        _files = [_.name for _ in self.bucket.list_blobs(prefix=self.original_data_bucket)]
        matched_file = fnmatch.filter(_files, '*/' + file_loc)[0]  # poor man's glob
        self.bucket.blob(matched_file).download_to_filename(self.temp_dir + '/' + matched_file.split('/')[-1])
        data_file = self.temp_dir + '/' + matched_file.split('/')[-1]
    except IndexError:
        raise ValueError('Cannot find {} in the GCS original_data directory of the current build'.format(file_loc))

    if header is None or isinstance(header, int):
        data = pd.read_csv(data_file, header=header, delimiter=delm, skiprows=skip, low_memory=False)
    else:
        data = pd.read_csv(data_file, header=None, names=header, delm=delimiter, skiprows=skip, low_memory=False)

    return data

def creates_genomic_identifier_data(lod_class):

    return


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

    # STEP 3 - Ontology Data Sources

    # STEP 4 - Update Required Input Build Resources

    # clean up environment after uploading all processed data
    shutil.rmtree(temp_dir)


if __name__ == '__main__':
    main()
