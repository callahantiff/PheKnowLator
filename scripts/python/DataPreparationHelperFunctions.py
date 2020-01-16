#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import ftplib
import gzip
import numpy
import os
import pandas
import re
import requests
import shutil

from contextlib import closing
from difflib import SequenceMatcher
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

# disable chained assignment warning
# rationale: https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pandas.options.mode.chained_assignment = None


# functions to download data
def url_download(url: str, write_location: str, filename: str):
    """Downloads a file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None

    """
    print('Downloading data file')

    r = requests.get(url, allow_redirects=True, verify=False)
    open(write_location + '{filename}'.format(filename=filename), 'wb').write(r.content)


def ftp_url_download(url: str, write_location: str, filename: str):
    """Downloads a file from an ftp server.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None

    """
    print('Downloading data from ftp server')

    with closing(urlopen(url)) as r:
        with open(write_location + '{filename}'.format(filename=filename), 'wb') as f:
            shutil.copyfileobj(r, f)


def gzipped_ftp_url_download(url: str, write_location: str, filename: str):
    """Downloads a gzipped file from an ftp server.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None

    """

    # get ftp server info
    server = url.replace('ftp://', '').split('/')[0]
    directory = '/'.join(url.replace('ftp://', '').split('/')[1:-1])
    file = url.replace('ftp://', '').split('/')[-1]
    write_loc = write_location + '{filename}'.format(filename=file)

    # download ftp gzipped file
    print('Downloading gzipped data from ftp server')
    with closing(ftplib.FTP(server)) as ftp, open(write_loc, 'wb') as fid:
        ftp.login()
        ftp.cwd(directory)
        ftp.retrbinary('RETR {}'.format(file), fid.write)

    # read in gzipped file,uncompress, and write to directory
    print('Decompressing and writing gzipped data')
    with gzip.open(write_loc, 'rb') as fid_in:
        with open(write_loc.replace('.gz', ''), 'wb') as f:
            f.write(fid_in.read())

    # change filename
    if filename != '':
        os.rename(re.sub('.gz|.zip', '', write_loc), write_location + filename)

    # remove gzipped and original files
    os.remove(write_loc)


def zipped_url_download(url: str, write_location: str, filename: str = ''):
    """Downloads a zipped file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None

    """
    print('Downloading zipped data file')

    with requests.get(url, allow_redirects=True) as zip_data:
        with ZipFile(BytesIO(zip_data.content)) as zip_file:
            zip_file.extractall(write_location[:-1])

    # change filename
    if filename != '':
        os.rename(write_location + re.sub('.gz|.zip', '', url.split('/')[-1]), write_location + filename)


def gzipped_url_download(url: str, write_location: str, filename: str):
    """Downloads a gzipped file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None

    """
    print('Downloading gzipped data file')

    with open(write_location + '{filename}'.format(filename=filename), 'wb') as outfile:
        outfile.write(gzip.decompress(requests.get(url, allow_redirects=True, verify=False).content))


# function to download data from a URL
def data_downloader(url: str, write_location: str, filename: str = ''):
    """Downloads data from a URL and saves the file to the `/resources/processed_data/unprocessed_data' directory.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None

    """

    # get filename from url
    file = re.sub('.gz|.zip', '', filename) if filename != '' else re.sub('.gz|.zip', '', url.split('/')[-1])

    # zipped data
    if '.zip' in url:
        zipped_url_download(url, write_location, file)

    elif '.gz' in url or '.gz' in filename:
        if 'ftp' in url:
            gzipped_ftp_url_download(url, write_location, file)
        else:
            gzipped_url_download(url, write_location, file)

    # not zipped data
    else:
        # download and write data
        if 'ftp' in url:
            ftp_url_download(url, write_location, file)
        else:
            # download data from URL
            url_download(url, write_location, file)


# function to explode nested DataFrames
def explode(df: pandas.DataFrame, lst_cols: list, splitter: str, fill_value: str = 'None', preserve_idx: bool = False):
    """Function takes a Pandas DataFrame containing a mix of nested and un-nested data and un-nests the data by
    expanding each column in a user-defined list. This function is a modification of the explode function provided in
    the following stack overflow post:
    https://stackoverflow.com/questions/12680754/split-explode-pandas-dataframe-string-entry-to-separate-rows.
    The original function was unable to handle multiple columns which expand to different lengths. The modification
    treats the user-provided column list as a stack and recursively un-nests each column.

    Args:
        df: a Pandas DataFrame containing nested columns
        lst_cols: a list of columns to unnest
        splitter: a character delimiter used in nested columns
        fill_value: a string value to fill empty cell values with
        preserve_idx: whether or not thee original index should be preserved or reset

    Returns:
        An exploded Pandas DataFrame

    """

    if not lst_cols:
        return df

    else:

        # pop column to process off the stack
        lst = [lst_cols.pop()]

        # convert string columns to list
        df[lst[0]] = df[lst[0]].apply(lambda x: [j for j in x.split(splitter) if j != ''])

        # all columns except `lst_cols`
        idx_cols = df.columns.difference(lst)

        # calculate lengths of lists
        lens = df[lst[0]].str.len()

        # preserve original index values
        idx = numpy.repeat(df.index.values, lens)

        # create "exploded" DF
        res = (pandas.DataFrame({
            col: numpy.repeat(df[col].values, lens)
            for col in idx_cols},
            index=idx).assign(**{col: numpy.concatenate(df.loc[lens > 0, col].values)
                                 for col in lst}))

        # append those rows that have empty lists
        if (lens == 0).any():
            # at least one list in cells is empty
            res = (res.append(df.loc[lens == 0, idx_cols], sort=False).fillna(fill_value))

        # revert the original index order
        res = res.sort_index()

        # reset index if requested
        if not preserve_idx:
            res = res.reset_index(drop=True)

        # return columns in original order
        res = res[list(df)]

        return explode(res, lst_cols, splitter)


# human disease and phenotype ontology identifiers mapping
def mesh_finder(data: pandas.DataFrame, x_id: str, id_type: str, id_dic: dict):
    """Function takes a Pandas DataFrame, a dictionary, and an id and updates the dictionary by searching additional
    identifiers linked to the id.

    Args:
        data: a Pandas DataFrame containing columns of identifiers
        x_id: a string containing a MeSH or OMIM identifier
        id_type: a string containing the types of the identifier
        id_dic: a dictionary where the keys are CUI and MeSH identifiers and the values are lists of DO and HPO
            identifiers

    Returns:
        None

    """

    for x in list(data.loc[data['code'] == x_id]['diseaseId']):
        for idx, row in data.loc[data['diseaseId'] == x].iterrows():

            if id_type + x_id in id_dic.keys():
                if row['vocabulary'] == 'HPO' and row['code'].replace('HP:', 'HP_') not in id_dic[id_type + x_id]:
                    id_dic[id_type + x_id].append(row['code'].replace('HP:', 'HP_'))

                if row['vocabulary'] == 'DO' and 'DOID_' + row['code'] not in id_dic[id_type + x_id]:
                    id_dic[id_type + x_id].append('DOID_' + row['code'])

            else:
                if row['vocabulary'] == 'HPO' or row['vocabulary'] == 'DO':
                    id_dic[id_type + x_id] = []

                    if row['vocabulary'] == 'HPO' and row['code'].replace('HP:', 'HP_') not in id_dic[id_type + x_id]:
                        id_dic[id_type + x_id].append(row['code'].replace('HP:', 'HP_'))

                    if row['vocabulary'] == 'DO' and 'DOID_' + row['code'] not in id_dic[id_type + x_id]:
                        id_dic[id_type + x_id].append('DOID_' + row['code'])

    return None


def identify_header(path: str, sep: str):
    """Compares the similarity of the first line of a Pandas DataFrame to the column headers when read in with and
    without a header to determine whether or not the data frame should be built with a header or not. This
    function was modified from a Stack Overflow
    post: https://stackoverflow.com/questions/40193388/how-to-check-if-a-csv-has-a-header-using-python/40193509

    Args:
        path: A filepath to a data file.
        sep: A character specifying how the data is delimited.

    Returns:
        if the similarity between the first line and the columns, when a header is inferred,
        then the Pandas.DataFrame is read in with the header.

    """

    # read in data
    df_with_header = pandas.read_csv(path, header='infer', nrows=1, delimiter=sep)
    df_without_header = pandas.read_csv(path, header=None, nrows=1, delimiter=sep)

    # calculate similarity between header and first row
    with_header_test = SequenceMatcher(None, '|'.join([str(x) for x in list(df_with_header.iloc[0])]),
                                       '|'.join([str(x) for x in list(df_with_header)])).ratio()

    without_header_test = SequenceMatcher(None, '|'.join([str(x) for x in list(df_without_header.iloc[0])]),
                                          '|'.join([str(x) for x in list(df_without_header)])).ratio()

    # determine if header should be used
    if abs(with_header_test-without_header_test) < 0.05:
        return 0

    elif with_header_test >= without_header_test:
        return None

    else:
        return 0


def data_reader(data_filepath: str, file_splitter: str = '\n', line_splitter: str = 't'):
    """Takes a filepath pointing to data source and reads it into a Pandas DataFrame using information in the
    file and line splitter variables.

    Args:
        data_filepath: Filepath to data.
        file_splitter: Character to split data, used when a file contains metadata information.
        line_splitter: Character used to split rows into columns.

    Return:
        A Pandas DataFrame containing the data from the data_filepath.

    Raises:
        An exception is raised if the Pandas DataFrame does not contain at least 2 columns and more than 10 rows.

    """

    # identify line splitter
    splitter = '\t' if 't' in line_splitter else " " if ' ' in line_splitter else line_splitter

    # read in data
    if '!' in file_splitter or '#' in file_splitter:
        # ASSUMPTION: assumes data is read in without a header
        decoded_data = open(data_filepath).read().split(file_splitter)[-1]
        edge_data = pandas.DataFrame([x.split(splitter) for x in decoded_data.split('\n') if x != ''])

    else:
        # determine if file contains a header
        header = identify_header(data_filepath, splitter)
        edge_data = pandas.read_csv(data_filepath, header=header, delimiter=splitter, low_memory=False)

    # CHECK - verify data
    if len(list(edge_data)) >= 2 and len(edge_data) > 10:
        return edge_data.dropna(inplace=False)

    else:
        raise Exception('ERROR: Data could not be properly read in')
