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

    r = requests.get(url, allow_redirects=True)
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
        os.rename(write_loc, write_location + filename)

    # remove gzipped and original files
    os.remove(re.sub('.gz|.zip|\?.*', '', write_loc))


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

    if filename == '':
        with requests.get(url, allow_redirects=True) as zip_data:
            with ZipFile(BytesIO(zip_data.content)) as zip_file:
                zip_file.extractall(write_location[:-1])

    else:
        response = requests.get(url)
        content = BytesIO(response.content).read()
        file = open(write_location + filename, 'w')
        file.write(str(content))
        file.close()


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
        outfile.write(gzip.decompress(requests.get(url, allow_redirects=True).content))


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
    file = filename if filename != '' else re.sub('.gz|.zip', '', url.split('/')[-1])

    # zipped data
    if '.zip' in url:
        zipped_url_download(url, write_location, file)

    elif '.gz' in url:
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
