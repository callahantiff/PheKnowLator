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
from reactome2py import content
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from urllib.request import urlopen
from zipfile import ZipFile

# disable chained assignment warning
# rationale: https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pandas.options.mode.chained_assignment = None
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# functions to download data
def url_download(url: str, write_location: str, filename: str):
    """Downloads a file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None.
    """

    print('Downloading data file')

    r = requests.get(url, allow_redirects=True, verify=False)

    if 'Content-Length' in r.headers:
        while r.ok and int(r.headers['Content-Length']) < 1000:
            r = requests.get(url, allow_redirects=True, verify=False)

        data = r.content
        open(write_location + '{filename}'.format(filename=filename), 'wb').write(data)

    else:
        if len(r.content) > 10:
            open(write_location + '{filename}'.format(filename=filename), 'wb').write(r.content)

    return None


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


# list chunking function -- used for making requests to the reactome api
def chunks(lst: list, chunk_size: int):
    """Takes a list an integer and creates a list of lists, where each nested list is length chunk_size.

    Modified from: https://chrisalbon.com/python/data_wrangling/break_list_into_chunks_of_equal_size/

    Args:
        lst: list of objects, can be strings or integers.
        chunk_size: an integer which specifies the how big each chunk should be.

    Returns:
        A nested list, where the length of each nested list is the size of the integer passed by the user.
    """

    for i in range(0, len(lst), chunk_size):
        yield lst[i:i+chunk_size]


def metadata_dictionary_mapper(nodes: list, metadata_dictionaries: dict):
    """Takes a list of nodes and a dictionary of metadata and returns a pandas.DataFrame containing the mapped
    metadata for each of the nodes.

    Args:
        nodes: the list of identifiers to obtain metadata information for.
        metadata_dictionaries: the metadata dictionary to obtain metadata from.

    Returns:
        A pandas.DataFrame of metadata results.
    """

    # data to gather
    ids, labels, desc, synonyms = [], [], [], []

    # map nodes
    for x in nodes:
        if x in metadata_dictionaries.keys():
            ids.append(str(x))

            # get labels
            if 'Label' in metadata_dictionaries[x].keys():
                labels.append(metadata_dictionaries[x]['Label'])

            else:
                labels.append('None')

            # get descriptions
            if 'Description' in metadata_dictionaries[x].keys():
                desc.append(metadata_dictionaries[x]['Description'])

            else:
                desc.append('None')

            # get synonyms
            if 'Synonym' in metadata_dictionaries[x].keys():
                if metadata_dictionaries[x]['Synonym'].endswith('|'):
                    synonyms.append('|'.join(metadata_dictionaries[x]['Synonym'].split('|')[0:-1]))

                else:
                    synonyms.append(metadata_dictionaries[x]['Synonym'])

            else:
                synonyms.append('None')

    # combine into new data frame
    node_metadata_final = pandas.DataFrame(list(zip(ids, labels, desc, synonyms)),
                                           columns=['ID', 'Label', 'Description', 'Synonym'])

    # make all variables string
    node_metadata_final = node_metadata_final.astype(str)

    return node_metadata_final


def metadata_api_mapper(nodes: list):
    """Takes a list of nodes and queries them, in chunks of 20, against the Reactome API.

    Args:
        nodes: the list of identifiers to obtain metadata information for.

    Returns:
        A pandas.DataFrame of metadata results.
    """

    # data to gather
    ids, labels, desc, synonyms = [], [], [], []

    # get data from reactome API
    for request_ids in list(chunks(nodes, 20)):
        results = content.query_ids(ids=','.join(request_ids))

        for row in results:
            ids.append(row['stId'])

            # get labels
            labels.append(row['displayName'])

            # get descriptions
            desc.append('None')

            # get synonyms
            if row['displayName'] != row['name']:
                synonyms.append('|'.join(row['name']))
            else:
                synonyms.append('None')

    # combine into new data frame
    node_metadata_final = pandas.DataFrame(list(zip(ids, labels, desc, synonyms)),
                                           columns=['ID', 'Label', 'Description', 'Synonym'])

    # make all variables string
    node_metadata_final = node_metadata_final.astype(str)

    return node_metadata_final
