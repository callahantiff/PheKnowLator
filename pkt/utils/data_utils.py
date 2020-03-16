#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data PreProcessing Utility Functions.

Downloads Data from a Url
* url_download
* ftp_url_download
* gzipped_ftp_url_download
* zipped_url_download
* gzipped_url_download
* data_downloader

Generates Metadata
* chunks
* metadata_dictionary_mapper
* metadata_api_mapper

Miscellaneous data Processing Methods
* explodes_data
* mesh_finder
* genomic_id_mapper
"""

# import needed libraries
import ftplib
import gzip
import numpy as np  # type: ignore
import os
import pandas as pd  # type: ignore
import re
import requests
import shutil

from contextlib import closing
from io import BytesIO
from reactome2py import content  # type: ignore
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tqdm import tqdm  # type: ignore
from typing import Dict, Generator, List, Union
from urllib.request import urlopen
from zipfile import ZipFile

# ENVIRONMENT WARNINGS
# WARNING 1 - Pandas: disable chained assignment warning rationale:
# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pd.options.mode.chained_assignment = None

# WARNING 2 - urllib3: disable insecure request warning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def url_download(url: str, write_location: str, filename: str) -> None:
    """Downloads a file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Returns:
        None.

    Raises:
        HTTPError: If the response returns a status code other than 200.
    """

    print('Downloading Data from {}'.format(url))
    r = requests.get(url, allow_redirects=True, verify=False)

    if r.ok is False:
        raise HTTPError('{status} error! Data could not be downloaded from {url}'.format(status=r.status_code, url=url))
    else:
        data = None
        if 'Content-Length' in r.headers:
            while r.ok and int(r.headers['Content-Length']) < 1000:
                r = requests.get(url, allow_redirects=True, verify=False)
            data = r.content
        else:
            if len(r.content) > 10:
                data = r.content

        # download and save data
        if data:
            with open(write_location + '{filename}'.format(filename=filename), 'wb') as outfile:
                outfile.write(data)
            outfile.close()

    return None


def ftp_url_download(url: str, write_location: str, filename: str) -> None:
    """Downloads a file from an ftp server.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Returns:
        None.
    """

    print('Downloading Data from FTP Server: {}'.format(url))

    with closing(urlopen(url)) as downloaded_data:
        with open(write_location + '{filename}'.format(filename=filename), 'wb') as outfile:
            shutil.copyfileobj(downloaded_data, outfile)
        outfile.close()

    return None


def gzipped_ftp_url_download(url: str, write_location: str, filename: str) -> None:
    """Downloads a gzipped file from an ftp server.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Returns:
        None.
    """

    # get ftp server info
    server = url.replace('ftp://', '').split('/')[0]
    directory = '/'.join(url.replace('ftp://', '').split('/')[1:-1])
    file = url.replace('ftp://', '').split('/')[-1]
    write_loc = write_location + '{filename}'.format(filename=file)

    # download ftp gzipped file
    print('Downloading Gzipped data from FTP Server: {}'.format(url))
    with closing(ftplib.FTP(server)) as ftp, open(write_loc, 'wb') as fid:
        ftp.login()
        ftp.cwd(directory)
        ftp.retrbinary('RETR {}'.format(file), fid.write)

    # read in gzipped file,uncompress, and write to directory
    print('Decompressing and Writing Gzipped Data to File')
    with gzip.open(write_loc, 'rb') as fid_in:
        with open(write_loc.replace('.gz', ''), 'wb') as file_loc:
            file_loc.write(fid_in.read())

    # change filename and remove gzipped and original files
    if filename != '':
        os.rename(re.sub('.gz|.zip', '', write_loc), write_location + filename)

    # remove compressed file
    os.remove(write_loc)

    return None


def zipped_url_download(url: str, write_location: str, filename: str = '') -> None:
    """Downloads a zipped file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Returns:
        None.
    """

    print('Downloading Zipped Data from {}'.format(url))
    r = requests.get(url, allow_redirects=True)

    if r.ok is False:
        raise HTTPError('{status} error! Data could not be downloaded from {url}'.format(status=r.status_code, url=url))
    else:
        with r as zip_data:
            with ZipFile(BytesIO(zip_data.content)) as zip_file:
                zip_file.extractall(write_location[:-1])
        zip_data.close()

        # change filename
        if filename != '':
            os.rename(write_location + re.sub('.gz|.zip', '', url.split('/')[-1]), write_location + filename)

    return None


def gzipped_url_download(url: str, write_location: str, filename: str) -> None:
    """Downloads a gzipped file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Returns:
        Nones
    """

    print('Downloading Gzipped Data from {}'.format(url))
    r = requests.get(url, allow_redirects=True, verify=False)

    if r.ok is False:
        raise HTTPError('{status} error! Data could not be downloaded from {url}'.format(status=r.status_code, url=url))
    else:
        with open(write_location + '{filename}'.format(filename=filename), 'wb') as outfile:
            outfile.write(gzip.decompress(r.content))
        outfile.close()

    return None


def data_downloader(url: str, write_location: str, filename: str = '') -> None:
    """Downloads data from a URL and saves the file to the `/resources/processed_data/unprocessed_data' directory.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Returns:
        None.
    """

    # get filename from url
    file = re.sub('.gz|.zip', '', filename) if filename != '' else re.sub('.gz|.zip', '', url.split('/')[-1])

    if '.zip' in url:
        zipped_url_download(url, write_location, file)
    elif '.gz' in url or '.gz' in filename:
        if 'ftp' in url:
            gzipped_ftp_url_download(url, write_location, file)
        else:
            gzipped_url_download(url, write_location, file)
    else:
        if 'ftp' in url:
            ftp_url_download(url, write_location, file)
        else:
            url_download(url, write_location, file)

    return None


def chunks(lst: List[str], chunk_size: int) -> Generator:
    """Takes a list an integer and creates a list of lists, where each nested list is length chunk_size.

    Modified from: https://chrisalbon.com/python/data_wrangling/break_list_into_chunks_of_equal_size/

    Args:
        lst: A list of objects, can be strings or integers.
        chunk_size: An integer which specifies the how big each chunk should be.

    Returns:
        A nested list, where the length of each nested list is the size of the integer passed by the user.
    """

    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def metadata_dictionary_mapper(nodes: List[str], metadata_dictionaries: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """Takes a list of nodes and a dictionary of metadata and returns a pandas.DataFrame containing the mapped
    metadata for each of the nodes.

    Args:
        nodes: A list of identifiers to obtain metadata information for.
        metadata_dictionaries: A metadata dictionary to obtain metadata from.

    Returns:
        A pandas.DataFrame that contains the metadata results.
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
    node_metadata_final = pd.DataFrame(list(zip(ids, labels, desc, synonyms)),
                                       columns=['ID', 'Label', 'Description', 'Synonym'])

    # make all variables string
    node_metadata_final = node_metadata_final.astype(str)

    return node_metadata_final


def metadata_api_mapper(nodes: List[str]) -> pd.DataFrame:
    """Takes a list of nodes and queries them, in chunks of 20, against the Reactome API.

    Args:
        nodes: A list of identifiers to obtain metadata information for.

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
            labels.append(row['displayName'])
            desc.append('None')

            if row['displayName'] != row['name']:
                synonyms.append('|'.join(row['name']))
            else:
                synonyms.append('None')

    # combine into new data frame
    node_metadata_final = pd.DataFrame(list(zip(ids, labels, desc, synonyms)),
                                       columns=['ID', 'Label', 'Description', 'Synonym'])

    # make all variables string
    node_metadata_final = node_metadata_final.astype(str)

    return node_metadata_final


def explodes_data(df: pd.DataFrame, lst_cols: list, splitter: str, fill_value: str = 'None',
                  preserve_idx: bool = False) -> pd.DataFrame:
    """Function takes a Pandas DataFrame containing a mix of nested and un-nested data and un-nests the data by
    expanding each column in a user-defined list. This function is a modification of the explode function provided
    in the following stack overflow post:
    https://stackoverflow.com/questions/12680754/split-explode-pandas-dataframe-string-entry-to-separate-rows.
    The original function was unable to handle multiple columns which expand to different lengths. The modification
    treats the user-provided column list as a stack and recursively un-nests each column.

    Args:
        df: A Pandas DataFrame containing nested columns
        lst_cols: A list of columns to unnest
        splitter: A character delimiter used in nested columns
        fill_value: A string value to fill empty cell values with
        preserve_idx: Whether or not thee original index should be preserved or reset.

    Returns:
        An exploded Pandas DataFrame.
    """

    if not lst_cols:
        return df
    else:
        lst = [lst_cols.pop()]  # pop column to process off the stack
        df[lst[0]] = df[lst[0]].apply(lambda x: [j for j in x.split(splitter) if j != ''])  # convert col to list
        idx_cols = df.columns.difference(lst)  # all columns except `lst_cols`
        lens = df[lst[0]].str.len()  # calculate lengths of lists
        idx = np.repeat(df.index.values, lens)  # preserve original index values

        # create "exploded" DF
        res = (pd.DataFrame({
            col: np.repeat(df[col].values, lens)
            for col in idx_cols},
            index=idx).assign(**{col: np.concatenate(df.loc[lens > 0, col].values)
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

        return explodes_data(res, lst_cols, splitter)


def mesh_finder(data: pd.DataFrame, xid: str, id_typ: str, id_dic: Dict[str, List[str]]) -> None:
    """Function takes a Pandas DataFrame, a dictionary, and an id and updates the dictionary by searching additional
    identifiers linked to the id.

    Args:
        data: A Pandas DataFrame containing columns of identifiers.
        xid: A string containing a MeSH or OMIM identifier.
        id_typ: A string containing the types of the identifier.
        id_dic: A dictionary where the keys are CUI and MeSH identifiers and the values are lists of DO and HPO
            identifiers.

    Returns:
        None
    """

    for x in list(data.loc[data['code'] == xid]['diseaseId']):
        for idx, row in data.loc[data['diseaseId'] == x].iterrows():

            if id_typ + xid in id_dic.keys():
                if row['vocabulary'] == 'HPO' and row['code'].replace('HP:', 'HP_') not in id_dic[id_typ + xid]:
                    id_dic[id_typ + xid].append(row['code'].replace('HP:', 'HP_'))

                if row['vocabulary'] == 'DO' and 'DOID_' + row['code'] not in id_dic[id_typ + xid]:
                    id_dic[id_typ + xid].append('DOID_' + row['code'])

            else:
                if row['vocabulary'] == 'HPO' or row['vocabulary'] == 'DO':
                    id_dic[id_typ + xid] = []

                    if row['vocabulary'] == 'HPO' and row['code'].replace('HP:', 'HP_') not in id_dic[id_typ + xid]:
                        id_dic[id_typ + xid].append(row['code'].replace('HP:', 'HP_'))

                    if row['vocabulary'] == 'DO' and 'DOID_' + row['code'] not in id_dic[id_typ + xid]:
                        id_dic[id_typ + xid].append('DOID_' + row['code'])

    return None


def genomic_id_mapper(id_dict: Dict[str, str], filename: str, genomic1: str, genomic2: str,
                      genomic_type: str = None) -> None:
    """Searches a dictionary of genomic identifier mappings and processes them, writing out

    Args:
        id_dict: A dictionary where the key is genomic identifier and the value is a list of other genomic
            identifiers mapped to the key.
        filename: A string containing a filename to write results to.
        genomic1: A string indicating a genomic identifier type (i.e. transcript_stable_id, ensembl_gene_id,
            entrez_id, hgnc_id, symbol).
        genomic2: A string indicating a genomic identifier type (i.e. transcript_stable_id, ensembl_gene_id,
            entrez_id, hgnc_id, symbol).
        genomic_type: A string indicating whether or not to save the gene or transcript type.

    Return:
        None.
    """

    with open(filename, 'w') as outfile:
        for key in tqdm(id_dict.keys()):
            id_data = id_dict[key]
            gene_type = [x.split('_')[-1] for x in id_data if x.startswith(genomic_type)][0] if genomic_type else \
                'Nones'

            for res in id_data:
                if genomic1 in key and res.startswith(genomic2):
                    outfile.write(key.split('_')[-1] + '\t' + res.split('_')[-1] + '\t' + gene_type + '\n')
                elif genomic2 in key and res.startswith(genomic1):
                    outfile.write(res.split('_')[-1] + '\t' + key.split('_')[-1] + '\t' + gene_type + '\n')
                else:
                    continue
    outfile.close()

    return None
