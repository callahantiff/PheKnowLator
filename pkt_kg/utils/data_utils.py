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
* genomic_id_mapper
* deduplicates_file
* merges_files
* sublist_creator

Outputs data
* outputs_dictionary_data
"""

# import needed libraries
import ftplib
import gzip
import heapq
import json
import numpy as np  # type: ignore
import os
import pandas as pd  # type: ignore
import re
import requests
import shutil
import urllib3  # type: ignore

from contextlib import closing
from io import BytesIO
from reactome2py import content  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Dict, Generator, List, Optional, Union
from urllib.request import urlopen
from zipfile import ZipFile

# GLOBAL ENVIRONMENT VARIABLE
zip_pat = '.gz|.zip'

# WARNING 1 - Pandas: disable chained assignment warning rationale:
# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pd.options.mode.chained_assignment = None
# WARNING 2 - urllib3: disable insecure request warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def url_download(url: str, write_location: str, filename: str) -> None:
    """Downloads a file from a URL and saves it locally.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Returns:
        None.
    """

    print('Downloading Data from {}'.format(url))

    r = requests.get(url, allow_redirects=True, verify=False)
    with open(write_location + '{filename}'.format(filename=filename), 'wb') as outfile:
        try: outfile.write(r.content)
        except OSError:
            block_size = 1000000000
            for i in range(0, len(r.content), block_size):
                outfile.write(r.content[i:i + block_size])
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
    print('Downloading Gzipped data from FTP Server: {}'.format(url))
    with closing(ftplib.FTP(server)) as ftp, open(write_loc, 'wb') as fid:
        ftp.login(); ftp.cwd(directory); ftp.retrbinary('RETR {}'.format(file), fid.write)
    print('Decompressing and Writing Gzipped Data to File')
    with gzip.open(write_loc, 'rb') as fid_in:
        with open(write_loc.replace('.gz', ''), 'wb') as file_loc:
            file_loc.write(fid_in.read())
    # change filename and remove gzipped and original files
    if filename != '': os.rename(re.sub(zip_pat, '', write_loc), write_location + filename)
    os.remove(write_loc)  # remove compressed file

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

    with requests.get(url, allow_redirects=True) as zip_data:
        with ZipFile(BytesIO(zip_data.content)) as zip_file:
            zip_file.extractall(write_location[:-1])
    zip_data.close()
    if filename != '': os.rename(write_location + re.sub(zip_pat, '', url.split('/')[-1]), write_location + filename)

    return None


def gzipped_url_download(url: str, write_location: str, filename: str) -> None:
    """Downloads a gzipped file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Returns:
        None.
    """

    print('Downloading Gzipped Data from {}'.format(url))

    with open(write_location + '{filename}'.format(filename=filename), 'wb') as outfile:
        outfile.write(gzip.decompress(requests.get(url, allow_redirects=True, verify=False).content))
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

    file = re.sub(zip_pat, '', filename) if filename != '' else re.sub(zip_pat, '', url.split('/')[-1])
    if '.zip' in url: zipped_url_download(url, write_location, file)
    elif '.gz' in url or '.gz' in filename:
        if 'ftp' in url: gzipped_ftp_url_download(url, write_location, file)
        else: gzipped_url_download(url, write_location, file)
    else:
        if 'ftp' in url: ftp_url_download(url, write_location, file)
        else: url_download(url, write_location, file)

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

    ids, labels, desc, synonyms = [], [], [], []

    for x in nodes:
        if x in metadata_dictionaries.keys():
            ids.append(str(x))
            # get labels
            if 'Label' in metadata_dictionaries[x].keys(): labels.append(metadata_dictionaries[x]['Label'])
            else: labels.append('None')
            # get descriptions
            if 'Description' in metadata_dictionaries[x].keys(): desc.append(metadata_dictionaries[x]['Description'])
            else: desc.append('None')
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
    node_metadata_final = node_metadata_final.astype(str)

    return node_metadata_final


def metadata_api_mapper(nodes: List[str]) -> pd.DataFrame:
    """Takes a list of nodes and queries them, in chunks of 20, against the Reactome API.

    Args:
        nodes: A list of identifiers to obtain metadata information for.

    Returns:
        A pandas.DataFrame of metadata results.
    """

    ids, labels, desc, synonyms = [], [], [], []

    for request_ids in tqdm(list(chunks(nodes, 20))):
        results = content.query_ids(ids=','.join(request_ids))
        for row in results:
            ids.append(row['stId'])
            labels.append(row['displayName'])
            desc.append('None')
            if row['displayName'] != row['name']: synonyms.append('|'.join(row['name']))
            else: synonyms.append('None')

    # combine into new data frame
    metadata = pd.DataFrame(list(zip(ids, labels, desc, synonyms)), columns=['ID', 'Label', 'Description', 'Synonym'])
    node_metadata_final = metadata.astype(str)

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
        # create 'exploded' df
        res = (pd.DataFrame({col: np.repeat(df[col].values, lens) for col in idx_cols},
                            index=idx).assign(**{col: np.concatenate(df.loc[lens > 0, col].values) for col in lst}))
        # append those rows that have empty lists
        if (lens == 0).any(): res = (res.append(df.loc[lens == 0, idx_cols], sort=False).fillna(fill_value))
        res = res.sort_index()  # revert the original index order
        if not preserve_idx: res = res.reset_index(drop=True)  # reset index if requested
        res = res[list(df)]  # return columns in original order

        return explodes_data(res, lst_cols, splitter)


def genomic_id_mapper(id_dict: Dict[str, str], filename: str, genomic1: str, genomic2: str,
                      src_genomic_type: Optional[str], tgt_genomic_type: Optional[str], src_update: Optional[str],
                      tgt_update: Optional[str]) -> None:
    """Searches a dictionary of genomic identifier mappings and processes them, writing out

    Args:
        id_dict: A dict where keys are genomic identifiers and values are lists of cross-mappings.
        filename: A string containing a filename to write results to.
        genomic1: A string indicating a genomic identifier type (e.g. transcript_stable_id, ensembl_gene_id).
        genomic2: A string indicating a genomic identifier type (e.g. transcript_stable_id, ensembl_gene_id).
        src_genomic_type: A string indicating the prefix for the source genomic_type.
        tgt_genomic_type: A string indicating the prefix for the target genomic_type.
        src_update: A string indicating the prefix for the source genomic_type; protein-coding or NOT.
        tgt_update: A string indicating the prefix for the target genomic_type; protein-coding or NOT.

    Return:
        None.
    """

    prots = ['uniprot_id', 'pro_id', 'protein_stable_id']; prot_types = genomic1 in prots and genomic2 in prots
    results = set(); keys = set(x for x in id_dict.keys() if x.startswith(genomic1))
    for key in tqdm(keys):
        source, targets = key, [x for x in id_dict[key] if x.startswith(genomic2)]
        src_type = 'None'; src_type_update = 'None'
        if src_genomic_type is not None:
            src = [x for x in id_dict[key] if x.startswith(src_genomic_type)]
            src_type = src[0].replace(src_genomic_type + '_', '') if len(src) > 0 else 'None'
        if src_update is not None:
            src2 = [x for x in id_dict[key] if x.startswith(src_update)]
            src_type_update = src2[0].replace(src_update + '_', '') if len(src2) > 0 else 'None'
        for target in targets:
            tgt_type = 'None'; tgt_type_update = 'None'
            if tgt_genomic_type is not None:
                tgt = [x for x in id_dict[target] if x.startswith(tgt_genomic_type)]
                tgt_type = tgt[0].replace(tgt_genomic_type + '_', '') if len(tgt) > 0 else 'None'
            if tgt_update is not None:
                tgt2 = [x for x in id_dict[target] if x.startswith(tgt_update)]
                tgt_type_update = tgt2[0].replace(tgt_update + '_', '') if len(tgt2) > 0 else 'None'
            # format source and target entities
            res1 = source.replace(genomic1 + '_', ''); res2 = target.replace(genomic2 + '_', '')
            # write out protein entities -- they have no genomic type
            if prot_types: results |= {(res1, res2, src_type, tgt_type, src_type_update, tgt_type_update)}
            # if gene or transcript, only write out entities with a genomic type
            elif (genomic1 in prots and genomic2 not in prots) and ('None' not in [tgt_type_update, tgt_type]):
                results |= {(res1, res2, src_type, tgt_type, src_type_update, tgt_type_update)}
            elif (genomic1 not in prots and genomic2 in prots) and ('None' not in [src_type_update, src_type]):
                results |= {(res1, res2, src_type, tgt_type, src_type_update, tgt_type_update)}
            else:
                if 'None' not in [src_type_update, tgt_type_update, tgt_type, src_type]:
                    results |= {(res1, res2, src_type, tgt_type, src_type_update, tgt_type_update)}

    with open(filename, 'w') as outfile:
        for row in results: outfile.write(
            row[0] + '\t' + row[1] + '\t' + row[2] + '\t' + row[3] + '\t' + row[4] + '\t' + row[5] + '\n')
    outfile.close()

    return None


def outputs_dictionary_data(dict_object: Optional[Dict], filename: str) -> None:
    """Outputs a dictionary of data as a json file.

    Args:
        dict_object: A dictionary object containing data.
        filename: A string containing a filepath.

    Returns:
        None
    """

    if dict_object:
        with open(filename, 'w') as file_name:
            json.dump(dict_object, file_name)
        file_name.close()

    return None


def deduplicates_file(src_filepath: str) -> None:
    """Removes duplicates from a file.

    Args:
        src_filepath: A string specifying a path to an existing file.

    Returns:
         None.
    """

    print('Depduplicating File: {}'.format(src_filepath))

    lines = list(set(open(src_filepath, 'r').readlines())); pbar = tqdm(total=len(lines))
    with open(src_filepath, 'w') as f:
        while len(lines) > 0:
            x = lines.pop(); pbar.update(1)
            f.write(x) if x.endswith('\n') else f.write(x + '\n')
        pbar.close()

    return None


def merges_files(filepath1: str, filepath2: str, merged_filepath: str) -> None:
    """Merges two files together.

    Args:
        filepath1: A string specifying a path to an existing file.
        filepath2: A string specifying a path to an existing file.
        merged_filepath: A string specifying the file name for the merged files.

    Returns:
         None.
    """

    print('Merging Files: {} and {}'.format(filepath1, filepath2))

    os.system('( cat {} ; echo ''; cat {} ) > {}'.format(filepath1, filepath2, merged_filepath))

    return None


def sublist_creator(actors: Union[Dict, List], chunk_size: int) -> List:
    """Takes a list of lists and returns sublists, where the sublists are balanced according to their length.

    SOURCE: https://stackoverflow.com/questions/61648065

    Args:
        actors: A list or a dictionary keyed by edge identifier with the length of each associated edge list
            stored as the values.
        chunk_size: An integer specifying the number of sublists that should be returned.

    Returns:
         updated_lists: A list of lists, where the inner lists have been balanced by their size.
    """

    if isinstance(actors, Dict): values = sorted(list(actors.values()), reverse=True)
    else: values = sorted(actors, reverse=True)
    lists: List = [[] for _ in range(chunk_size)]; totals = [(0, i) for i in range(chunk_size)]; heapq.heapify(totals)
    for value in values:
        total, index = heapq.heappop(totals); lists[index].append(value); heapq.heappush(totals, (total + value, index))

    # update list to return string identifier associated with each list length
    if isinstance(actors, Dict):
        updated_lists = []; used_ids = set()
        for sub in lists:
            sub_list = [[k for k, v in actors.items() if v == x and k not in used_ids][0] for x in sub]
            updated_lists += [sub_list]; used_ids |= set(x for y in sub_list for x in y)
    else: updated_lists = lists

    return updated_lists
