#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import ftplib
# import glob
import gzip
import os
# import pandas
import re
import requests
import shutil

from contextlib import closing
from io import BytesIO
# from tqdm import tqdm
from urllib.request import urlopen
from zipfile import ZipFile


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

    r = requests.get(url, allow_redirects=True)

    # save results
    open(write_location + '{filename}'.format(filename=filename), 'wb').write(r.content)


def ftp_url_download(url: str, write_location: str, filename: str):
    """Downloads a file from an ftp server.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None.
    """
    print('Downloading data from ftp server')

    with closing(urlopen(url)) as r:
        with open(write_location + '{filename}'.format(filename=filename), 'wb') as f:
            shutil.copyfileobj(r, f)


def gzipped_ftp_url_download(url: str, write_location: str):
    """Downloads a gzipped file from an ftp server.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.

    Return:
        None.
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

    # remove gzipped file
    os.remove(write_loc)


def zipped_url_download(url: str, write_location: str):
    """Downloads a zipped file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.

    Return:
        None.
    """
    print('Downloading zipped data file')

    with urlopen(url) as zip_data:
        with ZipFile(BytesIO(zip_data.read())) as zip_file:
            zip_file.extractall(write_location[:-1])


def gzipped_url_download(url: str, write_location: str, filename: str):
    """Downloads a gzipped file from a URL.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None.
    """
    print('Downloading gzipped data file')

    with open(write_location + '{filename}'.format(filename=filename), 'wb') as outfile:
        outfile.write(gzip.decompress(urlopen(url).read()))


# function to download data from a URL
def data_downloader(url: str, write_location: str, filename: str = ''):
    """Downloads data from a URL and saves the file to the `/resources/processed_data/unprocessed_data' directory.

    Args:
        url: A string that points to the location of a temp mapping file that needs to be processed.
        write_location: A string that points to a file directory.
        filename: A string containing a filepath for where to write data to.

    Return:
        None.
    """

    # get filename from url
    file = filename if filename != '' else re.sub('.gz|.zip', '', url.split('/')[-1])

    # zipped data
    if '.zip' in url:
        zipped_url_download(url, file)

    elif '.gz' in url:
        if 'ftp' in url:
            gzipped_ftp_url_download(url, write_location)
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


# function to reformat data files
def data_processor(filepath: str, row_splitter: str, column_list: list, output_name: str, write_location: str,
                   line_splitter: str = ''):
    """Reads in a file using input file path and reduces the file to only include specific columns specified by the
    input var. The reduced file is then saved as a text file and written to the `/resources/processed_data' directory.

    Args:
        filepath: A string that points to the location of a temp mapping file that needs to be processed.
        row_splitter: A string that contains a character used to split rows.
        column_list: A list that contains two numbers, which correspond to indices in the input data file and
                     which appear in the order of write preference.
        output_name: A string naming the processed data file.
        write_location: A string that points to a file directory.
        line_splitter: A character used to separate multiple data points from a string. Defaults to an empty
                       string which is used to indicate the string contains a single value.

    Return:
        None.
    """

    # read in data
    data = open(filepath).readlines()

    # process and write out data
    with open(write_location + '{filename}'.format(filename=output_name), 'w') as outfile:

        for line in data:
            subj = line.split(row_splitter)[column_list[0]]
            obj = line.split(row_splitter)[column_list[1]]

            if subj != '' and obj != '':
                for i in [subj.split(line_splitter) if line_splitter != '' else [subj]][0]:
                    for j in [obj.split(line_splitter) if line_splitter != '' else [obj]][0]:
                        outfile.write(i.strip() + '\t' + j.strip() + '\n')

    outfile.close()
