#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import glob
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

from tqdm import tqdm
from urllib.error import HTTPError


def gets_json_results_from_api_call(url):
    """Function makes API requests and returns results as a json file. API documentation can be found here:
    http://data.bioontology.org/documentation.

    Args:
        url (str): A string containing a URL to be run against an API.

    Return:
        A json-formatted file of API results.

    Raises:
        An exception is raised if a 500 HTTP server-side code is raised.
    """

    try:
        # to ease rate limiting sleep for random amount of time between 10-60 seconds
        opener = urllib.request.build_opener()

    except HTTPError:
        # pause for 1 minutes and try again
        time.sleep(30)
        opener = urllib.request.build_opener()

    # fetch data
    opener.addheaders = [('Authorization', 'apikey token=' + open('resources/bioportal_api_key.txt').read())]

    return json.loads(opener.open(url).read())


def writes_data_to_file(file_out, results):
    """Function iterates over set of tuples and writes data to text file locally.

    Args:
        file_out (str): File path for location to write data to.
        results (set): A set of tuples, where each tuple represents a mapping between two identifiers.

    Returns:
        None.
    """

    print('\n' + '=' * 50)
    print('Writing results to {location}'.format(location=file_out))
    print('=' * 50 + '\n')

    outfile = open(file_out, 'w')

    for res in results:
        outfile.write(res[0] + '\t' + res[1] + '\n')

    outfile.write('\n')
    outfile.close()


def extracts_mapping_data(source1, source2, file_out):
    """Function uses the BioPortal API to retrieve mappings between two sources. The function batch processes the
    results in chunks of 1000, writes the data to a temporary directory and then once all batches have been
    processed, the data is concatenated into a single file.

    Args:
        source1 (str): An ontology.
        source2 (str): An ontology.
        file_out (str): File path for location to write data to.

    Returns:
        None.
    """

    print('\n' + '=' * 50)
    print('Running REST API to map {source1} to {source2}'.format(source1=source1, source2=source2))
    print('=' * 50 + '\n')

    # get the available resources for mappings to source
    ont_source = 'http://data.bioontology.org/ontologies/{source}/mappings/'.format(source=source1)
    api_results = gets_json_results_from_api_call(ont_source)

    # enable batch processing
    total_pages = list(range(1, api_results['pageCount'] + 1))
    n = 500 if len(total_pages) > 5000 else 100
    batches = [total_pages[i:i + n] for i in range(0, len(total_pages), n)]

    for batch in tqdm(range(0, len(batches))):
        unique_edges = set()

        # iterate over each page of results
        for page in tqdm(batches[batch]):
            time.sleep(5)
            content = gets_json_results_from_api_call(ont_source + '?page={page}'.format(page=page))

            for result in content['collection']:
                if source2 in result['classes'][1]['links']['ontology']:
                    unique_edges.add((result['classes'][0]['@id'], result['classes'][1]['@id']))

        writes_data_to_file(file_out + '_{batch_num}'.format(batch_num=batch) + '.txt', unique_edges)

    return None


def main():

    # get user info for sources to map
    source1 = input('Enter ontology source 1: ')
    source2 = input('Enter ontology source 2: ')

    # run API call in batches + save data
    file_path = 'resources/data_maps/'
    file_name = '{source1}_{source2}_MAP'.format(source1=source1.upper(), source2=source2.upper())
    temp_directory = file_path + 'temp'

    # create temp directory to store batches
    os.mkdir(temp_directory)

    # run program to map identifiers between source1 and source2
    extracts_mapping_data(source1, source2, temp_directory + '/' + file_name)

    # concatenate batch data into single file
    print('\n' + '=' * 50)
    print('Concatenating Batch-Processed Data')
    print('=' * 50 + '\n')

    write_location = file_path + file_name + '.txt'

    with open(write_location, 'w') as outfile:
        for filename in glob.glob(temp_directory + '/*.txt'):
            file_data = list(filter(None, open(filename, 'r').read().split('\n')))

            for line in file_data:
                mesh = '_'.join(line.split('\t')[0].split('/')[-2:])
                chebi = line.split('\t')[1].split('/')[-1]
                outfile.write(mesh + '\t' + chebi + '\n')

    outfile.close()


if __name__ == '__main__':
    main()
