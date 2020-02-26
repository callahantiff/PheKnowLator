#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import json
import os
import requests
import time

from tqdm import tqdm
from typing import Dict, IO, Set


def gets_json_results_from_api_call(url: str, api_key: str) -> Dict[str, str]:
    """Function makes API requests and returns results as a json file. API documentation can be found here:
    http://data.bioontology.org/documentation. If a 500 HTTP server-side code is return from "status_code" then the
    algorithm pauses for 30 seconds before trying the request again.

    Args:
        url: A string containing a URL to be run against an API.
        api_key: A string containing an API key.

    Return:
        A json-formatted file containing API results.

    Raises:
        An exception is raised if a 500 HTTP server-side code is raised.
    """

    response = requests.get(url, headers={'Authorization': 'apikey token=' + api_key})

    if response.status_code == 500:
        # to ease rate limiting sleep for random amount of time between 10-60 seconds
        time.sleep(30)
        response = requests.get(url, headers={'Authorization': 'apikey token=' + api_key})

    return json.loads(response.text)


def writes_data_to_file(file_out: str, results: Set[Tuple[str, str]]) -> IO[str]:
    """Function iterates over set of tuples and writes data to text file locally.

    Args:
        file_out: A filepath to write data to.
        results: A set of tuples, where each tuple represents a mapping between two identifiers.

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

    return None


def extracts_mapping_data(api_key: str, source1: str, source2: str, file_out: str) -> None:
    """Function uses the BioPortal API to retrieve mappings between two sources. The function batch processes the
    results in chunks of 1000, writes the data to a temporary directory and then once all batches have been
    processed, the data is concatenated into a single file.

    Args:
        api_key: A string containing a user BiPortal API key.
        source1: A string naming a source ontology that you want to map from.
        source2: A string naming an ontology that you want to map identifiers from source1 to.
        file_out: A filepath to write data to.

    Returns:
        None.
    """

    print('\n' + '=' * 50)
    print('Running REST API - Mapping: {source1} to {source2}'.format(source1=source1, source2=source2))
    print('=' * 50 + '\n')

    # get the available resources for mappings to source
    ont_source = 'http://data.bioontology.org/ontologies/{source}/mappings/'.format(source=source1)
    api_results = gets_json_results_from_api_call(ont_source, api_key)

    # enable batch processing of api result pages
    total_pages = list(range(1, api_results['pageCount'] + 1))
    n = 500 if len(total_pages) > 5000 else 100
    batches = [total_pages[i:i + n] for i in range(0, len(total_pages), n)]

    for batch in tqdm(range(0, len(batches))):
        unique_edges = set()

        # iterate over each page of results
        for page in tqdm(batches[batch]):
            time.sleep(5)
            content = gets_json_results_from_api_call(ont_source + '?page={page}'.format(page=page), api_key)

            for result in content['collection']:
                if source2 in result['classes'][1]['links']['ontology']:
                    unique_edges.add((result['classes'][0]['@id'], result['classes'][1]['@id']))

        writes_data_to_file(file_out + '_{batch_num}'.format(batch_num=batch) + '.txt', unique_edges)

    return None


def main() -> None:
    # get api key
    api_key = input('Please provide your BioPortal API Key: ')

    # get user info for sources to map
    source1 = input('Enter ontology source 1: ').upper()
    source2 = input('Enter ontology source 2: ').upper()

    # run API call in batches + save data
    file_path = './resources/processed_data/'
    temp_directory = file_path + 'temp'

    # create temp directory to store batches
    if not os.path.isdir(temp_directory):
        print('Creating a temporary directory to write API results to: {}'.format(temp_directory))
        os.mkdir(temp_directory)

    # run program to map identifiers between source1 and source2
    file_name = '/{source1}_{source2}_MAP'.format(source1=source1, source2=source2)
    extracts_mapping_data(api_key, source1, source2, temp_directory + file_name)


if __name__ == '__main__':
    main()
