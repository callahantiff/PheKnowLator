#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import json
import urllib.error
import urllib.parse
import urllib.request

from tqdm import tqdm


def gets_json_results_from_api_call(url):
    """Function makes API requests and returns results as a json file.s

    Args:
        url (str): A string containing a URL to be run against an API.

    Return:
        A json-formatted file of API results.
    """
    opener = urllib.request.build_opener()
    opener.addheaders = [('Authorization', 'apikey token=' + open('resources/Other/bioportal_api_key.txt').read())]

    return json.loads(opener.open(url).read())


def extracts_mapping_data(source1, source2):
    """Function uses the BioPortal API to retrieve mappings between two sources.

    Args:
        source1 (str): An ontology.
        source2 (str): An ontology.

    Returns:
        A set of tuples, where each tuple represents a mapping between two identifiers (one from each source).
    """

    unique_edges = set()

    # get the available resources for mappings to source
    ont_source = 'http://data.bioontology.org/ontologies/{source}/mappings/'.format(source=source1)
    api_results = gets_json_results_from_api_call(ont_source)

    # iterate over results
    for page in tqdm(range(api_results['pageCount'] - 1)):
        page_url = 'http://data.bioontology.org/ontologies/{source}/mappings/?page={page}'.format(source=source2,
                                                                                                  page=page + 1)
        content = gets_json_results_from_api_call(page_url)

        for result in content['collection']:
            if source2 in result['classes'][1]['links']['ontology']:

                unique_edges.add((result['classes'][0]['@id'], result['classes'][1]['@id']))

    return unique_edges


def writes_data_to_file(file_out, results):
    """Function iterates over set of tuples and writes data to text file locally.

    Args:
        file_out (str): File path for location to write data to.
        results (set): A set of tuples, where each tuple represents a mapping between two identifiers.

    Returns:
        None.
    """

    # write location + open file
    outfile = open(file_out, 'w')

    for res in results:
        outfile.write(res[0] + '\t' + res[1] + '\n')

    outfile.write('\n')
    outfile.close()


def main():

    # get info
    source1 = input('Enter ontology source 1: ')
    source2 = input('Enter ontology source 2: ')
    filename = input('What would you like this file to be called?: ')

    # run API call
    print('RUNNING API CALL\n')
    results = extracts_mapping_data(source1, source2)

    # save data
    print('WRITING DATA TO FILE\n')
    file_out = './resources/text_files/{filename}.txt'.format(filename=filename)
    writes_data_to_file(file_out, results)


if __name__ == '__main__':
    main()
