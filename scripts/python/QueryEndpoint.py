################################################################################################
# QueryEndpoint.py
# Purpose: script runs SPARQL queries against a user-specified endpoint
# version 1.0.0
# date: 10.24.2017
################################################################################################


## import module/script dependencies
import requests
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib
import urllib.request
from datetime import datetime
import json


def RunQuery(query_body, output, url, username="", password=""):
    '''
    Function takes four strings representing: the body of a query, the filepath/name for where to write results, the
    endpoint url, username, and password. Once authenticated, the function runs the query in query_body and returns a
    JSON files containing the query results.
    :param query_body: updated SPARQL query represented as a single string
    :param output: string containing the file path and name of json file to write results to
    :param url: a string containing the url to SPARQL Endpoint
    :param username: a string containing the endpoint username, default is empty string
    :param password: a string containing the endpoint password, default is empty string
    '''

    # CHECK: verify provided endpoint credentials
    request = requests.get(url, auth=(username, password))

    if request.status_code != 200:
        request.raise_for_status()

    else:
        # set-up endpoint proxy
        proxy_support = urllib.request.ProxyHandler()
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)

        # connect to knowledge source
        endpoint = SPARQLWrapper(url)
        endpoint.setCredentials(user = username, passwd = password)
        endpoint.setReturnFormat(JSON)  # query output format

        print(str('Started running query at: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        # run query against KaBOB
        endpoint.setQuery(query_body)
        query_results = endpoint.query().convert()

        print(str('Finished running query at: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        print('\n')

        # verify that query worked
        if len(query_results['results'].items()) < 1:
            print('ERROR: query returned no results')
        else:
            # export results to json file
            with open(str(output + ".json"), 'w', encoding='utf8') as outfile:
                json.dump(query_results, outfile)

            return query_results