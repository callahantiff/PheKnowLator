##########################################################################################
# OntologyData_ontologies.py
# Purpose: script to download OWL ontology files and store source metadata
# version 1.0.0
# date: 11.06.2017
# Python 3.6.2
##########################################################################################


# import needed libraries
from scripts.python.DataSources import DataSource
from owlready2 import *
import subprocess


class OntData(DataSource):

    """Subclass of DataSource, dedicated to processing text ontologies.

    The class takes as input a string that represents the file path/name of a text file that stores 1 to many
    different data sources. Each source is shown as a URL. The class is comprised of several methods that are all
    designed to perform a different function on the data sources. In the same directory that each type of data is
    downloaded to, a metadata file is created that stores important information on each of the files that is downloaded

        :param
            data_path (str): a file path/name to a text file storing URLs of different sources to download

            data_type (str): type of data sources

            source_list (list): a list of URLs representing the data sources to download/process

            data_files (list): the full file path and name of each downloaded data source

    """

    def file_parser(self):
        """Function verifies that the file contains data and then outputs the a list where each item is a line from the
        input text file. The function performs a check before processing any data to ensure that the data file is not
        empty. The function also verifies the URL for each ontology to ensure that they are OBO ontologies before
        processing any text
        :param
            input_file (str): file path/name to a text file containing data sources to parse

        :return:
            source_list (list): each item represents a data source from the input text file
        """

        # CHECK - file has data
        if os.stat(self.data_path).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(self.data_path))
        else:
            source_list = list(filter(None, [row.strip() for row in open(self.data_path).read().split('\n')]))

            # # CHECK - all sources have correct URL
            good_sources = [url for url in source_list if 'purl.obolibrary.org/obo' in url]

            if len(source_list) == len(good_sources):
                self.source_list = source_list
            else:
                raise Exception('ERROR: Not all URLs were formatted properly')


    def url_download(self):
        '''Function takes a string representing a file path/name to a text file as an argument. The function assumes that
    each item in the input file list is an URL to an OWL ontology. For each URL, the referenced ontology is downloaded,
    and used as input to an OWLTools command line argument (
    https://github.com/owlcollab/owltools/wiki/Extract-Properties-Command) that facilitating the downloading of
    ontologies imported by the primary ontology. The function will save the downloaded ontology + imported
    ontologies. The function outputs a list of URLs. The function performs two verification steps, the first to
    ensure that the command line argument passed to OWLTools returned data and the second is that for each URL
    provided as input, a data file is returned

    :param
        input_file (str): a string representing a file path/name to a text file

    :return:
        source_list (list): each item in the list represents an ontology via URL

    '''

        print('\n')
        print('#' * 100)
        print('Downloading and Processing sources from ' + str(self.data_type))
        print('\n')

        for i in range(0, len(self.source_list)):

            source = self.source_list[i]
            file_prefix = source.split('/')[-1].split('.')[0]
            print('Downloading ' + str(file_prefix))

            # set command line argument
            try:
                subprocess.check_call(['./resources/lib/owltools',
                                       str(source),
                                       '--merge-import-closure',
                                       '-o',
                                       './resources/ontologies/'
                                       + str(file_prefix) + '_with_imports.owl'])
            except subprocess.CalledProcessError as error:
                print(error.output)

            self.data_files.append('./resources/ontologies/' + str(file_prefix) + '_with_imports.owl')

        # CHECK - all URLs returned an data file
        if len(self.source_list) != i+1:
            raise Exception('ERROR: Not all URLs returned a data file')
