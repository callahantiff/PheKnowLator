##########################################################################################
# DataSources.py
# Purpose: script to download files and store source metadata
##########################################################################################


import datetime
import gzip
import io
import os.path
import requests
# import subprocess

from abc import ABCMeta, abstractmethod
from owlready2 import subprocess


class DataSource(object):
    """The class takes as input a string that represents the file path/name of a text file that stores 1 to many
    different data sources. Each source is shown as a URL. The class is comprised of several methods that are all
    designed to perform a different function on the data sources. In the same directory that each type of data is
    downloaded to, a metadata file is created that stores important information on each of the files that is
    downloaded. The class has two subclasses which inherit its methods. Each subclass contains an altered version of
    the primary classes methods that are specialized for that specific data type.

    Attributes:
        data_path: a string file path/name to a text file storing URLs of different sources to download.
        data_type: a string specifying the type of data source.
        source_list: a list of URLs containing the data sources to download/process.
        data_files: a list of strings, which contain the full file path/name of each downloaded data source.

    """

    __metaclass__ = ABCMeta

    def __init__(self, data_path):
        self.data_path = data_path
        self.data_type = data_path.split('/')[-1].split('.')[0]
        self.source_list = []
        self.data_files = []
        self.metadata = []

    def get_data_type(self):
        """Function returns the input value for data type"""
        return self.data_type

    def file_parser(self):
        """Verifies a file contains data and then outputs a list where each item is a line from the
        input text file.

        Returns:
            source_list: a list, where each item represents a data source from the input text file.

        Raises:
            If input file is empty an exception is raised.

        """

        if os.stat(self.data_path).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(self.data_path))
        else:
            self.source_list = list(filter(None, [row.strip() for row in open(self.data_path).read().split('\n')]))

    def get_source_list(self):
        """Gets the data sources parsed from input text file"""
        return self.source_list

    def url_download(self, download_type):
        """Downloads each source from a list and writes the downloaded file to a directory.

        Args:
            download_type: A string that is used to indicate whether or not the ontologies should be downloaded with
            imported ontologies ('imports').

        """
        pass

    def get_data_files(self):
        """Gets the list of data sources with file path"""
        return self.data_files

    def source_metadata(self):
        """Extracts and stores metadata for imported data sources. Metadata includes the date of download,
        date of last modification to the file, the difference in days between last date of modification and current
        download date, file size in bytes, path to file, and URL from which the file was downloaded for each data source

        Returns:
            metadata: a nested list, where first item is today's date and each remaining item is a list that
            contains metadata information for each downloaded data source.
        """

        self.metadata.append(['#' + str(datetime.datetime.utcnow().strftime('%a %b %d %X UTC %Y')) + ' \n'])

        for i in range(0, len(self.data_files)):
            source = self.data_files[i]

            # get vars for metadata file
            file_info = requests.head(self.source_list[i])

            if 'modified' in [x.lower() for x in file_info.headers.keys()]:
                mod_info = file_info.headers['modified'][0]
            elif 'Last-Modified' in [x.lower() for x in file_info.headers.keys()]:
                mod_info = file_info.headers['Last-Modified'][0]
            elif 'Date' in [x.lower() for x in file_info.headers.keys()]:
                mod_info = file_info.headers['Date']
            else:
                mod_info = datetime.datetime.now().strftime('%a, %d %b %Y %X GMT')

            # reformat date
            mod_date = datetime.datetime.strptime(mod_info, '%a, %d %b %Y %X GMT').strftime('%m/%d/%Y')

            # difference in days between download date and last modified date
            diff_date = (datetime.datetime.now() - datetime.datetime.strptime(mod_date, '%m/%d/%Y')).days

            # add metadata for each source as nested list
            source_metadata = ['DOWNLOAD_URL= {}'.format(str(self.source_list[i])),
                               'DOWNLOAD_DATE= {}'.format(str(datetime.datetime.now().strftime('%m/%d/%Y'))),
                               'FILE_SIZE_IN_BYTES= {}'.format(str(os.stat(source).st_size)),
                               'FILE_AGE_IN_DAYS= {}'.format(str(diff_date)),
                               'DOWNLOADED_FILE_LOCATION= {}'.format(str(source)),
                               'FILE_LAST_MOD_DATE= {}'.format(str(mod_date))]

            self.metadata.append(source_metadata)

    def get_source_metadata(self):
        """Prints a list of data source metadata"""
        return self.metadata

    def write_source_metadata(self):
        """Generates a text file that stores metadata for the data sources that it imports.

        Returns:
            Writes a text file storing metadata for the imported ontologies and names it.

        """

        # open file to write to and specify output location
        write_location = str('/'.join(self.data_files[0].split('/')[:-1]) + '/') + str('_'.join(self.data_type.split(
            '_')[:-1])) + '_metadata.txt'
        outfile = open(write_location, 'w')
        outfile.write(self.metadata[0][0])
        print('Writing ' + str(self.data_type) + ' metadata \n')

        for i in range(1, len(self.metadata)):
            source = self.metadata[i]

            # write file
            outfile.write(str(source[0]) + '\n')
            outfile.write(str(source[1]) + '\n')
            outfile.write(str(source[2]) + '\n')
            outfile.write(str(source[3]) + '\n')
            outfile.write(str(source[4]) + '\n')
            outfile.write(str(source[5]) + '\n')
            outfile.write('\n')
        outfile.close()

        return None

    @abstractmethod
    def data_type(self):
        """"A string representing the type of data being processed."""
        pass


class OntData(DataSource):

    def data_type(self):
        """"A string representing the type of data being processed."""
        return 'Ontology Data'

    def file_parser(self):
        """Verifies that the file contains data and then outputs a list where each item is a line from the
        input text file. A check is performed before processing any data to ensure that the data file is
        not empty. The function also verifies the URL for each ontology to ensure that they are OBO/OWL ontologies
        before processing any text.

        Returns:
            source_list: A list, where each item represents a data source from the input text file.

        Raises:
            An exception is raised if the file contains data.
            An exception is raised if a URL does point to a source containing data.
        """

        # CHECK - file has data
        if os.stat(self.data_path).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(self.data_path))
        else:
            source_list = list(filter(None, [row.strip() for row in open(self.data_path).read().split('\n')]))

            # CHECK - all sources have correct URL
            valid_sources = [url for url in source_list if 'purl.obolibrary.org/obo' in url]

            if len(source_list) == len(valid_sources):
                self.source_list = source_list
            else:
                raise Exception('ERROR: Not all URLs were formatted properly')

    def url_download(self, download_type):
        """Takes a string representing a file path/name to a text file as an argument. The function assumes
        that each item in the input file list is an URL to an OWL/OBO ontology. For each URL, the referenced ontology is
        downloaded, and used as input to an OWLTools command line argument (
        https://github.com/owlcollab/owltools/wiki/Extract-Properties-Command), which facilitates the download and
        saving of ontologies imported by the primary ontology. The function will save the downloaded ontology + imported
        ontologies.

        Args:
            download_type: A string that is used to indicate whether or not the ontologies should be downloaded with
            imported ontologies ('imports').

        Returns:
            source_list: A list, where each item in the list represents an ontology via URL.

        Raises:
            An exception is raised if any of the URLs passed as command line arguments fails to return data.

        """

        file_loc = './resources/ontologies/'
        print('\n' + '#' * 100 + '\n Downloading Ontology Data: {0} to {1}'.format(self.data_type, file_loc) + '\n')

        i = 0
        for i in range(0, len(self.source_list)):
            source = self.source_list[i]
            file_prefix = source.split('/')[-1].split('.')[0]
            print('Downloading: {}'.format(str(file_prefix)))

            if download_type == 'imports':

                try:
                    subprocess.check_call(['./resources/lib/owltools',
                                           str(source),
                                           '--merge-import-closure',
                                           '-o',
                                           './resources/ontologies/'
                                           + str(file_prefix) + '_with_imports.owl'])

                    self.data_files.append('./resources/ontologies/' + str(file_prefix) + '_with_imports.owl')

                except subprocess.CalledProcessError as error:
                    print(error.output)
            else:
                try:
                    subprocess.check_call(['./resources/lib/owltools',
                                           str(source),
                                           '-o',
                                           './resources/ontologies/'
                                           + str(file_prefix) + '_without_imports.owl'])

                except subprocess.CalledProcessError as error:
                    print(error.output)

        # CHECK - all URLs returned a data file
        if len(self.source_list) != i + 1:
            raise Exception('ERROR: Not all URLs returned a data file')


class Data(DataSource):

    def data_type(self):
        """"A string representing the type of data being processed."""
        return 'Instance Data'

    def url_download(self, download_type):
        """Takes a string representing a file path/name to a text file as an argument. The function assumes that
    each item in the input file list is a valid URL. For each URL, the referenced data source is downloaded and the
    output is written to a specific directory and named according to the string included in the URL.

    Args:
        download_type: A string that is used to indicate whether or not the ontologies should be downloaded with
        imported ontologies ('imports'). Within this subclass, this argument is currently ignored.

    Returns:
        source_list: A list, where each item in the list represents a data source.

    Raises:
        An exception is raised if any URL does point to a valid endpoint containing data.

    """

        file_loc = './resources/text_files/'
        print('\n' + '#' * 100 + '\n Downloading Instance Data: {0} to {1}'.format(self.data_type, file_loc) + '\n')

        i = 0
        for i in range(0, len(self.source_list)):
            source = self.source_list[i]
            file_prefix = source.split('/')[-1].split('.')[0]
            print('Downloading ' + str(file_prefix))

            self.data_files.append('./resources/text_files/' + str(file_prefix) + '.txt')

            # verify endpoint and download data -- checks whether downloaded data is compressed
            if '.gz' in source:
                response = requests.get(source)
                content = gzip.GzipFile(fileobj=io.BytesIO(response.content)).read()
            else:
                response = requests.get(source)
                content = io.BytesIO(response.content).read()

            # write downloaded file to directory
            filename = './resources/text_files/' + str(file_prefix) + '.txt'
            file = open(filename, 'w')
            file.write(str(content))
            file.close()

        # CHECK - all URLs returned an data file
        if len(self.source_list) != i+1:
            raise Exception('ERROR: Not all URLs returned a data file')
