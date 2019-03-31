##########################################################################################
# DataSources_data.py
# Purpose: script to download data files and store source metadata - inherits functions from DataSources
# version 1.0.0
# date: 11.12.2017
# Python 3.6.2
##########################################################################################


# import needed libraries
from scripts.python.DataSources import DataSource
import gzip
import io
import requests



class Data(DataSource):

    """Subclass of DataSource, dedicated to processing text data.

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

    def url_download(self):
        """Function takes a string representing a file path/name to a text file as an argument. The function assumes that
    each item in the input file list is a valid URL. For each URL, the referenced data source is downloaded and the
    output is written to a specific directory and named according to the string included in the URL. The function
    provides a verification at the end to ensure that a file was downloaded for each URL in the input text file

    :param
        input_file (str): a string representing a file path/name to a text file

    :return:
        source_list (list): each item in the list represents a data source

    """

        print('\n')
        print('#' * 100)
        print('Downloading and Processing sources from ' + str(self.data_type))
        print('\n')

        for i in range(0, len(self.source_list)):
            source = self.source_list[i]
            file_prefix = source.split('/')[-1].split('.')[0]
            print('Downloading ' + str(file_prefix))

            self.data_files.append('./resources/text_files/' + str(file_prefix) + '.txt')

            ## download source data - verify endpoint and download data
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