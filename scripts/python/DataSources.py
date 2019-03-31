##########################################################################################
# DataSources.py
# Purpose: script to download files and store source metadata
# version 2.1.0
# date: 11.12.2017
# Python 3.6.2
##########################################################################################


# import needed libraries
from datetime import *
import requests
import os
import os.path


class DataSource(object):
    """Class creates an object that stores different aspects of data.

    The class takes as input a string that represents the file path/name of a text file that stores 1 to many
    different data sources. Each source is shown as a URL. The class is comprised of several methods that are all
    designed to perform a different function on the data sources. In the same directory that each type of data is
    downloaded to, a metadata file is created that stores important information on each of the files that is downloaded

        :param
            data_path (str): a file path/name to a text file storing URLs of different sources to download

            data_type (str): type of data sources

            source_list (list): a list of URLs representing the data sources to download/process

            data_files (list): the full file path and name of each downloaded data source

    The class has two subclasses which inherit its methods. Each subclass contains an altered version of the primary
    classes methods that are specialized for that specific data type. The subclasses can be identified in scripts
    with the same name, followed by "_data type".

    """

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
        """Function verifies that the file contains data and then outputs the a list where each item is a line from the
        input text file. Prior to reading in any data the function verifies that the text file is not empty

        :param
            input_file (str): file path/name to a text file containing data sources to parse

        :return:
            source_list (list): each item represents a data source from the input text file

        """

        if os.stat(self.data_path).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(self.data_path))
        else:
            self.source_list = list(filter(None, [row.strip() for row in open(self.data_path).read().split('\n')]))



    def get_source_list(self):
        """Function returns the data sources parsed from input text file"""

        return self.source_list


    def url_download(self):
        """
        Function downloads each source from a list and writes the downloaded file to a directory. Specific details
        for the function are documented in each data-type specific subclass.
        """

        pass

    def get_data_files(self):
        """Function returns the list of data sources with file path"""

        return self.data_files

    def source_metadata(self):
        """Function generates metadata for the data sources that it imports. Metadata includes the date of download,
        date of last modification to the file, the difference in days between last date of modification and current
        download date, file size in bytes, path to file, and URL from which the file was downloaded for each data source

            :param
                data_file (list): each item in the list contains the full path and filename of an input data source

             :return:
                 metadata (list): nested list where first item is today's date and each remaining item is a list that
                 contains metadata information for each source that was downloaded
        """

        self.metadata.append(['#' + str(datetime.utcnow().strftime('%a %b %d %X UTC %Y')) + ' \n'])

        for i in range(0, len(self.data_files)):
            source = self.data_files[i]
        # for i in range(0, len(data_files)):
        #     source = data_files[i]

            # get vars for metadata file
            file_info = requests.head(self.source_list[i])
            # file_info = requests.head(source_list[i])

            if ".owl" in source:
                mod_date = "NONE"
                diff_date = "N/A"

            if ".owl" not in source:
                mod_info = file_info.headers[[x for x in file_info.headers.keys() if 'modified' in x.lower() or
                                              'Last-Modified' in x.lower()][0]]
                mod_date = datetime.strptime(mod_info, '%a, %d %b %Y %X GMT').strftime('%m/%d/%Y')

                # difference in days between download date and last modified date
                diff_date = (datetime.now() - datetime.strptime(mod_date,'%m/%d/%Y')).days

            # add metadata for each source as nested list
            source_metadata = ['DOWNLOAD_URL= %s' % str(self.source_list[i]),
                               'DOWNLOAD_DATE= %s' % str(datetime.now().strftime('%m/%d/%Y')),
                               'FILE_SIZE_IN_BYTES= %s' % str(os.stat(source).st_size),
                               'FILE_AGE_IN_DAYS= %s' % str(diff_date),
                               'DOWNLOADED_FILE= %s' % str(source),
                               'FILE_LAST_MOD_DATE= %s' % str(mod_date)]

            self.metadata.append(source_metadata)


    def get_source_metadata(self):
        """Function returns the list of data source metadata"""

        return self.metadata


    def write_source_metadata(self):
        """Function generates a text file that stores metadata for the data sources that it imports.

        :param
            metadata (list): nested list where first item is today's date and each remaining item is a list that
                 contains metadata information for each source that was downloaded

            data_type (str): the type of data files being processed

        :return:
            writes a text file storing metadata for the imported ontologies and names it

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