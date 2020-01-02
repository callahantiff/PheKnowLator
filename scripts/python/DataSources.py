#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import datetime
import glob
import os.path

from abc import ABCMeta, abstractmethod
from owlready2 import subprocess
from scripts.python.DataPreparationHelperFunctions import *
from tqdm import tqdm


class DataSource(object):
    """The class takes an input string that contains the file path/name of a text file listing different data sources.
    Each source is shown as a URL.

    The class initiates the downloading of different data sources and generates a metadata file that contains
    important information on each of the files that is downloaded.

    The class has two subclasses which inherit its methods. Each subclass contains an altered version of the primary
    classes methods that are specialized for that specific data type.

    Attributes:
        data_path: A string file path/name to a text file storing URLs of different sources to download.
        data_type: A string specifying the type of data source.
        source_list: A list of URLs containing the data sources to download/process.
        data_files: A list of strings, which contain the full file path/name of each downloaded data source.
        metadata: An empty list that will be used to store metadata information for each downloaded resource.

    """

    __metaclass__ = ABCMeta

    def __init__(self, data_path: str):
        self.data_path: str = data_path
        self.data_type: str = data_path.split('/')[-1].split('.')[0]
        self.source_list: dict = {}
        self.data_files: dict = {}
        self.metadata: list = []

    def parses_resource_file(self):
        """Verifies a file contains data and then outputs a list where each item is a line from the input text file.

        Returns:
            source_list: A dictionary, where the key is the type of data and the value is the file path or url.

        Raises:
            An exception is raised if the input file is empty.

        """

        if os.stat(self.data_path).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(self.data_path))

        else:
            self.source_list = {row.strip().split(',')[0]: row.strip().split(',')[1].strip() for row in open(
                self.data_path).read().split('\n')}

    def downloads_data_from_url(self, download_type: str):
        """Downloads each source from a list and writes the downloaded file to a directory.

        Args:
            download_type: A string that indicates whether or not the ontologies should be downloaded
                with imported ontologies ('imports').

        """

        pass

    def generates_source_metadata(self):
        """Extracts and stores metadata for imported data sources. Metadata includes the date of download,
        date of last modification to the file, the difference in days between last date of modification and current
        download date, file size in bytes, path to file, and URL from which the file was downloaded for each data source

        Returns:
            None.

        """

        print('\n*** Generating Metadata ***\n')
        self.metadata.append(['#' + str(datetime.datetime.utcnow().strftime('%a %b %d %X UTC %Y')) + ' \n'])

        for i in tqdm(self.data_files.keys()):
            source = self.data_files[i]

            # get vars for metadata file
            try:
                file_info = requests.head(self.source_list[i])

                if 'modified' in [x.lower() for x in file_info.headers.keys()]:
                    mod_info = file_info.headers['modified'][0]

                elif 'Last-Modified' in [x.lower() for x in file_info.headers.keys()]:
                    mod_info = file_info.headers['Last-Modified'][0]

                elif 'Date' in [x.lower() for x in file_info.headers.keys()]:
                    mod_info = file_info.headers['Date']

                else:
                    mod_info = datetime.datetime.now().strftime('%a, %d %b %Y %X GMT')

            # for ftp downloads that don't have header info
            except requests.exceptions.InvalidSchema:
                mod_info = datetime.datetime.now().strftime('%a, %d %b %Y %X GMT')

            # reformat date
            mod_date = datetime.datetime.strptime(mod_info, '%a, %d %b %Y %X GMT').strftime('%m/%d/%Y')

            # add metadata for each source as nested list
            source_metadata = ['DOWNLOAD_URL= {}'.format(str(self.source_list[i])),
                               'DOWNLOAD_DATE= {}'.format(str(datetime.datetime.now().strftime('%m/%d/%Y'))),
                               'FILE_SIZE_IN_BYTES= {}'.format(str(os.stat(self.data_files[i]).st_size)),
                               'DOWNLOADED_FILE_LOCATION= {}'.format(str(source)),
                               'FILE_LAST_MOD_DATE= {}'.format(str(mod_date))]

            self.metadata.append(source_metadata)

        return None

    def writes_source_metadata_locally(self):
        """Writes metadata for each imported data source to a text file.

        Returns:
            None

        """

        # generate metadata
        self.generates_source_metadata()

        # open file to write to and specify output location
        write_loc_part1 = str('/'.join(list(self.data_files.values())[1].split('/')[:-1]) + '/')
        write_loc_part2 = str('_'.join(self.data_type.split('_')[:-1]))
        outfile = open(write_loc_part1 + write_loc_part2 + '_metadata.txt', 'w')
        outfile.write(self.metadata[0][0])

        for i in tqdm(range(1, len(self.data_files.keys()) + 1)):
            outfile.write(str(self.metadata[i][0]) + '\n')
            outfile.write(str(self.metadata[i][1]) + '\n')
            outfile.write(str(self.metadata[i][2]) + '\n')
            outfile.write(str(self.metadata[i][3]) + '\n')
            outfile.write(str(self.metadata[i][4]) + '\n')
            outfile.write('\n')

        outfile.close()

        return None

    @abstractmethod
    def gets_data_type(self):
        """"A string representing the type of data being processed."""

        pass


class OntData(DataSource):

    def gets_data_type(self):
        """"A string representing the type of data being processed."""

        return 'Ontology Data'

    def parses_resource_file(self):
        """Parses data from a file and outputs a list where each item is a line from the input text file.

        Returns:
            source_list: A list, where each item represents a data source from the input text file.

        Raises:
            An exception is raised if the file contains data.
            An exception is raised if a URL isn't correctly formatted.

        """

        # CHECK - file has data
        if os.stat(self.data_path).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(self.data_path))

        else:
            source_list = {row.strip().split(',')[0]: row.strip().split(',')[1].strip()
                           for row in open(self.data_path).read().split('\n')}

            # CHECK - verify formatting of urls
            valid_sources = [url for url in source_list.values()
                             if 'purl.obolibrary.org/obo' in url or 'owl' in url]

            if len(source_list) == len(valid_sources):
                self.source_list = source_list

            else:
                raise Exception('ERROR: Not all URLs were formatted properly')

    def downloads_data_from_url(self, download_type: str):
        """Takes a string representing a file path/name to a text file as an argument. The function assumes
        that each item in the input file list is an URL to an OWL/OBO ontology.

        For each URL, the referenced ontology is downloaded, and used as input to an OWLTools command line argument (
        https://github.com/owlcollab/owltools/wiki/Extract-Properties-Command), which facilitates the downloading of
        ontologies that are imported by the primary ontology. The function will save the downloaded ontology + imported
        ontologies.

        Args:
            download_type: A string that is used to indicate whether or not the ontologies should be downloaded
                with imported ontologies ('imports').

        Returns:
            source_list: A list, where each item in the list represents an ontology via URL.

        Raises:
            An exception is raised if any of the URLs passed as command line arguments fails to return data.

        """

        # check data before download
        self.parses_resource_file()

        # set location where to write data
        file_loc = './' + str(self.data_path.split('/')[:-1][0]) + '/ontologies/'
        print('\n ***Downloading: {0} to "{1}" ***\n'.format(self.data_type, file_loc))

        # process data
        for i in tqdm(self.source_list.keys()):
            source = self.source_list[i]
            file_prefix = source.split('/')[-1].split('.')[0]
            write_loc = './resources/ontologies/' + file_prefix

            print('\nDownloading: {}'.format(str(file_prefix)))

            # don't re-download ontologies
            if any(x for x in os.listdir(write_loc.strip(file_prefix)) if re.sub('_with.*.owl', '', x) == file_prefix):
                self.data_files[i] = glob.glob(write_loc.strip(file_prefix) + '*' + file_prefix + '*')[0]

            else:

                if download_type == 'imports' and 'purl' in source:
                    try:
                        subprocess.check_call(['./resources/lib/owltools',
                                               str(source),
                                               '--merge-import-closure',
                                               '-o',
                                               str(write_loc) + '_with_imports.owl'])

                        self.data_files[i] = str(write_loc) + '_with_imports.owl'

                    except subprocess.CalledProcessError as error:
                        print(error.output)

                elif download_type != 'imports' and 'purl' in source:
                    try:
                        subprocess.check_call(['./resources/lib/owltools',
                                               str(source),
                                               '-o',
                                               str(write_loc) + '_without_imports.owl'])

                        self.data_files[i] = str(write_loc) + '_without_imports.owl'

                    except subprocess.CalledProcessError as error:
                        print(error.output)

                else:
                    data_downloader(source, str(write_loc), str(file_prefix) + '_with_imports.owl')
                    self.data_files[i] = str(write_loc) + str(file_prefix) + '_with_imports.owl'

        # CHECK
        if len(self.source_list) != len(self.data_files):
            raise Exception('ERROR: Not all URLs returned a data file')


class Data(DataSource):

    def gets_data_type(self):
        """"A string representing the type of data being processed."""

        return 'Edge Data'

    def downloads_data_from_url(self, download_type: str):
        """Takes a string representing a file path/name to a text file as an argument. The function assumes that
        each item in the input file list is a valid URL.

        Args:
            download_type: A string that is used to indicate whether or not the ontologies should be downloaded
                with imported ontologies ('imports'). Within this subclass, this argument is currently ignored.

        Returns:
            source_list: A list, where each item in the list represents a data source.

        Raises:
            An exception is raised if any URL does point to a valid endpoint containing data.

        """

        # check data before download
        self.parses_resource_file()

        # set location where to write data
        file_loc = './' + str(self.data_path.split('/')[:-1][0]) + '/edge_data/'
        print('\n*** Downloading : {0} to "{1}" ***\n'.format(self.data_type, file_loc))

        for i in tqdm(self.source_list.keys()):
            source = self.source_list[i]
            file_name = re.sub('.gz|.zip|\?.*', '', source.split('/')[-1])
            write_path = './resources/edge_data/'
            print('\nEdge: {edge}'.format(edge=i))

            # if file has already been downloaded, rename it
            if any(x for x in os.listdir(write_path) if '_'.join(x.split('_')[1:]) == file_name):
                self.data_files[i] = write_path + i + '_' + file_name

                try:
                    shutil.copy(glob.glob(write_path + '*' + file_name)[0], write_path + i + '_' + file_name)

                except shutil.SameFileError:
                    pass

            else:
                # download data
                self.data_files[i] = write_path + i + '_' + file_name
                data_downloader(source, write_path, i + '_' + file_name)

        # CHECK
        if len(self.source_list) != len(self.data_files):
            raise Exception('ERROR: Not all URLs returned a data file')
