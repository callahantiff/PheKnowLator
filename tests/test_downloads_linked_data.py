#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
import glob

from unittest import TestCase

from pkt_kg.downloads import LinkedData


class TestLinkedData(TestCase):
    """Class to test functions used when downloading linked data sources."""

    def setUp(self):

        # initialize OntData instance
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data')
        self.dir_loc = os.path.abspath(dir_loc)
        self.data = LinkedData(self.dir_loc + '/class_source_list.txt')

        return None

    def test_gets_data_type(self):
        """Tests class initialization to ensure correct data type is registered."""

        self.assertTrue(isinstance(self.data.gets_data_type(), str))
        self.assertEqual('Edge Data', self.data.gets_data_type())

        return None

    def test_input_file(self):
        """Tests data file passed to initialize class."""

        self.assertTrue(isinstance(self.data.data_path, str))
        self.assertTrue(os.stat(self.data.data_path).st_size != 0)

        return None

    def test_parses_resource_file(self):
        """Tests parses_resource_file method."""

        # load data
        self.data.parses_resource_file()

        # make sure a dictionary is returned
        self.assertTrue(isinstance(self.data.source_list, dict))

        return None

    def test_downloads_data_from_url(self):
        """Tests downloads_data_from_url method."""

        # check path to write linked data correctly derived
        derived_path = '/'.join(self.data.data_path.split('/')[:-1]) + '/edge_data/'
        self.assertEqual(self.dir_loc + '/edge_data/', derived_path)

        # checks that the file downloads
        self.assertTrue(os.path.exists(derived_path + 'chemical-disease_CTD_chemicals_diseases.tsv'))

        return None

    def test_generates_source_metadata(self):
        """Tests whether or not metadata is being generated."""

        # set dict for downloaded data
        self.data.source_list = {'chemical-disease': 'http://ctdbase.org/reports/CTD_chemicals_diseases.tsv.gz'}
        self.data.data_files = {'chemical-disease': './data/edge_data/chemical-disease_CTD_chemicals_diseases.tsv'}

        # generate metadata for downloaded file
        self.data.generates_source_metadata()

        # check that metadata was generated
        self.assertTrue(len(self.data.metadata) == 2)

        # check that the metadata content is correct
        self.assertEqual(8, len(self.data.metadata[1]))
        self.assertTrue('EDGE' in self.data.metadata[1][0])
        self.assertTrue('DATA PROCESSING INFO' in self.data.metadata[1][1])
        self.assertTrue('FILTERING CRITERIA' in self.data.metadata[1][2])
        self.assertTrue('EVIDENCE CRITERIA' in self.data.metadata[1][3])
        self.assertTrue('DATA INFO' in self.data.metadata[1][4])
        self.assertTrue('DOWNLOAD_DATE' in self.data.metadata[1][5])
        self.assertTrue('FILE_SIZE_IN_BYTES' in self.data.metadata[1][6])
        self.assertTrue('DOWNLOADED_FILE_LOCATION' in self.data.metadata[1][7])

        return None
