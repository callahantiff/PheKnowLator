#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
import glob

from unittest import TestCase

from pkt.downloads import OntData


class TestOntData(TestCase):
    """Class to test functions used when downloading ontology data sources."""

    def setUp(self):

        # initialize OntData instance
        self.ontologies = OntData('data/ontology_source_list.txt')

        return None

    def test_gets_data_type(self):
        """Tests class initialization to ensure correct data type is registered."""

        self.assertIsInstance(self.ontologies.gets_data_type(), str)
        self.assertEqual('Ontology Data', self.ontologies.gets_data_type())

        return None

    def test_input_file(self):
        """Tests data file passed to initialize class."""

        self.assertIsInstance(self.ontologies.data_path, str)
        self.assertTrue(os.stat(self.ontologies.data_path).st_size != 0)

        return None

    def test_parses_resource_file(self):
        """Tests parses_resource_file method."""

        # load data
        self.ontologies.parses_resource_file()

        # make sure a dictionary is returned
        self.assertIsInstance(self.ontologies.source_list, dict)

        # check and make sure dictionary value contains an owl or obo ontology
        self.assertTrue(all(x for x in list(self.ontologies.source_list.values()) if 'owl' in x))
        self.assertFalse(any(x for x in list(self.ontologies.source_list.values()) if x.endswith('.com')))

        return None

    def test_downloads_data_from_url(self):
        """Tests downloads_data_from_url method."""

        # check path to write ontology data correctly derived
        derived_path = './' + '/'.join(self.ontologies.data_path.split('/')[:-1]) + '/ontologies/'
        self.assertEqual('./data/ontologies/', derived_path)

        # checks that the file downloads
        self.assertTrue(os.path.exists(derived_path + 'hp_with_imports.owl'))

    def test_generates_source_metadata(self):
        """Tests whether or not metadata is being generated."""

        # set dict for downloaded data
        self.ontologies.source_list = {'phenotype': 'http://purl.obolibrary.org/obo/hp.owl'}
        self.ontologies.data_files = {'phenotype': './data/ontologies/hp_with_imports.owl'}

        # generate metadata for downloaded file
        self.ontologies.generates_source_metadata()

        # check that metadata was generated
        self.assertTrue(len(self.ontologies.metadata) == 2)

        # check that the metadata content is correct
        self.assertEqual(8, len(self.ontologies.metadata[1]))
        self.assertTrue('EDGE' in self.ontologies.metadata[1][0])
        self.assertTrue('DATA PROCESSING INFO' in self.ontologies.metadata[1][1])
        self.assertTrue('FILTERING CRITERIA' in self.ontologies.metadata[1][2])
        self.assertTrue('EVIDENCE CRITERIA' in self.ontologies.metadata[1][3])
        self.assertTrue('DATA INFO' in self.ontologies.metadata[1][4])
        self.assertTrue('DOWNLOAD_DATE' in self.ontologies.metadata[1][5])
        self.assertTrue('FILE_SIZE_IN_BYTES' in self.ontologies.metadata[1][6])
        self.assertTrue('DOWNLOADED_FILE_LOCATION' in self.ontologies.metadata[1][7])
