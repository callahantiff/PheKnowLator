#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
import glob

from unittest import TestCase

from pkt_kg.downloads import OntData


class TestOntData(TestCase):
    """Class to test functions used when downloading ontology data sources."""

    def setUp(self):

        # initialize OntData instance
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data')
        self.dir_loc = os.path.abspath(dir_loc)
        self.ontologies = OntData(self.dir_loc + '/ontology_source_list.txt', self.dir_loc + '/resource_info.txt')

        # pointer to owltools
        dir_loc2 = os.path.join(current_directory, 'utils/owltools')
        self.owltools_location = os.path.abspath(dir_loc2)

        return None

    def test_initialization_data_path(self):
        """Test class initialization for data_path attribute."""

        resource_data = self.dir_loc + '/resource_info.txt'

        # test if file is type string
        self.assertRaises(TypeError, OntData, list(self.dir_loc + '/ontology_source_list.txt'), resource_data)

        # test if file exists
        self.assertRaises(OSError, OntData, self.dir_loc + '/ontology_sources_lists.txt', resource_data)

        # test if file is empty
        self.assertRaises(TypeError, OntData, self.dir_loc + '/ontology_source_list_empty.txt', resource_data)

        return None

    def test_initialization_resource_data(self):
        """Test class initialization for resource_info attribute."""

        data_path = self.dir_loc + '/ontology_source_list.txt'

        # test if file is type string
        self.assertRaises(TypeError, OntData, data_path, list(self.dir_loc + '/resource_info.txt'))

        # test if file exists
        self.assertRaises(OSError, OntData, data_path, self.dir_loc + '/resource_infos.txt')

        # test if file is empty
        self.assertRaises(TypeError, OntData, data_path, self.dir_loc + '/resource_info_empty.txt')

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

        # load data -- bad file
        self.ontologies.data_path = self.dir_loc + '/ontology_source_list_empty.txt'
        self.assertRaises(TypeError, self.ontologies.parses_resource_file)

        # load data -- good file
        self.ontologies.data_path = self.dir_loc + '/ontology_source_list.txt'
        self.ontologies.parses_resource_file()

        # make sure a dictionary is returned
        self.assertIsInstance(self.ontologies.source_list, dict)

        # check and make sure dictionary value contains an owl or obo ontology
        self.assertTrue(all(x for x in list(self.ontologies.source_list.values()) if 'owl' in x))
        self.assertFalse(any(x for x in list(self.ontologies.source_list.values()) if x.endswith('.com')))

        return None

    def test_downloads_data_from_url(self):
        """Tests downloads_data_from_url method."""

        self.ontologies.parses_resource_file()

        # check path to write ontology data correctly derived
        derived_path = '/'.join(self.ontologies.data_path.split('/')[:-1]) + '/ontologies/'
        self.assertEqual(self.dir_loc + '/ontologies/', derived_path)

        # checks that the file downloads
        self.ontologies.downloads_data_from_url(self.owltools_location)
        self.assertTrue(os.path.exists(derived_path + 'hp_with_imports.owl'))

        return None

    def test_generates_source_metadata(self):
        """Tests whether or not metadata is being generated."""

        self.ontologies.parses_resource_file()
        self.ontologies.downloads_data_from_url(self.owltools_location)

        # generate metadata for downloaded file
        self.ontologies.generates_source_metadata()

        # check that metadata was generated
        self.assertTrue(len(self.ontologies.metadata) == 4)

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

        # check for metadata
        self.assertTrue(os.path.exists(self.dir_loc + '/ontologies/ontology_source_metadata.txt'))

        # clean up environment
        os.remove(self.dir_loc + '/ontologies/hp_with_imports.owl')
        os.remove(self.dir_loc + '/ontologies/ontology_source_metadata.txt')

        return None
