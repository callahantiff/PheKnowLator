import unittest

from pkt_kg.utils import *


class TestKGUtils(unittest.TestCase):
    """Class to test knowledge graph utility methods."""

    def setUp(self):

        # set some real and fake file name variables
        self.not_string_filename = ['./data/ontologies/hp_with_imports.owl']
        self.not_real_file_name = './data/ontologies/sop_with_imports.owl'
        self.empty_ontology_file_location = './data/ontologies/hp_with_imports.owl'
        self.good_ontology_file_location = './data/ontologies/so_with_imports.owl'

        return None

    def test_gets_ontology_statistics(self):
        """Tests gets_ontology_statistics method."""

        # test non-string file name
        self.assertRaises(TypeError, gets_ontology_statistics, self.not_string_filename)

        # test fake file name
        self.assertRaises(OSError, gets_ontology_statistics, self.not_real_file_name)

        # test empty file
        self.assertRaises(ValueError, gets_ontology_statistics, self.empty_ontology_file_location)

        # if file is good a FileNotFound error should be raised when attempting to run OWL Tools
        self.assertRaises(FileNotFoundError, gets_ontology_statistics, self.good_ontology_file_location)

        return None
