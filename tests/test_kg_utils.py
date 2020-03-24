import glob
import os.path
import unittest

from pkt_kg.utils import gets_ontology_statistics, merges_ontologies


class TestKGUtils(unittest.TestCase):
    """Class to test knowledge graph utility methods."""

    def setUp(self):

        # initialize data location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data/ontologies')
        self.dir_loc = os.path.abspath(dir_loc)

        # path to owltools
        self.owltools_location = os.path.relpath('PheKnowLator/pkt_kg/libs/owltools', self.dir_loc)

        # set some real and fake file name variables
        self.not_string_filename = [self.dir_loc + '/hp_with_imports.owl']
        self.not_real_file_name = self.dir_loc + '/sop_with_imports.owl'
        self.empty_ontology_file_location = self.dir_loc + '/hp_with_imports.owl'
        self.good_ontology_file_location = self.dir_loc + '/so_with_imports.owl'

        # set-up pointer to ontology repo
        self.ontology_repository = glob.glob(self.dir_loc + '/*.owl')
        self.merged_ontology_file = '/PheKnowLator_MergedOntologies.owl'

        return None

    def test_gets_ontology_statistics(self):
        """Tests gets_ontology_statistics method."""

        # test non-string file name
        self.assertRaises(TypeError, gets_ontology_statistics, self.not_string_filename)

        # test fake file name
        self.assertRaises(OSError, gets_ontology_statistics, self.not_real_file_name)

        # test empty file
        self.assertRaises(ValueError, gets_ontology_statistics, self.empty_ontology_file_location)

        # test good file
        self.assertIsNone(gets_ontology_statistics(self.good_ontology_file_location, self.owltools_location))

        return None

    def test_merges_ontologies(self):
        """Tests the merges_ontologies method."""

        # make sure that there is no merged ontology file in write location
        self.assertFalse(os.path.exists(self.dir_loc + self.merged_ontology_file))

        # run merge function and check that file was generated
        merges_ontologies(self.ontology_repository, self.dir_loc, self.merged_ontology_file, self.owltools_location)
        self.assertTrue(os.path.exists(self.dir_loc + self.merged_ontology_file))

        # remove file
        os.remove(self.dir_loc + self.merged_ontology_file)

        return None
