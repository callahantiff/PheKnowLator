import glob
import os
import os.path
from rdflib import Graph
import unittest

from pkt_kg.utils import gets_ontology_statistics, merges_ontologies, ontology_file_formatter, \
    maps_node_ids_to_integers, converts_rdflib_to_networkx


class TestKGUtils(unittest.TestCase):
    """Class to test knowledge graph utility methods."""

    def setUp(self):
        # initialize data location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data/ontologies')
        self.dir_loc = os.path.abspath(dir_loc)

        # set some real and fake file name variables
        self.not_string_filename = [self.dir_loc + '/hp_with_imports.owl']
        self.not_real_file_name = self.dir_loc + '/sop_with_imports.owl'
        self.empty_ontology_file_location = self.dir_loc + '/hp_with_imports.owl'
        self.good_ontology_file_location = self.dir_loc + '/so_with_imports.owl'

        # set-up pointer to ontology repo
        self.ontology_repository = glob.glob(self.dir_loc + '/*.owl')
        self.merged_ontology_file = '/PheKnowLator_MergedOntologies.owl'

        # pointer to owltools
        dir_loc2 = os.path.join(current_directory, 'utils/owltools')
        self.owltools_location = os.path.abspath(dir_loc2)

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

    def test_ontology_file_formatter(self):
        """Tests the ontology_file_formatter method."""

        # set-up input methods
        owltools = self.owltools_location

        # test method handling of bad file types
        # not an owl file
        self.assertRaises(TypeError, ontology_file_formatter, self.dir_loc, '/so_with_imports.txt', owltools)

        # a file that does not exist
        self.assertRaises(IOError, ontology_file_formatter, self.dir_loc, '/sop_with_imports.owl', owltools)

        # an empty file
        self.assertRaises(TypeError, ontology_file_formatter, self.dir_loc, '/hp_with_imports.txt', owltools)

        # make sure method runs on legitimate file
        self.assertTrue(ontology_file_formatter(write_location=self.dir_loc,
                                                full_kg='/so_with_imports.owl',
                                                owltools_location=owltools) is None)

        return None

    def test_maps_node_ids_to_integers(self):
        """Tests the maps_node_ids_to_integers method."""

        # set-up input variables
        graph = Graph()
        graph.parse(self.good_ontology_file_location)

        # run method
        maps_node_ids_to_integers(graph=graph,
                                  write_location=self.dir_loc,
                                  output_triple_integers='/so_with_imports_Triples_Integers.txt',
                                  output_triple_integers_map='/so_with_imports_Triples_Integer_Identifier_Map.json')

        # check that files were created
        self.assertTrue(os.path.exists(self.dir_loc + '/so_with_imports_Triples_Integers.txt'))
        self.assertTrue(os.path.exists(self.dir_loc + '/so_with_imports_Triples_Identifiers.txt'))
        self.assertTrue(os.path.exists(self.dir_loc + '/so_with_imports_Triples_Integer_Identifier_Map.json'))

        # clean up the environment
        os.remove(self.dir_loc + '/so_with_imports_Triples_Integers.txt')
        os.remove(self.dir_loc + '/so_with_imports_Triples_Identifiers.txt')
        os.remove(self.dir_loc + '/so_with_imports_Triples_Integer_Identifier_Map.json')

        return None

    def test_converts_rdflib_to_networkx(self):
        """Tests the converts_rdflib_to_networkx method."""

        converts_rdflib_to_networkx(write_location=self.dir_loc,
                                    full_kg='/so_with_imports.owl',
                                    graph=None)

        # check that files were created
        self.assertTrue(os.path.exists(self.dir_loc + '/so_with_imports_Networkx_MultiDiGraph.gpickle'))

        # clean up the environment
        os.remove(self.dir_loc + '/so_with_imports_Networkx_MultiDiGraph.gpickle')

        return None
