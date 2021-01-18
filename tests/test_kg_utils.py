import glob
import networkx as nx
import os
import os.path
import unittest

from typing import Dict, List, Set
from rdflib import BNode, Graph, Literal, URIRef

from pkt_kg.utils import gets_ontology_statistics, merges_ontologies, ontology_file_formatter, \
    maps_node_ids_to_integers, adds_edges_to_graph, remove_edges_from_graph, finds_node_type, updates_graph_namespace, \
    converts_rdflib_to_networkx, gets_ontology_classes, gets_deprecated_ontology_classes, gets_object_properties, \
    gets_ontology_class_dbxrefs, gets_ontology_class_synonyms, gets_class_ancestors


class TestKGUtils(unittest.TestCase):
    """Class to test knowledge graph utility methods."""

    def setUp(self):
        # initialize data location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data/ontologies')
        self.dir_loc = os.path.abspath(dir_loc)

        # set some real and fake file name variables
        self.not_string_filename = [self.dir_loc + '/empty_hp_with_imports.owl']
        self.not_real_file_name = self.dir_loc + '/sop_with_imports.owl'
        self.empty_ontology_file_location = self.dir_loc + '/empty_hp_with_imports.owl'
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
        self.assertRaises(TypeError, ontology_file_formatter, self.dir_loc, '/empty_hp_with_imports.txt', owltools)

        # make sure method runs on legitimate file
        self.assertTrue(ontology_file_formatter(write_location=self.dir_loc,
                                                full_kg='/so_with_imports.owl',
                                                owltools_location=owltools) is None)

        return None

    def test_adds_edges_to_graph(self):
        """Tests the adds_edges_to_graph method"""

        # set input variables
        edge_list = [(BNode('01a910b4-09fc-4d06-8951-3bc278eeaca9'),
                      URIRef('http://www.w3.org/2002/07/owl#onProperty'),
                      URIRef('http://purl.obolibrary.org/obo/RO_0002435'))]

        # set-up graph
        graph = Graph()
        graph.parse(self.good_ontology_file_location)
        initial_graph_len = len(graph)

        # test method
        graph = adds_edges_to_graph(graph, edge_list)

        # make sure edges were added
        self.assertTrue(initial_graph_len <= len(graph))

        return None

    def test_remove_edges_from_graph(self):
        """Tests the removes_edges_from_graph method"""

        # set-up graph
        graph = Graph()
        graph.parse(self.good_ontology_file_location)
        initial_graph_len = len(graph)

        # get edges to remove
        remove_edges = list(graph)[0:5]

        # test method
        remove_edges_from_graph(graph, remove_edges)

        # make sure edges were removed
        self.assertTrue(initial_graph_len >= len(graph))

        return None

    def test_finds_node_type(self):
        """Tests the finds_node_type method."""

        # test condition for subclass-subclass
        edge_info1 = {'n1': 'subclass', 'n2': 'subclass', 'edges': ['2', '3124'],
                      'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'https://www.ncbi.nlm.nih.gov/gene/']}

        map_vals1 = finds_node_type(edge_info1)

        self.assertEqual({'cls1': None,
                          'cls2': None,
                          'ent1': 'https://www.ncbi.nlm.nih.gov/gene/2',
                          'ent2': 'https://www.ncbi.nlm.nih.gov/gene/3124'},
                         map_vals1)

        # test condition for instance-instance
        edge_info2 = {'n1': 'instance', 'n2': 'instance', 'edges': ['2', '3124'],
                      'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'https://www.ncbi.nlm.nih.gov/gene/']}

        map_vals2 = finds_node_type(edge_info2)

        self.assertEqual({'cls1': None,
                          'cls2': None,
                          'ent1': 'https://www.ncbi.nlm.nih.gov/gene/2',
                          'ent2': 'https://www.ncbi.nlm.nih.gov/gene/3124'},
                         map_vals2)

        # test condition for class-subclass
        edge_info3 = {'n1': 'subclass', 'n2': 'class', 'edges': ['2', 'DOID_0110035'],
                      'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/']}

        map_vals3 = finds_node_type(edge_info3)

        self.assertEqual({'cls1': 'http://purl.obolibrary.org/obo/DOID_0110035',
                          'cls2': None,
                          'ent1': 'https://www.ncbi.nlm.nih.gov/gene/2',
                          'ent2': None},
                         map_vals3)

        # test condition for subclass-class
        edge_info4 = {'n1': 'class', 'n2': 'subclass', 'edges': ['DOID_0110035', '2'],
                      'uri': ['http://purl.obolibrary.org/obo/', 'https://www.ncbi.nlm.nih.gov/gene/']}

        map_vals4 = finds_node_type(edge_info4)

        self.assertEqual({'cls1': 'http://purl.obolibrary.org/obo/DOID_0110035',
                          'cls2': None,
                          'ent1': 'https://www.ncbi.nlm.nih.gov/gene/2',
                          'ent2': None},
                         map_vals4)

        # test condition for class-class
        edge_info5 = {'n1': 'class', 'n2': 'class', 'edges': ['DOID_162', 'DOID_0110035'],
                      'uri': ['http://purl.obolibrary.org/obo/', 'http://purl.obolibrary.org/obo/']}

        map_vals5 = finds_node_type(edge_info5)

        self.assertEqual({'cls1': 'http://purl.obolibrary.org/obo/DOID_162',
                          'cls2': 'http://purl.obolibrary.org/obo/DOID_0110035',
                          'ent1': None,
                          'ent2': None},
                         map_vals5)

        return None

    def test_updates_graph_namespace(self):
        """tests the updates_graph_namespace method."""

        # test method
        graph = updates_graph_namespace('phenotype', Graph(), 'http://purl.obolibrary.org/obo/HP_0100443')
        test_edge = (URIRef('http://purl.obolibrary.org/obo/HP_0100443'),
                     URIRef('http://www.geneontology.org/formats/oboInOwl#hasOBONamespace'),
                     Literal('phenotype'))
        self.assertIsInstance(graph, Graph)
        self.assertTrue(len(graph) == 1)
        self.assertIn(test_edge, graph)

        return None

    def test_maps_node_ids_to_integers(self):
        """Tests the maps_node_ids_to_integers method."""

        # set-up input variables
        graph = Graph().parse(self.good_ontology_file_location)

        # run method
        mapped_dict = maps_node_ids_to_integers(graph=graph,
                                                write_location=self.dir_loc,
                                                output_ints='/so_with_imports_Triples_Integers.txt',
                                                output_ints_map='/so_with_imports_Triples_Integer_Identifier_Map.json')

        # check that a dictionary is returned
        self.assertIsInstance(mapped_dict, Dict)

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

        # check that files were created
        converts_rdflib_to_networkx(write_location=self.dir_loc, full_kg='/so_with_imports', graph=None)
        self.assertTrue(os.path.exists(self.dir_loc + '/so_with_imports_NetworkxMultiDiGraph.gpickle'))

        # load graph and check structure
        s = URIRef('http://purl.obolibrary.org/obo/SO_0000288')
        o = URIRef('http://purl.obolibrary.org/obo/SO_0000287')
        p = URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf')
        graph = nx.read_gpickle(self.dir_loc + '/so_with_imports_NetworkxMultiDiGraph.gpickle')
        self.assertEqual(graph[s][o][p], {'predicate_key': '9cbd482627d217b38eb407d7eba48020', 'weight': 0.0})

        # clean up the environment
        os.remove(self.dir_loc + '/so_with_imports_NetworkxMultiDiGraph.gpickle')

        return None

    def test_gets_ontology_classes(self):
        """Tests the gets_ontology_classes method."""

        # read in ontology
        graph = Graph().parse(self.good_ontology_file_location)

        # retrieve classes form graph with data
        classes = gets_ontology_classes(graph)

        self.assertIsInstance(classes, Set)
        self.assertEqual(2573, len(classes))

        # retrieve classes form graph with no data
        no_data_graph = Graph()
        self.assertRaises(ValueError, gets_ontology_classes, no_data_graph)

        return None

    def test_gets_deprecated_ontology_classes(self):
        """Tests the gets_deprecated_ontology_classes method."""

        # read in ontology
        graph = Graph().parse(self.good_ontology_file_location)

        # retrieve classes form graph with data
        classes = gets_deprecated_ontology_classes(graph)

        self.assertIsInstance(classes, Set)
        self.assertEqual(336, len(classes))

        return None

    def test_gets_object_properties(self):
        """Tests the gets_object_properties method."""

        # read in ontology
        graph = Graph().parse(self.good_ontology_file_location)

        # retrieve object properties form graph with data
        object_properties = gets_object_properties(graph)

        self.assertIsInstance(object_properties, Set)
        self.assertEqual(50, len(object_properties))

        # retrieve object properties form graph with no data
        no_data_graph = Graph()
        self.assertRaises(ValueError, gets_object_properties, no_data_graph)

        return None

    def test_gets_ontology_class_synonyms(self):
        """Tests the  gets_ontology_class_synonyms method."""

        # read in ontology
        graph = Graph().parse(self.good_ontology_file_location)

        # retrieve object properties form graph with data
        synonym_dict, synonym_type_dict = gets_ontology_class_synonyms(graph)

        self.assertIsInstance(synonym_dict, Dict)
        self.assertIsInstance(synonym_type_dict, Dict)
        self.assertEqual(4056, len(synonym_dict))
        self.assertEqual(4056, len(synonym_type_dict))

        return None

    def test_gets_ontology_class_dbxrefs(self):
        """Tests the  gets_ontology_class_synonyms method."""

        # read in ontology
        graph = Graph().parse(self.good_ontology_file_location)

        # retrieve object properties form graph with data
        dbxref_dict, dbxref_type_dict = gets_ontology_class_dbxrefs(graph)

        self.assertIsInstance(dbxref_dict, Dict)
        self.assertIsInstance(dbxref_type_dict, Dict)
        self.assertEqual(393, len(dbxref_dict))
        self.assertEqual(393, len(dbxref_type_dict))

        return None

    def test_finds_class_ancestors(self):
        """Tests the finds_class_ancestors method."""

        # load ontology
        graph = Graph().parse(self.good_ontology_file_location, format='xml')
        so_class = [URIRef('http://purl.obolibrary.org/obo/SO_0000348')]

        # get ancestors when a valid class is provided -- class is URIRef
        ancestors1 = gets_class_ancestors(graph, so_class, set(so_class))
        self.assertIsInstance(ancestors1, Set)
        self.assertEqual(sorted(list(ancestors1)),
                         sorted(list({'http://purl.obolibrary.org/obo/SO_0000348',
                                      'http://purl.obolibrary.org/obo/SO_0000400',
                                      'http://purl.obolibrary.org/obo/SO_0000443'})))

        # get ancestors when a valid class is provided -- class is not URIRef
        class_uri = [str(x).split('/')[-1] for x in so_class]
        ancestors1 = gets_class_ancestors(graph, class_uri, set(class_uri))
        self.assertIsInstance(ancestors1, Set)
        self.assertEqual(sorted(list(ancestors1)),
                         sorted(list({'http://purl.obolibrary.org/obo/SO_0000348',
                                      'http://purl.obolibrary.org/obo/SO_0000400',
                                      'http://purl.obolibrary.org/obo/SO_0000443'})))

        # get ancestors when no class is provided
        ancestors2 = gets_class_ancestors(graph, set())
        self.assertIsInstance(ancestors2, Set)
        self.assertEqual(ancestors2, set())

        return None
