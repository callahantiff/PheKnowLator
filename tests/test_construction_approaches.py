import glob
import json
import os
import os.path
import pandas
import pickle
import shutil
import unittest

from rdflib import Graph, URIRef, BNode
from rdflib.namespace import OWL, RDF
from typing import Dict, List

from pkt_kg.construction_approaches import KGConstructionApproach
from pkt_kg.utils import adds_edges_to_graph


class TestKGConstructionApproach(unittest.TestCase):
    """Class to test the KGBuilder class from the knowledge graph script."""

    def setUp(self):
        # initialize file location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data')
        self.dir_loc = os.path.abspath(dir_loc)

        # set-up environment - make temp directory
        dir_loc_resources = os.path.join(current_directory, 'data/resources')
        self.dir_loc_resources = os.path.abspath(dir_loc_resources)
        os.mkdir(self.dir_loc_resources)
        os.mkdir(self.dir_loc_resources + '/construction_approach')

        # copy data
        # empty master edges
        shutil.copyfile(self.dir_loc + '/Master_Edge_List_Dict_empty.json',
                        self.dir_loc_resources + '/Master_Edge_List_Dict_empty.json')

        # empty subclass dict file
        shutil.copyfile(self.dir_loc + '/subclass_construction_map_empty.pkl',
                        self.dir_loc_resources + '/construction_approach/subclass_construction_map_empty.pkl')

        # create edge list
        self.edge_dict = {"gene-phenotype": {"data_type": "subclass-class",
                                             "edge_relation": "RO_0003302",
                                             "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                     "http://purl.obolibrary.org/obo/"],
                                             "edge_list": [["2", "HP_0002511"], ["2", "HP_0000716"],
                                                           ["2", "HP_0000100"], ["9", "HP_0030955"],
                                                           ["9", "HP_0009725"], ["9", "HP_0100787"],
                                                           ["9", "HP_0012125"], ["10", "HP_0009725"],
                                                           ["10", "HP_0010301"], ["10", "HP_0045005"]]},
                          "gene-gene": {"data_type": "subclass-subclass",
                                        "edge_relation": "RO_0002435",
                                        "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                "https://www.ncbi.nlm.nih.gov/gene/"],
                                        "edge_list": [["3075", "1080"], ["3075", "4267"], ["4800", "10190"],
                                                      ["4800", "80219"], ["2729", "1962"], ["2729", "5096"],
                                                      ["8837", "6774"], ["8837", "8754"]]},
                          "disease-disease": {"data_type": "class-class",
                                              "edge_relation": "RO_0002435",
                                              "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                      "https://www.ncbi.nlm.nih.gov/gene/"],
                                              "edge_list": [["DOID_3075", "DOID_1080"], ["DOID_3075", "DOID_4267"],
                                                            ["DOID_4800", "DOID_10190"], ["DOID_4800", "DOID_80219"],
                                                            ["DOID_2729", "DOID_1962"], ["DOID_2729", "DOID_5096"],
                                                            ["DOID_8837", "DOID_6774"], ["DOID_8837", "DOID_8754"]]}
                          }

        self.edge_dict_inst = {"gene-phenotype": {"data_type": "instance-class",
                                                  "edge_relation": "RO_0003302",
                                                  "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                          "http://purl.obolibrary.org/obo/"],
                                                  "edge_list": [["2", "HP_0002511"], ["2", "HP_0000716"],
                                                                ["2", "HP_0000100"], ["9", "HP_0030955"],
                                                                ["9", "HP_0009725"], ["9", "HP_0100787"],
                                                                ["9", "HP_0012125"], ["10", "HP_0009725"],
                                                                ["10", "HP_0010301"], ["10", "HP_0045005"]]},
                               "gene-gene": {"data_type": "instance-instance",
                                             "edge_relation": "RO_0002435",
                                             "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                     "https://www.ncbi.nlm.nih.gov/gene/"],
                                             "edge_list": [["3075", "1080"], ["3075", "4267"], ["4800", "10190"],
                                                           ["4800", "80219"], ["2729", "1962"], ["2729", "5096"],
                                                           ["8837", "6774"], ["8837", "8754"]]},
                               "disease-disease": {"data_type": "class-class",
                                                   "edge_relation": "RO_0002435",
                                                   "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                           "https://www.ncbi.nlm.nih.gov/gene/"],
                                                   "edge_list": [["DOID_3075", "DOID_1080"], ["DOID_3075", "DOID_4267"],
                                                                 ["DOID_4800", "DOID_10190"],
                                                                 ["DOID_4800", "DOID_80219"],
                                                                 ["DOID_2729", "DOID_1962"], ["DOID_2729", "DOID_5096"],
                                                                 ["DOID_8837", "DOID_6774"],
                                                                 ["DOID_8837", "DOID_8754"]]}
                               }

        # create subclass mapping data
        subcls_map = {"2": ['SO_0001217'], "9": ['SO_0001217'], "10": ['SO_0001217'], "1080": ['SO_0001217'],
                      "1962": ['SO_0001217'], "2729": ['SO_0001217'], "3075": ['SO_0001217'], "4267": ['SO_0001217'],
                      "4800": ['SO_0001217'], "5096": ['SO_0001217'], "6774": ['SO_0001217'], "8754": ['SO_0001217'],
                      "8837": ['SO_0001217'], "10190": ['SO_0001217'], "80219": ['SO_0001217']}

        # save data
        with open(self.dir_loc_resources + '/construction_approach/subclass_construction_map.pkl', 'wb') as f:
            pickle.dump(subcls_map, f, protocol=4)

        # instantiate class
        self.kg_builder = KGConstructionApproach(self.edge_dict, self.dir_loc_resources)

        return None

    def test_class_initialization_parameters_edge_dict(self):
        """Tests the class initialization parameters for edge_dict."""

        # if input edge_dict is not type dict
        self.assertRaises(TypeError, KGConstructionApproach, list(), self.dir_loc_resources)

        # if input edge_dict is empty
        self.assertRaises(TypeError, KGConstructionApproach, dict(), self.dir_loc_resources)

        return None

    def test_class_initialization_parameters_write_location(self):
        """Tests the class initialization parameters for write location."""

        self.assertRaises(ValueError, KGConstructionApproach, self.edge_dict, None)

        return None

    def test_class_initialization_parameters_subclass_dict(self):
        """Tests the class initialization parameters for subclass_dict."""

        # test when path does not exist
        self.assertRaises(OSError, KGConstructionApproach, self.edge_dict, self.dir_loc)

        # test when the dict is empty
        os.remove(self.dir_loc_resources + '/construction_approach/subclass_construction_map.pkl')
        self.assertRaises(TypeError, KGConstructionApproach, self.edge_dict, self.dir_loc_resources)

        # clean up environment
        os.remove(self.dir_loc_resources + '/construction_approach/subclass_construction_map_empty.pkl')

        return None

    def test_class_initialization(self):
        """Tests the class initialization."""

        # check write_location
        self.assertIsInstance(self.kg_builder.write_location, str)

        # edge dict
        self.assertIsInstance(self.kg_builder.edge_dict, Dict)
        self.assertTrue(len(self.kg_builder.edge_dict) == 3)

        # subclass dict
        self.assertIsInstance(self.kg_builder.subclass_dict, Dict)
        self.assertTrue(len(self.kg_builder.subclass_dict) == 15)

        # subclass_error dict
        self.assertIsInstance(self.kg_builder.subclass_error, Dict)
        self.assertTrue(len(self.kg_builder.subclass_error) == 0)

        return None

    def test_maps_node_to_class(self):
        """Tests the maps_node_to_class method"""

        # test when entity in subclass_dict
        result = self.kg_builder.maps_node_to_class('gene-phenotype', '2', ['2', 'HP_0002511'])
        self.assertEqual(['SO_0001217'], result)

        # test when entity not in subclass_dict
        # update subclass dict to remove an entry
        del self.kg_builder.subclass_dict['2']
        edge_list_length = len(self.kg_builder.edge_dict['gene-phenotype']['edge_list'])
        result = self.kg_builder.maps_node_to_class('gene-phenotype', '2', ['2', 'HP_0002511'])

        self.assertEqual(None, result)
        self.assertTrue(edge_list_length > len(self.kg_builder.edge_dict['gene-phenotype']['edge_list']))

        return None

    def test_class_edge_constructor_with_inverse(self):
        """Tests the class_edge_constructor method with inverse relations."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)

        # nodes
        node1, node2 = URIRef('https://www.ncbi.nlm.nih.gov/gene/2'), URIRef('https://www.ncbi.nlm.nih.gov/gene/10')

        # relations
        relation = URIRef('http://purl.obolibrary.org/obo/RO_0002435')
        inverse_relation = URIRef('http://purl.obolibrary.org/obo/RO_0002435')

        # add edges
        graph, edges = self.kg_builder.edge_constructor(graph, node1, node2, relation, inverse_relation)

        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 8)
        self.assertTrue(len(graph) > pre_length)

        return None

    def test_class_edge_constructor_without_inverse(self):
        """Tests the class_edge_constructor method without inverse relations."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)

        # nodes
        node1, node2 = URIRef('https://www.ncbi.nlm.nih.gov/gene/2'), URIRef('https://www.ncbi.nlm.nih.gov/gene/10')

        # relations
        relation = URIRef('http://purl.obolibrary.org/obo/RO_0002435')
        inverse_relation = None

        # add edges
        graph, edges = self.kg_builder.edge_constructor(graph, node1, node2, relation, inverse_relation)

        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 4)
        self.assertTrue(len(graph) > pre_length)

        return None

    def test_subclass_constructor_bad_map(self):
        """Tests the subclass_constructor method for an edge that contains an identifier that is not included in the
        subclass_map_dict."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)
        del self.kg_builder.subclass_dict['2']

        # edge_info
        edge_info = {'n1': 'subclass', 'n2': 'class', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['2', 'HP_0000716']}

        # test method
        dic, graph, edges = self.kg_builder.subclass_constructor(graph, edge_info, 'gene-phenotype')

        # check returned results
        self.assertIsInstance(dic, Dict)
        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 0)
        self.assertTrue(len(graph) == pre_length)

        # check subclass error log
        self.assertIsInstance(self.kg_builder.subclass_error, Dict)
        self.assertIn('gene-phenotype', self.kg_builder.subclass_error.keys())
        self.assertEqual(self.kg_builder.subclass_error['gene-phenotype'], ['2'])

        return None

    def test_subclass_constructor_class_subclass(self):
        """Tests the subclass_constructor method for edge with class-subclass data type."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)

        # edge information
        edge_info = {'n1': 'subclass', 'n2': 'class', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['2', 'HP_0110035']}

        # test method
        dic, graph, edges = self.kg_builder.subclass_constructor(graph, edge_info, 'gene-phenotype')

        # check returned results
        self.assertIsInstance(dic, Dict)
        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 6)
        self.assertTrue(len(graph) > pre_length)

        return None

    def test_subclass_constructor_class_class(self):
        """Tests the subclass_constructor method for edge with class-class data type."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)

        # edge information
        edge_info = {'n1': 'class', 'n2': 'class', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['http://purl.obolibrary.org/obo/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['DOID_3075', 'DOID_1080']}

        # test method
        dic, graph, edges = self.kg_builder.subclass_constructor(graph, edge_info, 'disease-disease')

        # check returned results
        self.assertIsInstance(dic, Dict)
        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 4)
        self.assertTrue(len(graph) > pre_length)

        return None

    def test_instance_constructor_class_class(self):
        """Tests the instance_constructor method for edge with class-class data type."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)

        # edge information
        edge_info = {'n1': 'class', 'n2': 'class', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['http://purl.obolibrary.org/obo/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['DOID_3075', 'DOID_1080']}

        # test method
        dic, graph, edges = self.kg_builder.instance_constructor(graph, edge_info, 'disease-disease')

        # check returned results
        self.assertIsInstance(dic, Dict)
        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 4)
        self.assertTrue(len(graph) > pre_length)

        return None

    def test_instance_constructor_instance_instance(self):
        """Tests the instance_constructor method for edge with instance-instance data type."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)

        # edge information
        edge_info = {'n1': 'instance', 'n2': 'instance', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'https://www.ncbi.nlm.nih.gov/gene/'],
                     'edges': ['2', '10']}

        # test method
        dic, graph, edges = self.kg_builder.instance_constructor(graph, edge_info, 'gene-gene')

        # check returned results
        self.assertIsInstance(dic, Dict)
        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 5)
        self.assertTrue(len(graph) > pre_length)

        return None

    def test_instance_constructor_instance_class(self):
        """Tests the instance_constructor method for edge with instance-class data type."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)

        # edge information
        edge_info = {'n1': 'instance', 'n2': 'class', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['2', 'HP_0110035']}

        # test method
        dic, graph, edges = self.kg_builder.instance_constructor(graph, edge_info, 'gene-phenotype')

        # check returned results
        self.assertIsInstance(dic, Dict)
        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 4)
        self.assertTrue(len(graph) > pre_length)

        return None

    def test_subclass_constructor_subclass_subclass(self):
        """Tests the subclass_constructor method for edge with subclass-subclass data type."""

        # prepare input vars
        # graph
        graph = Graph()
        pre_length = len(graph)

        # edge information
        edge_info = {'n1': 'subclass', 'n2': 'subclass', 'rel': 'RO_0003302', 'inv_rel': 'RO_0003302',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'https://www.ncbi.nlm.nih.gov/gene/'],
                     'edges': ['2', '10']}

        # test method
        dic, graph, edges = self.kg_builder.subclass_constructor(graph, edge_info, 'gene-gene')

        # check returned results
        self.assertIsInstance(dic, Dict)
        self.assertIsInstance(graph, Graph)
        self.assertIsInstance(edges, List)
        self.assertEqual(len(edges), 12)
        self.assertTrue(len(graph) > pre_length)

        return None

    def tearDown(self):
        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
