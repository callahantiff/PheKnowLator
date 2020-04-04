import glob
import json
import os
import os.path
import pandas
import pickle
import shutil
import unittest

from rdflib import Graph
from typing import Dict, List

from pkt_kg.knowledge_graph import FullBuild, PartialBuild, PostClosureBuild
from pkt_kg.metadata import Metadata


class TestKGBuilder(unittest.TestCase):
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
        os.mkdir(self.dir_loc_resources + '/knowledge_graphs')
        os.mkdir(self.dir_loc_resources + '/relations_data')
        os.mkdir(self.dir_loc_resources + '/node_data')
        os.mkdir(dir_loc_resources + '/ontologies')
        os.mkdir(self.dir_loc_resources + '/construction_approach')
        os.mkdir(self.dir_loc_resources + '/construction_approach/instance')
        os.mkdir(self.dir_loc_resources + '/construction_approach/subclass')

        # copy data
        # node metadata
        shutil.copyfile(self.dir_loc + '/node_data/gene-phenotype_GENE_METADATA.txt',
                        self.dir_loc_resources + '/node_data/gene-phenotype_GENE_METADATA.txt')
        shutil.copyfile(self.dir_loc + '/node_data/gene-gene_GENE_METADATA.txt',
                        self.dir_loc_resources + '/node_data/gene-gene_GENE_METADATA.txt')

        # ontology data
        shutil.copyfile(self.dir_loc + '/ontologies/empty_hp_with_imports.owl',
                        self.dir_loc_resources + '/ontologies/hp_with_imports.owl')

        # merged ontology data
        shutil.copyfile(self.dir_loc + '/ontologies/so_with_imports.owl',
                        self.dir_loc_resources + '/knowledge_graphs/PheKnowLator_MergedOntologies.owl')

        # relations data
        shutil.copyfile(self.dir_loc + '/RELATIONS_LABELS.txt',
                        self.dir_loc_resources + '/relations_data/RELATIONS_LABELS.txt')

        # inverse relations
        shutil.copyfile(self.dir_loc + '/INVERSE_RELATIONS.txt',
                        self.dir_loc_resources + '/relations_data/INVERSE_RELATIONS.txt')

        # empty master edges
        shutil.copyfile(self.dir_loc + '/Master_Edge_List_Dict_empty.json',
                        self.dir_loc_resources + '/Master_Edge_List_Dict_empty.json')

        # create edge list
        edge_dict = {"gene-phenotype": {"data_type": "subclass-class",
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
                                                 ["8837", "6774"], ["8837", "8754"]]}
                     }

        edge_dict_inst = {"gene-phenotype": {"data_type": "instance-class",
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
                                                      ["8837", "6774"], ["8837", "8754"]]}
                          }

        # save data
        with open(self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(edge_dict, filepath)

        with open(self.dir_loc_resources + '/Master_Edge_List_Dict_instance.json', 'w') as filepath:
            json.dump(edge_dict_inst, filepath)

        # create subclass mapping data
        subcls_map = {"2": ['SO_0001217'], "9": ['SO_0001217'], "10": ['SO_0001217'], "1080": ['SO_0001217'],
                      "1962": ['SO_0001217'], "2729": ['SO_0001217'], "3075": ['SO_0001217'], "4267": ['SO_0001217'],
                      "4800": ['SO_0001217'], "5096": ['SO_0001217'], "6774": ['SO_0001217'], "8754": ['SO_0001217'],
                      "8837": ['SO_0001217'], "10190": ['SO_0001217'], "80219": ['SO_0001217']}

        # save data
        with open(self.dir_loc_resources + '/construction_approach/subclass/subclass_construction_map.pkl', 'wb') as f:
            pickle.dump(subcls_map, f, protocol=4)

        # set-up input arguments
        write_loc = self.dir_loc_resources + '/knowledge_graphs'
        edges = self.dir_loc_resources + '/Master_Edge_List_Dict.json'
        edges_inst = self.dir_loc_resources + '/Master_Edge_List_Dict_instance.json'

        # build 3 different knowledge graphs
        self.kg_subclass = FullBuild('v2.0.0', write_loc, 'subclass', edges, 'yes', 'yes', 'yes')
        self.kg_instance = PartialBuild('v2.0.0', write_loc, 'instance', edges_inst, 'yes', 'no', 'no')
        self.kg_instance2 = PartialBuild('v2.0.0', write_loc, 'instance', edges_inst, 'yes', 'yes', 'no')
        self.kg_closure = PostClosureBuild('v2.0.0', write_loc, 'instance', edges_inst, 'yes', 'no', 'no')

        # update class attributes
        dir_loc_owltools = os.path.join(current_directory, 'utils/owltools')
        self.kg_subclass.owltools = os.path.abspath(dir_loc_owltools)
        self.kg_instance.owltools = os.path.abspath(dir_loc_owltools)
        self.kg_instance2.owltools = os.path.abspath(dir_loc_owltools)

        return None

    def test_class_initialization_parameters(self):
        """Tests the class initialization parameters."""

        # edge data
        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          None, 'yes', 'yes', 'yes')

        self.assertRaises(OSError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dicts.json', 'yes', 'yes', 'yes')

        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict_empty.json', 'yes', 'yes', 'yes')

        # relations
        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'ye', 'yes')

        # remove relations and inverse relations data
        os.remove(self.dir_loc_resources + '/relations_data/RELATIONS_LABELS.txt')
        os.remove(self.dir_loc_resources + '/relations_data/INVERSE_RELATIONS.txt')
        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 'yes')

        # add back deleted data
        shutil.copyfile(self.dir_loc + '/RELATIONS_LABELS.txt',
                        self.dir_loc_resources + '/relations_data/RELATIONS_LABELS.txt')
        shutil.copyfile(self.dir_loc + '/INVERSE_RELATIONS.txt',
                        self.dir_loc_resources + '/relations_data/INVERSE_RELATIONS.txt')

        # node metadata
        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'ye', 'yes', 'yes')

        self.assertRaises(Exception,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'no', 'yes', 'yes', 'yes')

        # remove node metadata
        os.remove(self.dir_loc_resources + '/node_data/gene-phenotype_GENE_METADATA.txt')
        os.remove(self.dir_loc_resources + '/node_data/gene-gene_GENE_METADATA.txt')
        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 'yes')

        # add back deleted data
        shutil.copyfile(self.dir_loc + '/node_data/gene-phenotype_GENE_METADATA.txt',
                        self.dir_loc_resources + '/node_data/gene-phenotype_GENE_METADATA.txt')
        shutil.copyfile(self.dir_loc + '/node_data/gene-gene_GENE_METADATA.txt',
                        self.dir_loc_resources + '/node_data/gene-gene_GENE_METADATA.txt')

        # decoding owl
        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 'ye')

        return None

    def test_class_initialization(self):
        """Tests the class initialization."""

        # check class attributes
        self.assertTrue(self.kg_subclass.build == 'full')
        self.assertTrue(self.kg_subclass.kg_version == 'v2.0.0')
        self.assertTrue(self.kg_subclass.write_location == self.dir_loc_resources + '/knowledge_graphs')

        # edge list
        self.assertIsInstance(self.kg_subclass.edge_dict, Dict)
        self.assertIn('gene-phenotype', self.kg_subclass.edge_dict.keys())
        self.assertIn('data_type', self.kg_subclass.edge_dict['gene-phenotype'].keys())
        self.assertTrue(self.kg_subclass.edge_dict['gene-phenotype']['data_type'] == 'subclass-class')
        self.assertIn('uri', self.kg_subclass.edge_dict['gene-phenotype'].keys())
        self.assertTrue(len(self.kg_subclass.edge_dict['gene-phenotype']['uri']) == 2)
        self.assertIn('edge_list', self.kg_subclass.edge_dict['gene-phenotype'].keys())
        self.assertTrue(len(self.kg_subclass.edge_dict['gene-phenotype']['edge_list']) == 10)
        self.assertIn('edge_relation', self.kg_subclass.edge_dict['gene-phenotype'].keys())

        # node metadata
        self.assertIsInstance(self.kg_subclass.node_dict, Dict)
        self.assertTrue(len(self.kg_subclass.node_dict) == 0)

        # relations
        self.assertIsInstance(self.kg_subclass.inverse_relations, List)
        self.assertIsInstance(self.kg_subclass.relations_dict, Dict)
        self.assertTrue(len(self.kg_subclass.relations_dict) == 0)
        self.assertIsInstance(self.kg_subclass.inverse_relations_dict, Dict)
        self.assertTrue(len(self.kg_subclass.inverse_relations_dict) == 0)

        # ontologies
        self.assertIsInstance(self.kg_subclass.ontologies, List)
        self.assertTrue(len(self.kg_subclass.ontologies) == 1)

        # owl semantics
        self.assertIsInstance(self.kg_subclass.decode_owl_semantics, str)
        self.assertTrue(self.kg_subclass.decode_owl_semantics == 'yes')

        # check post closure
        self.assertTrue(self.kg_closure.build == 'post-closure')

        return None

    def test_class_initialization_subclass(self):
        """Tests the subclass construction approach class initialization."""

        # check construction type
        self.assertTrue(self.kg_subclass.construct_approach == 'subclass')
        self.assertIsInstance(self.kg_subclass.subclass_data_dict, Dict)
        self.assertTrue(len(self.kg_subclass.subclass_data_dict.keys()) == 15)
        self.assertIsNone(self.kg_subclass.kg_uuid_map)

        return None

    def test_class_initialization_instance(self):
        """Tests the instance construction approach class initialization."""

        # check build type
        self.assertTrue(self.kg_instance.build == 'partial')

        # check relations and owl decoding
        self.assertIsNone(self.kg_instance.decode_owl_semantics)
        self.assertIsNone(self.kg_instance.inverse_relations)

        # check construction type
        self.assertTrue(self.kg_instance.construct_approach == 'instance')
        self.assertIsNone(self.kg_instance.subclass_data_dict)
        self.assertIsInstance(self.kg_instance.kg_uuid_map, Dict)

        return None

    def test_sets_up_environment(self):
        """Tests the sets_up_environment method."""

        # set environment for subclass full build - see if inverse_relations directory got added to knowledge_graphs
        self.kg_subclass.sets_up_environment()
        self.assertTrue(os.path.isdir(self.kg_subclass.write_location + '/inverse_relations'))

        # set environment for instance partial build - see if relations_only directory got added to knowledge_graphs
        self.kg_instance.sets_up_environment()
        self.assertTrue(os.path.isdir(self.kg_instance.write_location + '/relations_only'))

        return None

    def test_reverse_relation_processor(self):
        """Tests the reverse_relation_processor method."""

        self.kg_subclass.reverse_relation_processor()

        # check if data was successfully processed
        self.assertIsInstance(self.kg_subclass.inverse_relations_dict, Dict)
        self.assertTrue(len(self.kg_subclass.inverse_relations_dict) > 0)
        self.assertIsInstance(self.kg_subclass.relations_dict, Dict)
        self.assertTrue(len(self.kg_subclass.relations_dict) > 0)

        self.kg_instance.reverse_relation_processor()

        # check if data was successfully processed
        self.assertIsNone(self.kg_instance.inverse_relations_dict)
        self.assertIsNone(self.kg_instance.relations_dict)

        return None

    def test_finds_node_type(self):
        """Tests the finds_node_type method."""

        # test condition for subclass-subclass
        edge_info1 = {'n1': 'subclass', 'n2': 'subclass', 'edges': ['2', '3124'],
                      'uri': ['https://www.ncbi.nlm.nih.gov/gene/',
                              'https://www.ncbi.nlm.nih.gov/gene/']}
        map_vals1 = self.kg_subclass.finds_node_type(edge_info1)
        self.assertEqual(([None, None], ['2', 'https://www.ncbi.nlm.nih.gov/gene/'],
                          ['3124', 'https://www.ncbi.nlm.nih.gov/gene/']),
                         map_vals1)

        # test condition for instance-instance
        edge_info2 = {'n1': 'instance', 'n2': 'instance', 'edges': ['2', '3124'],
                      'uri': ['https://www.ncbi.nlm.nih.gov/gene/',
                              'https://www.ncbi.nlm.nih.gov/gene/']}
        map_vals2 = self.kg_subclass.finds_node_type(edge_info2)
        self.assertEqual(([None, None], ['2', 'https://www.ncbi.nlm.nih.gov/gene/'],
                          ['3124', 'https://www.ncbi.nlm.nih.gov/gene/']), map_vals2)

        # test condition for class-subclass
        edge_info3 = {'n1': 'subclass', 'n2': 'class', 'edges': ['2', 'DOID_0110035'],
                      'uri': ['https://www.ncbi.nlm.nih.gov/gene/',
                              'http://purl.obolibrary.org/obo/']}
        map_vals3 = self.kg_subclass.finds_node_type(edge_info3)
        self.assertEqual((['DOID_0110035', 'http://purl.obolibrary.org/obo/'],
                          ['2', 'https://www.ncbi.nlm.nih.gov/gene/'],
                          [None, None]), map_vals3)

        # test condition for subclass-class
        edge_info4 = {'n1': 'class', 'n2': 'subclass', 'edges': ['DOID_0110035', '2'],
                      'uri': ['http://purl.obolibrary.org/obo/',
                              'https://www.ncbi.nlm.nih.gov/gene/']}
        map_vals4 = self.kg_subclass.finds_node_type(edge_info4)
        self.assertEqual((['DOID_0110035', 'http://purl.obolibrary.org/obo/'],
                          ['2', 'https://www.ncbi.nlm.nih.gov/gene/'],
                          [None, None]), map_vals4)

        # test condition for class-class
        edge_info5 = {'n1': 'class', 'n2': 'class', 'edges': ['2', 'DOID_0110035'],
                      'uri': ['http://purl.obolibrary.org/obo/',
                              'https://www.ncbi.nlm.nih.gov/gene/']}
        map_vals5 = self.kg_subclass.finds_node_type(edge_info5)
        self.assertEqual(([None, None], [None, None], [None, None]), map_vals5)

        return None

    def test_subclass_constructor_bad_map(self):
        """Tests the subclass_constructor method for an edge that contains an identifier that is not included
        in the subclass_map_dict."""

        # prepare input vars
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_subclass.graph)

        edge_info = {'n1': 'subclass', 'n2': 'class', 'relation': 'RO_0003302',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['14', 'DOID_0110035']}

        # test method
        results = self.kg_subclass.subclass_constructor(edge_info, 'gene-disease')

        # check returned results
        self.assertIsInstance(results[0], List)
        self.assertEqual(len(results[0]), 0)
        self.assertIsNone(results[1])
        self.assertTrue(len(self.kg_subclass.graph) == pre_length)

        # check subclass error log
        self.assertIsInstance(self.kg_subclass.subclass_error, Dict)
        self.assertIn('gene-disease', self.kg_subclass.subclass_error.keys())
        self.assertEqual(self.kg_subclass.subclass_error['gene-disease'], ['https://www.ncbi.nlm.nih.gov/gene/14'])

        return None

    def test_subclass_constructor_class_subclass(self):
        """Tests the subclass_constructor method for edge with class-subclass data type."""

        # prepare input vars
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_subclass.graph)

        edge_info = {'n1': 'subclass', 'n2': 'class', 'relation': 'RO_0003302',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['2', 'DOID_0110035']}

        # test method
        results = self.kg_subclass.subclass_constructor(edge_info, 'gene-disease')
        self.assertIsInstance(results[0], List)
        self.assertEqual(len(results[0]), 2)
        self.assertIsInstance(results[1], Dict)
        self.assertTrue(len(self.kg_subclass.graph) > pre_length)

        return None

    def test_subclass_constructor_class_class(self):
        """Tests the subclass_constructor method for edge with class-class data type."""

        # prepare input vars
        # graph
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_subclass.graph)

        # edge information
        edge_info = {'n1': 'class', 'n2': 'class', 'relation': 'RO_0003302',
                     'uri': ['http://purl.obolibrary.org/obo/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['DOID_0123512', 'DOID_0110035']}

        results = self.kg_subclass.subclass_constructor(edge_info, 'disease-disease')
        self.assertIsInstance(results[0], List)
        self.assertEqual(len(results[0]), 0)
        self.assertIsInstance(results[1], Dict)
        self.assertTrue(len(self.kg_subclass.graph) == pre_length)

        return None

    def test_instance_constructor_class_class(self):
        """Tests the instance_constructor method for edge with class-class data type."""

        # prepare input vars
        # graph
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_subclass.graph)

        # edge information
        edge_info = {'n1': 'class', 'n2': 'class', 'relation': 'RO_0003302',
                     'uri': ['http://purl.obolibrary.org/obo/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['DOID_0123512', 'DOID_0110035']}

        results = self.kg_subclass.instance_constructor(edge_info)
        self.assertIsInstance(results[0], List)
        self.assertEqual(len(results[0]), 0)
        self.assertIsInstance(results[1], Dict)
        self.assertTrue(len(self.kg_subclass.graph) == pre_length)

        return None

    def test_instance_constructor_instance_instance(self):
        """Tests the instance_constructor method for edge with instance-instance data type."""

        # prepare input vars
        # graph
        self.kg_instance.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_instance.graph)

        # edge information
        edge_info = {'n1': 'instance', 'n2': 'instance', 'relation': 'RO_0003302',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'https://www.ncbi.nlm.nih.gov/gene/'],
                     'edges': ['2', '10']}

        results = self.kg_instance.instance_constructor(edge_info)
        self.assertIsInstance(results[0], List)
        self.assertEqual(len(results[0]), 0)
        self.assertIsInstance(results[1], Dict)
        self.assertTrue(len(self.kg_instance.graph) == pre_length)

        return None

    def test_instance_constructor_instance_class(self):
        """Tests the instance_constructor method for edge with instance-class data type."""

        # prepare input vars
        # graph
        self.kg_instance.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_instance.graph)

        # edge information
        edge_info = {'n1': 'instance', 'n2': 'class', 'relation': 'RO_0003302',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['2', 'DOID_0110035']}

        results = self.kg_instance.instance_constructor(edge_info)

        self.assertIsInstance(results[0], List)
        self.assertEqual(len(results[0]), 1)
        self.assertIsInstance(results[1], Dict)
        self.assertIn('https://github.com/callahantiff/PheKnowLator/obo/ext/', results[1]['edges'][1])
        self.assertTrue(len(self.kg_instance.graph) > pre_length)

        return None

    def test_subclass_constructor_subclass_subclass(self):
        """Tests the subclass_constructor method for edge with subclass-subclass data type."""

        # prepare input vars
        # graph
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_subclass.graph)

        # edge information
        edge_info = {'n1': 'subclass', 'n2': 'subclass', 'relation': 'RO_0003302',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'https://www.ncbi.nlm.nih.gov/gene/'],
                     'edges': ['2', '10']}

        results = self.kg_subclass.subclass_constructor(edge_info, 'gene-gene')
        self.assertIsInstance(results[0], List)
        self.assertEqual(len(results[0]), 4)
        self.assertIsInstance(results[1], Dict)
        self.assertTrue(len(self.kg_subclass.graph) > pre_length)

        return None

    def test_checks_for_inverse_relations(self):
        """Tests the checks_for_inverse_relations method."""

        self.kg_subclass.reverse_relation_processor()

        # test 1
        edge_list1 = self.kg_subclass.edge_dict['gene-phenotype']['edge_list']
        rel1_check = self.kg_subclass.checks_for_inverse_relations('RO_0003302', edge_list1)

        self.assertIsNone(rel1_check)

        # test 2
        edge_list2 = self.kg_subclass.edge_dict['gene-gene']['edge_list']
        rel2_check = self.kg_subclass.checks_for_inverse_relations('RO_0002435', edge_list2)

        self.assertEqual(rel2_check, 'RO_0002435')

        return None

    def test_adds_edge_relations_with_inverse(self):
        """Tests the adds_edge_relations method with inverse relations."""

        self.kg_subclass.reverse_relation_processor()

        # prepare input vars
        # graph
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_subclass.graph)

        # edge information
        edge_info = {'n1': 'subclass', 'n2': 'subclass', 'relation': 'RO_0002435',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'https://www.ncbi.nlm.nih.gov/gene/'],
                     'edges': ['2', '10']}

        # check inverse relations
        edge_list = self.kg_subclass.edge_dict['gene-gene']['edge_list']
        inverse_relation = self.kg_subclass.checks_for_inverse_relations('RO_0002435', edge_list)

        # add edges
        results = self.kg_subclass.adds_edge_relations(edge_info, inverse_relation)

        self.assertIsInstance(results, List)
        self.assertEqual(len(results), 2)
        self.assertTrue(len(self.kg_subclass.graph) > pre_length)

        return None

    def test_adds_edge_relations_without_inverse(self):
        """Tests the adds_edge_relations method without inverse relations."""

        self.kg_subclass.reverse_relation_processor()

        # prepare input vars
        # graph
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        pre_length = len(self.kg_subclass.graph)

        # edge information
        edge_info = {'n1': 'subclass', 'n2': 'subclass', 'relation': 'RO_0003302',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'https://www.ncbi.nlm.nih.gov/gene/'],
                     'edges': ['2', '10']}

        # check inverse relations
        edge_list = self.kg_subclass.edge_dict['gene-gene']['edge_list']
        inverse_relation = self.kg_subclass.checks_for_inverse_relations('RO_0003302', edge_list)

        # add edges
        results = self.kg_subclass.adds_edge_relations(edge_info, inverse_relation)

        self.assertIsInstance(results, List)
        self.assertEqual(len(results), 1)
        self.assertTrue(len(self.kg_subclass.graph) > pre_length)

        return None

    def test_creates_knowledge_graph_edges_not_adding_metadata_to_kg(self):
        """Tests the creates_knowledge_graph_edges method without adding node metadata to the KG."""

        self.kg_subclass.sets_up_environment()
        self.kg_subclass.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_subclass.graph = Graph()

        # initialize metadata class
        metadata = Metadata(self.kg_subclass.kg_version, self.kg_subclass.write_location, self.kg_subclass.full_kg,
                            self.kg_subclass.node_data, self.kg_subclass.node_dict)

        # test method
        self.kg_subclass.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_subclass.graph) > 0)
        self.assertEqual(len(self.kg_subclass.graph), 63)

        # check that no UUID map was written out -- this is only for instance-based builds
        uuid_file = 'PheKnowLator_full_InverseRelations_NotClosed_NoOWLSemantics_ClassInstanceMap.json'
        self.assertFalse(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + uuid_file))

        # check graph was saved
        kg_filename = 'PheKnowLator_full_InverseRelations_NotClosed_NoOWLSemantics_KG.owl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + kg_filename))

        return None

    def test_creates_knowledge_graph_edges_adding_metadata_to_kg(self):
        """Tests the creates_knowledge_graph_edges method and adds node metadata to the KG."""

        self.kg_subclass.sets_up_environment()
        self.kg_subclass.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_subclass.graph = Graph()

        # make sure to add node_metadata
        self.kg_subclass.adds_metadata_to_kg = 'yes'

        # initialize metadata class
        metadata = Metadata(self.kg_subclass.kg_version, self.kg_subclass.write_location, self.kg_subclass.full_kg,
                            self.kg_subclass.node_data, self.kg_subclass.node_dict)
        metadata.node_metadata_processor()

        # test method
        self.kg_subclass.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_subclass.graph) > 0)
        self.assertEqual(len(self.kg_subclass.graph), 239)

        # check that no UUID map was written out -- this is only for instance-based builds
        uuid_file = 'PheKnowLator_full_InverseRelations_NotClosed_NoOWLSemantics_ClassInstanceMap.json'
        self.assertFalse(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + uuid_file))

        # check graph was saved
        kg_filename = 'PheKnowLator_full_InverseRelations_NotClosed_NoOWLSemantics_KG.owl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + kg_filename))

        return None

    def test_creates_knowledge_graph_edges_instance_no_inverse(self):
        """Tests the creates_knowledge_graph_edges method when applied to a kg with instance-based construction
        without inverse relations."""

        self.kg_instance.sets_up_environment()
        self.kg_instance.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_instance.graph = Graph()

        # initialize metadata class
        metadata = Metadata(self.kg_instance.kg_version, self.kg_instance.write_location, self.kg_instance.full_kg,
                            self.kg_instance.node_data, self.kg_instance.node_dict)
        metadata.node_metadata_processor()

        # test method
        self.kg_instance.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_instance.graph) > 0)
        self.assertEqual(len(self.kg_instance.graph), 34)

        # check that UUID map was written out
        uuid_file = 'PheKnowLator_partial_NotClosed_OWLSemantics_ClassInstanceMap.json'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/construction_approach/instance/' + uuid_file))

        # check graph was saved
        kg_filename = 'PheKnowLator_partial_NotClosed_OWLSemantics_KG.owl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/relations_only/' + kg_filename))

        return None

    def test_creates_knowledge_graph_edges_instance_inverse(self):
        """Tests the creates_knowledge_graph_edges method when applied to a kg with instance-based construction with
        inverse relations."""

        self.kg_instance2.sets_up_environment()
        self.kg_instance2.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_instance2.graph = Graph()

        # initialize metadata class
        metadata = Metadata(self.kg_instance2.kg_version, self.kg_instance2.write_location, self.kg_instance2.full_kg,
                            self.kg_instance2.node_data, self.kg_instance2.node_dict)
        metadata.node_metadata_processor()

        # test method
        self.kg_instance2.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_instance2.graph) > 0)
        self.assertEqual(len(self.kg_instance2.graph), 42)

        # check that UUID map was written out
        uuid_file = 'PheKnowLator_partial_InverseRelations_NotClosed_OWLSemantics_ClassInstanceMap.json'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/construction_approach/instance/' + uuid_file))

        # check graph was saved
        kg_filename = 'PheKnowLator_partial_InverseRelations_NotClosed_OWLSemantics_KG.owl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + kg_filename))

        return None

    def test_creates_knowledge_graph_edges_adding_metadata_to_kg_bad(self):
        """Tests the creates_knowledge_graph_edges method and adds node metadata to the KG, but also makes sure that
        a log file is writen for genes that are not in the subclass_map."""

        self.kg_subclass.sets_up_environment()
        self.kg_subclass.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_subclass.graph = Graph()

        # make sure to add node_metadata
        self.kg_subclass.adds_metadata_to_kg = 'yes'

        # initialize metadata class
        metadata = Metadata(self.kg_subclass.kg_version, self.kg_subclass.write_location, self.kg_subclass.full_kg,
                            self.kg_subclass.node_data, self.kg_subclass.node_dict)
        metadata.node_metadata_processor()

        # alter gene list - adding genes not in the subclass_map dictionary
        self.kg_subclass.edge_dict['gene-gene']['edge_list'] = [["1", "1080"], ["1", "4267"], ["4800", "10190"],
                                                                ["4800", "80219"], ["2729", "1962"], ["2729", "5096"],
                                                                ["8837", "6774"], ["8837", "8754"]]

        # test method
        self.kg_subclass.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # check that log file was written out
        log_file = '/construction_approach/subclass/subclass_map_missing_node_log.json'
        self.assertTrue(os.path.exists(self.dir_loc_resources + log_file))

        return None

    def tearDown(self):

        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
