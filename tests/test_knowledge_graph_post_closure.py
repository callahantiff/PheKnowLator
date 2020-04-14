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
from unittest import mock

from pkt_kg.knowledge_graph import PostClosureBuild


class TestPostClosureBuild(unittest.TestCase):
    """Class to test the PostClosureBuild class from the knowledge graph script."""

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

        # copy data
        # subclass dict file
        shutil.copyfile(self.dir_loc + '/subclass_construction_map_empty.pkl',
                        self.dir_loc_resources + '/construction_approach/subclass_construction_map_empty.pkl')

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

        # save data
        with open(self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(edge_dict, filepath)

        # create subclass mapping data
        subcls_map = {"2": ['SO_0001217'], "9": ['SO_0001217'], "10": ['SO_0001217'], "1080": ['SO_0001217'],
                      "1962": ['SO_0001217'], "2729": ['SO_0001217'], "3075": ['SO_0001217'],
                      "4267": ['SO_0001217'],
                      "4800": ['SO_0001217'], "5096": ['SO_0001217'], "6774": ['SO_0001217'],
                      "8754": ['SO_0001217'],
                      "8837": ['SO_0001217'], "10190": ['SO_0001217'], "80219": ['SO_0001217']}

        # save data
        with open(self.dir_loc_resources + '/construction_approach/subclass_construction_map.pkl', 'wb') as f:
            pickle.dump(subcls_map, f, protocol=4)

        # set-up input arguments
        write_loc = self.dir_loc_resources + '/knowledge_graphs'
        edges = self.dir_loc_resources + '/Master_Edge_List_Dict.json'

        # build 3 different knowledge graphs
        self.kg = PostClosureBuild('v2.0.0', write_loc, 'subclass', edges, 'yes', 'yes', 'yes')

        # update class attributes
        dir_loc_owltools = os.path.join(current_directory, 'utils/owltools')
        self.kg.owl_tools = os.path.abspath(dir_loc_owltools)

        return None

    def test_class_initialization(self):
        """Tests initialization of the class."""

        # check build type
        self.assertEqual(self.kg.gets_build_type(), 'Post-Closure Build')
        self.assertFalse(self.kg.gets_build_type() == 'Partial Build')
        self.assertFalse(self.kg.gets_build_type() == 'Full Build')

        return None

    @mock.patch('builtins.input', create=True)
    def test_instance_uuid_map(self, mocked_input):
        """Tests importing instance construction approach."""

        # check instance file reqs
        self.kg.construct_approach = 'instance'

        # test file is empty
        mocked_input.side_effect = [self.dir_loc + '/Master_Edge_List_Dict_empty.json']
        self.assertRaises(TypeError, self.kg.construct_knowledge_graph)

        return None

    @mock.patch('builtins.input', create=True)
    def test_instance_owl_file(self, mocked_input):
        """Tests importing instance construction approach."""

        # test wrong file extension
        mocked_input.side_effect = [self.dir_loc + '/PheKnowLator_Closed.txt']
        self.assertRaises(TypeError, self.kg.construct_knowledge_graph)

        # test file does not exist
        mocked_input.side_effect = [self.dir_loc + '/PheKnowLator_Closed_KG.owl']
        self.assertRaises(OSError, self.kg.construct_knowledge_graph)

        # test file is empty
        mocked_input.side_effect = [self.dir_loc + '/Master_Edge_List_Dict_empty.json']
        self.assertRaises(TypeError, self.kg.construct_knowledge_graph)

        return None

    @mock.patch('builtins.input', create=True)
    def test_instance_owl_annotation_assertions_file(self, mocked_input):
        """Tests importing instance construction approach."""

        # test wrong file extension
        mocked_input.side_effect = [self.dir_loc + '/PheKnowLator_Closed.owl',
                                    self.dir_loc + '/PheKnowLator_Full.txt']
        self.assertRaises(TypeError, self.kg.construct_knowledge_graph)

        # test file does not exist
        mocked_input.side_effect = [self.dir_loc + '/PheKnowLator_Closed.owl',
                                    self.dir_loc + '/PheKnowLator_Full_KG.owl']
        self.assertRaises(OSError, self.kg.construct_knowledge_graph)

        # test file is empty
        mocked_input.side_effect = [self.dir_loc + '/PheKnowLator_Closed.owl',
                                    self.dir_loc + '/Master_Edge_List_Dict_empty.json']
        self.assertRaises(TypeError, self.kg.construct_knowledge_graph)

        return None

    @mock.patch('builtins.input', create=True)
    def test_construct_knowledge_graph(self, mocked_input):
        """Tests the construct_knowledge_graph method."""

        mocked_input.side_effect = [self.dir_loc + '/PheKnowLator_Full.owl', self.dir_loc + '/PheKnowLator_Closed.owl']

        # test out the build
        self.kg.construct_knowledge_graph()

        # check for output files
        # kg - owl
        kg_owl = 'PheKnowLator_post-closure_InverseRelations_Closed_NoOWLSemantics_KG.owl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + kg_owl))

        # kg - nx.multiDiGraph
        kg_mdg = 'PheKnowLator_post-closure_InverseRelations_Closed_NoOWLSemantics_KG_Networkx_MultiDiGraph.gpickle'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + kg_mdg))

        # node metadata
        meta = 'PheKnowLator_post-closure_InverseRelations_Closed_NoOWLSemantics_NodeLabels.txt'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + meta))

        # edge list - identifiers
        ids = 'PheKnowLator_post-closure_InverseRelations_Closed_NoOWLSemantics_Triples_Identifiers.txt'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + ids))

        # edge list - integers
        ints = 'PheKnowLator_post-closure_InverseRelations_Closed_NoOWLSemantics_Triples_Integers.txt'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + ints))

        # edge list map
        int_map = 'PheKnowLator_post-closure_InverseRelations_Closed_NoOWLSemantics_Triples_Integer_Identifier_Map.json'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + int_map))

        # # decoding owl classes
        # owlnets = 'PheKnowLator_full_InverseRelations_NotClosed_OWLSemantics_ClassInstanceMap.json'
        # self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + owlnets))

        return None

    def tearDown(self):

        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
