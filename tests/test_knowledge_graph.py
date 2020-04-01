import glob
import json
import os
import os.path
import pandas
import pickle
import shutil
import unittest

from typing import Dict, List

from pkt_kg.knowledge_graph import FullBuild, PartialBuild, PostClosureBuild


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

        # save data
        with open(self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(edge_dict, filepath)

        # create subclass mapping data
        subcls_map = {"2": ['SO_0001217'], "9": ['SO_0001217'], "10": ['SO_0001217'], "1080": ['SO_0001217'],
                      "1962": ['SO_0001217'], "2729": ['SO_0001217'], "3075": ['SO_0001217'], "4267": ['SO_0001217'],
                      "4800": ['SO_0001217'], "5096": ['SO_0001217'], "6774": ['SO_0001217'], "8754": ['SO_0001217'],
                      "8837": ['SO_0001217'], "10190": ['SO_0001217'], "80219": ['SO_0001217']}

        # save data
        with open(self.dir_loc_resources + '/construction_approach/subclass/subclass_construction_map.pkl', 'wb') as f:
            pickle.dump(subcls_map, f, protocol=4)

        # set-up input arguments
        self.kg_subclass = FullBuild(kg_version='v2.0.0',
                                     write_location=self.dir_loc_resources + '/knowledge_graphs',
                                     construction='subclass',
                                     edge_data=self.dir_loc_resources + '/Master_Edge_List_Dict.json',
                                     node_data='yes',
                                     inverse_relations='yes',
                                     decode_owl_semantics='yes')

        self.kg_instance = PartialBuild(kg_version='v2.0.0',
                                        write_location=self.dir_loc_resources + '/knowledge_graphs',
                                        construction='instance',
                                        edge_data=self.dir_loc_resources + '/Master_Edge_List_Dict.json',
                                        node_data='yes',
                                        inverse_relations='no',
                                        decode_owl_semantics='no')

        self.kg_closure = PostClosureBuild(kg_version='v2.0.0',
                                           write_location=self.dir_loc_resources + '/knowledge_graphs',
                                           construction='instance',
                                           edge_data=self.dir_loc_resources + '/Master_Edge_List_Dict.json',
                                           node_data='yes',
                                           inverse_relations='no',
                                           decode_owl_semantics='no')

        # update class attributes
        dir_loc_owltools = os.path.join(current_directory, 'utils/owltools')
        self.kg_subclass.owltools = os.path.abspath(dir_loc_owltools)
        self.kg_instance.owltools = os.path.abspath(dir_loc_owltools)

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

        # def test_creates_instance_instance_data_edges(self):
        #     """Tests the creates_instance_instance_data_edges method."""
        #
        #     # (self, edge_type: str, creates_node_metadata_func: Callable)
        #
        #     return None

        # def test_creates_class_class_data_edges(self):
        #     """Tests the creates_class_class_data_edges method."""
        #
        #     # (self, edge_type: str)
        #
        #     return None
        #
        # def test_creates_instance_class_data_edges(self):
        #     """Tests the creates_instance_class_data_edges method."""
        #
        #     # (self, edge_type: str, creates_node_metadata_func: Callable)
        #
        #     return None
        #
        # def test_creates_knowledge_graph_edges(self):
        #     """Tests the creates_knowledge_graph_edges method."""
        #
        #     # (self, creates_node_metadata_func: Callable, ontology_annotator_func: Callable)
        #
        #     return None
        #
        # def test_construct_knowledge_graph(self):
        #     """Tests the construct_knowledge_graph(self) method."""
        #
        #     return None

    def tearDown(self):
        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
