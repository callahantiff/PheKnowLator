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

from pkt_kg.knowledge_graph import FullBuild, PartialBuild, PostClosureBuild
from pkt_kg.metadata import Metadata
from pkt_kg.utils import gets_object_properties, gets_ontology_classes


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
        edge_dict = {"gene-phenotype": {"data_type": "entity-class",
                                        "edge_relation": "RO_0003302",
                                        "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                "http://purl.obolibrary.org/obo/"],
                                        "edge_list": [["2", "HP_0002511"], ["2", "HP_0000716"],
                                                      ["2", "HP_0000100"], ["9", "HP_0030955"],
                                                      ["9", "HP_0009725"], ["9", "HP_0100787"],
                                                      ["9", "HP_0012125"], ["10", "HP_0009725"],
                                                      ["10", "HP_0010301"], ["10", "HP_0045005"]]},
                     "gene-gene": {"data_type": "entity-entity",
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

        edge_dict_inst = {"gene-phenotype": {"data_type": "entity-class",
                                             "edge_relation": "RO_0003302",
                                             "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                     "http://purl.obolibrary.org/obo/"],
                                             "edge_list": [["2", "HP_0002511"], ["2", "HP_0000716"],
                                                           ["2", "HP_0000100"], ["9", "HP_0030955"],
                                                           ["9", "HP_0009725"], ["9", "HP_0100787"],
                                                           ["9", "HP_0012125"], ["10", "HP_0009725"],
                                                           ["10", "HP_0010301"], ["10", "HP_0045005"]]},
                          "gene-gene": {"data_type": "entity-entity",
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

        # save data
        with open(self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(edge_dict, filepath)

        with open(self.dir_loc_resources + '/Master_Edge_List_Dict_instance.json', 'w') as filepath:
            json.dump(edge_dict_inst, filepath)

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
        edges_inst = self.dir_loc_resources + '/Master_Edge_List_Dict_instance.json'

        # build 3 different knowledge graphs
        self.kg_subclass = FullBuild('v2.0.0', write_loc, 'subclass', edges, 'yes', 'yes', 'yes', 'yes')
        self.kg_instance = PartialBuild('v2.0.0', write_loc, 'instance', edges_inst, 'yes', 'yes', 'no', 'no')
        self.kg_instance2 = PartialBuild('v2.0.0', write_loc, 'instance', edges_inst, 'yes', 'yes', 'yes', 'no')
        self.kg_closure = PostClosureBuild('v2.0.0', write_loc, 'instance', edges_inst, 'yes', 'yes', 'no', 'no')

        # update class attributes
        dir_loc_owltools = os.path.join(current_directory, 'utils/owltools')
        self.kg_subclass.owl_tools = os.path.abspath(dir_loc_owltools)
        self.kg_instance.owl_tools = os.path.abspath(dir_loc_owltools)
        self.kg_instance2.owl_tools = os.path.abspath(dir_loc_owltools)

        return None

    def test_class_initialization_parameters_version(self):
        """Tests the class initialization parameters for version."""

        self.assertRaises(ValueError,
                          FullBuild, None, self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          None, 'yes', 'yes', 'yes', 'yes')

        return None

    def test_class_initialization_parameters_write_location(self):
        """Tests the class initialization parameters for write location."""

        self.assertRaises(ValueError, FullBuild, 'v2.0.0', None, 'subclass', None, 'yes', 'yes', 'yes', 'yes')

        return None

    def test_class_initialization_parameters_ontologies(self):
        """Tests the class initialization parameters for ontologies."""

        self.kg_subclass.res_dir = 'data'
        self.assertRaises(ValueError, FullBuild, 'v2.0.0', None, 'subclass', None, 'yes', 'yes', 'yes', 'yes')
        self.kg_subclass.res_dir = 'data/resources'

        return None

    def test_class_initialization_parameters_construction_approach(self):
        """Tests the class initialization parameters for construction_approach."""

        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 1,
                          None, 'yes', 'yes', 'yes', 'yes')
        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subcls',
                          None, 'yes', 'yes', 'yes', 'yes')
        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'inst',
                          None, 'yes', 'yes', 'yes', 'yes')

        return None

    def test_class_initialization_parameters_edge_data(self):
        """Tests the class initialization parameters for edge_data."""

        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          None, 'yes', 'yes', 'yes', 'yes')

        self.assertRaises(OSError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dicts.json', 'yes', 'yes', 'yes', 'yes')

        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict_empty.json', 'yes', 'yes', 'yes', 'yes')

        return None

    def test_class_initialization_parameter_relations(self):
        """Tests the class initialization parameters for relations."""

        self.assertRaises(TypeError, FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 1, 'yes')

        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 'ye', 'yes')

        # remove relations and inverse relations data
        os.remove(self.dir_loc_resources + '/relations_data/RELATIONS_LABELS.txt')
        os.remove(self.dir_loc_resources + '/relations_data/INVERSE_RELATIONS.txt')
        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 'yes', 'yes')

        # add back deleted data
        shutil.copyfile(self.dir_loc + '/RELATIONS_LABELS.txt',
                        self.dir_loc_resources + '/relations_data/RELATIONS_LABELS.txt')
        shutil.copyfile(self.dir_loc + '/INVERSE_RELATIONS.txt',
                        self.dir_loc_resources + '/relations_data/INVERSE_RELATIONS.txt')

        return None

    def test_class_initialization_parameters_node_metadata(self):
        """Tests the class initialization parameters for node_metadata."""

        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 1, 'yes', 'yes')

        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'ye', 'yes', 'yes')

        # remove node metadata
        os.remove(self.dir_loc_resources + '/node_data/gene-phenotype_GENE_METADATA.txt')
        os.remove(self.dir_loc_resources + '/node_data/gene-gene_GENE_METADATA.txt')
        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 'yes', 'yes')

        # add back deleted data
        shutil.copyfile(self.dir_loc + '/node_data/gene-phenotype_GENE_METADATA.txt',
                        self.dir_loc_resources + '/node_data/gene-phenotype_GENE_METADATA.txt')
        shutil.copyfile(self.dir_loc + '/node_data/gene-gene_GENE_METADATA.txt',
                        self.dir_loc_resources + '/node_data/gene-gene_GENE_METADATA.txt')

        return None

    def test_class_initialization_parameters_kg_metadata_flag(self):
        """Tests the class initialization parameters for setting the kg_metadata flag parameter."""

        # initialize input parameters
        write_loc = self.dir_loc_resources + '/knowledge_graphs'
        edges = self.dir_loc_resources + '/Master_Edge_List_Dict.json'

        # test kg_metadata flag when node_data is None
        self.kg_subclass = FullBuild('v2.0.0', write_loc, 'subclass', edges, 'no', None, 'yes', 'yes')
        self.assertEqual('no', self.kg_subclass.kg_metadata)

        # test kg_metadata flag when node_data is None
        self.kg_subclass = FullBuild('v2.0.0', write_loc, 'subclass', edges, 'yes', 'yes', 'yes', 'yes')
        self.assertEqual('yes', self.kg_subclass.kg_metadata)

        return None

    def test_class_initialization_parameters_decoding_owl(self):
        """Tests the class initialization parameters for decoding owl."""

        self.assertRaises(TypeError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 'yes', 1)

        self.assertRaises(ValueError,
                          FullBuild, 'v2.0.0', self.dir_loc_resources + '/knowledge_graphs', 'subclass',
                          self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'yes', 'yes', 'yes', 'ye')

        return None

    def test_class_initialization(self):
        """Tests the class initialization."""

        # check class attributes
        self.assertTrue(self.kg_subclass.build == 'full')
        self.assertTrue(self.kg_subclass.kg_version == 'v2.0.0')

        path = os.path.relpath(self.dir_loc_resources + '/knowledge_graphs')
        self.assertTrue(self.kg_subclass.write_location == path)

        # edge list
        self.assertIsInstance(self.kg_subclass.edge_dict, Dict)
        self.assertIn('gene-phenotype', self.kg_subclass.edge_dict.keys())
        self.assertIn('data_type', self.kg_subclass.edge_dict['gene-phenotype'].keys())
        self.assertTrue(self.kg_subclass.edge_dict['gene-phenotype']['data_type'] == 'entity-class')
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
        self.assertIsInstance(self.kg_subclass.decode_owl, str)
        self.assertTrue(self.kg_subclass.decode_owl == 'yes')

        return None

    def test_class_initialization_subclass(self):
        """Tests the subclass construction approach class initialization."""

        # check construction type
        self.assertTrue(self.kg_subclass.construct_approach == 'subclass')

        return None

    def test_class_initialization_instance(self):
        """Tests the instance construction approach class initialization."""

        # check build type
        self.assertTrue(self.kg_instance.build == 'partial')

        # check relations and owl decoding
        self.assertIsNone(self.kg_instance.decode_owl)
        self.assertIsNone(self.kg_instance.inverse_relations)

        # check construction type
        self.assertTrue(self.kg_instance.construct_approach == 'instance')

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
        self.assertIsInstance(self.kg_instance.relations_dict, Dict)

        return None

    def test_verifies_object_property(self):
        """Tests the verifies_object_property method."""

        # load graph
        self.kg_subclass.graph = Graph()
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')

        # get object properties
        self.kg_subclass.obj_properties = gets_object_properties(self.kg_subclass.graph)

        # check for presence of existing obj_prop
        self.assertIn(URIRef('http://purl.obolibrary.org/obo/so#position_of'), self.kg_subclass.obj_properties)

        # test adding bad relation
        self.assertRaises(TypeError, self.kg_subclass.verifies_object_property, 'RO_0002200')

        # test adding a good relation
        new_relation = URIRef('http://purl.obolibrary.org/obo/' + 'RO_0002566')
        self.kg_subclass.verifies_object_property(new_relation)

        # update list of object properties
        self.kg_subclass.obj_properties = gets_object_properties(self.kg_subclass.graph)

        # make sure that object property was added to the graph
        self.assertTrue(new_relation in self.kg_subclass.obj_properties)

        return None

    def test_check_ontology_class_classes(self):
        """Tests the check_ontology_class_nodes method for class-class edges."""

        # set-up inputs for class-class
        edge_info = {'n1': 'class', 'n2': 'class', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['CHEBI_81395', 'DOID_12858']}

        self.kg_subclass.ont_classes = [URIRef('http://purl.obolibrary.org/obo/CHEBI_81395'),
                                        URIRef('http://purl.obolibrary.org/obo/DOID_12858')]

        self.assertTrue(self.kg_subclass.check_ontology_class_nodes(edge_info))

        # set-up inputs for class-class (FALSE)
        edge_info = {'n1': 'class', 'n2': 'class', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['CHEBI_81395', 'DOID_1']}

        self.kg_subclass.ont_classes = ['http://purl.obolibrary.org/obo/CHEBI_81395',
                                        'http://purl.obolibrary.org/obo/DOID_128987']

        self.assertFalse(self.kg_subclass.check_ontology_class_nodes(edge_info))

        return None

    def test_check_ontology_class_subclasses(self):
        """Tests the check_ontology_class_nodes method for subclass edges."""

        # set-up inputs for subclass-subclass
        self.kg_subclass.ont_classes = {URIRef('http://purl.obolibrary.org/obo/DOID_12858')}

        edge_info = {'n1': 'entity', 'n2': 'entity', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['14', '134056']}

        self.assertTrue(self.kg_subclass.check_ontology_class_nodes(edge_info))

        # set-up inputs for subclass-class
        edge_info = {'n1': 'entity', 'n2': 'class', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['14', 'DOID_12858']}

        self.assertTrue(self.kg_subclass.check_ontology_class_nodes(edge_info))

        # set-up inputs for class-subclass
        edge_info = {'n1': 'class', 'n2': 'entity', 'rel': 'RO_0003302', 'inv_rel': None,
                     'uri': ['http://purl.obolibrary.org/obo/', 'https://www.ncbi.nlm.nih.gov/gene/'],
                     'edges': ['DOID_12858', '14']}

        self.assertTrue(self.kg_subclass.check_ontology_class_nodes(edge_info))

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

    def test_creates_knowledge_graph_edges_not_adding_metadata_to_kg(self):
        """Tests the creates_knowledge_graph_edges method without adding node metadata to the KG."""

        self.kg_subclass.sets_up_environment()
        self.kg_subclass.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        self.kg_subclass.obj_properties = gets_object_properties(self.kg_subclass.graph)
        self.kg_subclass.ont_classes = gets_ontology_classes(self.kg_subclass.graph)

        # make sure to add node_metadata
        self.kg_subclass.kg_metadata = 'no'

        # initialize metadata class
        metadata = Metadata(self.kg_subclass.kg_version, self.kg_subclass.write_location, self.kg_subclass.full_kg,
                            self.kg_subclass.node_data, self.kg_subclass.node_dict)

        # test method
        self.kg_subclass.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_subclass.graph) > 0)
        self.assertEqual(len(self.kg_subclass.graph), 42327)

        # check graph was saved
        kg_filename = 'PheKnowLator_full_InverseRelations_NotClosed_NoOWLSemantics_KG.owl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/inverse_relations/' + kg_filename))

        return None

    def test_creates_knowledge_graph_edges_adding_metadata_to_kg(self):
        """Tests the creates_knowledge_graph_edges method and adds node metadata to the KG."""

        self.kg_subclass.sets_up_environment()
        self.kg_subclass.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        self.kg_subclass.obj_properties = gets_object_properties(self.kg_subclass.graph)
        self.kg_subclass.ont_classes = gets_ontology_classes(self.kg_subclass.graph)

        # make sure to add node_metadata
        self.kg_subclass.kg_metadata = 'yes'

        # initialize metadata class
        metadata = Metadata(self.kg_subclass.kg_version, self.kg_subclass.write_location, self.kg_subclass.full_kg,
                            self.kg_subclass.node_data, self.kg_subclass.node_dict)
        metadata.node_metadata_processor()

        # test method
        self.kg_subclass.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_subclass.graph) > 0)
        self.assertEqual(len(self.kg_subclass.graph), 42477)

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
        self.assertEqual(len(self.kg_instance.graph), 192)

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
        self.assertEqual(len(self.kg_instance2.graph), 200)

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
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        self.kg_subclass.obj_properties = gets_object_properties(self.kg_subclass.graph)
        self.kg_subclass.ont_classes = gets_ontology_classes(self.kg_subclass.graph)

        # make sure to add node_metadata
        self.kg_subclass.kg_metadata = 'yes'

        # initialize metadata class
        metadata = Metadata(self.kg_subclass.kg_version,
                            self.kg_subclass.write_location,
                            self.kg_subclass.full_kg,
                            self.kg_subclass.node_data,
                            self.kg_subclass.node_dict)

        metadata.node_metadata_processor()

        # alter gene list - adding genes not in the subclass_map dictionary
        self.kg_subclass.edge_dict['gene-gene']['edge_list'] = [["1", "1080"], ["1", "4267"], ["4800", "10190"],
                                                                ["4800", "80219"], ["2729", "1962"], ["2729", "5096"],
                                                                ["8837", "6774"], ["8837", "8754"]]

        # test method
        self.kg_subclass.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # check that log file was written out
        log_file = '/construction_approach/subclass_map_missing_node_log.json'
        self.assertTrue(os.path.exists(self.dir_loc_resources + log_file))

        return None

    def tearDown(self):

        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
