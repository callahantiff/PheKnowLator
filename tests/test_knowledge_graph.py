import glob
import json
import logging
import networkx  # type: ignore
import os
import os.path
import pandas
import pickle
import shutil
import unittest

from mock import patch
from rdflib import Graph, URIRef, BNode
from rdflib.namespace import OWL, RDF
from typing import Dict, List

from pkt_kg.__version__ import __version__
from pkt_kg.knowledge_graph import FullBuild, PartialBuild, PostClosureBuild
from pkt_kg.metadata import Metadata
from pkt_kg.utils import appends_to_existing_file, gets_ontology_classes, gets_object_properties, splits_knowledge_graph


class TestKGBuilder(unittest.TestCase):
    """Class to test the KGBuilder class from the knowledge graph script."""

    def setUp(self):
        # initialize file location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data')
        self.dir_loc = os.path.abspath(dir_loc)

        # set-up environment - make temp directory
        dir_loc_resources = os.path.join(current_directory, 'resources')
        self.dir_loc_resources = os.path.abspath(dir_loc_resources)
        os.mkdir(self.dir_loc_resources)
        os.mkdir(self.dir_loc_resources + '/knowledge_graphs')
        os.mkdir(self.dir_loc_resources + '/relations_data')
        os.mkdir(self.dir_loc_resources + '/node_data')
        os.mkdir(self.dir_loc_resources + '/ontologies')
        os.mkdir(self.dir_loc_resources + '/construction_approach')

        # handle logging
        self.logs = os.path.abspath(current_directory + '/builds/logs')
        logging.disable(logging.CRITICAL)
        if len(glob.glob(self.logs + '/*.log')) > 0: os.remove(glob.glob(self.logs + '/*.log')[0])

        # copy needed data data
        # node metadata
        shutil.copyfile(self.dir_loc + '/node_data/node_metadata_dict.pkl',
                        self.dir_loc_resources + '/node_data/node_metadata_dict.pkl')
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
                                        "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                                "http://purl.obolibrary.org/obo/"],
                                        "edge_list": [["2", "SO_0000162"], ["2", "SO_0000196"],
                                                      ["2", "SO_0000323"], ["9", "SO_0001490"],
                                                      ["9", "SO_0000301"], ["9", "SO_0001560"],
                                                      ["9", "SO_0001560"], ["10", "SO_0000444"],
                                                      ["10", "SO_0002138"], ["10", "SO_0000511"]]},
                     "gene-gene": {"data_type": "entity-entity",
                                   "edge_relation": "RO_0002435",
                                   "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                           "http://www.ncbi.nlm.nih.gov/gene/"],
                                   "edge_list": [["1", "2"], ["2", "3"], ["3", "18"],
                                                 ["17", "19"], ["4", "17"], ["5", "11"],
                                                 ["11", "12"], ["4", "5"]]},
                     "disease-disease": {"data_type": "class-class",
                                         "edge_relation": "RO_0002435",
                                         "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                                 "http://www.ncbi.nlm.nih.gov/gene/"],
                                         "edge_list": [["DOID_3075", "DOID_1080"], ["DOID_3075", "DOID_4267"],
                                                       ["DOID_4800", "DOID_10190"], ["DOID_4800", "DOID_80219"],
                                                       ["DOID_2729", "DOID_1962"], ["DOID_2729", "DOID_5096"],
                                                       ["DOID_8837", "DOID_6774"], ["DOID_8837", "DOID_8754"]]},
                     "entity_namespaces": {"gene": "http://purl.uniprot.org/geneid/"}
                     }

        edge_dict_inst = {"gene-phenotype": {"data_type": "entity-class",
                                             "edge_relation": "RO_0003302",
                                             "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                                     "http://purl.obolibrary.org/obo/"],
                                             "edge_list": [["2", "SO_0000162"], ["2", "SO_0000196"],
                                                           ["2", "SO_0000323"], ["9", "SO_0001490"],
                                                           ["9", "SO_0000301"], ["9", "SO_0001560"],
                                                           ["9", "SO_0001560"], ["10", "SO_0000444"],
                                                           ["10", "SO_0002138"], ["10", "SO_0000511"]]},
                          "gene-gene": {"data_type": "entity-entity",
                                        "edge_relation": "RO_0002435",
                                        "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                                "http://www.ncbi.nlm.nih.gov/gene/"],
                                        "edge_list": [["1", "2"], ["2", "3"], ["3", "18"],
                                                      ["17", "19"], ["4", "17"], ["5", "11"],
                                                      ["11", "12"], ["4", "5"]]},
                          "disease-disease": {"data_type": "class-class",
                                              "edge_relation": "RO_0002435",
                                              "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                                      "http://www.ncbi.nlm.nih.gov/gene/"],
                                              "edge_list": [["DOID_3075", "DOID_1080"], ["DOID_3075", "DOID_4267"],
                                                            ["DOID_4800", "DOID_10190"], ["DOID_4800", "DOID_80219"],
                                                            ["DOID_2729", "DOID_1962"], ["DOID_2729", "DOID_5096"],
                                                            ["DOID_8837", "DOID_6774"], ["DOID_8837", "DOID_8754"]]},
                          "entity_namespaces": {"gene": "http://purl.uniprot.org/geneid/"}
                          }

        # save data
        with open(self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(edge_dict, filepath)

        with open(self.dir_loc_resources + '/Master_Edge_List_Dict_instance.json', 'w') as filepath:
            json.dump(edge_dict_inst, filepath)

        # create subclass mapping data
        subcls_map = {"1": ['SO_0001217'], "2": ['SO_0001217'], "3": ['SO_0001217'], "4": ['SO_0001217'],
                      "5": ['SO_0001217'], "11": ['SO_0001217'], "12": ['SO_0001217'], "17": ['SO_0001217'],
                      "18": ['SO_0001217'], "5096": ['SO_0001217'], "6774": ['SO_0001217'], "19": ['SO_0001217']}

        # save data
        with open(self.dir_loc_resources + '/construction_approach/subclass_construction_map.pkl', 'wb') as f:
            pickle.dump(subcls_map, f, protocol=4)

        # set write location
        self.write_location = self.dir_loc_resources + '/knowledge_graphs'

        # build 3 different knowledge graphs
        self.kg_subclass = FullBuild(construction='subclass', node_data='yes',
                                     inverse_relations='yes', decode_owl='yes', write_location=self.write_location)
        self.kg_instance = PartialBuild(construction='instance', node_data='yes',
                                        inverse_relations='no', decode_owl='no', write_location=self.write_location)
        self.kg_instance2 = PartialBuild(construction='instance', node_data='yes',
                                         inverse_relations='yes', decode_owl='yes', write_location=self.write_location)
        self.kg_closure = PostClosureBuild(construction='instance', node_data='yes',
                                           inverse_relations='yes', decode_owl='no', write_location=self.write_location)

        # update class attributes for the location of owltools
        dir_loc_owltools = os.path.join(current_directory, 'utils/owltools')
        self.kg_subclass.owl_tools = os.path.abspath(dir_loc_owltools)
        self.kg_instance.owl_tools = os.path.abspath(dir_loc_owltools)
        self.kg_instance2.owl_tools = os.path.abspath(dir_loc_owltools)

        # get release
        self.current_release = 'v' + __version__

        return None

    def test_class_initialization_parameters_version(self):
        """Tests the class initialization parameters for version."""

        self.assertEqual(self.kg_subclass.kg_version, self.current_release)

        return None

    def test_class_initialization_parameters_ontologies_missing(self):
        """Tests the class initialization parameters for ontologies when the directory is missing."""

        # run test when there is no ontologies directory
        shutil.rmtree(self.dir_loc_resources + '/ontologies')
        self.assertRaises(OSError, FullBuild, 'subclass', 'yes', 'yes', 'yes', self.write_location)

        return None

    def test_class_initialization_parameters_ontologies_empty(self):
        """Tests the class initialization parameters for ontologies when it's empty."""

        # create empty ontologies directory
        shutil.rmtree(self.dir_loc_resources + '/ontologies')
        os.mkdir(self.dir_loc_resources + '/ontologies')
        self.assertRaises(TypeError, FullBuild, 'subclass', 'yes', 'yes', 'yes', self.write_location)

        return None

    def test_class_initialization_parameters_construction_approach(self):
        """Tests the class initialization parameters for construction_approach."""

        self.assertRaises(ValueError, FullBuild, 1, 'yes', 'yes', 'yes', self.write_location)
        self.assertRaises(ValueError, FullBuild, 'subcls', 'yes', 'yes', 'yes', self.write_location)
        self.assertRaises(ValueError, FullBuild, 'inst', 'yes', 'yes', 'yes', self.write_location)

        return None

    def test_class_initialization_parameters_edge_data_missing(self):
        """Tests the class initialization parameters for edge_data when the file is missing."""

        # remove file to trigger OSError
        os.remove(self.dir_loc_resources + '/Master_Edge_List_Dict.json')
        self.assertRaises(OSError, FullBuild, 'subclass', 'yes', 'yes', 'yes', self.write_location)

        return None

    def test_class_initialization_parameters_edge_data_empty(self):
        """Tests the class initialization parameters for edge_data when the file is empty."""

        # rename empty to be main file
        os.rename(self.dir_loc_resources + '/Master_Edge_List_Dict_empty.json',
                  self.dir_loc_resources + '/Master_Edge_List_Dict.json')
        self.assertRaises(TypeError, FullBuild, 'subclass', 'yes', 'yes', 'yes', self.write_location)

        return None

    def test_class_initialization_parameter_relations_format(self):
        """Tests the class initialization parameters for relations when the input parameter is formatted wrong."""

        self.assertRaises(ValueError, FullBuild, 'subclass', 'yes', 1, 'yes', self.write_location)
        self.assertRaises(ValueError, FullBuild, 'subclass', 'yes', 'ye', 'yes', self.write_location)

        return None

    def test_class_initialization_parameter_relations_missing(self):
        """Tests the class initialization parameters for relations when the files are missing."""

        # remove relations and inverse relations data
        rel_loc = self.dir_loc_resources + '/relations_data/RELATIONS_LABELS.txt'
        invrel_loc = self.dir_loc_resources + '/relations_data/INVERSE_RELATIONS.txt'
        os.remove(rel_loc); os.remove(invrel_loc)
        self.assertRaises(TypeError, FullBuild, 'subclass', 'yes', 'yes', 'yes', self.write_location)

        # add back deleted data
        shutil.copyfile(self.dir_loc + '/RELATIONS_LABELS.txt', rel_loc)
        shutil.copyfile(self.dir_loc + '/INVERSE_RELATIONS.txt', invrel_loc)

        return None

    def test_class_initialization_parameters_node_metadata_format(self):
        """Tests the class initialization parameters for node_metadata with different formatting."""

        self.assertRaises(ValueError, FullBuild, 'subclass', 1, 'yes', 'yes', self.write_location)
        self.assertRaises(ValueError, FullBuild, 'subclass', 'ye', 'yes', 'yes', self.write_location)

        return None

    def test_class_initialization_parameters_node_metadata_missing(self):
        """Tests the class initialization parameters for node_metadata."""

        # remove node metadata
        gene_phen_data = self.dir_loc_resources + '/node_data/node_metadata_dict.pkl'
        os.remove(gene_phen_data)

        # test method
        self.assertRaises(TypeError, FullBuild, 'subclass', 'yes', 'yes', 'yes', self.write_location)

        # add back deleted data
        shutil.copyfile(self.dir_loc + '/node_data/node_metadata_dict.pkl', gene_phen_data)

        return None

    def test_class_initialization_parameters_decoding_owl(self):
        """Tests the class initialization parameters for decoding owl."""

        self.assertRaises(ValueError, FullBuild, 'subclass', 'yes', 'yes', 1, self.write_location)
        self.assertRaises(ValueError, FullBuild, 'subclass', 'yes', 'yes', 'ye', self.write_location)

        return None

    def test_class_initialization_ontology_data(self):
        """Tests the class initialization for when no merged ontology file is created."""

        # removed merged ontology file
        os.remove(self.dir_loc_resources + '/knowledge_graphs/PheKnowLator_MergedOntologies.owl')

        # run test
        self.kg_subclass = FullBuild('subclass', 'yes', 'yes', 'yes', self.write_location)
        # check that there is 1 ontology file
        self.assertIsInstance(self.kg_subclass.ontologies, List)
        self.assertTrue(len(self.kg_subclass.ontologies) == 1)

        return None

    def test_class_initialization_attributes(self):
        """Tests the class initialization for class attributes."""

        self.assertTrue(self.kg_subclass.build == 'full')
        self.assertTrue(self.kg_subclass.construct_approach == 'subclass')
        self.assertTrue(self.kg_subclass.kg_version == self.current_release)
        path = os.path.abspath(self.dir_loc_resources + '/knowledge_graphs')
        self.assertTrue(self.kg_subclass.write_location == path)

        return None

    def test_class_initialization_edgelist(self):
        """Tests the class initialization for edge_list inputs."""

        self.assertIsInstance(self.kg_subclass.edge_dict, Dict)
        self.assertIn('gene-phenotype', self.kg_subclass.edge_dict.keys())
        self.assertIn('data_type', self.kg_subclass.edge_dict['gene-phenotype'].keys())
        self.assertTrue(self.kg_subclass.edge_dict['gene-phenotype']['data_type'] == 'entity-class')
        self.assertIn('uri', self.kg_subclass.edge_dict['gene-phenotype'].keys())
        self.assertTrue(len(self.kg_subclass.edge_dict['gene-phenotype']['uri']) == 2)
        self.assertIn('edge_list', self.kg_subclass.edge_dict['gene-phenotype'].keys())
        self.assertTrue(len(self.kg_subclass.edge_dict['gene-phenotype']['edge_list']) == 10)
        self.assertIn('edge_relation', self.kg_subclass.edge_dict['gene-phenotype'].keys())

        return None

    def test_class_initialization_node_metadata(self):
        """Tests the class initialization for node metadata inputs."""

        self.assertIsInstance(self.kg_subclass.node_dict, Dict)
        self.assertTrue(len(self.kg_subclass.node_dict) == 0)

        return None

    def test_class_initialization_relations(self):
        """Tests the class initialization for relations input."""

        self.assertIsInstance(self.kg_subclass.inverse_relations, List)
        self.assertIsInstance(self.kg_subclass.relations_dict, Dict)
        self.assertTrue(len(self.kg_subclass.relations_dict) == 0)
        self.assertIsInstance(self.kg_subclass.inverse_relations_dict, Dict)
        self.assertTrue(len(self.kg_subclass.inverse_relations_dict) == 0)

        return None

    def test_class_initialization_ontologies(self):
        """Tests the class initialization for ontology inputs."""

        self.assertIsInstance(self.kg_subclass.ontologies, List)
        self.assertTrue(len(self.kg_subclass.ontologies) == 1)
        self.assertTrue(os.path.exists(self.kg_subclass.merged_ont_kg))

        return None

    def test_class_initialization_owl_decoding(self):
        """Tests the class initialization for the decode_owl input."""

        self.assertTrue(self.kg_subclass.decode_owl == 'yes')

        return None

    def test_class_initialization_subclass(self):
        """Tests the subclass construction approach class initialization."""

        # check construction type
        self.assertTrue(self.kg_subclass.construct_approach == 'subclass')

        # check filepath and write location for knowledge graph
        write_file = '/PheKnowLator_' + self.current_release + '_full_subclass_inverseRelations_noOWL.owl'
        self.assertEqual(self.kg_subclass.full_kg, write_file)

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

        # check filepath and write location for knowledge graph
        write_file = '/PheKnowLator_' + self.current_release + '_partial_instance_relationsOnly_OWL.owl'
        self.assertEqual(self.kg_instance.full_kg, write_file)

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
        edge_list1 = set(tuple(x) for x in self.kg_subclass.edge_dict['gene-phenotype']['edge_list'])
        rel1_check = self.kg_subclass.checks_for_inverse_relations('RO_0003302', edge_list1)
        self.assertIsNone(rel1_check)

        # test 2
        edge_list2 = set(tuple(x) for x in self.kg_subclass.edge_dict['gene-gene']['edge_list'])
        rel2_check = self.kg_subclass.checks_for_inverse_relations('RO_0002435', edge_list2)
        self.assertEqual(rel2_check, 'RO_0002435')

        return None

    def test_creates_new_edges_not_adding_metadata_to_kg(self):
        """Tests the creates_new_edges method without adding node metadata to the KG."""

        self.kg_subclass.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_subclass.graph = Graph().parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        self.kg_subclass.obj_properties = gets_object_properties(self.kg_subclass.graph)
        self.kg_subclass.ont_classes = gets_ontology_classes(self.kg_subclass.graph)

        # make sure to not add node_metadata
        self.kg_subclass.node_dict, self.kg_subclass.node_data = None, None

        # initialize metadata class
        meta = Metadata(self.kg_subclass.kg_version, self.kg_subclass.write_location, self.kg_subclass.full_kg,
                        self.kg_subclass.node_data, self.kg_subclass.node_dict)
        if self.kg_subclass.node_data:
            meta.metadata_processor(); meta.extract_metadata(self.kg_subclass.graph)
            self.kg_subclass.node_dict = meta.node_dict

        # create graph subsets
        self.kg_subclass.graph, annotation_triples = splits_knowledge_graph(self.kg_subclass.graph)
        if self.kg_subclass.decode_owl == 'yes': full_kg_owl = self.kg_subclass.full_kg.replace('noOWL', 'OWL')
        else: full_kg_owl = self.kg_subclass.full_kg
        annot, full = full_kg_owl[:-4] + '_AnnotationsOnly.nt', full_kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, self.kg_subclass.write_location + annot, ' ')

        # test method
        self.kg_subclass.creates_new_edges(meta.creates_node_metadata)
        shutil.copy(self.kg_subclass.write_location + annot, self.kg_subclass.write_location + full)
        appends_to_existing_file(set(self.kg_subclass.graph), self.kg_subclass.write_location + full, ' ')

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_subclass.graph) > 0)
        self.assertEqual(len(self.kg_subclass.graph), 9796)

        # check graph files were saved
        f_name = full_kg_owl[:-4] + '_LogicOnly.owl'
        self.assertTrue(os.path.exists(self.kg_subclass.write_location + f_name))
        f_name = full_kg_owl[:-4] + '_AnnotationsOnly.nt'
        self.assertTrue(os.path.exists(self.kg_subclass.write_location + f_name))
        f_name = full_kg_owl[:-4] + '.nt'
        self.assertTrue(os.path.exists(self.kg_subclass.write_location + f_name))

        return None

    def test_creates_new_edges_adding_metadata_to_kg(self):
        """Tests the creates_new_edges method and adds node metadata to the KG."""

        self.kg_subclass.reverse_relation_processor()
        # make sure that kg is empty
        self.kg_subclass.graph = Graph().parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        self.kg_subclass.obj_properties = gets_object_properties(self.kg_subclass.graph)
        self.kg_subclass.ont_classes = gets_ontology_classes(self.kg_subclass.graph)

        # make sure to add node_metadata
        meta = Metadata(self.kg_subclass.kg_version, self.kg_subclass.write_location, self.kg_subclass.full_kg,
                        self.kg_subclass.node_data, self.kg_subclass.node_dict)
        if self.kg_subclass.node_data:
            meta.metadata_processor(); meta.extract_metadata(self.kg_subclass.graph)
            self.kg_subclass.node_dict = meta.node_dict

        # create graph subsets
        self.kg_subclass.graph, annotation_triples = splits_knowledge_graph(self.kg_subclass.graph)
        if self.kg_subclass.decode_owl == 'yes': full_kg_owl = self.kg_subclass.full_kg.replace('noOWL', 'OWL')
        else: full_kg_owl = self.kg_subclass.full_kg
        annot, full = full_kg_owl[:-4] + '_AnnotationsOnly.nt', full_kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, self.kg_subclass.write_location + annot, ' ')

        # test method
        self.kg_subclass.creates_new_edges(meta.creates_node_metadata)
        shutil.copy(self.kg_subclass.write_location + annot, self.kg_subclass.write_location + full)
        appends_to_existing_file(set(self.kg_subclass.graph), self.kg_subclass.write_location + full, ' ')

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_subclass.graph) > 0)
        self.assertEqual(len(self.kg_subclass.graph), 9784)

        # check graph files were saved
        f_name = full_kg_owl[:-4] + '_LogicOnly.owl'
        self.assertTrue(os.path.exists(self.kg_subclass.write_location + f_name))
        f_name = full_kg_owl[:-4] + '_AnnotationsOnly.nt'
        self.assertTrue(os.path.exists(self.kg_subclass.write_location + f_name))
        f_name = full_kg_owl[:-4] + '.nt'
        self.assertTrue(os.path.exists(self.kg_subclass.write_location + f_name))

        return None

    @patch('builtins.print')
    def test_gets_edge_statistics(self, mock_print):
        """Tests the gets_edge_statistics method."""

        # no inverse edges
        edges = [(1, 2, 3), (3, 2, 5), (4, 6, 7)]
        self.kg_subclass.gets_edge_statistics('gene-gene', edges, [{1, 2, 3}, {1, 2, 3}, 8])
        expected_str = '3 OWL Edges, 8 Original Edges; 5 OWL Nodes, Original Nodes: 3 gene(s), 3 gene(s)'
        mock_print.assert_called_with(expected_str)

        return None

    @patch('builtins.print')
    def test_gets_edge_statistics_inverse_relations(self, mock_print):
        """Tests the gets_edge_statistics method when including inverse relations."""

        # no inverse edges
        edges = [(1, 2, 3), (3, 2, 5), (4, 6, 7)]
        self.kg_subclass.gets_edge_statistics('drug-gene', edges, [{1, 2, 3}, {1, 2, 3}, 8])
        expected_str = '3 OWL Edges, 8 Original Edges; 5 OWL Nodes, Original Nodes: 3 drug(s), 3 gene(s)'
        mock_print.assert_called_with(expected_str)

        return None

    def test_creates_new_edges_instance_no_inverse(self):
        """Tests the creates_new_edges method when applied to a kg with instance-based construction without inverse
        relations."""

        self.kg_instance.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_instance.graph = Graph()

        # initialize metadata class
        meta = Metadata(self.kg_instance.kg_version, self.kg_instance.write_location, self.kg_instance.full_kg,
                        self.kg_instance.node_data, self.kg_instance.node_dict)
        if self.kg_instance.node_data:
            meta.metadata_processor(); meta.extract_metadata(self.kg_instance.graph)
        self.kg_instance.node_dict = meta.node_dict

        # create graph subsets
        self.kg_instance.graph, annotation_triples = splits_knowledge_graph(self.kg_instance.graph)
        if self.kg_instance.decode_owl == 'yes': full_kg_owl = self.kg_instance.full_kg.replace('noOWL', 'OWL')
        else: full_kg_owl = self.kg_instance.full_kg
        annot, full = full_kg_owl[:-4] + '_AnnotationsOnly.nt', full_kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, self.kg_instance.write_location + annot, ' ')

        # test method
        self.kg_instance.creates_new_edges(meta.creates_node_metadata)
        shutil.copy(self.kg_instance.write_location + annot, self.kg_instance.write_location + full)
        appends_to_existing_file(set(self.kg_instance.graph), self.kg_instance.write_location + full, ' ')

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_instance.graph) > 0)
        self.assertEqual(len(self.kg_instance.graph), 57)

        # check graph files were saved
        f_name = full_kg_owl[:-4] + '_LogicOnly.owl'
        self.assertTrue(os.path.exists(self.kg_instance.write_location + f_name))
        f_name = full_kg_owl[:-4] + '_AnnotationsOnly.nt'
        self.assertTrue(os.path.exists(self.kg_instance.write_location + f_name))
        f_name = full_kg_owl[:-4] + '.nt'
        self.assertTrue(os.path.exists(self.kg_instance.write_location + f_name))

        return None

    def test_creates_new_edges_instance_inverse(self):
        """Tests the creates_new_edges method when applied to a kg with instance-based construction with
        inverse relations."""

        self.kg_instance2.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_instance2.graph = Graph()

        # initialize metadata class
        meta = Metadata(self.kg_instance2.kg_version, self.kg_instance2.write_location, self.kg_instance2.full_kg,
                        self.kg_instance2.node_data, self.kg_instance2.node_dict)
        if self.kg_instance2.node_data:
            meta.metadata_processor(); meta.extract_metadata(self.kg_instance2.graph)
        self.kg_instance2.node_dict = meta.node_dict

        # create graph subsets
        self.kg_instance2.graph, annotation_triples = splits_knowledge_graph(self.kg_instance2.graph)
        if self.kg_instance2.decode_owl == 'yes': full_kg_owl = self.kg_instance2.full_kg.replace('noOWL', 'OWL')
        else: full_kg_owl = self.kg_instance2.full_kg
        annot, full = full_kg_owl[:-4] + '_AnnotationsOnly.nt', full_kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, self.kg_instance2.write_location + annot, ' ')

        # test method
        self.kg_instance2.creates_new_edges(meta.creates_node_metadata)
        shutil.copy(self.kg_instance2.write_location + annot, self.kg_instance2.write_location + full)
        appends_to_existing_file(set(self.kg_instance2.graph), self.kg_instance2.write_location + full, ' ')

        # check that edges were added to the graph
        self.assertTrue(len(self.kg_instance2.graph) > 0)
        self.assertEqual(len(self.kg_instance2.graph), 64)

        # check graph files were saved
        f_name = full_kg_owl[:-4] + '_LogicOnly.owl'
        self.assertTrue(os.path.exists(self.kg_instance2.write_location + f_name))
        f_name = full_kg_owl[:-4] + '_AnnotationsOnly.nt'
        self.assertTrue(os.path.exists(self.kg_instance2.write_location + f_name))
        f_name = full_kg_owl[:-4] + '.nt'
        self.assertTrue(os.path.exists(self.kg_instance2.write_location + f_name))

        return None

    def test_creates_new_edges_adding_metadata_to_kg_bad(self):
        """Tests the creates_new_edges method and adds node metadata to the KG, but also makes sure that a
        log file is written for genes that are not in the subclass_map."""

        self.kg_subclass.reverse_relation_processor()

        # make sure that kg is empty
        self.kg_subclass.graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        self.kg_subclass.obj_properties = gets_object_properties(self.kg_subclass.graph)
        self.kg_subclass.ont_classes = gets_ontology_classes(self.kg_subclass.graph)

        # initialize metadata class
        meta = Metadata(self.kg_subclass.kg_version, self.kg_subclass.write_location, self.kg_subclass.full_kg,
                        self.kg_subclass.node_data, self.kg_subclass.node_dict)
        if self.kg_subclass.node_data:
            meta.metadata_processor(); meta.extract_metadata(self.kg_subclass.graph)
            self.kg_subclass.node_dict = meta.node_dict

        # alter gene list - adding genes not in the subclass_map dictionary
        self.kg_subclass.edge_dict['gene-gene']['edge_list'] = [["1", "1080"], ["1", "4267"], ["4800", "10190"],
                                                                ["4800", "80219"], ["2729", "1962"], ["2729", "5096"],
                                                                ["8837", "6774"], ["8837", "8754"]]

        # test method
        self.kg_subclass.creates_new_edges(meta.creates_node_metadata)

        # check that log file was written out
        log_file = '/construction_approach/subclass_map_log.json'
        self.assertTrue(os.path.exists(self.dir_loc_resources + log_file))

        return None

    def tearDown(self):

        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
