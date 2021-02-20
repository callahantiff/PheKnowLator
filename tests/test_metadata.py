import glob
import logging
import os
import os.path
import pickle
import unittest

from rdflib import Graph
from typing import Dict

from pkt_kg.metadata import *


class TestMetadata(unittest.TestCase):
    """Class to test the metadata class from the metadata script."""

    def setUp(self):
        # initialize file location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data')
        self.dir_loc = os.path.abspath(dir_loc)

        # handle logging
        self.logs = os.path.abspath(current_directory + '/builds/logs')
        logging.disable(logging.CRITICAL)
        if len(glob.glob(self.logs + '/*.log')) > 0: os.remove(glob.glob(self.logs + '/*.log')[0])

        # pointer to owltools
        dir_loc2 = os.path.join(current_directory, 'utils/owltools')
        self.owltools_location = os.path.abspath(dir_loc2)

        # create graph data
        self.graph = Graph().parse(self.dir_loc + '/ontologies/so_with_imports.owl')

        # set-up input arguments
        self.metadata = Metadata(kg_version='v2.0.0',
                                 write_location=self.dir_loc,
                                 kg_location=self.dir_loc + '/ontologies/so_with_imports.owl',
                                 node_data=glob.glob(self.dir_loc + '/node_data/*.pkl'),
                                 node_dict=dict())

        # load dictionary
        self.metadata.node_metadata_processor()

        return None

    def test_node_metadata_processor(self):
        """Tests the node_metadata_processor method."""

        # set-up input arguments
        self.metadata = Metadata(kg_version='v2.0.0', write_location=self.dir_loc,
                                 kg_location=self.dir_loc + '/ontologies/so_with_imports.owl',
                                 node_data=glob.glob(self.dir_loc + '/node_data/*dict.pkl'),
                                 node_dict=dict())
        self.metadata.node_metadata_processor()  # load dictionary

        # make sure that the dictionary has the "schtuff"
        self.assertIsInstance(self.metadata.node_dict, Dict)
        self.assertTrue('nodes' in self.metadata.node_dict.keys())
        self.assertTrue('relations' in self.metadata.node_dict.keys())

        # check node dict
        node_key = 'http://www.ncbi.nlm.nih.gov/gene/1'
        self.assertIsInstance(self.metadata.node_dict['nodes'], Dict)
        self.assertTrue(len(self.metadata.node_dict['nodes']) == 20)
        self.assertIn('Label', self.metadata.node_dict['nodes'][node_key].keys())
        self.assertIn('Synonym', self.metadata.node_dict['nodes'][node_key].keys())
        self.assertIn('Description', self.metadata.node_dict['nodes'][node_key].keys())

        # check relations dict
        relations_key = 'http://purl.obolibrary.org/obo/RO_0002597'
        self.assertIsInstance(self.metadata.node_dict['relations'], Dict)
        self.assertTrue(len(self.metadata.node_dict['relations']) == 20)
        self.assertIn('Label', self.metadata.node_dict['relations'][relations_key].keys())
        self.assertIn('Synonym', self.metadata.node_dict['relations'][relations_key].keys())
        self.assertIn('Description', self.metadata.node_dict['relations'][relations_key].keys())

        return None

    def test_creates_node_metadata_nodes(self):
        """Tests the creates_node_metadata method."""

        self.metadata.node_data = [self.metadata.node_data[0].replace('.pkl', '_test.pkl')]
        self.metadata.extracts_class_metadata(self.graph)

        # test when the node has metadata
        updated_graph_1 = self.metadata.creates_node_metadata(ent=['http://www.ncbi.nlm.nih.gov/gene/1',
                                                                   'http://www.ncbi.nlm.nih.gov/gene/2'],
                                                              e_type=['entity', 'entity'])
        self.assertTrue(len(updated_graph_1) == 16)

        # check that the correct info is returned if only one is an entity
        updated_graph_2 = self.metadata.creates_node_metadata(ent=['http://www.ncbi.nlm.nih.gov/gene/1',
                                                                   'http://www.ncbi.nlm.nih.gov/gene/2'],
                                                              e_type=['entity', 'class'])
        self.assertTrue(len(updated_graph_2) == 8)
        # check that nothing is returned if the entities are classes
        updated_graph_3 = self.metadata.creates_node_metadata(ent=['http://www.ncbi.nlm.nih.gov/gene/1',
                                                                   'http://www.ncbi.nlm.nih.gov/gene/2'],
                                                              e_type=['class', 'class'])
        self.assertTrue(updated_graph_3 is None)

        # test when the node does not have metadata
        updated_graph_4 = self.metadata.creates_node_metadata(ent=['http://www.ncbi.nlm.nih.gov/gene/None',
                                                                   'http://www.ncbi.nlm.nih.gov/gene/None'],
                                                              e_type=['entity', 'entity'])
        self.assertTrue(updated_graph_4 is None)

        # test when node_data is None
        self.metadata.node_data = None
        updated_graph_5 = self.metadata.creates_node_metadata(ent=['http://www.ncbi.nlm.nih.gov/gene/None',
                                                                   'http://www.ncbi.nlm.nih.gov/gene/None'],
                                                              e_type=['entity', 'entity'])
        self.assertTrue(updated_graph_5 is None)

        return None

    def test_creates_node_metadata_relations(self):
        """Tests the creates_node_metadata method."""

        self.metadata.node_data = [self.metadata.node_data[0].replace('.pkl', '_test.pkl')]
        self.metadata.extracts_class_metadata(self.graph)

        # test when the node has metadata
        updated_graph_1 = self.metadata.creates_node_metadata(ent=['http://purl.obolibrary.org/obo/RO_0002310'],
                                                              key_type='relations')
        self.assertTrue(len(updated_graph_1) == 2)

        # check that nothing is returned if the entities are classes
        updated_graph_2 = self.metadata.creates_node_metadata(ent=['http://purl.obolibrary.org/obo/RO_0002597'],
                                                              e_type=['class'], key_type='relations')
        self.assertTrue(updated_graph_2 is None)

        # test when the node does not have metadata
        updated_graph_3 = self.metadata.creates_node_metadata(['http://www.ncbi.nlm.nih.gov/gene/None'],
                                                              key_type='relations')
        self.assertTrue(updated_graph_3 is None)

        return None

    def test_creates_node_metadata_none(self):
        """Tests the creates_node_metadata method when node_dict is None."""

        self.metadata.node_data = [self.metadata.node_data[0].replace('.pkl', '_test.pkl')]
        self.metadata.extracts_class_metadata(self.graph)
        self.metadata.node_dict = None

        # relations -- with valid input
        updated_graph_1 = self.metadata.creates_node_metadata(ent=['http://purl.obolibrary.org/obo/RO_0002597'],
                                                              key_type='relations')
        self.assertTrue(updated_graph_1 is None)

        # relations -- without valid input
        updated_graph_2 = self.metadata.creates_node_metadata(ent=['http://purl.obolibrary.org/obo/None'],
                                                              key_type='relations')
        self.assertTrue(updated_graph_2 is None)

        # nodes -- with valid input
        updated_graph_3 = self.metadata.creates_node_metadata(ent=['http://www.ncbi.nlm.nih.gov/gene/1',
                                                                   'http://www.ncbi.nlm.nih.gov/gene/2'],
                                                              e_type=['class', 'class'])
        self.assertTrue(updated_graph_3 is None)

        # nodes -- without valid input
        updated_graph_4 = self.metadata.creates_node_metadata(ent=['http://www.ncbi.nlm.nih.gov/gene/None',
                                                                   'http://www.ncbi.nlm.nih.gov/gene/None'],
                                                              e_type=['entity', 'entity'])
        self.assertTrue(updated_graph_4 is None)

        return None

    def test_extracts_class_metadata(self):
        """Tests the extracts_class_metadata data."""

        org_file_size = os.path.getsize(self.metadata.node_data[0])

        # extract metadata
        self.metadata.node_data = [self.metadata.node_data[0].replace('.pkl', '_test.pkl')]
        self.metadata.extracts_class_metadata(graph=self.graph)

        # check that it worked
        # nodes
        node_key = 'http://purl.obolibrary.org/obo/SO_0000373'
        self.assertTrue(len(self.metadata.node_dict['nodes']) == 2461)
        self.assertIn('Label', self.metadata.node_dict['nodes'][node_key])
        self.assertIn('Description', self.metadata.node_dict['nodes'][node_key])
        self.assertIn('Synonym', self.metadata.node_dict['nodes'][node_key])
        # relations
        relation_key = 'http://purl.obolibrary.org/obo/so#genome_of'
        self.assertTrue(len(self.metadata.node_dict['relations']) == 72)
        self.assertIn('Label', self.metadata.node_dict['relations'][relation_key])
        self.assertIn('Description', self.metadata.node_dict['relations'][relation_key])
        self.assertIn('Synonym', self.metadata.node_dict['relations'][relation_key])

        # check that larger dict was saved
        self.assertTrue(os.path.getsize(self.metadata.node_data[0]) >= org_file_size)

        return None

    def test_output_knowledge_graph_metadata(self):
        """Tests the output_knowledge_graph_metadata method."""

        self.metadata.write_location = ''  # update environment var
        self.metadata.node_metadata_processor()  # load dictionary
        graph = Graph().parse(self.dir_loc + '/ontologies/so_with_imports.owl')  # load graph
        filename = self.dir_loc + '/ontologies/'

        # get node integer map
        node_ints = maps_node_ids_to_integers(graph, filename, 'SO_Triples_Integers.txt',
                                              'SO_Triples_Integer_Identifier_Map.json')

        # run function
        self.metadata.output_knowledge_graph_metadata(node_ints, graph)

        # make sure that node data wrote out
        self.assertTrue(os.path.exists(self.dir_loc + '/ontologies/so_with_imports_NodeLabels.txt'))

        # remove file
        os.remove(self.dir_loc + '/ontologies/so_with_imports_NodeLabels.txt')
        os.remove(filename + 'SO_Triples_Integers.txt')
        os.remove(filename + 'SO_Triples_Identifiers.txt')
        os.remove(filename + 'SO_Triples_Integer_Identifier_Map.json')

        return None

    def test_adds_ontology_annotations(self):
        """Tests the adds_ontology_annotations method."""

        # load graph
        graph = Graph().parse(self.dir_loc + '/ontologies/so_with_imports.owl')

        # run function
        updated_graph = self.metadata.adds_ontology_annotations(filename='tests/data/so_tests_file.owl', graph=graph)

        # check that the annotations were added
        results = updated_graph.query(
            """SELECT DISTINCT ?o ?p ?s
                WHERE {
                    ?o rdf:type owl:Ontology .
                    ?o ?p ?s . }
            """, initNs={'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                         'owl': 'http://www.w3.org/2002/07/owl#'})

        results_list = [str(x) for y in results for x in y]
        self.assertTrue(len(results_list) == 21)
        self.assertIn('https://pheknowlator.com/pheknowlator_file.owl', results_list)

        return None

    def tearDown(self):

        if self.metadata.node_data:
            test_data_location = glob.glob(self.dir_loc + '/node_data/*_test.pkl')
            if len(test_data_location) > 0:
                os.remove(test_data_location[0])

        return None
