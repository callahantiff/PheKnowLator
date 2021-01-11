import glob
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

        # pointer to owltools
        dir_loc2 = os.path.join(current_directory, 'utils/owltools')
        self.owltools_location = os.path.abspath(dir_loc2)

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

        # test when the node has metadata
        updated_graph_1 = self.metadata.creates_node_metadata(['http://www.ncbi.nlm.nih.gov/gene/1',
                                                               'http://www.ncbi.nlm.nih.gov/gene/2'],
                                                              ['entity', 'entity'])
        self.assertTrue(len(updated_graph_1) == 16)

        # check that the correct info is returned if only one is an entity
        updated_graph_2 = self.metadata.creates_node_metadata(['http://www.ncbi.nlm.nih.gov/gene/1',
                                                               'http://www.ncbi.nlm.nih.gov/gene/2'],
                                                              ['entity', 'class'])
        self.assertTrue(len(updated_graph_2) == 8)
        # check that nothing is returned if the entities are classes
        updated_graph_3 = self.metadata.creates_node_metadata(['http://www.ncbi.nlm.nih.gov/gene/1',
                                                               'http://www.ncbi.nlm.nih.gov/gene/2'],
                                                              ['class', 'class'])
        self.assertTrue(len(updated_graph_3) == 0)

        # test when the node does not have metadata
        updated_graph_4 = self.metadata.creates_node_metadata(['http://www.ncbi.nlm.nih.gov/gene/None',
                                                               'http://www.ncbi.nlm.nih.gov/gene/None'],
                                                              ['entity', 'entity'])
        self.assertTrue(updated_graph_4 is None)

        # test when node_data is None
        self.metadata.node_data = None
        updated_graph_5 = self.metadata.creates_node_metadata(['http://www.ncbi.nlm.nih.gov/gene/None',
                                                               'http://www.ncbi.nlm.nih.gov/gene/None'],
                                                              ['entity', 'entity'])
        self.assertTrue(updated_graph_5 is None)

        return None

    def test_creates_node_metadata_relations(self):
        """Tests the creates_node_metadata method."""

        # test when the node has metadata
        updated_graph_1 = self.metadata.creates_node_metadata(['http://purl.obolibrary.org/obo/RO_0002597'],
                                                              ['entity', 'entity'], 'relations')
        self.assertTrue(len(updated_graph_1) == 2)

        # check that the correct info is returned if only one is an entity
        updated_graph_2 = self.metadata.creates_node_metadata(['http://purl.obolibrary.org/obo/RO_0002597'],
                                                              ['entity', 'class'], 'relations')
        self.assertTrue(len(updated_graph_2) == 2)

        # check that nothing is returned if the entities are classes
        updated_graph_3 = self.metadata.creates_node_metadata(['http://purl.obolibrary.org/obo/RO_0002597'],
                                                              ['class', 'class'], 'relations')
        self.assertTrue(len(updated_graph_3) == 2)

        # test when the node does not have metadata
        updated_graph_4 = self.metadata.creates_node_metadata(['http://www.ncbi.nlm.nih.gov/gene/None'],
                                                              ['entity', 'entity'], 'relations')
        self.assertTrue(updated_graph_4 is None)

        return None

    def test_removes_annotation_assertions(self):
        """Tests the removes_annotation_assertions method."""

        # check inputs
        self.assertIsInstance(self.metadata.write_location, str)
        self.assertTrue(self.metadata.write_location == self.dir_loc)
        self.assertIsInstance(self.metadata.full_kg, str)
        self.assertTrue(self.metadata.full_kg == self.dir_loc + '/ontologies/so_with_imports.owl')

        # run function
        self.metadata.write_location = ''
        self.metadata.removes_annotation_assertions(self.owltools_location)

        # check that annotations were removed
        no_assert_loc = self.metadata.full_kg[:-4] + '_NoAnnotationAssertions.owl'
        self.assertTrue(os.path.exists(no_assert_loc))

        # remove file
        os.remove(self.dir_loc + '/ontologies/so_with_imports_NoAnnotationAssertions.owl')

        return None

    def test_adds_annotation_assertions(self):
        """Tests the adds_annotation_assertions method."""

        # remove annotation assertions
        self.metadata.write_location = ''
        self.metadata.removes_annotation_assertions(self.owltools_location)

        # update inputs
        filename = self.metadata.full_kg[:-4] + '_NoAnnotationAssertions.owl'

        # load graph
        graph = Graph().parse(filename)
        no_annot_graph_len = len(graph)

        # add back annotation assertions
        updated_graph = self.metadata.adds_annotation_assertions(graph=graph, filename=filename)

        # verify that new edges were added to the graph
        self.assertTrue(len(updated_graph) > no_annot_graph_len)

        # remove file
        os.remove(self.dir_loc + '/ontologies/so_with_imports_NoAnnotationAssertions.owl')

        return None

    def test_extracts_class_metadata(self):
        """Tests the extracts_class_metadata data."""

        # load dictionary and graph
        self.metadata.node_metadata_processor()
        graph = Graph().parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        org_file_size = os.path.getsize(self.metadata.node_data[0])

        # extract metadata
        self.metadata.node_data = [self.metadata.node_data[0].replace('.pkl', '_test.pkl')]
        self.metadata.extracts_class_metadata(graph=graph)

        # check that it worked
        # nodes
        node_key = 'http://purl.obolibrary.org/obo/SO_0000373'
        self.assertTrue(len(self.metadata.node_dict['nodes']) == 2461)
        self.assertIn('Label', self.metadata.node_dict['nodes'][node_key])
        self.assertIn('Description', self.metadata.node_dict['nodes'][node_key])
        self.assertIn('Synonym', self.metadata.node_dict['nodes'][node_key])
        # relations
        relation_key = 'http://purl.obolibrary.org/obo/so#genome_of'
        self.assertTrue(len(self.metadata.node_dict['relations']) == 2511)
        self.assertIn('Label', self.metadata.node_dict['relations'][relation_key])
        self.assertIn('Description', self.metadata.node_dict['relations'][relation_key])
        self.assertIn('Synonym', self.metadata.node_dict['relations'][relation_key])

        # check that larger dict was saved
        self.assertTrue(os.path.getsize(self.metadata.node_data[0]) >= org_file_size)

        # clean up environment
        os.remove(self.metadata.node_data[0].replace('.pkl', '_test.pkl'))

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
        self.metadata.output_knowledge_graph_metadata(node_ints)

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
        updated_graph = self.metadata.adds_ontology_annotations(filename='tests/data/so_tests_test_file', graph=graph)

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
        self.assertIn('https://pheknowlator.com/pheknowlator_test_file.owl', results_list)
        self.assertIn('tests/data/so_tests_test_file', results_list)

        return None
