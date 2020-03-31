import glob
import os
import os.path
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
                                 flag='yes',
                                 write_location=self.dir_loc,
                                 kg_location=self.dir_loc + '/ontologies/so_with_imports.owl',
                                 node_data=glob.glob(self.dir_loc + '/node_data/*.txt'),
                                 node_dict=dict())

        # load dictionary
        self.metadata.node_metadata_processor()

        return None

    def test_node_metadata_processor(self):
        """Tests the node_metadata_processor method."""

        # make sure that the dictionary has the "schtuff"
        self.assertIsInstance(self.metadata.node_dict, Dict)
        self.assertTrue('gene-phenotype' in self.metadata.node_dict.keys())
        self.assertIsInstance(self.metadata.node_dict['gene-phenotype'], Dict)
        self.assertTrue(len(self.metadata.node_dict['gene-phenotype']) == 3)
        self.assertIn('Label', self.metadata.node_dict['gene-phenotype']['2'].keys())
        self.assertIn('Synonym', self.metadata.node_dict['gene-phenotype']['2'].keys())
        self.assertIn('Description', self.metadata.node_dict['gene-phenotype']['2'].keys())

        return None

    def test_creates_node_metadata(self):
        """Tests the creates_node_metadata method."""

        # load graph
        graph = Graph()
        graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        org_graph_len = len(graph)

        # test when the node has metadata
        updated_graph_good = self.metadata.creates_node_metadata(node='2',
                                                                 edge_type='gene-phenotype',
                                                                 url='https://www.ncbi.nlm.nih.gov/gene/',
                                                                 graph=graph)
        # verify that new edges were added to the graph
        self.assertTrue(len(updated_graph_good) > org_graph_len)

        # check that the right number of new edges were added to the graph
        graph_diff = len(updated_graph_good) - org_graph_len
        new_edges = sum([len(x[1].split('|')) for x in self.metadata.node_dict['gene-phenotype']['2'].items()])
        self.assertTrue(graph_diff == new_edges)

        # test when the node does not have metadata
        self.assertRaises(KeyError, self.metadata.creates_node_metadata,
                          '0',
                          'gene-phenotype',
                          'https://www.ncbi.nlm.nih.gov/gene/',
                          graph)

        return None

    def test_adds_node_metadata(self):
        """Tests the adds_node_metadata method."""

        # load graph
        graph = Graph()
        graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')
        org_graph_len = len(graph)

        # set up edge_dictionary
        edge_dict = {"gene-phenotype": {"data_type": "subclass-class",
                                        "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                "http://purl.obolibrary.org/obo/"],
                                        "edge_list": [["2", "HP_0002511"],
                                                      ["2", "HP_0000716"],
                                                      ["2", "HP_0000100"],
                                                      ["9", "HP_0030955"],
                                                      ["9", "HP_0009725"],
                                                      ["9", "HP_0100787"],
                                                      ["9", "HP_0012125"],
                                                      ["10", "HP_0009725"],
                                                      ["10", "HP_0010301"],
                                                      ["10", "HP_0045005"]]}}

        # test when the node has metadata
        updated_graph = self.metadata.adds_node_metadata(graph=graph, edge_dict=edge_dict)

        # verify that new edges were added to the graph
        self.assertTrue(len(updated_graph) > org_graph_len)

        # check that the right number of new edges were added to the graph
        graph_diff = len(updated_graph) - org_graph_len

        new_edges = 0
        for node in ["2", "9", "10"]:
            new_edges += sum([len(x[1].split('|')) for x in self.metadata.node_dict['gene-phenotype'][node].items()])

        self.assertTrue(graph_diff == new_edges)

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
        graph = Graph()
        graph.parse(filename)
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

        # load dictionary
        self.metadata.node_metadata_processor()

        # load graph
        graph = Graph()
        graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')

        # extract metadata
        self.metadata.extracts_class_metadata(graph=graph)

        # check that it worked
        self.assertIn('classes', self.metadata.node_dict.keys())
        self.assertTrue(len(self.metadata.node_dict['classes']) == 1897)
        self.assertIn('Label', self.metadata.node_dict['classes']['SO_0000373'].keys())
        self.assertIn('Synonym', self.metadata.node_dict['classes']['SO_0000373'].keys())
        self.assertIn('Description', self.metadata.node_dict['classes']['SO_0000373'].keys())

        return None

    def test_output_knowledge_graph_metadata(self):
        """Tests the output_knowledge_graph_metadata method."""

        # update environment var
        self.metadata.write_location = ''

        # load dictionary
        self.metadata.node_metadata_processor()

        # load graph
        graph = Graph()
        graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')

        # run function
        self.metadata.output_knowledge_graph_metadata(graph=graph)

        # make sure that node data wrote out
        filename = self.metadata.full_kg[:-6] + 'NodeLabels.txt'
        self.assertTrue(os.path.exists(filename))

        # remove file
        os.remove(self.dir_loc + '/ontologies/so_with_imporNodeLabels.txt')

        return None

    def test_adds_ontology_annotations(self):
        """Tests the adds_ontology_annotations method."""

        # load graph
        graph = Graph()
        graph.parse(self.dir_loc + '/ontologies/so_with_imports.owl')

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
