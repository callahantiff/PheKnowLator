import os
import os.path
import shutil
import unittest

from unittest import mock

from pkt_kg import DocumentationMaker


class TestDocumentationMaker(unittest.TestCase):
    """Class to test dependency document builder methods."""

    def setUp(self):

        # create temporary directory to store data for testing
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data/resources')
        self.dir_loc = os.path.abspath(dir_loc)
        os.mkdir(self.dir_loc)

        # initialize class
        self.edge_maker = DocumentationMaker(1, self.dir_loc)

        return None

    @mock.patch('builtins.input', create=True)
    def test_information_getter(self, mocked_input):
        """Tests the information_getter method."""

        mocked_input.side_effect = ['go-gene', 'one', 'go', 'http://purl.obolibrary.org/obo/go.owl', 'class-class',
                                    't', '0;1', 'None', 'None', 'None', 'RO_0000056',
                                    'http://purl.obolibrary.org/obo/', 'http://purl.uniprot.org/geneid/', ':;GO_;',
                                    'http://geneontology.org/gene-associations/goa_human.gaf.gz']

        self.result = self.edge_maker.information_getter()

        # create result dicts
        resource_info = {'go-gene': ':;GO_;|class-class|RO_0000056|http://purl.obolibrary.org/obo/|http://purl'
                                    '.uniprot.org/geneid/|t|0;1|None|None|None'}
        ont_info = {'go': 'http://purl.obolibrary.org/obo/go.owl'}
        edge_info = {'go-gene': 'http://geneontology.org/gene-associations/goa_human.gaf.gz'}

        self.assertEqual(self.result[0], resource_info)
        self.assertEqual(self.result[1], ont_info)
        self.assertEqual(self.result[2], edge_info)

        return None

    @mock.patch('builtins.input', create=True)
    def test_writes_out_document(self, mocked_input):
        """Tests the writes_out_document method."""

        mocked_input.side_effect = ['go-gene', 'one', 'go', 'http://purl.obolibrary.org/obo/go.owl', 'class-class',
                                    'n', 't', '0;1', 'None', 'None', 'None', 'RO_0000056',
                                    'http://purl.obolibrary.org/obo/', 'http://purl.uniprot.org/geneid/', ':;GO_;',
                                    'http://geneontology.org/gene-associations/goa_human.gaf.gz']

        # initialize class
        self.result = self.edge_maker.information_getter()

        # write out ontology data
        self.edge_maker.writes_out_document(self.result[0], '|', 'resource_info.txt')
        self.assertTrue(os.path.exists(self.dir_loc + '/resource_info.txt'))

        # write out ontology data
        self.edge_maker.writes_out_document(self.result[1], ', ', 'ontology_source_list.txt')
        self.assertTrue(os.path.exists(self.dir_loc + '/ontology_source_list.txt'))

        # write out class data
        self.edge_maker.writes_out_document(self.result[2], ', ', 'edge_source_list.txt')
        self.assertTrue(os.path.exists(self.dir_loc + '/edge_source_list.txt'))

        return None

    def tearDown(self):

        # remove temp directory
        shutil.rmtree(self.dir_loc)

        return None
