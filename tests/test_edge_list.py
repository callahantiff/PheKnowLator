import unittest

from pkt_kg.edge_list import CreatesEdgeList


class TestEdgeList(unittest.TestCase):
    """Class to test functions used when processing edge data sources."""

    def setUp(self):

        # create dictionary to store data
        self.edge_data_files = {'chemical-disease': './data/edge_data/chemical-disease_CTD_chemicals_diseases.tsv',
                                'gene-disease': './data/edge_data/gene-disease_curated_gene_disease_associations.tsv'}

        # initialize class
        self.master_edge_list = CreatesEdgeList(data_files=self.edge_data_files, source_file='./data/resource_info.txt')

        return None

    def test_initialization_state(self):
        """Checks that a dictionary is created when CreatesEdgeList class is initialized."""

        # make sure that the source info dict is not none
        self.assertTrue(self.master_edge_list.source_info is not None)

        # make sure type is dict
        self.assertIsInstance(self.master_edge_list.source_info, dict)

        # check dictionary content
        self.assertTrue('chemical-disease' in self.master_edge_list.source_info.keys())
        self.assertTrue('gene-disease' in self.master_edge_list.source_info.keys())
        self.assertFalse('protein-disease' in self.master_edge_list.source_info.keys())

        return None

    def test_identify_header(self):
        """Tests the identify_header method."""

        # test chemical-disease data (no header)
        chem_dis_file = self.edge_data_files['chemical-disease']
        chem_splitter = self.master_edge_list.source_info['chemical-disease']['column_splitter']
        splitter = '\t' if 't' in chem_splitter else " " if ' ' in chem_splitter else chem_splitter

        self.assertIsNone(self.master_edge_list.identify_header(chem_dis_file, splitter))

        # test gene-disease data (header)
        gene_dis_file = self.edge_data_files['gene-disease']
        gene_splitter = self.master_edge_list.source_info['gene-disease']['column_splitter']
        splitter = '\t' if 't' in gene_splitter else " " if ' ' in gene_splitter else gene_splitter

        self.assertEqual(0, self.master_edge_list.identify_header(gene_dis_file, splitter))

        return None
    #
    # def test_data_reader(self):
    #     """Tests the data_reader method."""

