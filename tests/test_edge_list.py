import os.path
import pandas
import re
import unittest

from typing import Tuple

from pkt_kg.edge_list import CreatesEdgeList


class TestCreatesEdgeList(unittest.TestCase):
    """Class to test functions used when processing edge data sources."""

    def setUp(self):

        # initialize file location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data')
        self.dir_loc = os.path.abspath(dir_loc)

        # create dictionary to store data
        file_loc1 = self.dir_loc + '/edge_data/chemical-disease_CTD_chemicals_diseases.tsv'
        file_loc2 = self.dir_loc + '/edge_data/gene-disease_curated_gene_disease_associations.tsv'
        self.edge_data_files = {'chemical-disease': file_loc1, 'gene-disease': file_loc2}

        # initialize class
        file_loc = self.dir_loc + '/resource_info.txt'
        self.master_edge_list = CreatesEdgeList(data_files=self.edge_data_files, source_file=file_loc)

        # edge type 1
        mapping_data1 = '0:' + self.dir_loc + '/MESH_CHEBI_MAP.txt' + ';1:' + self.dir_loc + '/DISEASE_DOID_MAP.txt'
        self.master_edge_list.source_info['chemical-disease']['identifier_maps'] = mapping_data1

        # edge type 2
        mapping_data2 = '1:' + self.dir_loc + '/DISEASE_DOID_MAP.txt'
        self.master_edge_list.source_info['gene-disease']['identifier_maps'] = mapping_data2

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
        delimiter1 = self.master_edge_list.source_info['chemical-disease']['delimiter']
        splitter = '\t' if 't' in delimiter1 else " " if ' ' in delimiter1 else delimiter1
        skip_rows = list(range(27)) + [29]

        self.assertEqual(0, self.master_edge_list.identify_header(chem_dis_file, splitter, skip_rows))

        # test gene-disease data (header)
        gene_dis_file = self.edge_data_files['gene-disease']
        delimiter2 = self.master_edge_list.source_info['gene-disease']['delimiter']
        splitter = '\t' if 't' in delimiter2 else " " if ' ' in delimitir2 else delimiter2

        self.assertEqual(0, self.master_edge_list.identify_header(gene_dis_file, splitter, []))

        return None

    def test_data_reader(self):
        """Tests the data_reader method."""

        # set up input variables
        file_path1 = self.edge_data_files['chemical-disease']
        delimiter1 = self.master_edge_list.source_info['chemical-disease']['delimiter']

        file_path2 = self.edge_data_files['gene-disease']
        delimiter2 = self.master_edge_list.source_info['gene-disease']['delimiter']

        # read in data set 1
        data1 = self.master_edge_list.data_reader(file_path1, delimiter1)
        self.assertIsInstance(data1, pandas.DataFrame)

        # read in data set 2
        data2 = self.master_edge_list.data_reader(file_path2, delimiter2)
        self.assertIsInstance(data2, pandas.DataFrame)

        return None

    def test_filter_fixer(self):
        """Tests the filter_fixer method."""

        test_string1 = '5;!=;'
        self.assertEqual('5;!=;None', self.master_edge_list.filter_fixer(test_string1))

        test_string2 = '5;!=;" '
        self.assertEqual('5;!=;None', self.master_edge_list.filter_fixer(test_string2))

        test_string3 = '5;!=;2'
        self.assertEqual('5;!=;2', self.master_edge_list.filter_fixer(test_string3))

        return None

    def test_filter_data(self):
        """Tests the filter_data method."""

        # data set 1
        file_path1 = self.edge_data_files['chemical-disease']
        delimiter1 = self.master_edge_list.source_info['chemical-disease']['delimiter']
        edge_data1 = self.master_edge_list.data_reader(file_path1, delimiter1)
        evidence1 = self.master_edge_list.source_info['chemical-disease']['evidence_criteria']
        filtering1 = self.master_edge_list.source_info['chemical-disease']['filter_criteria']

        # read in filtered data
        filtered_data1 = self.master_edge_list.filter_data(edge_data1, evidence1, filtering1)
        self.assertIsInstance(filtered_data1, pandas.DataFrame)
        self.assertTrue(len(edge_data1) > len(filtered_data1))

        # data set 2
        file_path2 = self.edge_data_files['gene-disease']
        delimiter2 = self.master_edge_list.source_info['gene-disease']['delimiter']
        edge_data2 = self.master_edge_list.data_reader(file_path2, delimiter2)
        evidence2 = self.master_edge_list.source_info['gene-disease']['evidence_criteria']
        filtering2 = self.master_edge_list.source_info['gene-disease']['filter_criteria']

        # read in filtered data
        filtered_data2 = self.master_edge_list.filter_data(edge_data2, evidence2, filtering2)
        self.assertIsInstance(filtered_data2, pandas.DataFrame)
        self.assertTrue(len(edge_data2) > len(filtered_data2))

        return None

    def test_data_reducer(self):
        """Tests the data_reducer method."""

        # data set 1
        file_path1 = self.edge_data_files['chemical-disease']
        delimiter1 = self.master_edge_list.source_info['chemical-disease']['delimiter']
        edge_data1 = self.master_edge_list.data_reader(file_path1, delimiter1)
        col1 = self.master_edge_list.source_info['chemical-disease']['column_idx']

        self.assertEqual(len(col1.split(';')), 2)

        # read in filtered data
        reduced_data1 = self.master_edge_list.data_reducer(col1, edge_data1)
        self.assertIsInstance(reduced_data1, pandas.DataFrame)
        self.assertTrue(len(reduced_data1.columns), 2)

        # data set 2
        file_path2 = self.edge_data_files['gene-disease']
        delimiter2 = self.master_edge_list.source_info['gene-disease']['delimiter']
        edge_data2 = self.master_edge_list.data_reader(file_path2, delimiter2)
        col2 = self.master_edge_list.source_info['gene-disease']['column_idx']

        self.assertEqual(len(col2.split(';')), 2)

        # read in filtered data
        reduced_data2 = self.master_edge_list.data_reducer(col2, edge_data2)
        self.assertIsInstance(reduced_data2, pandas.DataFrame)
        self.assertTrue(len(reduced_data2.columns), 2)

        return None

    def tests_label_formatter(self):
        """Tests label_formatter method."""

        # data set 1
        file_path1 = self.edge_data_files['chemical-disease']
        delimiter1 = self.master_edge_list.source_info['chemical-disease']['delimiter']
        edge_data1 = self.master_edge_list.data_reader(file_path1, delimiter1)
        label_criteria1 = self.master_edge_list.source_info['chemical-disease']['source_labels']

        # read in filtered data
        labeled_data1 = self.master_edge_list.label_formatter(edge_data1, label_criteria1)
        self.assertIsInstance(labeled_data1, pandas.DataFrame)
        self.assertTrue(all(x for x in labeled_data1['DiseaseID'] if x.startswith(label_criteria1.split(';')[1][:-1])))
        self.assertEqual(list(edge_data1['ChemicalID']), list(labeled_data1['ChemicalID']))

        # data set 2
        file_path2 = self.edge_data_files['gene-disease']
        delimiter2 = self.master_edge_list.source_info['gene-disease']['delimiter']
        edge_data2 = self.master_edge_list.data_reader(file_path2, delimiter2)
        label_criteria2 = self.master_edge_list.source_info['gene-disease']['source_labels']

        # read in filtered data
        labeled_data2 = self.master_edge_list.label_formatter(edge_data2, label_criteria2)
        self.assertIsInstance(labeled_data2, pandas.DataFrame)
        self.assertEqual(list(edge_data2['geneId']), list(labeled_data2['geneId']))
        self.assertEqual(list(edge_data2['diseaseId']), list(labeled_data2['diseaseId']))

        return None

    def tests_data_merger(self):
        """Tests the data_merger method."""

        # data set 1
        file_path1 = self.edge_data_files['chemical-disease']
        delimiter1 = self.master_edge_list.source_info['chemical-disease']['delimiter']
        edge_data1 = self.master_edge_list.data_reader(file_path1, delimiter1)

        # reduce data
        col1 = self.master_edge_list.source_info['chemical-disease']['column_idx']
        reduced_data1 = self.master_edge_list.data_reducer(col1, edge_data1)

        # relabel data
        label_criteria1 = self.master_edge_list.source_info['chemical-disease']['source_labels']
        labeled_data1 = self.master_edge_list.label_formatter(reduced_data1, label_criteria1)

        # mapping data
        mapping_data1 = '0:' + self.dir_loc + '/MESH_CHEBI_MAP.txt' + ';1:' + self.dir_loc + '/DISEASE_DOID_MAP.txt'
        merged_data1_col1 = self.master_edge_list.data_merger(0, mapping_data1, labeled_data1)
        self.assertIsInstance(merged_data1_col1, list)
        self.assertIsInstance(merged_data1_col1[0], str)
        self.assertIsInstance(merged_data1_col1[1], pandas.DataFrame)

        merged_data1_col2 = self.master_edge_list.data_merger(1, mapping_data1, labeled_data1)
        self.assertIsInstance(merged_data1_col2, list)
        self.assertIsInstance(merged_data1_col2[0], str)
        self.assertIsInstance(merged_data1_col2[1], pandas.DataFrame)

        # data set 2
        file_path2 = self.edge_data_files['gene-disease']
        delimiter2 = self.master_edge_list.source_info['gene-disease']['delimiter']
        edge_data2 = self.master_edge_list.data_reader(file_path2, delimiter2)

        # reduce data
        col2 = self.master_edge_list.source_info['gene-disease']['column_idx']
        reduced_data2 = self.master_edge_list.data_reducer(col2, edge_data2)

        # relabel data
        label_criteria2 = self.master_edge_list.source_info['gene-disease']['source_labels']
        labeled_data2 = self.master_edge_list.label_formatter(reduced_data2, label_criteria2)

        # mapping data
        mapping_data2 = '1:' + self.dir_loc + '/DISEASE_DOID_MAP.txt'
        merged_data2_col2 = self.master_edge_list.data_merger(1, mapping_data2, labeled_data2)
        self.assertIsInstance(merged_data2_col2, list)
        self.assertIsInstance(merged_data2_col2[0], str)
        self.assertIsInstance(merged_data2_col2[1], pandas.DataFrame)

        return None

    def tests_process_mapping_data(self):
        """Tests the process_mapping_data method."""

        # data set 1
        file_path1 = self.edge_data_files['chemical-disease']
        delimiter1 = self.master_edge_list.source_info['chemical-disease']['delimiter']
        edge_data1 = self.master_edge_list.data_reader(file_path1, delimiter1)

        # reduce data
        col1 = self.master_edge_list.source_info['chemical-disease']['column_idx']
        reduced_data1 = self.master_edge_list.data_reducer(col1, edge_data1)

        # relabel data
        label_criteria1 = self.master_edge_list.source_info['chemical-disease']['source_labels']
        labeled_data1 = self.master_edge_list.label_formatter(reduced_data1, label_criteria1)

        # mapping data
        mapping_data1 = self.master_edge_list.source_info['chemical-disease']['identifier_maps']
        process_mapping_data1 = self.master_edge_list.process_mapping_data(mapping_data1, labeled_data1)
        self.assertIsInstance(process_mapping_data1, Tuple)
        self.assertIsInstance(process_mapping_data1[0], Tuple)
        self.assertEqual(5, len(process_mapping_data1))
        self.assertIn(('CHEBI_8093', 'DOID_2841'), process_mapping_data1)

        # data set 2
        file_path2 = self.edge_data_files['gene-disease']
        delimiter2 = self.master_edge_list.source_info['gene-disease']['delimiter']
        edge_data2 = self.master_edge_list.data_reader(file_path2, delimiter2)

        # reduce data
        col2 = self.master_edge_list.source_info['gene-disease']['column_idx']
        reduced_data2 = self.master_edge_list.data_reducer(col2, edge_data2)

        # relabel data
        label_criteria2 = self.master_edge_list.source_info['gene-disease']['source_labels']
        labeled_data2 = self.master_edge_list.label_formatter(reduced_data2, label_criteria2)

        # mapping data
        mapping_data2 = self.master_edge_list.source_info['gene-disease']['identifier_maps']
        process_mapping_data2 = self.master_edge_list.process_mapping_data(mapping_data2, labeled_data2)
        self.assertIsInstance(process_mapping_data2, Tuple)
        self.assertIsInstance(process_mapping_data2[0], Tuple)
        self.assertEqual(23, len(process_mapping_data2))
        self.assertIn(('19', 'DOID_1936'), process_mapping_data2)

        return None

    def tests_creates_knowledge_graph_edges(self):
        """Tests creates_knowledge_graph_edges method."""

        self.master_edge_list.creates_knowledge_graph_edges()

        # edge type 1
        self.assertIsInstance(self.master_edge_list.source_info['chemical-disease']['edge_list'], Tuple)
        self.assertEqual(0, len(self.master_edge_list.source_info['chemical-disease']['edge_list']))

        # edge type 2
        self.assertIsInstance(self.master_edge_list.source_info['gene-disease']['edge_list'], Tuple)
        self.assertIsInstance(self.master_edge_list.source_info['gene-disease']['edge_list'][0], Tuple)
        self.assertEqual(5, len(self.master_edge_list.source_info['gene-disease']['edge_list']))
        self.assertIn(('19', 'DOID_1936'), self.master_edge_list.source_info['gene-disease']['edge_list'])

        return None
