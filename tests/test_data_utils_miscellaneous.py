import os.path
import pandas
import random
import shutil
import unittest

from tqdm import tqdm
from typing import List

from pkt_kg.utils import *


class TestDataUtilsMisc(unittest.TestCase):
    """Class to test the miscellaneous methods from the data utility script."""

    def setUp(self):

        # create temporary directory to store data for testing
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data/temp')
        self.dir_loc = os.path.abspath(dir_loc)
        os.mkdir(self.dir_loc)

        # set-up pandas data frame
        hgnc_ids = ['HGNC:5', 'HGNC:37133', 'HGNC:24086', 'HGNC:7', 'HGNC:27057']
        hgnc_symbols = ['A1BG', 'A1BG-AS1', 'A1CF', 'A2M', 'A2M-AS1']
        hgnc_synonyms = ['None', 'FLJ23569', 'ACF|ASP|ACF64|ACF65|APOBEC1CF', 'FWP007|S863-7|CPAMD5', 'None']

        self.test_data = pandas.DataFrame(list(zip(hgnc_ids, hgnc_symbols, hgnc_synonyms)),
                                          columns=['hgnc_id', 'hgnc_symbols', 'hgnc_synonyms'])

        # create pandas DataFrame with MeSH identifiers
        url = 'https://www.disgenet.org/static/disgenet_ap1/files/downloads/disease_mappings.tsv.gz'
        data_downloader(url, self.dir_loc + '/')

        # read in small random sample (10% of original size)
        self.disease_data = pandas.read_csv(self.dir_loc + '/disease_mappings.tsv',
                                            header=0,
                                            delimiter='\t',
                                            skiprows=lambda i: i > 0 and random.random() > 0.10)

        # read in genomic data
        self.genomic_id_dict = {"ensembl_gene_id_ENSG00000000003": ["entrez_id_7105",
                                                                    "gene_type_update_protein-coding"],
                                "ensembl_gene_id_ENSG00000000457": ["entrez_id_57147",
                                                                    "gene_type_update_protein-coding"],
                                "entrez_id_57147": ["ensembl_gene_id_ENSG00000000457",
                                                    "gene_type_update_protein-coding"],
                                "entrez_id_7105": ["ensembl_gene_id_ENSG00000000003",
                                                   "gene_type_update_protein-coding"]
                                }

        return None

    def test_explodes_data(self):
        """Tests the explodes_data method."""

        # data frame with string identifiers
        exploded_data = explodes_data(self.test_data.copy(), ['hgnc_synonyms'], '|')

        self.assertIsInstance(exploded_data, pandas.DataFrame)
        self.assertFalse(len(exploded_data) == len(self.test_data))
        self.assertTrue(len(exploded_data) > len(self.test_data))
        self.assertFalse('|' in list(exploded_data['hgnc_synonyms']))

        return None

    def test_genomic_id_mapper(self):
        """Tests the genomic_id_mapper method."""

        # write location
        write_location = self.dir_loc + '/genomic_maps.txt'

        genomic_id_mapper(self.genomic_id_dict, write_location,
                          'ensembl_gene_id', 'entrez_id',
                          'ensembl_gene_type', 'entrez_gene_type',
                          'gene_type_update', 'gene_type_update')
        self.assertTrue(os.path.exists(write_location))

        return None

    def test_outputs_dictionary_data(self):
        """Tests the outputs_dictionary_data method."""

        outputs_dictionary_data(self.genomic_id_dict, self.dir_loc + '/genomic_maps.json')

        self.assertTrue(os.path.exists(self.dir_loc + '/genomic_maps.json'))

        return None

    def test_deduplicates_file(self):
        """Tests the deduplicates_file method when a destination location is not provided."""

        data_dir = os.path.dirname(__file__)
        src_filepath = data_dir + '/data/test_file_2.nt'
        shutil.copy(data_dir + '/data/test_file.nt', src_filepath)
        deduplicates_file(src_filepath)

        # test method
        with open(src_filepath) as f: data = f.readlines()
        self.assertTrue(len(data) == 5)

        # clean up environment
        if os.path.exists(src_filepath): os.remove(src_filepath)

        return None

    def test_merges_files(self):
        """Tests the merges_files method when a destination location is not provided."""

        data_dir = os.path.dirname(__file__)
        filepath1 = data_dir + '/data/INVERSE_RELATIONS.txt'
        filepath2 = data_dir + '/data/RELATIONS_LABELS.txt'
        merge_filepath = data_dir + '/data/MERGED_RELATIONS.txt'
        merges_files(filepath1, filepath2, merge_filepath)

        # test method
        with open(merge_filepath) as f: data = f.readlines()
        self.assertTrue(len(data) == 5)

        # clean up environment
        if os.path.exists(merge_filepath): os.remove(merge_filepath)

        return None

    def tests_sublist_creator_dict(self):
        """Tests the sublist_creator method when the input is a dictionary."""

        actors = {'protein-cell': 75308, 'protein-cofactor': 1994, 'variant-disease': 35686, 'rna-anatomy': 444974,
                  'protein-catalyst': 24311, 'chemical-protein': 64330, 'chemical-gene': 16695, 'protein-gobp': 137926,
                  'protein-pathway': 114807, 'protein-anatomy': 30677, 'chemical-pathway': 28357, 'gene-gene': 23525}
        lists = sublist_creator(actors, 5)

        self.assertIsInstance(lists, List)
        self.assertTrue(len(lists), 5)
        self.assertEqual(lists,
                         [['rna-anatomy'], ['protein-gobp'], ['protein-pathway', 'gene-gene'],
                          ['protein-cell', 'protein-anatomy', 'protein-catalyst', 'protein-cofactor'],
                          ['chemical-protein', 'variant-disease', 'chemical-pathway', 'chemical-gene']])

        return None

    def tests_sublist_creator_list(self):
        """Tests the sublist_creator method when the input is a dictionary."""

        actors = [75308, 1994, 35686, 444974, 24311, 64330, 16695, 137926, 114807, 30677, 28357, 23525]
        lists = sublist_creator(actors, 5)

        self.assertIsInstance(lists, List)
        self.assertTrue(len(lists), 5)
        self.assertEqual(lists,
                         [[444974], [137926], [114807, 23525],
                          [75308, 30677, 24311, 1994], [64330, 35686, 28357, 16695]])

        return None

    def tests_obtains_entity_url_good(self):
        """Tests the obtains_entity_url method when a valid prefix and identifier are passed."""

        # set-up input
        prefix = 'chebi'; identifier = '138488'

        # test function
        entity_uri = obtains_entity_url(prefix, identifier)
        self.assertEqual(entity_uri, 'https://bioregistry.io/chebi:138488')

        return None

    def tests_obtains_entity_url_bad(self):
        """Tests the obtains_entity_url method when an invalid identifier is passed."""

        # set-up input
        prefix = 'chebi'; identifier = 't'

        # test function
        self.assertRaises(ValueError, obtains_entity_url, prefix, identifier)

        return None

    def tearDown(self):

        # remove temp directory
        shutil.rmtree(self.dir_loc)

        return None
