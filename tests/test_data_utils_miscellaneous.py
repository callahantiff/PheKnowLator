import os.path
import pandas
import random
import shutil
import unittest

from tqdm import tqdm

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

        genomic_id_mapper(self.genomic_id_dict, write_location, 'ensembl_gene_id', 'entrez_id', 'gene_type_update')
        self.assertTrue(os.path.exists(write_location))

        return None

    def test_outputs_dictionary_data(self):
        """Tests the outputs_dictionary_data method."""

        outputs_dictionary_data(self.genomic_id_dict, self.dir_loc + '/genomic_maps.json')

        self.assertTrue(os.path.exists(self.dir_loc + '/genomic_maps.json'))

        return None

    def tearDown(self):

        # remove temp directory
        shutil.rmtree(self.dir_loc)

        return None
