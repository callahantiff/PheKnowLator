import os.path
import pandas
import unittest

from typing import Generator

from pkt.utils import *


class TestDataUtilsMetadata(unittest.TestCase):
    """Class to test the metadata processing methods from the data utility script."""

    def setUp(self):
        # generate mock dictionary to check mapping functions
        self.metadata_dictionary = {'1': {'Label': 'A1BG',
                                          'Description': "A1BG has locus group 'protein-coding' and is located on "
                                                         "chromosome 19 (map_location: 19q13.43).",
                                          'Synonym': 'A1B|HYST2477alpha-1B-glycoprotein|HEL-S-163pA|ABG|epididymis'
                                                     'secretory sperm binding protein Li 163pA|GAB'},
                                    '2': {'Label': 'A2M',
                                          'Description': "A2M has locus group 'protein-coding' and is located on "
                                                         "chromosome 12 (map_location: 12p13.31).",
                                          'Synonym': 'C3 and PZP-like alpha-2-macroglobulin domain-containing protein '
                                                     '5|alpha-2-M|FWP007|A2MD|CPAMD5|S863-7alpha-2-macroglobulin'},
                                    '3': {'Label': 'A2MP1',
                                          'Description': "A2MP1 has locus group 'pseudo' and is located on chromosome "
                                                         "12 (map_location: 12p13.31).",
                                          'Synonym': 'A2MPpregnancy-zone protein pseudogene'},
                                    '9': {'Label': 'NAT1',
                                          'Description': "NAT1 has locus group 'protein-coding' and is located on "
                                                         "chromosome 8 (map_location: 8p22).",
                                          'Synonym': 'N-acetyltransferase type 1|arylamide acetylase '
                                                     '1|MNAT|NAT-1|NATIarylamine N-acetyltransferase 1|monomorphic '
                                                     'arylamine N-acetyltransferase|AAC1|N-acetyltransferase 1 ('
                                                     'arylamine N-acetyltransferase)'},
                                    '10': {'Label': 'NAT2',
                                           'Description': "NAT2 has locus group 'protein-coding' and is located on "
                                                          "chromosome 8 (map_location: 8p22).",
                                           'Synonym': 'NAT-2|N-acetyltransferase 2 (arylamine '
                                                      'N-acetyltransferase)|N-acetyltransferase type 2|PNATarylamine '
                                                      'N-acetyltransferase 2|arylamide acetylase 2|AAC2'}
                                    }

        # generate mock id lists
        self.gene_ids = list(self.metadata_dictionary.keys())
        self.pathway_ids = ['R-HSA-1430728', 'R-HSA-196854', 'R-HSA-6806664', 'R-HSA-2173782', 'R-HSA-166663']

        return None

    def test_chunks(self):
        """Tests the chunks method."""

        # ensure the right type of object is returned
        list1 = list(range(20))
        chunk1 = chunks(list1, 4)
        chunk1_list = list(chunk1).copy()

        self.assertIsInstance(chunk1, Generator)
        self.assertEqual(5, len(chunk1_list))
        self.assertIn(list(range(4)), chunk1_list)

        # check list with odd number of chunks
        list2 = list(range(20))
        chunk2 = chunks(list2, 3)
        chunk2_list = list(chunk2).copy()

        self.assertEqual(7, len(chunk2_list))
        self.assertIn(list(range(3)), chunk2_list)
        self.assertTrue(all(len(x) == 3 for x in chunk2_list[:-1]))
        self.assertEqual(2, len(chunk2_list[-1]))

        return None

    def test_metadata_dictionary_mapper(self):
        """Tests metadata_dictionary_mapper method."""

        # generate metadata dictionary df
        metadata = metadata_dictionary_mapper(self.gene_ids, self.metadata_dictionary)

        self.assertIsInstance(metadata, pandas.DataFrame)
        self.assertTrue(all(x for x in list(metadata['ID']) if x in self.gene_ids))
        self.assertEqual(['ID', 'Label', 'Description', 'Synonym'], list(metadata.columns))

        return None

    def test_metadata_api_mapper(self):
        """Tests metadata_api_mapper method."""

        metadata = metadata_api_mapper(self.pathway_ids)

        self.assertIsInstance(metadata, pandas.DataFrame)
        self.assertTrue(all(x for x in list(metadata['ID']) if x in self.pathway_ids))
        self.assertEqual(['ID', 'Label', 'Description', 'Synonym'], list(metadata.columns))

        return None
