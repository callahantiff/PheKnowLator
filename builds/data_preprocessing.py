#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import fnmatch
import glob
import itertools
import logging.config
import networkx  # type: ignore
import numpy  # type: ignore
import os
import pandas  # type: ignore
import pickle
import sys

from google.cloud import storage  # type: ignore
from rdflib import Graph, Namespace, URIRef  # type: ignore
from rdflib.namespace import RDFS  # type: ignore
from reactome2py import content  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Dict, List, Optional, Tuple, Union

# import script containing helper functions
from builds.build_utilities import *
from pkt_kg.utils import *

# set environment variables
log_dir, log, log_config = 'builds/logs', 'pkt_builder_phases12_log.log', glob.glob('**/logging.ini', recursive=True)
try:
    if not os.path.exists(log_dir): os.mkdir(log_dir)
except FileNotFoundError:
    log_dir, log_config = '../builds/logs', glob.glob('../builds/logging.ini', recursive=True)
    if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


class DataPreprocessing(object):
    """Class provides a container for the data preprocessing methods, original housed in the Data_Preprocessing.ipynb
    Jupyter Notebook. See notebook (https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb) for
    more detailed descriptions of each processed data source and the rationale behind the different filtering and
    processing approaches.

    Companion Notebook: https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/Data_Preparation.ipynb

    Attributes:
        gcs_bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        org_data: A string specifying the location of the original_data directory for a specific build.
        processed_data: A string specifying the location of the original_data directory for a specific build.
        temp_dir: A string specifying a temporary directory to use while processing data locally.
    """

    def __init__(self, gcs_bucket: storage.bucket.Bucket, org_data: str, processed_data: str, temp_dir: str) -> None:

        # GOOGLE CLOUD STORAGE VARIABLES
        self.bucket: storage.bucket.Bucket = gcs_bucket
        self.original_data: str = org_data
        self.processed_data: str = processed_data
        self.log_location = 'temp_build_inprogress/'  # directory for storing logs
        # SETTING LOCAL VARIABLES
        self.temp_dir = temp_dir
        self.owltools_location = './builds/owltools'
        # self.owltools_location = './pkt_kg/libs/owltools'
        # OTHER CLASS VARIABLES
        self.genomic_type_mapper: Dict = {}

    def reads_gcs_bucket_data_to_df(self, f_name: str, delm: str, skip: int = 0,
                                    head: Optional[Union[int, List]] = None,
                                    sht: Optional[Union[int, str]] = None) -> pandas.DataFrame:
        """Reads data corresponding to the input file_location variable into a Pandas DataFrame.

        Args:
            f_name: A string containing the name of file that exists in a Google Cloud Storage bucket.
            delm: A string specifying a file delimiter.
            skip: An integer specifying the number of rows to skip when reading in the data.
            head: An integer specifying the header row, None for no header or a list of header names.
            sht: Used for reading xlsx files. If not None, an integer or string specifying which sheet to read in.

        Returns:
             df: A Pandas DataFrame object containing data read from a Google Cloud Storage bucket.
        """

        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)

        if not isinstance(head, List):
            if sht is not None:
                df = pandas.read_excel(x, sheet_name=sht, header=head, skiprows=skip, engine='openpyxl')
            else:
                df = pandas.read_csv(x, header=head, delimiter=delm, skiprows=skip, low_memory=0)
        else:
            if sht is not None:
                df = pandas.read_excel(x, sheet_name=sht, header=None, names=head, skiprows=skip, engine='openpyxl')
            else:
                df = pandas.read_csv(x, header=None, names=head, delimiter=delm, skiprows=skip, low_memory=0)

        return df

    def _loads_genomic_typing_dictionary(self) -> None:
        """Downloads and loads genomic typing dictionary needed to process the genomic identifier data. This
        dictionary object is keyed by specific column names in each genomic identifier Pandas DataFrame and has
        values which are a dictionary where keys are values in the specific column and values are a new string to
        translate that key string into.

        Returns:
            None.
        """

        logger.info('Loading Genomic Typing Dictionary')

        f_name = 'genomic_typing_dict.pkl'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        self.genomic_type_mapper = pickle.load(open(x, 'rb'))

        return None

    def _preprocess_hgnc_data(self) -> pandas.DataFrame:
        """Processes HGNC data in order to prepare it for combination with other gene identifier data sources. Data
        needs to be lightly cleaned before it can be merged with other data. This light cleaning includes renaming
        columns, replacing NaN with None, updating data types (i.e. making all columns type str), and unnesting '|'-
        delimited data. The final step is to update the gene_type variable such that each of the variable values is
        re-grouped to be protein-coding, other or ncRNA.

        Returns:
            explode_df_hgnc: A Pandas DataFrame containing processed and filtered data.
        """

        logger.info('Preprocessing HGNC Data')

        hgnc = self.reads_gcs_bucket_data_to_df(f_name='hgnc_complete_set.txt', delm='\t', skip=0, head=0, sht=None)
        hgnc = hgnc.loc[hgnc['status'].apply(lambda x: x == 'Approved')]
        hgnc = hgnc[['hgnc_id', 'entrez_id', 'ensembl_gene_id', 'uniprot_ids', 'symbol', 'locus_type', 'alias_symbol',
                     'name', 'location', 'alias_name']]
        hgnc.rename(columns={'uniprot_ids': 'uniprot_id', 'location': 'map_location', 'locus_type': 'hgnc_gene_type'},
                    inplace=True)
        hgnc['hgnc_id'].replace('.*\:', '', inplace=True, regex=True)  # strip 'HGNC' off of the identifiers
        hgnc.fillna('None', inplace=True)  # replace NaN with 'None'
        hgnc['entrez_id'] = hgnc['entrez_id'].apply(lambda x: str(int(x)) if x != 'None' else 'None')  # make col str
        # combine certain columns into single column
        hgnc['name'] = hgnc['name'] + '|' + hgnc['alias_name']
        hgnc['synonyms'] = hgnc['alias_symbol'] + '|' + hgnc['alias_name'] + '|' + hgnc['name']
        hgnc['symbol'] = hgnc['symbol'] + '|' + hgnc['alias_symbol']
        # explode nested data and reformat values in preparation for combining it with other gene identifiers
        explode_df_hgnc = explodes_data(hgnc.copy(), ['ensembl_gene_id', 'uniprot_id', 'symbol',
                                                      'name', 'synonyms'], '|')
        # reformat hgnc gene type
        for v in self.genomic_type_mapper['hgnc_gene_type'].keys():
            explode_df_hgnc['hgnc_gene_type'].replace(v, self.genomic_type_mapper['hgnc_gene_type'][v], inplace=True)
        # reformat master hgnc gene type
        explode_df_hgnc['master_gene_type'] = explode_df_hgnc['hgnc_gene_type']
        master_dict = self.genomic_type_mapper['hgnc_master_gene_type']
        for val in master_dict.keys():
            explode_df_hgnc['master_gene_type'].replace(val, master_dict[val], inplace=True)
        # post-process reformatted data
        explode_df_hgnc.drop(['alias_symbol', 'alias_name'], axis=1, inplace=True)  # remove original gene type column
        explode_df_hgnc.drop_duplicates(subset=None, keep='first', inplace=True)

        return explode_df_hgnc

    def _preprocess_ensembl_data(self) -> pandas.DataFrame:
        """Processes Ensembl data in order to prepare it for combination with other gene identifier data sources. Data
        needs to be reformatted in order for it to be able to be merged with the other gene, RNA, and protein identifier
        data. To do this, we iterate over each row of the data and extract the fields shown below in column_names,
        making each of these extracted fields their own column. The final step is to update the gene_type variable such
        that each of the variable values is re-grouped to be protein-coding, other or ncRNA.

        Returns:
            ensembl_geneset: A Pandas DataFrame containing processed and filtered data.
        """

        logger.info('Preprocessing Ensembl Data')

        f_name = 'Homo_sapiens.GRCh38.*.gtf'
        ensembl = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=5, head=None, sht=None)
        ensembl_data = list(ensembl[8]); ensembl_df_data = []
        for i in tqdm(range(0, len(ensembl_data))):
            if 'gene_id' in ensembl_data[i] and 'transcript_id' in ensembl_data[i]:
                row = {x.split(' "')[0].lstrip(): x.split(' "')[1].strip('"') for x in ensembl_data[i].split(';')[0:-1]}
                ensembl_df_data += [(row['gene_id'], row['transcript_id'], row['gene_name'], row['gene_biotype'],
                                     row['transcript_name'], row['transcript_biotype'])]
        # convert to data frame
        ensembl_geneset = pandas.DataFrame(ensembl_df_data,
                                           columns=['ensembl_gene_id', 'transcript_stable_id', 'symbol',
                                                    'ensembl_gene_type', 'transcript_name', 'ensembl_transcript_type'])
        # reformat ensembl gene type
        gene_dict = self.genomic_type_mapper['ensembl_gene_type']
        for val in gene_dict.keys(): ensembl_geneset['ensembl_gene_type'].replace(val, gene_dict[val], inplace=True)
        # reformat master gene type
        ensembl_geneset['master_gene_type'] = ensembl_geneset['ensembl_gene_type']
        gene_dict = self.genomic_type_mapper['ensembl_master_gene_type']
        for val in gene_dict.keys(): ensembl_geneset['master_gene_type'].replace(val, gene_dict[val], inplace=True)
        # reformat master transcript type
        ensembl_geneset['ensembl_transcript_type'].replace('vault_RNA', 'vaultRNA', inplace=True, regex=False)
        ensembl_geneset['master_transcript_type'] = ensembl_geneset['ensembl_transcript_type']
        trans_d = self.genomic_type_mapper['ensembl_master_transcript_type']
        for val in trans_d.keys(): ensembl_geneset['master_transcript_type'].replace(val, trans_d[val], inplace=True)
        # post-process reformatted data
        ensembl_geneset.drop_duplicates(subset=None, keep='first', inplace=True)

        return ensembl_geneset

    def merges_ensembl_mapping_data(self) -> pandas.DataFrame:
        """Processes Ensembl uniprot and entrez mapping data in order to prepare it for combination with other gene
        identifier data sources. After merging the annotation together the main gene data is merged with the
        annotation data. The cleaned Ensembl data is saved so that it can be used when generating node metadata for
        transcript identifiers.

        Returns:
            ensembl: A Pandas DataFrame containing processed and filtered data that has been merged with
                additional annotation  mapping data from uniprot and entrez.
        """

        logger.info('Merging Ensembl Annotation Data')

        drop_cols = ['db_name', 'info_type', 'source_identity', 'xref_identity', 'linkage_type']
        un_name, ent_name = 'Homo_sapiens.GRCh38.*.uniprot.tsv', 'Homo_sapiens.GRCh38.*.entrez.tsv'
        # uniprot data
        ensembl_uniprot = self.reads_gcs_bucket_data_to_df(f_name=un_name, delm='\t', skip=0, head=0, sht=None)
        ensembl_uniprot.rename(columns={'xref': 'uniprot_id', 'gene_stable_id': 'ensembl_gene_id'}, inplace=True)
        ensembl_uniprot.replace('-', 'None', inplace=True); ensembl_uniprot.fillna('None', inplace=True)
        ensembl_uniprot = ensembl_uniprot.loc[ensembl_uniprot['uniprot_id'].apply(lambda x: '-' not in x)]
        ensembl_uniprot = ensembl_uniprot.loc[ensembl_uniprot['info_type'].apply(lambda x: x == 'DIRECT')]
        ensembl_uniprot = ensembl_uniprot.loc[ensembl_uniprot['xref_identity'].apply(lambda x: x != 'None')]
        # ensembl_uniprot['master_gene_type'] = ['protein-coding'] * len(ensembl_uniprot)
        # ensembl_uniprot['master_transcript_type'] = ['protein-coding'] * len(ensembl_uniprot)
        ensembl_uniprot.drop(drop_cols, axis=1, inplace=True)
        ensembl_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)
        # entrez data
        ensembl_entrez = self.reads_gcs_bucket_data_to_df(f_name=ent_name, delm='\t', skip=0, head=0, sht=None)
        ensembl_entrez.rename(columns={'xref': 'entrez_id', 'gene_stable_id': 'ensembl_gene_id'}, inplace=True)
        ensembl_entrez = ensembl_entrez.loc[ensembl_entrez['db_name'].apply(lambda x: x == 'EntrezGene')]
        ensembl_entrez = ensembl_entrez.loc[ensembl_entrez['info_type'].apply(lambda x: x == 'DEPENDENT')]
        ensembl_entrez.replace('-', 'None', inplace=True); ensembl_entrez.fillna('None', inplace=True)
        ensembl_entrez.drop_duplicates(subset=None, keep='first', inplace=True)
        ensembl_entrez.drop(drop_cols, axis=1, inplace=True)
        # merge annotation data
        merge_cols = list(set(ensembl_entrez).intersection(set(ensembl_uniprot)))
        ensembl_annot = pandas.merge(ensembl_uniprot, ensembl_entrez, on=merge_cols, how='outer')
        ensembl_annot.fillna('None', inplace=True)
        ensembl_annot.drop_duplicates(subset=None, keep='first', inplace=True)
        # merge annotation and gene data
        ensembl_geneset = self._preprocess_ensembl_data()
        merge_cols = list(set(ensembl_annot).intersection(set(ensembl_geneset)))
        ensembl = pandas.merge(ensembl_geneset, ensembl_annot, on=merge_cols, how='outer')
        ensembl.fillna('None', inplace=True); ensembl.replace('NA', 'None', inplace=True, regex=False)
        ensembl.drop_duplicates(subset=None, keep='first', inplace=True)
        # save data locally and push to gcs bucket
        filename = 'ensembl_identifier_data_cleaned.txt'
        ensembl.to_csv(self.temp_dir + '/' + filename, header=True, sep='\t', index=False)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return ensembl

    def _preprocess_uniprot_data(self) -> pandas.DataFrame:
        """Processes Uniprot data in order to prepare it for combination with other gene identifier data sources. The
        reformatting performed on the data includes removing unnecessary columns and reformatting column values to a
        common set of terms that will be applied universally to all gene and protein identifier data sources.

        Returns:
            explode_df_uniprot: A Pandas DataFrame containing processed and filtered data.
        """

        logger.info('Preprocessing UniProt Data')

        f_name = 'uniprot_identifier_mapping.tab'
        uniprot = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=0, head=0, sht=None)
        uniprot.fillna('None', inplace=True)
        uniprot.rename(columns={'Entry': 'uniprot_id', 'Cross-reference (GeneID)': 'entrez_id',
                                'Ensembl transcript': 'transcript_stable_id', 'Cross-reference (HGNC)': 'hgnc_id',
                                'Gene names  (synonym )': 'synonyms', 'Gene names  (primary )': 'symbol'}, inplace=True)
        uniprot['synonyms'] = uniprot['synonyms'].apply(lambda x: '|'.join(x.split()) if x.isupper() else x)
        uniprot = uniprot.loc[uniprot['Status'].apply(lambda x: x != 'unreviewed')]  # keeping only reviewed entries

        # explode nested data and perform light value reformatting
        explode_df_uniprot = explodes_data(uniprot.copy(), ['transcript_stable_id', 'entrez_id', 'hgnc_id'], ';')
        explode_df_uniprot = explodes_data(explode_df_uniprot.copy(), ['symbol', 'synonyms'], '|')
        explode_df_uniprot['transcript_stable_id'].replace('\s.*', '', inplace=True, regex=True)  # strip uniprot names
        explode_df_uniprot.drop(['Status'], axis=1, inplace=True)
        explode_df_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)

        return explode_df_uniprot

    def _preprocess_ncbi_data(self) -> pandas.DataFrame:
        """Processes NCBI Gene data in order to prepare it for combination with other gene identifier data sources.
        Data needs to be lightly cleaned before it can be merged with other data. This light cleaning includes renaming
        columns, replacing NaN with None, updating data types (i.e. making all columns type str), and unnesting '|'-
        delimited data. Then, the gene_type variable is cleaned such that each of the variable's values are re-grouped
        to be protein-coding, other or ncRNA.

        Returns:
            explode_df_ncbi_gene: A Pandas DataFrame containing processed and filtered data.
        """

        logger.info('Preprocessing Entrez Data')

        f_name = 'Homo_sapiens.gene_info'
        ncbi_gene = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=0, head=0, sht=None)
        ncbi_gene = ncbi_gene.loc[ncbi_gene['#tax_id'].apply(lambda x: x == 9606)]  # remove non-human rows
        ncbi_gene.replace('-', 'None', inplace=True)
        ncbi_gene.rename(columns={'GeneID': 'entrez_id', 'Symbol': 'symbol', 'Synonyms': 'synonyms'}, inplace=True)
        ncbi_gene['synonyms'] = ncbi_gene['synonyms'] + '|' + ncbi_gene['description'] + '|' + ncbi_gene[
            'Full_name_from_nomenclature_authority'] + '|' + ncbi_gene['Other_designations']
        ncbi_gene['symbol'] = ncbi_gene['Symbol_from_nomenclature_authority'] + '|' + ncbi_gene['symbol']
        ncbi_gene['name'] = ncbi_gene['Full_name_from_nomenclature_authority'] + '|' + ncbi_gene['description']
        # explode nested data
        explode_df_ncbi_gene = explodes_data(ncbi_gene.copy(), ['symbol', 'synonyms', 'name', 'dbXrefs'], '|')

        # clean up results
        explode_df_ncbi_gene['entrez_id'] = explode_df_ncbi_gene['entrez_id'].astype(str)
        explode_df_ncbi_gene = explode_df_ncbi_gene.loc[
            explode_df_ncbi_gene['dbXrefs'].apply(lambda x: x.split(':')[0] in ['Ensembl', 'HGNC', 'IMGT/GENE-DB'])]
        explode_df_ncbi_gene['hgnc_id'] = explode_df_ncbi_gene['dbXrefs'].loc[
            explode_df_ncbi_gene['dbXrefs'].apply(lambda x: x.startswith('HGNC'))]
        explode_df_ncbi_gene['ensembl_gene_id'] = explode_df_ncbi_gene['dbXrefs'].loc[
            explode_df_ncbi_gene['dbXrefs'].apply(lambda x: x.startswith('Ensembl'))]
        explode_df_ncbi_gene.fillna('None', inplace=True)

        # reformat entrez gene type
        explode_df_ncbi_gene['entrez_gene_type'] = explode_df_ncbi_gene['type_of_gene']
        gene_dict = self.genomic_type_mapper['entrez_gene_type']
        for val in gene_dict.keys():
            explode_df_ncbi_gene['entrez_gene_type'].replace(val, gene_dict[val], inplace=True)
        # reformat master gene type
        explode_df_ncbi_gene['master_gene_type'] = explode_df_ncbi_gene['entrez_gene_type']
        gene_dict = self.genomic_type_mapper['master_gene_type']
        for val in gene_dict.keys():
            explode_df_ncbi_gene['master_gene_type'].replace(val, gene_dict[val], inplace=True)
        # post-process reformatted data
        explode_df_ncbi_gene['hgnc_id'] = explode_df_ncbi_gene['hgnc_id'].replace('HGNC:', '', regex=True)
        explode_df_ncbi_gene['ensembl_gene_id'] = explode_df_ncbi_gene['ensembl_gene_id'].replace('Ensembl:', '',
                                                                                                  regex=True)
        explode_df_ncbi_gene.drop(['type_of_gene', 'dbXrefs', 'description', 'Nomenclature_status', 'Modification_date',
                                   'LocusTag', '#tax_id', 'Full_name_from_nomenclature_authority', 'Feature_type',
                                   'Symbol_from_nomenclature_authority'], axis=1, inplace=True)
        explode_df_ncbi_gene.drop_duplicates(subset=None, keep='first', inplace=True)

        return explode_df_ncbi_gene

    def _preprocess_protein_ontology_mapping_data(self) -> pandas.DataFrame:
        """Processes PRotein Ontology identifier mapping data in order to prepare it for combination with other gene
        identifier data sources.

        Returns:
            pro: A Pandas DataFrame containing processed and filtered data.
        """

        logger.info('Preprocessing Protein Ontology Data')

        col_names = ['pro_id', 'Entry', 'pro_mapping']
        pro = self.reads_gcs_bucket_data_to_df(f_name='promapping.txt', delm='\t', skip=0, head=col_names, sht=None)
        pro = pro.loc[pro['Entry'].apply(lambda x: x.startswith('UniProtKB:') and '_VAR' not in x and ', ' not in x)]
        pro = pro.loc[pro['pro_mapping'].apply(lambda x: x.startswith('exact'))]
        pro['pro_id'].replace('PR:', 'PR_', inplace=True, regex=True)  # replace PR: with PR_
        pro['Entry'].replace('(^\w*\:)', '', inplace=True, regex=True)  # remove ids which appear before ':'
        pro = pro.loc[pro['pro_id'].apply(lambda x: '-' not in x)]  # remove isoforms
        pro.rename(columns={'Entry': 'uniprot_id'}, inplace=True)
        pro.drop(['pro_mapping'], axis=1, inplace=True); pro.drop_duplicates(subset=None, keep='first', inplace=True)

        return pro

    def _merges_genomic_identifier_data(self) -> pandas.DataFrame:
        """Merges HGNC, Ensembl, Uniprot, and PRotein Ontology identifiers into a single Pandas DataFrame.

        Returns:
            merged_data: A Pandas DataFrame of merged genomic identifier information.
        """

        print('\t- Loading, Processing, and Merging Genomic ID Data'); logger.info('Merging Genomic ID Data')

        # hgnc + ensembl
        hgnc, ensembl = self._preprocess_hgnc_data(), self.merges_ensembl_mapping_data()
        merge_cols = list(set(hgnc.columns).intersection(set(ensembl.columns)))
        ensembl_hgnc = pandas.merge(ensembl, hgnc, on=merge_cols, how='outer')
        ensembl_hgnc.fillna('None', inplace=True); ensembl_hgnc.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc + uniprot
        uniprot = self._preprocess_uniprot_data()
        merge_cols = list(set(ensembl_hgnc.columns).intersection(set(uniprot.columns)))
        ensembl_hgnc_uniprot = pandas.merge(ensembl_hgnc, uniprot, on=merge_cols, how='outer')
        ensembl_hgnc_uniprot.fillna('None', inplace=True)
        ensembl_hgnc_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc_uniprot + Homo_sapiens.gene_info
        ncbi = self._preprocess_ncbi_data()
        merge_cols = list(set(ensembl_hgnc_uniprot.columns).intersection(set(ncbi.columns)))
        ensembl_hgnc_uniprot_ncbi = pandas.merge(ensembl_hgnc_uniprot, ncbi, on=merge_cols, how='outer')
        ensembl_hgnc_uniprot_ncbi.fillna('None', inplace=True)
        ensembl_hgnc_uniprot_ncbi.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc_uniprot_ncbi + promapping.txt
        pro = self._preprocess_protein_ontology_mapping_data()
        merged_data = pandas.merge(ensembl_hgnc_uniprot_ncbi, pro, on='uniprot_id', how='outer')
        merged_data.fillna('None', inplace=True); merged_data.drop_duplicates(subset=None, keep='first', inplace=True)

        return merged_data

    def _fixes_genomic_symbols(self) -> pandas.DataFrame:
        """Takes a Pandas DataFrame of genomic identifier data and fixes gene symbol information.

        Returns:
            merged_data_clean: A Pandas DataFrame with fix genomic symbols.
        """

        logger.info('Fixing genomic Symbols')

        clean_dates, merged_data = [], self._merges_genomic_identifier_data()
        for x in tqdm(list(merged_data['symbol'])):
            if '-' in x and len(x.split('-')[0]) < 3 and len(x.split('-')[1]) == 3:
                clean_dates.append(x.split('-')[1].upper() + x.split('-')[0])
            else: clean_dates.append(x)
        merged_data['symbol'] = clean_dates; merged_data.fillna('None', inplace=True)
        # make sure that all gene and transcript type columns have none recoded to unknown or not protein-coding
        merged_data['hgnc_gene_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data['ensembl_gene_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data['entrez_gene_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data['master_gene_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data['master_transcript_type'].replace('None', 'not protein-coding', inplace=True, regex=False)
        merged_data['ensembl_transcript_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data_clean = merged_data.drop_duplicates(subset=None, keep='first')

        return merged_data_clean

    def _cross_maps_genomic_identifier_data(self) -> Dict:
        """Takes a Pandas Dataframe of merged genomic identifiers and expands them to create a complete mapping between
        the identifiers. A master dictionary is built, where the keys are ensembl_gene_id, transcript_stable_id,
        protein_stable_id, uniprot_id, entrez_id, hgnc_id, and pro_id identifiers and values are the list of identifiers
        that match to each identifier. This function takes 40-50 minutes to complete.

        Returns:
            master_dict: A dict where keys are genomic identifiers and values are lists of other identifiers and
                metadata mapped to that identifier.
        """

        logger.info('Cross-Mapping Genomic Identifier Data')

        # reformat data to convert all nones, empty values, and unknowns to NaN
        merged_data: pandas.DataFrame = self._fixes_genomic_symbols(); master_dict: Dict = {}
        for col in merged_data.columns:
            merged_data[col] = merged_data[col].apply(lambda x: '|'.join([i for i in x.split('|') if i != 'None']))
        merged_data.replace(to_replace=['None', '', 'unknown'], value=numpy.nan, inplace=True)
        identifiers = [x for x in merged_data.columns if x.endswith('_id')] + ['symbol']
        # convert data to dictionary
        for idx in tqdm(identifiers):
            grouped_data = merged_data.groupby(idx)
            grp_ids = set([x for x in list(grouped_data.groups.keys()) if x != numpy.nan])
            for grp in grp_ids:
                df = grouped_data.get_group(grp).dropna(axis=1, how='all')
                df_cols, key = df.columns, idx + '_' + grp
                val_df = [[c + '_' + x for x in set(df[c]) if isinstance(x, str)] for c in df_cols if c != idx]
                if len(val_df) > 0:
                    if key in master_dict.keys(): master_dict[key] += [i for j in val_df for i in j if len(i) > 0]
                    else: master_dict[key] = [i for j in val_df for i in j if len(i) > 0]

        return master_dict

    def creates_master_genomic_identifier_map(self) -> Dict:
        """Identifies a master gene and transcript type for each entity because the last ran code chunk can result in
        several genes and transcripts with differing types (i.e. protein-coding or not protein-coding). The next step
        collects all information for each gene and transcript and performs a voting procedure to select a single
        primary gene and transcript type.

        Returns:
            reformatted_mapped_ids: A dict containing genomic identifier information which is keyed by genomic
                identifier types and where values are lists of all other genomic identifiers that map to that key.
        """

        print('\t- Creating Genomic ID Cross-Map Dictionary'); logger.info('Creating Genomic ID Cross-Map Dictionary')

        master_dict = self._cross_maps_genomic_identifier_data(); reformatted_mapped_identifiers = dict()
        for key, values in tqdm(master_dict.items()):
            identifier_info = set(values); gene_prefix = 'master_gene_type_'; trans_prefix = 'master_transcript_type_'
            if key.split('_')[0] in ['protein', 'uniprot', 'pro']: pass
            elif 'transcript' in key:
                trans_match = [x.replace(trans_prefix, '') for x in values if trans_prefix in x]
                if len(trans_match) > 0:
                    t_type_list = ['protein-coding'
                                   if ('protein-coding' in trans_match or 'protein_coding' in trans_match)
                                   else 'not protein-coding']
                    identifier_info |= {'transcript_type_update_' + max(set(t_type_list), key=t_type_list.count)}
            else:
                gene_match = [x.replace(gene_prefix, '') for x in values if x.startswith(gene_prefix) and 'type' in x]
                if len(gene_match) > 0:
                    g_type_list = ['protein-coding'
                                   if ('protein-coding' in gene_match or 'protein_coding' in gene_match)
                                   else 'not protein-coding']
                    identifier_info |= {'gene_type_update_' + max(set(g_type_list), key=g_type_list.count)}
            reformatted_mapped_identifiers[key] = identifier_info
        # save results for output > 4GB requires special approach: https://stackoverflow.com/questions/42653386
        filename = 'Merged_gene_rna_protein_identifiers.pkl'
        with open(self.temp_dir + '/' + filename, 'wb') as f_out:
            for idx in range(0, sys.getsizeof(pickle.dumps(reformatted_mapped_identifiers)), 2 ** 31 - 1):
                f_out.write(pickle.dumps(reformatted_mapped_identifiers)[idx:idx + (2 ** 31 - 1)])
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return reformatted_mapped_identifiers

    def generates_specific_genomic_identifier_maps(self) -> None:
        """Method takes a list of information needed to create mappings between specific sets of genomic identifiers.

        Returns:
            None.
        """

        print('\t- Generating Genomic ID Cross-Map Sets'); logger.info('Generating Pairwise Genomic Cross-Map Sets')

        self._loads_genomic_typing_dictionary()  # creates genomic typing dictionary
        reformatted_mapped_identifiers = self.creates_master_genomic_identifier_map()
        gene_sets = [
            ['ENSEMBL_GENE_ENTREZ_GENE_MAP.txt', 'ensembl_gene_id', 'entrez_id', 'ensembl_gene_type',
             'entrez_gene_type', 'gene_type_update', 'gene_type_update', False, False],
            ['ENSEMBL_TRANSCRIPT_PROTEIN_ONTOLOGY_MAP.txt', 'transcript_stable_id', 'pro_id', 'ensembl_transcript_type',
             None, 'transcript_type_update', None, False, True],
            ['ENTREZ_GENE_ENSEMBL_TRANSCRIPT_MAP.txt', 'entrez_id', 'transcript_stable_id', 'entrez_gene_type',
             'ensembl_transcript_type', 'gene_type_update', 'transcript_type_update', False, False],
            ['ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt', 'entrez_id', 'pro_id', 'entrez_gene_type', None, 'gene_type_update',
             None, False, True],
            ['GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt', 'symbol', 'transcript_stable_id', 'master_gene_type',
             'ensembl_transcript_type', 'gene_type_update', 'transcript_type_update', False, False],
            ['STRING_PRO_ONTOLOGY_MAP.txt', 'protein_stable_id', 'pro_id', None, None, None, None, False, True],
            ['UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt', 'uniprot_id', 'pro_id', None, None, None, None, False, True]
        ]

        for x in gene_sets:
            genomic_id_mapper(reformatted_mapped_identifiers, self.temp_dir + '/' + x[0],  # type: ignore
                              x[1], x[2], x[3], x[4], x[5], x[6])  # type: ignore
            uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, x[0])  # type: ignore

        return None

    def _processes_mesh_data(self) -> pandas.DataFrame:
        """Parses MeSH data converting it from n-triples format into Pandas DataFrame that can be merged with ChEBI
        data.

        Returns:
            msh_df: A Pandas Data Frame containing three columns: ID (i.e. 'MESH' identifiers), STR (i.e. string
                labels or synonyms), and TYP (i.e. a string denoting if the STR column entry is a 'NAME' or 'SYNONYM').
            msh_dict: A nested dict where keys are MeSH identifiers and the values are a dict of labels, dbxrefs, and
                synonyms for each key.
        """

        print('\t- Processing MeSH Data'); logger.info('Preprocessing MeSH Data')

        f_name = 'mesh*.nt'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        mesh = [i.split('> ') for i in tqdm(open(x, 'r').readlines())]; msh_dict: Dict = {}; res: List = []
        for row in tqdm(mesh):
            s, p, o, dbx, lab, msh_type = row[0].split('/')[-1], row[1].split('#')[-1], row[2], None, None, None
            if s[0] in ['C', 'D'] and ('.' not in s and 'Q' not in s) and len(s) >= 5:
                s = 'MESH_' + s
                if p == 'preferredConcept' or p == 'concept': dbx = 'MESH_' + o.split('/')[-1]
                if 'label' in p.lower(): lab = o.split('"')[1]
                if 'type' in p.lower(): msh_type = o.split('#')[1]
                if s in msh_dict.keys():
                    if dbx is not None: msh_dict[s]['dbx'].add(dbx)
                    if lab is not None: msh_dict[s]['lab'].add(lab)
                    if msh_type is not None: msh_dict[s]['type'].add(msh_type)
                else:
                    msh_dict[s] = {'dbx': set() if dbx is None else {dbx}, 'lab': set() if lab is None else {lab},
                                   'type': set() if msh_type is None else {msh_type}, 'syn': set()}
        for key in tqdm(msh_dict.keys()):  # fine tune dictionary - obtain labels for each entry's synonym identifiers
            for i in msh_dict[key]['dbx']:
                if len(msh_dict[key]['dbx']) > 0 and i in msh_dict.keys(): msh_dict[key]['syn'] |= msh_dict[i]['lab']
        # expand data and convert to pandas DataFrame
        for key, value in tqdm(msh_dict.items()):
            res += [[key, list(value['lab'])[0], 'NAME']]
            if len(value['syn']) > 0:
                for i in value['syn']:
                    res += [[key, i, 'SYNONYM']]
        msh_df = pandas.DataFrame({'ID': [x[0] for x in res], 'TYP': [x[2] for x in res], 'STR': [x[1] for x in res]})
        msh_df['STR'] = msh_df['STR'].str.lower()
        msh_df['STR'] = msh_df['STR'].str.replace('[^\w]', '')  # remove white space and punctuation

        return msh_df, msh_dict

    def _processes_chebi_data(self) -> pandas.DataFrame:
        """Parses ChEBI data into a Pandas DataFrame that can be merged with MeSH data.

        Returns:
            chebi_df: A Pandas DataFrame containing three columns: ID (i.e. 'CHEBI' identifiers), STR (i.e. string
                labels or synonyms), and TYP (i.e. a string denoting if the STR column entry is a 'NAME' or 'SYNONYM').
        """

        print('\t- Processing ChEBI Data'); logger.info('Preprocessing ChEBI Data')

        chebi = self.reads_gcs_bucket_data_to_df(f_name='names.tsv', delm='\t', skip=0, head=0, sht=None)
        chebi_df = chebi[['COMPOUND_ID', 'TYPE', 'NAME']]  # remove unneeded columns
        chebi_df.drop_duplicates(subset=None, keep='first', inplace=True)
        chebi_df.columns = ['ID', 'TYP', 'STR']  # rename columns
        chebi_df['ID'] = chebi_df['ID'].apply(lambda x: "{}{}".format('CHEBI_', x))  # append CHEBI to # in each id
        chebi_df['STR'] = chebi_df['STR'].str.lower()
        chebi_df['STR'] = chebi_df['STR'].str.replace('[^\w]', '')  # remove white space and punctuation

        return chebi_df

    def creates_chebi_to_mesh_identifier_mappings(self) -> None:
        """Recapitulates the LOOM algorithm (https://www.bioontology.org/wiki/BioPortal_Mappings) utilized by the
        BioPortal API to create mappings between MeSH and ChEBI identifiers. This is accomplished by performing an
        inner join on the MeSH and ChEBI Pandas DataFrames. The resulting mappings are then written out locally and
        pushed to the process_data directory in the Google Cloud Storage bucket for the current build.

        Return:
            None.
        """

        print('Creating MeSH-ChEBI ID Cross-Map Data'); logger.info('Creating MeSH-ChEBI ID Cross-Map Data')

        mesh_df, mesh_dict = self._processes_mesh_data(); chebi_df = self._processes_chebi_data()
        merge_cols = ['STR', 'ID']
        identifier_merge = pandas.merge(chebi_df[merge_cols], mesh_df[merge_cols], on='STR', how='inner')
        # filter merged data
        mesh_edges = set()
        for idx, row in identifier_merge.drop_duplicates().iterrows():
            mesh, chebi = row['ID_y'], row['ID_x']; mesh_edges.add(tuple([mesh, chebi]))
            syns = [x for x in mesh_dict[mesh]['dbx'] if 'C' in x or 'D' in x]
            if len(syns) > 0:
                for x in syns: mesh_edges.add(tuple([x, chebi]))
        # write results and push data to gcs bucket
        filename = 'MESH_CHEBI_MAP.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out:
            for pair in mesh_edges: out.write(pair[0] + '\t' + pair[1] + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return None

    def _preprocess_mondo_mapping_data(self) -> Dict:
        """Method processes MonDO Disease Ontology (MONDO) ontology data in order to create a dictionary that aligns
        MONDO concepts with other types of disease terminology identifiers. This is done by obtaining database
        cross-references (dbxrefs) for each ontology and then combining the results into a single large dictionary
        keyed by dbxrefs with MONDO and HP identifiers as values.

        Returns:
            mondo_dict: A dict where disease identifiers mapped to mondo are keys and mondo identifiers are values.
        """

        print('\t- Loading MonDO Disease Ontology Data'); logger.info('Loading MonDO Disease Ontology Data')

        f_name = 'mondo_with_imports.owl'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        mondo_graph = Graph().parse(x); dbxref_res = gets_ontology_class_dbxrefs(mondo_graph)[0]
        mondo_dict = {str(k).lower().split('/')[-1]: {str(v).split('/')[-1].replace('_', ':')}
                      for k, v in dbxref_res.items() if 'MONDO' in str(v)}

        return mondo_dict

    def _preprocess_hpo_mapping_data(self) -> Dict:
        """Method processes Human Phenotype Ontology (HPO) ontology data in order to create a dictionary that aligns HPO
        concepts with other types of disease terminology identifiers. This is done by obtaining database
        cross-references (dbxrefs) for each ontology and then combining the results into a single large dictionary
        keyed by dbxrefs with MONDO and HPO identifiers as values.

        Returns:
            hp_dict: A dict where disease identifiers mapped to hpo are keys and mondo identifiers are values.
        """

        print('\t- Loading Human Phenotype Ontology Data'); logger.info('Loading Human Phenotype Ontology Data')

        f_name = 'hp_with_imports.owl'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        hp_graph = Graph().parse(x); dbxref_res = gets_ontology_class_dbxrefs(hp_graph)[0]
        hp_dict = {str(k).lower().split('/')[-1]: {str(v).split('/')[-1].replace('_', ':')}
                   for k, v in dbxref_res.items() if 'HP' in str(v)}

        return hp_dict

    def creates_disease_identifier_mappings(self) -> None:
        """Creates Human Phenotype Ontology (HPO) and MonDO Disease Ontology (MONDO) dbxRef maps and then uses them
        with the DisGEeNET UMLS disease mappings to create a master mapping between all disease identifiers to HPO
        and MONDO.

        Returns:
            None.
        """

        log_str = 'Creating Phenotype and Disease ID Cross-Map Data'; print(log_str); logger.info(log_str)

        mondo_dict, hp_dict = self._preprocess_mondo_mapping_data(), self._preprocess_hpo_mapping_data()
        data = self.reads_gcs_bucket_data_to_df(f_name='disease_mappings.tsv', delm='\t', skip=0, head=0, sht=None)
        data['vocabulary'], data['diseaseId'] = data['vocabulary'].str.lower(), data['diseaseId'].str.lower()
        data['vocabulary'] = ['doid' if x == 'do' else 'ordoid' if x == 'ordo' else x for x in data['vocabulary']]
        # get all CUIs mapped to HPO and MONDO
        ont_dict: Dict = {}; disease_data_keep = data.query('vocabulary == "hpo" | vocabulary == "mondo"')
        for idx, row in tqdm(disease_data_keep.iterrows(), total=disease_data_keep.shape[0]):
            if row['vocabulary'] == 'mondo': key, value = 'umls:' + row['diseaseId'], 'MONDO:' + row['code']
            else: key, value = 'umls:' + row['diseaseId'], row['code']
            if key in ont_dict.keys(): ont_dict[key] |= {value}
            else: ont_dict[key] = {value}
        for key in tqdm(ont_dict.keys()):  # add ontology mappings from MONDO and HPO
            if key in mondo_dict.keys(): ont_dict[key] = set(list(ont_dict[key]) + list(mondo_dict[key]))
            if key in hp_dict.keys(): ont_dict[key] = set(list(ont_dict[key]) + list(hp_dict[key]))
        # get all rows for HPO/MONDO CUIs to obtain mappings to other disease identifiers
        disease_dict: Dict = {}; disease_data_other = data[data.diseaseId.isin(disease_data_keep['diseaseId'])]
        for idx, row in tqdm(disease_data_other.iterrows(), total=disease_data_other.shape[0]):
            vocab, ids, code = row['vocabulary'], row['diseaseId'], row['code']
            if vocab == 'mondo' or vocab == 'hpo':
                key, value = 'umls:' + ids.lower(), code
                if key in disease_dict.keys(): disease_dict[key] |= {value}
                else: disease_dict[key] = {value}
            else:
                if 'mondo' not in code or 'hp' not in code:
                    if ':' not in code: key, value = vocab + ':' + code, ont_dict['umls:' + ids]
                    else: key, value = code, ont_dict['umls:' + ids]
                    if key in disease_dict.keys(): disease_dict[key] |= value
                    else: disease_dict[key] = value
        # save data and push to GCS bucket
        file1, file2 = 'DISEASE_MONDO_MAP.txt', 'PHENOTYPE_HPO_MAP.txt'
        with open(self.temp_dir + '/' + file1, 'w') as out1, open(self.temp_dir + '/' + file2, 'w') as out2:
            for k, v in tqdm({**disease_dict, **mondo_dict, **hp_dict}.items()):
                if any(x for x in v if x.startswith('MONDO')):
                    for idx in [x.replace(':', '_') for x in v if 'MONDO' in x]:
                        out1.write(k.upper().split(':')[-1] + '\t' + idx + '\n')
                if any(x for x in v if x.startswith('HP')):
                    for idx in [x.replace(':', '_') for x in v if 'HP' in x]:
                        out2.write(k.upper().split(':')[-1] + '\t' + idx + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, file1)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, file2)

        return None

    def _hpa_gtex_ontology_alignment(self) -> None:
        """Processes data to align Human Protein Atlas (HPA) and Genotype-Tissue Expression Project (GTEx) data to the
        Uber-Anatomy (UBERON), Cell Ontology (CL), and the Cell Line Ontology (CLO). The processed data is then
        written to a txt file and pushed to GCS.

        Returns:
            None.
        """

        logger.info('Preprocessing HPA Data')

        data_file, sheet = 'zooma_tissue_cell_mapping_04JAN2020.xlsx', 'Concept_Mapping - 04JAN2020'
        mapping_data = self.reads_gcs_bucket_data_to_df(f_name=data_file, delm='\t', skip=0, head=0, sht=sheet)
        mapping_data.fillna('NA', inplace=True)
        filename = 'HPA_GTEx_TISSUE_CELL_MAP.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out:
            for idx, row in tqdm(mapping_data.iterrows(), total=mapping_data.shape[0]):
                if row['UBERON'] != 'NA': out.write(str(row['TERM']).strip() + '\t' + str(row['UBERON']).strip() + '\n')
                if row['CL'] != 'NA': out.write(str(row['TERM']).strip() + '\t' + str(row['CL']).strip() + '\n')
                if row['CLO'] != 'NA': out.write(str(row['TERM']).strip() + '\t' + str(row['CLO']).strip() + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return None

    def _extracts_hpa_tissue_information(self) -> pandas.DataFrame:
        """Method reads in Human Protein Atlas (HPA) data and saves the columns, which contain the anatomical
        entities (i.e. cell types, cell lines, tissues, and fluids) that need manual alignment to ontologies. These
        data are not necessarily needed for every build, only when updating the ontology alignment mappings.

        Returns:
             hpa: A Pandas DataFrame object containing tissue data.
        """

        logger.info('Extracting HPA Tissue and Cell Information')

        hpa = self.reads_gcs_bucket_data_to_df(f_name='proteinatlas_search.tsv', delm='\t', skip=0, head=0, sht=None)
        hpa.fillna('None', inplace=True)
        # write results
        filename = 'HPA_tissues.txt'
        with open(self.temp_dir + '/' + filename, 'w') as outfile:
            for x in tqdm(list(hpa.columns)):
                if x.endswith('[NX]'): outfile.write(x.split('RNA - ')[-1].split(' [NX]')[:-1][0] + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return hpa

    def processes_hpa_gtex_data(self) -> None:
        """Method processes and combines gene expression experiment results from the Human protein Atlas (HPA) and the
        Genotype-Tissue Expression Project (GTEx). Additional details provided below on how each source are processed.
            - HPA: The HPA data is reformatted so all tissue, cell, cell lines, and fluid types are stored as a nested
              list. The anatomy type is specified as an item in the list according to its type.
            - GTEx: All protein-coding genes that appear in the HPA data set are removed. Then, only those non-coding
              genes with a median expression >= 1.0 are maintained. GTEx data are formatted such the anatomical
              entities are stored as columns and genes stored as rows, thus the expression filtering step is
              performed while also reformatting the file, resulting in a nested list.

        Returns:
            None.
        """

        log_str = 'Creating Human Protein Atlas and GTEx Cross-Map Data'; print(log_str); logger.info(log_str)

        hpa = self._extracts_hpa_tissue_information(); f_name = 'GTEx_Analysis_*_RNASeQC*_gene_median_tpm.gct'
        gtex = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=2, head=0, sht=None)
        gtex.fillna('None', inplace=True); gtex['Name'].replace('(\..*)', '', inplace=True, regex=True)
        # process human protein atlas data
        hpa_results = []
        for idx, row in tqdm(hpa.iterrows(), total=hpa.shape[0]):
            ens, gene, uniprot, evid = str(row['Ensembl']), str(row['Gene']), str(row['Uniprot']), str(row['Evidence'])
            if row['RNA tissue specific NX'] != 'None':
                for x in row['RNA tissue specific NX'].split(';'):
                    hpa_results += [[ens, gene, uniprot, evid, 'anatomy', str(x.split(':')[0])]]
            if row['RNA cell line specific NX'] != 'None':
                for x in row['RNA cell line specific NX'].split(';'):
                    hpa_results += [[ens, gene, uniprot, evid, 'cell line', str(x.split(':')[0])]]
            if row['RNA brain regional specific NX'] != 'None':
                for x in row['RNA brain regional specific NX'].split(';'):
                    hpa_results += [[ens, gene, uniprot, evid, 'anatomy', str(x.split(':')[0])]]
            if row['RNA blood cell specific NX'] != 'None':
                for x in row['RNA blood cell specific NX'].split(';'):
                    hpa_results += [[ens, gene, uniprot, evid, 'anatomy', str(x.split(':')[0])]]
            if row['RNA blood lineage specific NX'] != 'None':
                for x in row['RNA blood lineage specific NX'].split(';'):
                    hpa_results += [[ens, gene, uniprot, evid, 'anatomy', str(x.split(':')[0])]]
        # process gtex data -- using only those protein-coding genes not already in hpa
        gtex_results, hpa_genes = [], list(hpa['Ensembl'].drop_duplicates(keep='first', inplace=False))
        gtex = gtex.loc[gtex['Name'].apply(lambda i: i not in hpa_genes)]
        for idx, row in tqdm(gtex.iterrows(), total=gtex.shape[0]):
            for col in list(gtex.columns)[2:]:
                typ = 'cell line' if 'Cells' in col else 'anatomy'
                if row[col] >= 1.0:
                    evidence = 'Evidence at transcript level'
                    gtex_results += [[str(row['Name']), str(row['Description']), 'None', evidence, typ, str(col)]]
        # write results
        filename = 'HPA_GTEX_RNA_GENE_PROTEIN_EDGES.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out:
            for x in hpa_results + gtex_results:
                out.write(x[0] + '\t' + x[1] + '\t' + x[2] + '\t' + x[3] + '\t' + x[4] + '\t' + x[5] + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return None

    def _preprocess_pathway_mapping_data(self) -> Dict:
        """Method processes the Pathway Ontology (PW) data in order to create a dictionary that aligns PW concepts
        with other types of pathway identifiers.

        Returns:
             id_mappings: A dict containing mappings between the PW and other relevant ontologies.
        """

        log_str = 'Loading Protein Ontology Data'; print('\t- ' + log_str); logger.info(log_str)

        f_name = 'pw_with_imports.owl'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        pw_graph = Graph().parse(x); dbxref_res = gets_ontology_class_dbxrefs(pw_graph)[0]
        dbxref_dict = {str(k).lower().split('/')[-1]: {str(v).split('/')[-1].replace('_', ':')}
                       for k, v in dbxref_res.items() if 'PW_' in str(v)}
        syn_res = gets_ontology_class_synonyms(pw_graph)[0]
        synonym_dict = {str(k).lower().split('/')[-1]: {str(v).split('/')[-1].replace('_', ':')}
                        for k, v in syn_res.items() if 'PW_' in str(v)}
        id_mappings = {**dbxref_dict, **synonym_dict}

        return id_mappings

    def _processes_reactome_data(self) -> Dict:
        """Reads in different annotation data sets provided by reactome and combines them into a dictionary.

        Returns:
            reactome: A dict mapping different pathway identifiers to the Pathway Ontology.
        """

        log_str = 'Loading Reactome Annotation Data'; print('\t- ' + log_str); logger.info(log_str)

        r_name, g_name, c_name = 'ReactomePathways.txt', 'gene_association.reactome', 'ChEBI2Reactome_All_Levels.txt'
        # reactome pathways
        reactome_pathways = self.reads_gcs_bucket_data_to_df(f_name=r_name, delm='\t', skip=0, head=None, sht=None)
        reactome_pathways = reactome_pathways.loc[reactome_pathways[2].apply(lambda x: x == 'Homo sapiens')]
        reactome = {x: {'PW_0000001'} for x in set(list(reactome_pathways[0]))}
        # reactome - GO associations
        reactome_pathways2 = self.reads_gcs_bucket_data_to_df(f_name=g_name, delm='\t', skip=3, head=None, sht=None)
        reactome_pathways2 = reactome_pathways2.loc[reactome_pathways2[12].apply(lambda x: x == 'taxon:9606')]
        reactome.update({x.split(':')[-1]: {'PW_0000001'} for x in set(list(reactome_pathways2[5]))})
        # reactome CHEBI
        reactome_pathways3 = self.reads_gcs_bucket_data_to_df(f_name=c_name, delm='\t', skip=0, head=None, sht=None)
        reactome_pathways3 = reactome_pathways3.loc[reactome_pathways3[5].apply(lambda x: x == 'Homo sapiens')]
        reactome.update({x: {'PW_0000001'} for x in set(list(reactome_pathways3[1]))})

        return reactome

    def _processes_compath_pathway_data(self, reactome: Dict, pw_dict: Dict) -> Dict:
        """Processes compath pathway mappings data, extending the input reactome dictionary by extending it to add
        mappings from KEGG to reactome and the Pathway Ontology (PW).

        Args:
            reactome: A dict mapping different pathway identifiers to the Pathway Ontology.
            pw_dict: A dict containing dbxref mappings to PW identifiers.

        Returns:
             reactome: An extended dict mapping different pathway identifiers to the Pathway Ontology.
        """

        log_str = 'Loading ComPath Canonical Pathway Data'; print('\t- ' + log_str); logger.info(log_str)

        f_name = 'compath_canonical_pathway_mappings.txt'
        compath = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=0, head=None, sht=None)
        compath.fillna('None', inplace=True)
        for idx, row in tqdm(compath.iterrows(), total=compath.shape[0]):
            if row[6] == 'kegg' and 'kegg:' + row[5].strip('path:hsa') in pw_dict.keys() and row[2] == 'reactome':
                for x in pw_dict['kegg:' + row[5].strip('path:hsa')]:
                    if row[1] in reactome.keys(): reactome[row[1]] |= {x.split('/')[-1]}
                    else: reactome[row[1]] = {x.split('/')[-1]}
            if (row[2] == 'kegg' and 'kegg:' + row[1].strip('path:hsa') in pw_dict.keys()) and row[6] == 'reactome':
                for x in pw_dict['kegg:' + row[1].strip('path:hsa')]:
                    if row[5] in reactome.keys(): reactome[row[5]] |= {x.split('/')[-1]}
                    else: reactome[row[5]] = {x.split('/')[-1]}

        return reactome

    def _processes_kegg_pathway_data(self, reactome: Dict, pw_dict: Dict) -> Dict:
        """Processes KEGG-Reactome  data, extending the input reactome dictionary by extending it to add mappings from
        KEGG to reactome and the Pathway Ontology (PW).

        Args:
            reactome: A dict mapping different pathway identifiers to the Pathway Ontology.
            pw_dict: A dict containing dbxref mappings to PW identifiers.

        Returns:
             reactome: An extended dict mapping different pathway identifiers to the Pathway Ontology.
        """

        log_str = 'Loading KEGG Data'; print('\t- ' + log_str); logger.info(log_str)

        f_name = 'kegg_reactome.csv'
        kegg_reactome_map = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm=',', skip=0, head=0, sht=None)
        src, tar, tar_ids, src_ids = 'Source Resource', 'Target Resource', 'Target ID', 'Source ID'
        for idx, row in tqdm(kegg_reactome_map.iterrows(), total=kegg_reactome_map.shape[0]):
            if row[src] == 'reactome' and 'kegg:' + row[tar_ids].strip('path:hsa') in pw_dict.keys():
                for x in pw_dict['kegg:' + row[tar_ids].strip('path:hsa')]:
                    if row[src_ids] in reactome.keys(): reactome[row[src_ids]] |= {x.split('/')[-1]}
                    else: reactome[row[src_ids]] = {x.split('/')[-1]}
            if row[tar] == 'reactome' and 'kegg:' + row[src].strip('path:hsa') in pw_dict.keys():
                for x in pw_dict['kegg:' + row[src_ids].strip('path:hsa')]:
                    if row[tar_ids] in reactome.keys(): reactome[row[tar_ids]] |= {x.split('/')[-1]}
                    else: reactome[row[tar_ids]] = {x.split('/')[-1]}

        return reactome

    @staticmethod
    def _queries_reactome_api(reactome: Dict) -> Dict:
        """Runs a set of reactome identifiers against the reactome API in order to obtain mappings to the Gene
        Ontology, specifically to Biological Processes.

        Args:
            reactome: A dict mapping different pathway identifiers to the Pathway Ontology.

        Returns:
             reactome: An extended dict mapping different pathway identifiers to the Pathway Ontology.
        """

        log_str = 'Querying Reactome API for Reactome-GO BP Mappings'; print('\t- ' + log_str); logger.info(log_str)

        for request_ids in tqdm(list(chunks(list(reactome.keys()), 20))):
            result, key = content.query_ids(ids=','.join(request_ids)), 'goBiologicalProcess'
            if result is not None:
                for res in result:
                    if key in res.keys():
                        if res['stId'] in reactome.keys(): reactome[res['stId']] |= {'GO_' + res[key]['accession']}
                        else: reactome[res['stId']] = {'GO_' + res[key]['accession']}

        return reactome

    def _creates_pathway_identifier_mappings(self) -> Dict:
        """Processes the canonical pathways and other kegg-reactome pathway mapping files from the ComPath
        Ecosystem in order to create the following identifier mappings: Reactome Pathway Identifiers ➞ KEGG Pathway
        Identifiers ➞ Pathway Ontology Identifiers.

        Returns:
            reactome: A dict mapping different types of pathway identifiers to sequence ontology classes.
        """

        log_str = 'Creating Pathway Ontology ID Cross-Map Data'; print('\n' + log_str); logger.info(log_str)

        pw_dict = self._preprocess_pathway_mapping_data(); reactome = self._processes_reactome_data()
        compath_reactome = self._processes_compath_pathway_data(reactome, pw_dict)
        kegg_reactome = self._processes_kegg_pathway_data(compath_reactome, pw_dict)
        reactome = self._queries_reactome_api(kegg_reactome)
        filename = 'REACTOME_PW_GO_MAPPINGS.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out:
            for key in tqdm(reactome.keys()):
                for x in reactome[key]:
                    if x.startswith('PW') or x.startswith('GO'): out.write(key + '\t' + x + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)
        # update reactome dict to ensure identifiers are consistent -- replacing ontology concepts with ':' to '_'
        temp_dict = dict()
        for key, value in tqdm(reactome.items()): temp_dict[key] = set(x.replace(':', '_') for x in value)
        reactome = temp_dict  # overwrite original reactome dict with cleaned mappings

        return reactome

    def _preprocesses_gene_types(self, genomic_map: Dict) -> Dict:
        """Creates mappings between bio types for different gene identifiers to sequence ontology classes.

        Args:
            genomic_map: A dict containing mappings between gene identifier types and Sequence Ontology identifiers.

        Returns:
            sequence_map: A dict containing genomic identifiers as keys and Sequence Ontology classes as values.
        """

        log_str = 'Mapping Sequence Ontology Classes to Gene IDs'; print('\t- ' + log_str); logger.info(log_str)

        gene_ids = pickle.load(open(self.temp_dir + '/Merged_gene_rna_protein_identifiers.pkl', 'rb'), encoding='bytes')
        sequence_map: Dict = {}
        for ids in tqdm(gene_ids.keys()):
            if ids.startswith('entrez_id_') and ids.replace('entrez_id_', '') != 'None':
                id_clean = ids.replace('entrez_id_', '')
                ensembl = [x.replace('ensembl_gene_type_', '') for x in gene_ids[ids] if
                           x.startswith('ensembl_gene_type') and x != 'ensembl_gene_type_unknown']
                hgnc = [x.replace('hgnc_gene_type_', '') for x in gene_ids[ids] if
                        x.startswith('hgnc_gene_type') and x != 'hgnc_gene_type_unknown']
                entrez = [x.replace('entrez_gene_type_', '') for x in gene_ids[ids] if
                          x.startswith('entrez_gene_type') and x != 'entrez_gene_type_unknown']
                # determine gene type
                if len(ensembl) > 0: gene_type = genomic_map[ensembl[0].replace('ensembl_gene_type_', '') + '_Gene']
                elif len(hgnc) > 0: gene_type = genomic_map[hgnc[0].replace('hgnc_gene_type_', '') + '_Gene']
                elif len(entrez) > 0: gene_type = genomic_map[entrez[0].replace('entrez_gene_type_', '') + '_Gene']
                else: gene_type = 'SO_0000704'
                # update sequence map
                if id_clean in sequence_map.keys(): sequence_map[id_clean] += [gene_type]
                else: sequence_map[id_clean] = [gene_type]

        return sequence_map

    def _preprocesses_transcript_types(self, genomic_map: Dict, sequence_map: Dict) -> Dict:
        """Creates mappings between bio types for different transcript identifiers to sequence ontology classes.

        Args:
            genomic_map: A dict containing mappings between transcript identifier types and Sequence Ontology classes.
            sequence_map: A dict containing genomic identifiers as keys and Sequence Ontology classes as values.

        Returns:
            sequence_map: A dict containing genomic identifiers as keys and Sequence Ontology classes as values.
        """

        log_str = 'Mapping Sequence Ontology Classes to Transcript IDs'; print('\t- ' + log_str); logger.info(log_str)

        trans: Dict = {}; trans_id: str = 'transcript_stable_id'
        f_name = self.temp_dir + '/ensembl_identifier_data_cleaned.txt'
        trans_data = pandas.read_csv(f_name, header=0, delimiter='\t', low_memory=False)
        for idx, row in tqdm(trans_data.iterrows(), total=trans_data.shape[0]):
            if row[trans_id] != 'None':
                if row[trans_id].replace('transcript_stable_id_', '') in trans.keys():
                    trans[row[trans_id].replace('transcript_stable_id_', '')] += [row['ensembl_transcript_type']]
                else:
                    trans[row[trans_id].replace('transcript_stable_id_', '')] = [row['ensembl_transcript_type']]
        # update SO map dictionary
        for ids in tqdm(trans.keys()):
            if trans[ids][0] == 'protein_coding': trans_type = genomic_map['protein-coding_Transcript']
            elif trans[ids][0] == 'misc_RNA': trans_type = genomic_map['miscRNA_Transcript']
            else: trans_type = genomic_map[list(set(trans[ids]))[0] + '_Transcript']
            sequence_map[ids] = [trans_type, 'SO_0000673']

        return sequence_map

    def _preprocesses_variant_types(self, genomic_map: Dict, sequence_map: Dict) -> Dict:
        """Creates mappings between bio types for different transcript identifiers to sequence ontology classes.

        Args:
            genomic_map: A dict containing mappings between variant identifier types and Sequence Ontology identifiers.
             sequence_map: A dict containing genomic identifiers as keys and Sequence Ontology classes as values.

        Returns:
            sequence_map: A dict containing genomic identifiers as keys and Sequence Ontology classes as values.
        """

        log_str = 'Mapping Sequence Ontology Classes to Variant IDs'; print('\t- ' + log_str); logger.info(log_str)

        f_name: str = 'variant_summary.txt'; v_df: Dict = {}
        variant_data = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=0, head=0, sht=None)
        for idx, row in tqdm(variant_data.iterrows(), total=variant_data.shape[0]):
            if row['Assembly'] == 'GRCh38' and row['RS# (dbSNP)'] != -1:
                if 'rs' + str(row['RS# (dbSNP)']) in v_df.keys(): v_df['rs' + str(row['RS# (dbSNP)'])] |= {row['Type']}
                else: v_df['rs' + str(row['RS# (dbSNP)'])] = {row['Type']}
        # update SO map dictionary
        for identifier in tqdm(v_df.keys()):
            for typ in v_df[identifier]:
                var_type = genomic_map[typ.lower() + '_Variant']
                if identifier in sequence_map.keys(): sequence_map[identifier] += [var_type]
                else: sequence_map[identifier] = [var_type]

        return sequence_map

    def _creates_sequence_identifier_mappings(self) -> Dict:
        """Maps a Sequence Ontology concept to all gene, transcript, and variant identifiers.

        Returns:
            sequence_map: A dict containing different types of genomic identifiers as keys and Sequence Ontology
                classes as values.
        """

        log_str = 'Creating Sequence Ontology ID Cross-Map Data'; print('\n' + log_str); logger.info(log_str)

        f_name, sht = 'genomic_sequence_ontology_mappings.xlsx', 'GenomicType_SO_Map_09Mar2020'
        mapping_data = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=0, head=0, sht=sht)
        genomic_type_so_map = {}
        for idx, row in tqdm(mapping_data.iterrows(), total=mapping_data.shape[0]):
            genomic_type_so_map[row['source_*_type'] + '_' + row['Genomic']] = row['SO ID']
        # add genes, transcripts, and variants
        genomic_sequence_map = self._preprocesses_gene_types(genomic_type_so_map)
        trans_sequence_map = self._preprocesses_transcript_types(genomic_type_so_map, genomic_sequence_map)
        sequence_map = self._preprocesses_variant_types(genomic_type_so_map, trans_sequence_map)
        filename = 'SO_GENE_TRANSCRIPT_VARIANT_TYPE_MAPPING.txt'
        with open(self.temp_dir + '/' + filename, 'w') as outfile:
            for key in tqdm(sequence_map.keys()):
                for map_type in sequence_map[key]:
                    outfile.write(key + '\t' + map_type + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return sequence_map

    def combines_pathway_and_sequence_ontology_dictionaries(self) -> None:
        """Combines the Pathway Ontology and Sequence Ontology dictionaries into a dict. This data is needed for the
        subclass construction approach of the knowledge graph build process.

        Returns:
            None.
        """

        log_str = 'Creating Pathway and Sequence Ontology Mapping Dictionary'; print(log_str); logger.info(log_str)

        sequence_map = self._creates_sequence_identifier_mappings()
        reactome_map = self._creates_pathway_identifier_mappings()
        # combine genomic and pathway maps + iterate over pathway lists and combine them into a single dictionary
        sequence_map.update(reactome_map); subclass_mapping = {}
        for key in tqdm(sequence_map.keys()): subclass_mapping[key] = sequence_map[key]
        filename = 'subclass_construction_map.pkl'
        pickle.dump(subclass_mapping, open(self.temp_dir + '/' + filename, 'wb'), protocol=4)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return None

    def _processes_protein_ontology_data(self) -> networkx.MultiDiGraph:
        """Reads in the PRotein Ontology (PR) into an RDFLib graph object and converts it to a Networkx MultiDiGraph
        object.

        Returns:
            networkx_mdg: A Networkx MultiDiGraph object containing protein ontology data.
        """

        print('\t- Loading Protein Ontology Data'); logger.info('Loading Protein Ontology Data')

        f_name = 'pr_with_imports.owl'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        pr_graph = Graph().parse(x); networkx_mdg: networkx.MultiDiGraph = networkx.MultiDiGraph()
        for s, p, o in tqdm(pr_graph): networkx_mdg.add_edge(s, o, **{'key': p})

        return networkx_mdg

    def _logically_verifies_human_protein_ontology(self, in_filename, out_filename, reasoner) -> None:
        """Logically verifies constructed Human Protein Ontology by running a deductive logic reasoner.

        Args:
            in_filename: A string containing the name of the file to run the reasoner on.
            out_filename: A string containing the filename to write the reasoner results to.
            reasoner: A string containing the name of the deductive reasoner to use.

        Returns:
            None.
        """

        log_str = 'Logically Verifying Human Protein Ontology Subset'; print('\t- ' + log_str); logger.info(log_str)

        # run reasoner
        command = "{} ./{} --reasoner {} --run-reasoner --assert-implied -o ./{}"
        return_code = os.system(command.format(self.owltools_location, in_filename, reasoner.lower(), out_filename))
        if return_code == 0:
            ontology_file_formatter(self.temp_dir, '/' + in_filename.split('/')[-1], self.owltools_location)
            ontology_file_formatter(self.temp_dir, '/' + out_filename.split('/')[-1], self.owltools_location)
            uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, in_filename.split('/')[-1])
            uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, out_filename.split('/')[-1])
        else:
            logger.error('ERROR: Reasoner Finished with Errors - {}: {}'.format(in_filename, return_code))
            raise Exception('ERROR: Reasoner Finished with Errors - {}: {}'.format(in_filename, return_code))

        return None

    def constructs_human_protein_ontology(self) -> None:
        """Creates a human version of the PRotein Ontology (PRO) by traversing the ontology to obtain forward and
        reverse breadth first search. If the resulting human PRO contains a single connected component it's written
        locally. After building the human subset, we verify the number of connected components and get 1. However, after
        reformatting the graph using OWLTools you will see that there are 3 connected components: component 1
        (n=1051673); component 2 (n=12); and component 3 (n=2).

        Returns:
            None.

        Raises:
            ValueError: When the human versions of the PRO contains more than a single connected component.
        """

        log_str = 'Construct a Human PRotein Ontology'; print(log_str); logger.info(log_str)

        networkx_mdg = self._processes_protein_ontology_data(); f_name = 'human_pro_classes.html'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        df_list = pandas.read_html(x); human_pro_classes = list(df_list[-1]['PRO_term'])
        # create a new graph using breadth first search paths
        human_pro_graph, human_networkx_mdg = Graph(), networkx.MultiDiGraph()
        for node in tqdm(human_pro_classes):
            forward = list(networkx.edge_bfs(networkx_mdg, URIRef(node), orientation='original'))
            reverse = list(networkx.edge_bfs(networkx_mdg, URIRef(node), orientation='reverse'))
            # add edges from forward and reverse breadth first search paths
            for path in forward + reverse:
                human_pro_graph.add((path[0], path[2], path[1]))
                human_networkx_mdg.add_edge(path[0], path[1], **{'key': path[2]})
        # check data and write it locally
        components = list(networkx.connected_components(human_networkx_mdg.to_undirected()))
        component_dict = sorted(components, key=len, reverse=True)
        if len(component_dict) > 1:  # if more than 1 connected component remove all but largest
            for node in [x for y in component_dict[1:] for x in list(y)]: human_pro_graph.remove((node, None, None))
        human_pro_graph.serialize(destination=self.temp_dir + '/human_pro.owl', format='xml')
        f_name1, f_name2 = self.temp_dir + '/human_pro.owl', self.temp_dir + '/pr_with_imports.owl'
        self._logically_verifies_human_protein_ontology(f_name1, f_name2, 'elk')

        return None

    def processes_relation_ontology_data(self) -> None:
        """Processes the Relations Ontology (RO) in order to obtain all ObjectProperties and their inverse relations.
        Additionally, the method writes out a file of all of the labels for all relations.

        Returns:
             None.
        """

        log_str = 'Creating Required Relations Ontology Data'; print(log_str); logger.info(log_str)

        f_name = 'ro_with_imports.owl'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        ro_graph = Graph().parse(x)
        labs = {str(x[2]).lower(): str(x[0]) for x in ro_graph if '/RO_' in str(x[0]) and 'label' in str(x[1]).lower()}
        # identify relations and their inverses
        filename1 = 'INVERSE_RELATIONS.txt'
        with open(self.temp_dir + '/' + filename1, 'w') as out1:
            out1.write('Relation' + '\t' + 'Inverse_Relation' + '\n')
            for s, p, o in ro_graph:
                if 'owl#inverseOf' in str(p) and ('RO' in str(s) and 'RO' in str(o)):
                    out1.write(str(s.split('/')[-1]) + '\t' + str(o.split('/')[-1]) + '\n')
                    out1.write(str(o.split('/')[-1]) + '\t' + str(s.split('/')[-1]) + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename1)
        # identify relation labels
        filename2 = 'RELATIONS_LABELS.txt'
        with open(self.temp_dir + '/' + filename2, 'w') as out1:
            out1.write('Label' + '\t' + 'Relation' + '\n')
            for k, v in labs.items():
                out1.write(str(k).split('/')[-1] + '\t' + str(v) + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename2)

        return None

    def processes_clinvar_data(self) -> None:
        """Processes ClinVar data by performing light tidying and filtering and then outputs data needed to create
        mappings between genes, variants, and phenotypes.

        Returns:
            None.
        """

        log_str = 'Generating ClinVar Cross-Mapping Data'; print(log_str); logger.info(log_str)

        f_name = 'variant_summary.txt'
        clinvar_data = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=0, head=0, sht=None)
        clinvar_data.fillna('None', inplace=True)
        # explode nested data
        explode_df_clinvar = explodes_data(clinvar_data.copy(), ['PhenotypeIDS'], ';')
        explode_df_clinvar = explodes_data(explode_df_clinvar.copy(), ['PhenotypeIDS'], ',')
        explode_df_clinvar['PhenotypeIDS'].replace('Orphanet:ORPHA', 'ORPHA:', inplace=True, regex=True)
        explode_df_clinvar['PhenotypeIDS'].replace('Human Phenotype Ontology:HP:', 'HP_', inplace=True, regex=True)
        filename = 'CLINVAR_VARIANT_GENE_DISEASE_PHENOTYPE_EDGES.txt'
        explode_df_clinvar.to_csv(self.temp_dir + '/' + filename, header=True, sep='\t', encoding='utf-8', index=False)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return None

    def processes_cofactor_catalyst_data(self) -> None:
        """Processes uniprot-cofactor-catalyst.tab file from the Uniprot Knowledge Base in order to enable the building
        of protein-cofactor and protein-catalyst edges.

        Returns:
            None.
        """

        log_str = 'Creating Protein-Cofactor and Protein-Catalyst Cross-Mappings'; print(log_str); logger.info(log_str)

        # reformat data and write data
        f_name = 'uniprot-cofactor-catalyst.tab'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        data = open(x).readlines()
        filename1, filename2 = 'UNIPROT_PROTEIN_COFACTOR.txt', 'UNIPROT_PROTEIN_CATALYST.txt'
        with open(self.temp_dir + '/' + filename1, 'w') as out1, open(self.temp_dir + '/' + filename2, 'w') as out2:
            for line in tqdm(data):
                if 'CHEBI' in line.split('\t')[4]:  # cofactors
                    for i in line.split('\t')[4].split(';'):
                        chebi = i.split('[')[-1].replace(']', '').replace(':', '_')
                        out1.write('PR_' + line.split('\t')[3].strip(';') + '\t' + chebi + '\n')
                if 'CHEBI' in line.split('\t')[5]:  # catalysts
                    for j in line.split('\t')[5].split(';'):
                        chebi = j.split('[')[-1].replace(']', '').replace(':', '_')
                        out2.write('PR_' + line.split('\t')[3].strip(';') + '\t' + chebi + '\n')
        # push data to gsc bucket
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename1)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename2)

        return None

    def _creates_gene_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each Entrez gene identifier present in
        the input data file.

        Returns:
            gene_metadata_dict: A dict containing metadata that's keyed by Entrez gene identifier and whose values are
                dicts containing label, description, and synonym information. For example:
                    {{'http://www.ncbi.nlm.nih.gov/gene/1': {
                        'Label': 'A1BG',
                        'Description': "A1BG is 'protein-coding' and is located on chromosome 19 (19q13.43).",
                        'Synonym': 'HEL-S-163pA|A1B|ABG|HYST2477alpha-1B-glycoprotein|GAB'}, ...}
        """

        log_str = 'Generating Metadata for Gene Identifiers'; print('\t- ' + log_str); logger.info(log_str)

        f_name = 'Homo_sapiens.gene_info'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        data = pandas.read_csv(x, header=0, delimiter='\t', low_memory=False)
        data = data.loc[data['#tax_id'].apply(lambda i: i == 9606)]
        data.fillna('None', inplace=True); data.replace('-', 'None', inplace=True, regex=False)
        # create metadata
        genes, lab, desc, syn = [], [], [], []
        for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
            gene_id, sym, defn, gene_type = row['GeneID'], row['Symbol'], row['description'], row['type_of_gene']
            chrom, map_loc, s1, s2 = row['chromosome'], row['map_location'], row['Synonyms'], row['Other_designations']
            if gene_id != 'None':
                genes.append('http://www.ncbi.nlm.nih.gov/gene/' + str(gene_id))
                if sym != 'None' or sym != '': lab.append(sym)
                else: lab.append('Entrez_ID:' + gene_id)
                if 'None' not in [defn, gene_type, chrom, map_loc]:
                    desc_str = "{} has locus group '{}' and is located on chromosome {} ({})."
                    desc.append(desc_str.format(sym, gene_type, chrom, map_loc))
                else: desc.append("{} locus group '{}'.".format(sym, gene_type))
                if s1 != 'None' and s2 != 'None':
                    syn.append('|'.join(set([x for x in (s1 + s2).split('|') if x != 'None' or x != ''])))
                elif s1 != 'None': syn.append('|'.join(set([x for x in s1.split('|') if x != 'None' or x != ''])))
                elif s2 != 'None': syn.append('|'.join(set([x for x in s2.split('|') if x != 'None' or x != ''])))
                else: syn.append('None')
        # combine into new data frame then convert it to dictionary
        metadata = pandas.DataFrame(list(zip(genes, lab, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
        metadata = metadata.astype(str); metadata.drop_duplicates(subset='ID', keep='first', inplace=True)
        metadata.set_index('ID', inplace=True); gene_metadata_dict = metadata.to_dict('index')

        return gene_metadata_dict

    def _creates_transcript_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each Entrez gene identifier present
        in the input data file.

        Returns:
            rna_metadata_dict: A dict containing metadata that's keyed by Ensembl transcript identifier and whose values
                are a dict containing label, description, and synonym information. For example:
                    {'https://uswest.ensembl.org/Homo_sapiens/Transcript/Summary?t=ENST00000456328': {
                        'Label': 'DDX11L1-202',
                        'Description': "Transcript DDX11L1-202 is classified as type 'processed_transcript'.",
                        'Synonym': 'None'}, ...}
        """

        log_str = 'Generating Metadata for Transcript Identifiers'; print('\t- ' + log_str); logger.info(log_str)

        f_name = 'ensembl_identifier_data_cleaned.txt'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        dup_cols = ['transcript_stable_id', 'transcript_name', 'ensembl_transcript_type']
        data = pandas.read_csv(x, header=0, delimiter='\t', low_memory=False)
        data = data.loc[data['transcript_stable_id'].apply(lambda i: i != 'None')]
        data.drop(['ensembl_gene_id', 'symbol', 'protein_stable_id', 'uniprot_id', 'master_transcript_type',
                   'entrez_id', 'ensembl_gene_type', 'master_gene_type', 'symbol'], axis=1, inplace=True)
        data.drop_duplicates(subset=dup_cols, keep='first', inplace=True); data.fillna('None', inplace=True)
        # create metadata
        rna, lab, desc, syn = [], [], [], []
        for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
            rna_id, ent_type, nme = row[dup_cols[0]], row[dup_cols[2]], row[dup_cols[1]]
            rna.append('https://uswest.ensembl.org/Homo_sapiens/Transcript/Summary?t=' + rna_id)
            if nme != 'None': lab.append(nme)
            else: lab.append('Ensembl_Transcript_ID:' + rna_id); nme = 'Ensembl_Transcript_ID:' + rna_id
            if ent_type != 'None': desc.append("Transcript {} is classified as type '{}'.".format(nme, ent_type))
            else: desc.append('None')
            syn.append('None')
        # combine into new data frame then convert it to dictionary
        metadata = pandas.DataFrame(list(zip(rna, lab, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
        metadata = metadata.astype(str); metadata.drop_duplicates(subset='ID', keep='first', inplace=True)
        metadata.set_index('ID', inplace=True); rna_metadata_dict = metadata.to_dict('index')

        return rna_metadata_dict

    def _creates_variant_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each ClinVar variant identifier present
        in the input data file.

        Returns:
            variant_metadata_dict: A dict containing metadata that's keyed by ClinVar variant identifier and whose
                values are a dict containing label, description, and synonym information. For example:
                    {{'https://www.ncbi.nlm.nih.gov/snp/rs141138948': {
                        'Label': 'NM_016042.4(EXOSC3):c.395A>C (p.Asp132Ala)',
                        'Description': "This variant is a germline single nucleotide variant on chromosome 9
                        (NC_000009.12, start:37783993/stop:37783993 positions,cytogenetic location:9p13.2) and
                        has clinical significance 'Pathogenic/Likely pathogenic'. This entry is for the GRCh38 and was
                        last reviewed on Sep 30, 2020 with review status 'criteria provided, multiple submitters,
                        no conflict'.", 'Synonym': 'None'}, ...}
        """

        log_str = 'Generating Metadata for Variant IDs'; print('\t- ' + log_str); logger.info(log_str)

        f_name = 'variant_summary.txt'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        data = pandas.read_csv(x, header=0, delimiter='\t', low_memory=False)
        data = data.loc[data['Assembly'].apply(lambda i: i == 'GRCh38')]
        data = data.loc[data['RS# (dbSNP)'].apply(lambda i: i != -1)]
        data = data[['#AlleleID', 'Type', 'Name', 'ClinicalSignificance', 'RS# (dbSNP)', 'Origin', 'Start', 'Stop',
                     'ChromosomeAccession', 'Chromosome', 'ReferenceAllele', 'Assembly', 'AlternateAllele',
                     'Cytogenetic', 'ReviewStatus', 'LastEvaluated']]
        data.replace('na', 'None', inplace=True); data.fillna('None', inplace=True)
        data.sort_values('LastEvaluated', ascending=False, inplace=True)
        data.drop_duplicates(subset='RS# (dbSNP)', keep='first', inplace=True)
        # create metadata
        var, label, desc, syn = [], [], [], []
        for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
            var_id, lab = row['RS# (dbSNP)'], row['Name']
            if var_id != 'None':
                var.append('https://www.ncbi.nlm.nih.gov/snp/rs' + str(var_id))
                if lab != 'None': label.append(lab)
                else: label.append('dbSNP_ID:rs' + str(var_id))
                sent = "This variant is a {} {} located on chromosome {} ({}, start:{}/stop:{} positions, " + \
                       "cytogenetic location:{}) and has clinical significance '{}'. " + \
                       "This entry is for the {} and was last reviewed on {} with review status '{}'."
                desc.append(
                    sent.format(row['Origin'].replace(';', '/'), row['Type'].replace(';', '/'), row['Chromosome'],
                                row['ChromosomeAccession'], row['Start'], row['Stop'], row['Cytogenetic'],
                                row['ClinicalSignificance'], row['Assembly'], row['LastEvaluated'],
                                row['ReviewStatus']).replace('None', 'UNKNOWN'))
                syn.append('None')
        # combine into new data frame then convert it to dictionary
        metadata = pandas.DataFrame(list(zip(var, label, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
        metadata.drop_duplicates(subset=None, keep='first', inplace=True); metadata = metadata.astype(str)
        metadata.set_index('ID', inplace=True); variant_metadata_dict = metadata.to_dict('index')

        return variant_metadata_dict

    def _creates_pathway_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each Reactome Pathway identifier
        present in the human Reactome Pathway Database identifier data set (ReactomePathways.txt); Reactome-Gene
        Association data (gene_association.reactome.gz), and Reactome-ChEBI data (ChEBI2Reactome_All_Levels.txt). The
        keys of the dictionary are Reactome identifiers and the values are dictionaries for each metadata type.

        Returns:
            pathway_metadata_dict: A dict containing metadata that's keyed by Reactome Pathway identifier and whose
                values are a dict containing label, description, and synonym information. For example:
                    {{'https://reactome.org/content/detail/R-HSA-157858': {
                        'Label': 'Gap junction trafficking and regulation',
                        'Description': 'None',
                        'Synonym': 'Gap junction trafficking and regulation'}}, ...}
        """

        log_str = 'Generating Metadata for Pathway IDs'; print('\t- ' + log_str); logger.info(log_str)

        # reactome pathways
        f_name = 'ReactomePathways.txt'
        f = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        data = pandas.read_csv(f, header=None, delimiter='\t', low_memory=False)
        data = data.loc[data[2].apply(lambda x: x == 'Homo sapiens')]
        # reactome gene association data
        f_name1 = 'gene_association.reactome'
        g = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name1, self.temp_dir)
        data1 = pandas.read_csv(g, header=None, delimiter='\t', skiprows=3, low_memory=False)
        data1 = data1.loc[data1[12].apply(lambda x: x == 'taxon:9606')]
        data1[5].replace('REACTOME:', '', inplace=True, regex=True)
        # reactome CHEBI data
        f_name2 = 'ChEBI2Reactome_All_Levels.txt'
        h = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name2, self.temp_dir)
        data2 = pandas.read_csv(h, header=None, delimiter='\t', low_memory=False)
        data2 = data2.loc[data2[5].apply(lambda x: x == 'Homo sapiens')]
        # set unique node list
        nodes = set(list(data[0]) + list(data1[5]) + list(data2[1])); metadata = metadata_api_mapper(list(nodes))
        metadata['ID'] = metadata['ID'].map('https://reactome.org/content/detail/{}'.format)
        metadata.set_index('ID', inplace=True); pathway_metadata_dict = metadata.to_dict('index')

        return pathway_metadata_dict

    def _creates_relations_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each Relation Ontology identifier
        present in the input data file.

        Returns:
            relation_metadata_dict: A dict containing metadata that's keyed by Relations identifier and whose
                values are a dict containing label, description, and synonym information. For example:
                    {{'http://purl.obolibrary.org/obo/RO_0002310': {
                    'Label': 'exposure event or process',
                    'Description': 'A process occurring within or in the vicinity of an organism that exerts some causal
                    influence on the organism via the interaction between an exposure stimulus and an exposure receptor.
                    The exposure stimulus may be a process, material entity or condition (e.g. lack of nutrients).
                    The exposure receptor can be an organism, organism population or a part of an organism.',
                    'Synonym': 'None'}}, ...}
        """

        log_str = 'Generating Metadata for Relations IDs'; print('\t- ' + log_str); logger.info(log_str)

        # get ontology information
        f_name = 'ro_with_imports.owl'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        ro_graph = Graph().parse(x)
        relation_metadata_dict, obo = {}, Namespace('http://purl.obolibrary.org/obo/')
        cls = [x for x in gets_ontology_classes(ro_graph) if '/RO_' in str(x)] + \
              [x for x in gets_object_properties(ro_graph) if '/RO_' in str(x)]
        master_synonyms = [x for x in ro_graph if 'synonym' in str(x[1]).lower() and isinstance(x[0], URIRef)]
        for x in tqdm(cls):
            cls_label = list(ro_graph.objects(x, RDFS.label))
            labels = str(cls_label[0]) if len(cls_label) > 0 else 'None'
            cls_syn = [str(i[2]) for i in master_synonyms if x == i[0]]
            synonym = str(cls_syn[0]) if len(cls_syn) > 0 else 'None'
            cls_desc = list(ro_graph.objects(x, obo.IAO_0000115))
            desc = '|'.join([str(cls_desc[0])]) if len(cls_desc) > 0 else 'None'
            relation_metadata_dict[str(x)] = {'Label': labels, 'Description': desc, 'Synonym': synonym}

        return relation_metadata_dict

    def creates_non_ontology_class_metadata_dict(self) -> None:
        """Combines the gene metadata, transcript metadata, variant metadata, pathway metadata, and relations
        metadata dictionaries into a single large metadata dictionary. See example output below:
        {
            'nodes': {
                'http://www.ncbi.nlm.nih.gov/gene/1': {
                    'Label': 'A1BG',
                    'Description': "A1BG has locus group protein-coding' and is located on chromosome 19 (19q13.43).",
                    'Synonym': 'HYST2477alpha-1B-glycoprotein|HEL-S-163pA|ABG|A1B|GAB'} ... },
            'relations': {
                'http://purl.obolibrary.org/obo/RO_0002533': {
                    'Label': 'sequence atomic unit',
                    'Description': 'Any individual unit of a collection of like units arranged in a linear order',
                    'Synonym': 'None'} ... }
        }

        Returns:
            None.
        """

        log_str = 'Creating Master Metadata Dictionary for Non-Ontology Entities'; print(log_str); logger.info(log_str)

        # create single dictionary of
        master_metadata_dictionary = {'nodes': {**self._creates_gene_metadata_dict(),
                                                **self._creates_transcript_metadata_dict(),
                                                **self._creates_variant_metadata_dict(),
                                                **self._creates_pathway_metadata_dict()},
                                      'relations': self._creates_relations_metadata_dict()}
        # save data and push to gcs bucket
        filename = 'node_metadata_dict.pkl'
        pickle.dump(master_metadata_dictionary, open(self.temp_dir + '/' + filename, 'wb'), protocol=4)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return None

    def preprocesses_build_data(self) -> None:
        """Master method that performs all needed data preprocessing tasks in preparation of generating PheKnowLator
        knowledge graphs. This method completes this work in 10 steps.

        Returns:
            None.
        """

        log_str = '*** PROCESSING LINKED OPEN DATA SOURCES ***'; print(log_str); logger.info(log_str)

        # STEP 1: Human Transcript, Gene, and Protein Identifier Mapping
        log_str = 'STEP 1: HUMAN TRANSCRIPT, GENE, PROTEIN ID MAPPING'; print('\n' + log_str); logger.info(log_str)
        self.generates_specific_genomic_identifier_maps()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 2: MeSH-ChEBI Identifier Mapping
        log_str = 'STEP 2: MESH-CHEBI ID MAPPING'; print('\n' + log_str); logger.info(log_str)
        self.creates_chebi_to_mesh_identifier_mappings()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 3: Disease and Phenotype Identifier Mapping
        log_str = 'STEP 3: DISEASE-PHENOTYPE ID MAPPING'; print('\n' + log_str); logger.info(log_str)
        self.creates_disease_identifier_mappings()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 4: Human Protein Atlas/GTEx Tissue/Cells Edge Data
        log_str = 'STEP 4: CREATING HPA + GTEX ID EDGE DATA'; print('\n' + log_str); logger.info(log_str)
        self._hpa_gtex_ontology_alignment()
        self.processes_hpa_gtex_data()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 5: Creating Pathway and Sequence Ontology Mappings
        log_str = 'STEP 5: PATHWAY + SEQUENCE ONTOLOGY ID MAP'; print('\n' + log_str); logger.info(log_str)
        self.combines_pathway_and_sequence_ontology_dictionaries()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 6: Creating a Human Protein Ontology
        log_str = 'STEP 6: CREATING A HUMAN PROTEIN ONTOLOGY'; print('\n' + log_str); logger.info(log_str)
        self.constructs_human_protein_ontology()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 7: Extracting Relations Ontology Information
        log_str = 'STEP 7: EXTRACTING RELATION ONTOLOGY INFO'; print('\n' + log_str); logger.info(log_str)
        self.processes_relation_ontology_data()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 8: Clinvar Variant-Diseases and Phenotypes Edge Data
        log_str = 'STEP 8: CREATING CLINVAR VARIANT-DISEASE-PHENOTYPE DATA'; print('\n' + log_str); logger.info(log_str)
        self.processes_clinvar_data()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 9: Uniprot Protein-Cofactor and Protein-Catalyst Edge Data
        log_str = 'STEP 9: CREATING COFACTOR + CATALYST EDGE DATA'; print('\n' + log_str); logger.info(log_str)
        self.processes_cofactor_catalyst_data()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        # STEP 10: Non-Ontology Metadata Dictionary
        log_str = 'STEP 10: CREATING OBO-ONTOLOGY METADATA DICTIONARY'; print('\n' + log_str); logger.info(log_str)
        self.creates_non_ontology_class_metadata_dict()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        return None
