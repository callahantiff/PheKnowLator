#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
# import fnmatch
import glob
# import itertools
import json
import logging.config
import networkx  # type: ignore
import numpy  # type: ignore
import os
import pandas  # type: ignore
import pickle
import re
import requests
import shutil
import sys

from google.cloud import storage  # type: ignore
from rdflib import Graph, Namespace, URIRef  # type: ignore
from rdflib.namespace import RDFS, OWL  # type: ignore
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

# set external namespaces for use with RDFLib Graph objects
obo = Namespace('http://purl.obolibrary.org/obo/')


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

        hgnc = self.reads_gcs_bucket_data_to_df(f_name='hgnc_complete_set.txt', delm='\t', head=0)
        hgnc = hgnc.loc[hgnc['status'].apply(lambda x: x == 'Approved')]
        hgnc = hgnc[['hgnc_id', 'entrez_id', 'ensembl_gene_id', 'uniprot_ids', 'symbol', 'locus_type', 'alias_symbol',
                     'name', 'location', 'alias_name']]
        hgnc.rename(columns={'uniprot_ids': 'uniprot_id', 'location': 'map_location', 'locus_type': 'hgnc_gene_type'},
                    inplace=True)
        hgnc['hgnc_id'] = hgnc['hgnc_id'].str.replace('.*\:', '', regex=True)  # strip 'HGNC' off of the identifiers
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
            explode_df_hgnc['hgnc_gene_type'] = explode_df_hgnc['hgnc_gene_type'].str.replace(
                v, self.genomic_type_mapper['hgnc_gene_type'][v])
        # reformat master hgnc gene type
        explode_df_hgnc['master_gene_type'] = explode_df_hgnc['hgnc_gene_type']
        master_dict = self.genomic_type_mapper['hgnc_master_gene_type']
        for val in master_dict.keys():
            explode_df_hgnc['master_gene_type'] = explode_df_hgnc['master_gene_type'].str.replace(val, master_dict[val])
        # post-process reformatted data
        explode_df_hgnc.drop(['alias_symbol', 'alias_name'], axis=1, inplace=True)  # remove original gene type column
        explode_df_hgnc.drop_duplicates(inplace=True)

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
        ensembl = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=5)
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
        for val in gene_dict.keys():
            ensembl_geneset['ensembl_gene_type'] = ensembl_geneset['ensembl_gene_type'].str.replace(val, gene_dict[val])
        # reformat master gene type
        ensembl_geneset['master_gene_type'] = ensembl_geneset['ensembl_gene_type']
        gene_dict = self.genomic_type_mapper['ensembl_master_gene_type']
        for val in gene_dict.keys():
            ensembl_geneset['master_gene_type'] = ensembl_geneset['master_gene_type'].str.replace(val, gene_dict[val])
        # reformat master transcript type
        ensembl_geneset['ensembl_transcript_type'] = ensembl_geneset['ensembl_transcript_type'].str.replace(
            'vault_RNA', 'vaultRNA', regex=False)
        ensembl_geneset['master_transcript_type'] = ensembl_geneset['ensembl_transcript_type']
        trans_d = self.genomic_type_mapper['ensembl_master_transcript_type']
        for val in trans_d.keys():
            ensembl_geneset['master_transcript_type'] = ensembl_geneset['master_transcript_type'].str.replace(
                val, trans_d[val])
        # post-process reformatted data
        ensembl_geneset.drop_duplicates(inplace=True)

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
        ensembl_uniprot = self.reads_gcs_bucket_data_to_df(f_name=un_name, delm='\t', head=0)
        ensembl_uniprot.rename(columns={'xref': 'uniprot_id', 'gene_stable_id': 'ensembl_gene_id'}, inplace=True)
        ensembl_uniprot.replace('-', 'None', inplace=True); ensembl_uniprot.fillna('None', inplace=True)
        ensembl_uniprot = ensembl_uniprot.loc[ensembl_uniprot['uniprot_id'].apply(lambda x: '-' not in x)]
        ensembl_uniprot = ensembl_uniprot.loc[ensembl_uniprot['info_type'].apply(lambda x: x == 'DIRECT')]
        ensembl_uniprot = ensembl_uniprot.loc[ensembl_uniprot['xref_identity'].apply(lambda x: x != 'None')]
        ensembl_uniprot.drop(drop_cols, axis=1, inplace=True)
        ensembl_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)
        # entrez data
        ensembl_entrez = self.reads_gcs_bucket_data_to_df(f_name=ent_name, delm='\t', head=0)
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
        ensembl_annot.drop_duplicates(inplace=True)
        # merge annotation and gene data
        ensembl_geneset = self._preprocess_ensembl_data()
        merge_cols = list(set(ensembl_annot).intersection(set(ensembl_geneset)))
        ensembl = pandas.merge(ensembl_geneset, ensembl_annot, on=merge_cols, how='outer')
        ensembl.fillna('None', inplace=True); ensembl.replace('NA', 'None', inplace=True)
        ensembl.drop_duplicates(inplace=True)
        # save data locally and push to gcs bucket
        filename = 'ensembl_identifier_data_cleaned.txt'
        ensembl.to_csv(self.temp_dir + '/' + filename, sep='\t', index=False)
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
        uniprot = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', head=0)
        uniprot.fillna('None', inplace=True)
        uniprot.rename(columns={'Entry': 'uniprot_id', 'Cross-reference (GeneID)': 'entrez_id',
                                'Ensembl transcript': 'transcript_stable_id', 'Cross-reference (HGNC)': 'hgnc_id',
                                'Gene names  (synonym )': 'synonyms', 'Gene names  (primary )': 'symbol'}, inplace=True)
        uniprot['synonyms'] = uniprot['synonyms'].apply(lambda x: '|'.join(x.split()) if x.isupper() else x)
        uniprot = uniprot.loc[uniprot['Status'].apply(lambda x: x != 'unreviewed')]  # keeping only reviewed entries

        # explode nested data and perform light value reformatting
        explode_df_uniprot = explodes_data(uniprot.copy(), ['transcript_stable_id', 'entrez_id', 'hgnc_id'], ';')
        explode_df_uniprot = explodes_data(explode_df_uniprot.copy(), ['symbol', 'synonyms'], '|')
        explode_df_uniprot['transcript_stable_id'] = explode_df_uniprot['transcript_stable_id'].str.replace(
            '\s.*', '', regex=True)  # strip uniprot
        explode_df_uniprot.drop(['Status'], axis=1, inplace=True)
        explode_df_uniprot.drop_duplicates(inplace=True)

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
        ncbi_gene = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', head=0)
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
            explode_df_ncbi_gene['entrez_gene_type'] = explode_df_ncbi_gene['entrez_gene_type'].str.replace(
                val, gene_dict[val])
        # reformat master gene type
        explode_df_ncbi_gene['master_gene_type'] = explode_df_ncbi_gene['entrez_gene_type']
        gene_dict = self.genomic_type_mapper['master_gene_type']
        for val in gene_dict.keys():
            explode_df_ncbi_gene['master_gene_type'] = explode_df_ncbi_gene['master_gene_type'].str.replace(
                val, gene_dict[val])
        # post-process reformatted data
        explode_df_ncbi_gene['hgnc_id'] = explode_df_ncbi_gene['hgnc_id'].str.replace('HGNC:', '', regex=True)
        explode_df_ncbi_gene['ensembl_gene_id'] = explode_df_ncbi_gene['ensembl_gene_id'].str.replace('Ensembl:', '',
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
        pro = self.reads_gcs_bucket_data_to_df(f_name='promapping.txt', delm='\t', head=col_names)
        pro = pro.loc[pro['Entry'].apply(lambda x: x.startswith('UniProtKB:') and '_VAR' not in x and ', ' not in x)]
        pro = pro.loc[pro['pro_mapping'].apply(lambda x: x.startswith('exact'))]
        pro['pro_id'] = pro['pro_id'].str.replace('PR:', 'PR_', regex=True)  # replace PR: with PR_
        pro['Entry'] = pro['Entry'].str.replace('(^\w*\:)', '', regex=True)  # remove ids which appear before ':'
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
        ensembl_hgnc.fillna('None', inplace=True); ensembl_hgnc.drop_duplicates(inplace=True)
        # ensembl_hgnc + uniprot
        uniprot = self._preprocess_uniprot_data()
        merge_cols = list(set(ensembl_hgnc.columns).intersection(set(uniprot.columns)))
        ensembl_hgnc_uniprot = pandas.merge(ensembl_hgnc, uniprot, on=merge_cols, how='outer')
        ensembl_hgnc_uniprot.fillna('None', inplace=True)
        ensembl_hgnc_uniprot.drop_duplicates(inplace=True)
        # ensembl_hgnc_uniprot + Homo_sapiens.gene_info
        ncbi = self._preprocess_ncbi_data()
        merge_cols = list(set(ensembl_hgnc_uniprot.columns).intersection(set(ncbi.columns)))
        ensembl_hgnc_uniprot_ncbi = pandas.merge(ensembl_hgnc_uniprot, ncbi, on=merge_cols, how='outer')
        ensembl_hgnc_uniprot_ncbi.fillna('None', inplace=True)
        ensembl_hgnc_uniprot_ncbi.drop_duplicates(inplace=True)
        # ensembl_hgnc_uniprot_ncbi + promapping.txt
        pro = self._preprocess_protein_ontology_mapping_data()
        merged_data = pandas.merge(ensembl_hgnc_uniprot_ncbi, pro, on='uniprot_id', how='outer')
        merged_data.fillna('None', inplace=True); merged_data.drop_duplicates(inplace=True)

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
        merged_data['hgnc_gene_type'] = merged_data['hgnc_gene_type'].str.replace('None', 'unknown', regex=False)
        merged_data['ensembl_gene_type'] = merged_data['ensembl_gene_type'].str.replace('None', 'unknown', regex=False)
        merged_data['entrez_gene_type'] = merged_data['entrez_gene_type'].str.replace('None', 'unknown', regex=False)
        merged_data['master_gene_type'] = merged_data['master_gene_type'].str.replace('None', 'unknown', regex=False)
        merged_data['master_transcript_type'] = merged_data['master_transcript_type'].str.replace(
            'None', 'not protein-coding', regex=False)
        merged_data['ensembl_transcript_type'] = merged_data['ensembl_transcript_type'].str.replace(
            'None', 'unknown', regex=False)
        merged_data_clean = merged_data.drop_duplicates()

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

    def _write_genomic_entity_metadata(self):
        """Process the dictionary created in the prior steps in order to assist with creating a master metadata file
        for all nodes that are a genomic entity (i.e., genes, transcripts, or proteins).
        """

        reformatted_mapped_identifiers = self.creates_master_genomic_identifier_map()
        out_location = self.temp_dir + '/GENOMIC_ENTITY_METADATA.jsonl'

        for key, value in tqdm(reformatted_mapped_identifiers.items()):
            old_prefix = '_'.join(key.split('_')[0:-1]); idx = key.split('_')[-1]; pass_var = True; new_prefix = None
            if old_prefix == 'entrez_id': new_prefix = 'NCBIGene'
            elif old_prefix in ['ensembl_gene_id', 'protein_stable_id', 'transcript_stable_id']: new_prefix = 'ensembl'
            elif old_prefix == 'pro_id': new_prefix = 'PR'
            else: pass_var = False
            if pass_var and new_prefix is not None:
                updated_key = new_prefix + ':' + idx; master_metadata_dict = {updated_key: {}}
                for x in value:
                    i, j = '_'.join(x.split('_')[0:-1]), x.split('_')[-1]
                    if 'type' in i: continue
                    elif i == 'entrez_id': new_i = 'NCBIGene'; j = new_i + ':' + j
                    elif i == 'ensembl_gene_id': new_i = 'ensembl gene'; j = 'ensembl:' + j
                    elif i == 'protein_stable_id': new_i = 'ensembl protein'; j = 'ensembl:' + j
                    elif i == 'transcript_stable_id': new_i = 'ensembl transcript'; j = 'ensembl:' + j
                    elif i == 'pro_id_PR': new_i = 'PR'; j = new_i + ':' + j
                    elif i == 'hgnc_id': new_i = 'HGNC_ID'; j = new_i + ':' + j
                    elif i == 'uniprot_id': new_i = 'uniprot'; j = new_i + ':' + j
                    elif i == 'symbol': new_i = 'GeneSymbol'; j = new_i + ':' + j
                    else:
                        if i == 'synonyms': new_i = 'Synonyms'
                        elif i == 'name': new_i = 'Label'
                        elif i == 'Other_designations': new_i = 'Synonyms'; j = j.split('|')
                        else: new_i = i
                    if new_i in master_metadata_dict[updated_key].keys():
                        if isinstance(j, list): master_metadata_dict[updated_key][new_i] += j
                        else: master_metadata_dict[updated_key][new_i] += [j]
                    else: master_metadata_dict[updated_key][new_i] = [j]
                # write entry
                dump_jsonl([master_metadata_dict], out_location)

        # load data to cloud
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, '/GENOMIC_ENTITY_METADATA.jsonl')

        return None

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
             'entrez_gene_type', 'gene_type_update', 'gene_type_update', [1, 1, 'NCBIGene_']],
            ['ENSEMBL_TRANSCRIPT_PROTEIN_ONTOLOGY_MAP.txt', 'transcript_stable_id', 'pro_id', 'ensembl_transcript_type',
             None, 'transcript_type_update', None, [0, 5, 'ensembl_']],
            ['ENTREZ_GENE_ENSEMBL_TRANSCRIPT_MAP.txt', 'entrez_id', 'transcript_stable_id', 'entrez_gene_type',
             'ensembl_transcript_type', 'gene_type_update', 'transcript_type_update', [0, 7, 'NCBIGene_'],
             [1, 1, 'ensembl_']],
            ['ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt', 'entrez_id', 'pro_id', 'entrez_gene_type', None, 'gene_type_update',
             None, [0, 5, 'NCBIGene_']],
            ['GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt', 'symbol', 'transcript_stable_id', 'master_gene_type',
             'ensembl_transcript_type', 'gene_type_update', 'transcript_type_update', [1, 1, 'ensembl_']],
            ['STRING_PRO_ONTOLOGY_MAP.txt', 'protein_stable_id', 'pro_id', None, None, None, None, [0, 0, '9606.']],
            ['UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt', 'uniprot_id', 'pro_id', None, None, None, None, None],
            ['UNIPROT_ACCESSION_ENTREZ_GENE_MAP.txt', 'uniprot_id', 'entrez_id', None, 'master_gene_type', None,
             'gene_type_update', [1, 1, 'NCBIGene_']]
        ]

        for x in gene_sets:
            genomic_id_mapper(reformatted_mapped_identifiers, self.temp_dir + '/' + x[0],  # type: ignore
                              x[1], x[2], x[3], x[4], x[5], x[6])  # type: ignore

            if x[-1] is not None:
                df = pandas.read_csv(self.temp_dir + '/' + x[0], header=None, delimiter='\t', low_memory=False)
                for i in x[7:]:
                    df[i[1]] = i[2] + df[i[0]].astype(str)
                df = df.replace('None', numpy.nan).dropna(axis=1, how="all")
                df.to_csv(self.temp_dir + '/' + x[0], header=None, sep='\t', index=False)

            uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, x[0])  # type: ignore
            self._write_genomic_entity_metadata()  # write genomic metadata

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

        chebi = self.reads_gcs_bucket_data_to_df(f_name='names.tsv', delm='\t', head=0)
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
        identifier_merge = pandas.merge(chebi_df[merge_cols], mesh_df[merge_cols], on='STR')
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
            for pair in mesh_edges: out.write(pair[0].replace('_', ':') + '\t' + pair[1] + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return None

    def _preprocess_mondo_mapping_data(self) -> pandas.DataFrame:
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
        mondo_dict = {str(k).lower().split('/')[-1]: {str(i).split('/')[-1].replace('_', ':') for i in v}
                      for k, v in dbxref_res.items() if 'MONDO' in str(v)}

        # convert to pandas DataFrame
        temp_list = []
        for k, v in mondo_dict.items():
            if k.startswith('umls:'): new_k = k.split(':')[-1].upper()
            elif k.startswith('hp:'): new_k = k.upper()
            elif k.startswith('mesh:'): new_k = 'MESH:' + k.split(':')[-1].upper()
            elif k.startswith('orphanet:'): new_k = 'ORPHA:' + k.split(':')[-1].upper()
            elif k.startswith('omimps:'): new_k = 'OMIM:' + k.split(':')[-1].upper()
            else: new_k = k
            for i in v:
                temp_list += [[new_k, i.replace(':', '_')]]; temp_list += [[i, i.replace(':', '_')]]
        # convert to
        mondo_df = pandas.DataFrame({'other_id': [x[0] for x in temp_list], 'ontology_id': [x[1] for x in temp_list]})

        return mondo_df

    def _preprocess_hpo_mapping_data(self) -> pandas.DataFrame:
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
        hp_dict = {str(k).lower().split('/')[-1]: {str(i).split('/')[-1].replace('_', ':') for i in v}
                   for k, v in dbxref_res.items() if 'HP' in str(v)}

        # convert to pandas DataFrame
        temp_list = []
        for k, v in hp_dict.items():
            if k.startswith('umls:'): new_k = k.split(':')[-1].upper()
            elif k.startswith('mondo:'): new_k = k.upper()
            elif k.startswith('msh:'): new_k = 'MESH:' + k.split(':')[-1].upper()
            elif k.startswith('orpha:'): new_k = 'ORPHA:' + k.split(':')[-1].upper()
            else: new_k = k
            for i in v:
                temp_list += [[new_k, i.replace(':', '_')]]; temp_list += [[i, i.replace(':', '_')]]
        # convert to
        hp_df = pandas.DataFrame({'other_id': [x[0] for x in temp_list], 'ontology_id': [x[1] for x in temp_list]})

        return hp_df

    def reads_disgenet_data(self) -> pandas.DataFrame:
        """Reads in disease mapping data from DisGeNET.

        Returns:
            data: A pandas DataFrame object.
        """

        data = self.reads_gcs_bucket_data_to_df(f_name='disease_mappings.tsv', delm='\t', head=0)

        # reformat data
        data['vocabulary'] = data['vocabulary'].str.lower()
        data['diseaseId'] = data['diseaseId'].str.lower()
        data['vocabulary'] = data['vocabulary'].str.replace('hpo', 'HP')
        data['vocabulary'] = data['vocabulary'].str.replace('mondo', 'MONDO')
        data['vocabulary'] = data['vocabulary'].str.replace('msh', 'MESH')
        data['vocabulary'] = data['vocabulary'].str.replace('omim', 'OMIM')
        data['vocabulary'] = data['vocabulary'].str.replace('do', 'doid')
        data['vocabulary'] = data['vocabulary'].str.replace('ordo', 'ORPHA')
        data['vocabulary'] = data['vocabulary'].str.replace('ORPHAid', 'ORPHA')
        # capitalize UMLS id
        data['diseaseId'] = data['diseaseId'].str.upper()
        # create a disease code column
        data['code'] = data['vocabulary'] + ':' + data['code']
        data['code'] = data['code'].str.replace('HP:HP:', 'HP:')
        # rename columns
        data.rename(columns={'diseaseId': 'cui', 'vocabularyName': 'code_name'}, inplace=True)
        # remove unneeded columns
        data = data[['cui', 'code', 'code_name', 'vocabulary']].drop_duplicates()

        return data

    def reads_medgen_data(self) -> pandas.DataFrame:
        """Reads in disease mapping data from MedGen.

        Returns:
            data: A pandas DataFrame object.
        """

        data = self.reads_gcs_bucket_data_to_df(f_name='MGCONSO.RRF', delm='|', head=0)

        # reformat data
        data = data[data['SUPPRESS'] == 'N'].drop_duplicates()
        data = data[data['SAB'].isin(['HPO', 'MONDO', 'MSH', 'ORDO', 'OMIM'])].drop_duplicates()
        # reformat codes
        data['temp_code'] = data.apply(lambda x: 'MESH:' + x['CODE'] if x['SAB'] == 'MSH'
                                                 else 'OMIM:' + x['CODE'] if x['SAB'] == 'OMIM'
                                                 else 'ORPHA:' + x['SDUI'].split('_')[-1] if x['SAB'] == 'ORDO'
                                                 else x['SDUI'] if x['SAB'] == 'HPO'
                                                 else x['SDUI'] if x['SAB'] == 'MONDO'
                                                 else 'None', axis=1)
        # add rows for MedGen identifiers
        temp = data[['#CUI']]; temp['temp_code'] = 'MedGen:' + data['#CUI']
        data = pandas.concat([data, temp])
        # remove unneeded columns
        data = data[['#CUI', 'temp_code', 'STR', 'SAB']].drop_duplicates()
        # rename columns
        data.rename(columns={'#CUI': 'cui', 'STR': 'code_name',
                             'temp_code': 'code', 'SAB': 'vocabulary'}, inplace=True)
        # reformat vocabulary ids
        data['vocabulary'] = data['vocabulary'].str.replace('HPO', 'HP')
        data['vocabulary'] = data['vocabulary'].str.replace('MSH', 'MESH')

        return data

    def creates_disease_identifier_mappings(self) -> None:
        """Creates Human Phenotype Ontology (HPO) and MonDO Disease Ontology (MONDO) dbxRef maps and then uses them
        with the DisGEeNET UMLS and MedGen disease mappings to create a master mapping between all disease identifiers
        to HPO  and MONDO.

        Returns:
            None.
        """

        log_str = 'Creating Phenotype and Disease ID Cross-Map Data'; print(log_str); logger.info(log_str)

        disease_map_df = pandas.concat([self._preprocess_mondo_mapping_data(), self._preprocess_hpo_mapping_data()])
        disease_data = pandas.concat([self.reads_disgenet_data(), self.reads_medgen_data()]).drop_duplicates()

        # find cuis that map to HP or MONDO
        disease_data_keep = disease_data.copy()
        disease_data_keep = disease_data_keep.query('vocabulary == "HP" | vocabulary == "MONDO"')
        disease_data_keep = disease_data_keep[['cui', 'code']]
        cui_list = set(disease_data_keep['cui'])
        # obtain a list of other ids that map to the cuis
        temp_df = disease_data[disease_data['cui'].isin(cui_list)]
        # merge back with original data and rename the columns
        merged_temp = temp_df.merge(disease_data_keep, on='cui')
        merged_temp = merged_temp[['code_x', 'code_y', 'code_name', 'vocabulary']].drop_duplicates()
        merged_temp.rename(columns={'code_x': 'cui', 'code_y': 'code'}, inplace=True)
        # combine the columns back to main data
        disease_mapping_data = pandas.concat([disease_data, merged_temp]).drop_duplicates()
        disease_mapping_data = disease_mapping_data[['cui', 'code']].drop_duplicates()
        # merge ontology and other mappings together and clean up file
        cleaned_disease_map = disease_mapping_data.merge(disease_map_df, left_on='cui', right_on='other_id')
        cleaned_disease_map = cleaned_disease_map[['cui', 'ontology_id']]
        cleaned_disease_map.rename(columns={'cui': 'disease_id'}, inplace=True)
        # format ontology identifiers
        cleaned_disease_map['ontology_id'] = cleaned_disease_map['ontology_id'].str.replace(':', '_')
        cleaned_disease_map['vocabulary'] = cleaned_disease_map['ontology_id'].str.replace('\_.*', '', regex=True)
        cleaned_disease_map.drop_duplicates(inplace=True)

        # write data
        # split data by ontology and write to file
        mondo_map = cleaned_disease_map[cleaned_disease_map['vocabulary'] == 'MONDO'].drop_duplicates()
        hp_map = cleaned_disease_map[cleaned_disease_map['vocabulary'] == 'HP'].drop_duplicates()
        mondo_map = mondo_map[['disease_id', 'ontology_id']]; hp_map = hp_map[['disease_id', 'ontology_id']]

        # write data
        file1 = 'DISEASE_MONDO_MAP.txt'; file2 = 'PHENOTYPE_HPO_MAP.txt'
        mondo_map.to_csv(self.temp_dir + '/' + file1, header=None, index=False, sep='\t')
        hp_map.to_csv(self.temp_dir + '/' + file2, header=None, index=False, sep='\t')
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
        mapping_data = self.reads_gcs_bucket_data_to_df(f_name=data_file, delm='\t', head=0, sht=sheet)
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

        hpa = self.reads_gcs_bucket_data_to_df(f_name='proteinatlas_search.tsv', delm='\t', head=0)
        hpa.fillna('None', inplace=True)
        # write results
        filename = 'HPA_tissues.txt'
        with open(self.temp_dir + '/' + filename, 'w') as outfile:
            for x in tqdm(list(hpa.columns)):
                if x.endswith('[nTPM]'): outfile.write(x.split('RNA - ')[-1].split(' [nTPM]')[:-1][0] + '\n')
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)

        return hpa

    def _processes_hpa_data(self) -> Union[List, pandas.DataFrame]:
        """The HPA data is reformatted so all tissue, cell, cell lines, and fluid types are stored as a nested list.
        The anatomy type is specified as an item in the list according to its type.

        Returns:
             hpa_results: A nested list of processed HPA data.
        """

        hpa = self._extracts_hpa_tissue_information()

        # process human protein atlas data
        hpa_results = []
        for idx, row in tqdm(hpa.iterrows(), total=hpa.shape[0]):
            ens = str(row['Ensembl']); gene = str(row['Gene']); uni = str(row['Uniprot'])
            evid = str(row['Evidence']); sub = str(row['Subcellular location']); source = 'The Human Protein Atlas'
            if row['RNA tissue specific nTPM'] != 'None':
                row_val = row['RNA tissue specific nTPM']
                if ';' in row_val:
                    for x in row_val.split(';'):
                        x1 = str(x.split(':')[0]); x2 = float(x.split(': ')[1])
                        hpa_results += [[ens, gene, uni, evid, 'anatomy', 'None', x1, x2, source]]
                else:
                    x1 = str(row_val.split(':')[0]); x2 = float(row_val.split(': ')[1])
                    hpa_results += [[ens, gene, uni, evid, 'anatomy', 'None', x1, x2, source]]
            if row['RNA cell line specific nTPM'] != 'None':
                row_val = row['RNA cell line specific nTPM']
                if ';' in row_val:
                    for x in row_val.split(';'):
                        x1 = str(x.split(':')[0]); x2 = float(x.split(': ')[1])
                        hpa_results += [[ens, gene, uni, evid, 'cell line', sub, x1, x2, source]]
                else:
                    x1 = str(row_val.split(':')[0]); x2 = float(row_val.split(': ')[1])
                    hpa_results += [[ens, gene, uni, evid, 'cell line', sub, x1, x2, source]]
            if row['RNA brain regional specific nTPM'] != 'None':
                row_val = row['RNA brain regional specific nTPM']
                if ';' in row_val:
                    for x in row_val.split(';'):
                        x1 = str(x.split(':')[0]); x2 = float(x.split(': ')[1])
                        hpa_results += [[ens, gene, uni, evid, 'anatomy', 'None', x1, x2, source]]
                else:
                    x1 = str(row_val.split(':')[0]); x2 = float(row_val.split(': ')[1])
                    hpa_results += [[ens, gene, uni, evid, 'anatomy', 'None', x1, x2, source]]
            if row['RNA blood cell specific nTPM'] != 'None':
                row_val = row['RNA blood cell specific nTPM']
                if ';' in row_val:
                    for x in row_val.split(';'):
                        x1 = str(x.split(':')[0]); x2 = float(x.split(': ')[1])
                        hpa_results += [[ens, gene, uni, evid, 'cell line', sub, x1, x2, source]]
                else:
                    x1 = str(row_val.split(':')[0]); x2 = float(row_val.split(': ')[1])
                    hpa_results += [[ens, gene, uni, evid, 'cell line', sub, x1, x2, source]]
            if row['RNA blood lineage specific nTPM'] != 'None':
                row_val = row['RNA blood lineage specific nTPM']
                if ';' in row_val:
                    for x in row_val.split(';'):
                        x1 = str(x.split(':')[0]); x2 = float(x.split(': ')[1])
                        hpa_results += [[ens, gene, uni, evid, 'cell line', sub, x1, x2, source]]
                else:
                    x1 = str(row_val.split(':')[0]); x2 = float(row_val.split(': ')[1])
                    hpa_results += [[ens, gene, uni, evid, 'cell line', sub, x1, x2, source]]

        return hpa_results, hpa

    def _processes_gtex_data(self, hpa_df: pandas.DataFrame) -> List:
        """All protein-coding genes that appear in the HPA data set are removed. Then, only those non-coding genes
        with a median expression >= 1.0 are maintained. GTEx data are formatted such the anatomical entities are
        stored as columns and genes stored as rows, thus the expression filtering step is performed while also
        reformatting the file, resulting in a nested list.

        Args:
            hpa_df: A Pandas DataFrame containining HPA data.

        Returns:
             gtex_results: A nested list of processed HPA data.
        """

        f_name = 'GTEx_Analysis_*_RNASeQC*_gene_median_tpm.gct'
        gtex = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', skip=2, head=0)
        gtex.fillna('None', inplace=True); gtex['Name'] = gtex['Name'].str.replace('(\..*)', '', regex=True)

        # process gtex data -- using only those protein-coding genes not already in hpa
        gtex_results, hpa_genes = [], list(hpa_df['Ensembl'].drop_duplicates(keep='first', inplace=False))
        gtex = gtex.loc[gtex['Name'].apply(lambda i: i not in hpa_genes)]
        # loop over data and re-organize
        source = 'Genotype-Tissue Expression (GTEx) Project'
        for idx, row in tqdm(gtex.iterrows(), total=gtex.shape[0]):
            for col in list(gtex.columns)[2:]:
                typ = 'cell line' if 'Cells' in col else 'anatomy'; evid = 'Evidence at transcript level'
                gtex_results += [[str(row['Name']), str(row['Description']),
                                  'None', evid, typ, 'None', col, float(row[col]), source]]

        return gtex_results

    def processes_hpa_gtex_data(self) -> None:
        """Method processes and combines gene expression experiment results from the Human protein Atlas (HPA) and the
        Genotype-Tissue Expression Project (GTEx). Additional details provided below on how each source are processed.

        Returns:
            None.
        """

        log_str = 'Creating Human Protein Atlas and GTEx Cross-Map Data'; print(log_str); logger.info(log_str)

        hpa_results, hpa = self._processes_hpa_data()
        gtex_results = self._processes_gtex_data(hpa)

        # write results
        filename = 'HPA_GTEX_RNA_GENE_PROTEIN_EDGES.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out:
            for x in tqdm(hpa_results + gtex_results):
                out.write(x[0] + '\t' + x[1] + '\t' + x[2] + '\t' + x[3] + '\t' + x[4] + '\t' + x[5] + '\t' +
                          x[6] + '\t' + str(x[7]) + '\t' + x[8] + '\n')
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
        dbxref_dict = {str(k).lower().split('/')[-1]: {str(i).split('/')[-1].replace('_', ':') for i in v}
                       for k, v in dbxref_res.items() if 'PW_' in str(v)}
        syn_res = gets_ontology_class_synonyms(pw_graph)[0]
        synonym_dict = {str(k).lower().split('/')[-1]: {str(i).split('/')[-1].replace('_', ':') for i in v}
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
        reactome_pathways = self.reads_gcs_bucket_data_to_df(f_name=r_name, delm='\t')
        reactome_pathways = reactome_pathways.loc[reactome_pathways[2].apply(lambda x: x == 'Homo sapiens')]
        reactome = {x: {'PW_0000001'} for x in set(list(reactome_pathways[0]))}
        # reactome - GO associations
        reactome_pathways2 = self.reads_gcs_bucket_data_to_df(f_name=g_name, delm='\t', skip=4)
        reactome_pathways2 = reactome_pathways2.loc[reactome_pathways2[12].apply(lambda x: x == 'taxon:9606')]
        reactome.update({x.split(':')[-1]: {'PW_0000001'} for x in set(list(reactome_pathways2[5]))})
        # reactome CHEBI
        reactome_pathways3 = self.reads_gcs_bucket_data_to_df(f_name=c_name, delm='\t')
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
        compath = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t')
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
        kegg_reactome_map = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm=',', head=0)
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

        url = 'https://reactome.org/ContentService/data/query/ids'
        headers = {'accept': 'application/json', 'content-type': 'text/plain', }
        for request_ids in tqdm(list(chunks(list(reactome.keys()), 20))):
            data, key = ','.join(request_ids), 'goBiologicalProcess'
            result = requests.post(url=url, headers=headers, data=data, verify=False).json()
            if isinstance(result, List) or result['code'] != 404:
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
        variant_data = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', head=0)
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
        mapping_data = self.reads_gcs_bucket_data_to_df(f_name=f_name, delm='\t', head=0, sht=sht)
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

    def _processes_protein_ontology_data(self) -> Tuple:
        """Reads in the PRotein Ontology (PR) into an RDFLib graph object and converts it to a Networkx MultiDiGraph
        object.

        Returns:
            A tuple where the first item is an RDFLib Graph object and the second is a Networkx MultiDiGraph
            object; both contain the same data.
        """

        print('\t- Loading Protein Ontology Data'); logger.info('Loading Protein Ontology Data')

        f_name = 'pr_with_imports.owl'
        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        pr_graph = Graph().parse(x); networkx_mdg: networkx.MultiDiGraph = networkx.MultiDiGraph()
        for s, p, o in tqdm(pr_graph): networkx_mdg.add_edge(s, o, **{'key': p})

        return pr_graph, networkx_mdg

    @staticmethod
    def _queries_protein_ontology(protein_ont_graph: Graph) -> List:
        """Queries the Protein Ontology for all Human Protein Classes.

        Args:
            protein_ont_graph: A RDFLib Graph object containing the Protein Ontology.

        Returns:
            human_pro_classes: A set of protein ontology identifiers for protein classes from the homo sapiens taxon.
        """

        human_classes_restriction = list(protein_ont_graph.triples((None, OWL.someValuesFrom, obo.NCBITaxon_9606)))
        human_classes = [list(protein_ont_graph.subjects(RDFS.subClassOf, x[0])) for x in human_classes_restriction]
        human_pro_classes = list(str(i) for j in human_classes for i in j if 'PR_' in str(i))

        return human_pro_classes

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

        pro_ont, networkx_mdg = self._processes_protein_ontology_data()
        # f = 'human_pro_classes.html'
        # x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f, self.temp_dir)
        # df_list = pandas.read_html(x); human_pro_classes = list(df_list[-1]['PRO_term'])
        human_pro_classes = self._queries_protein_ontology(pro_ont)
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
        human_pro_graph.serialize(destination=self.temp_dir + '/human_pro.owl')
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

    def _processes_variant_summary_data(self) -> pandas.DataFrame:
        """Data from ClinVar (variant_summary) is downloaded and the file is cleaned to handle missing data, unneeded
        variables are removed, identifiers and date fields are cleaned and reformatted, and rows without valid
        disease/phenotype identifiers are removed.

        Returns:
            var_summary_update: A Pandas DataFrame containing processed clinvar data.
        """

        var_summary = self.reads_gcs_bucket_data_to_df(f_name='variant_summary.txt', delm='\t', head=0)

        # replace "na" and "-" with NaN
        var_summary = var_summary.replace('na', numpy.nan); var_summary = var_summary.replace('-', numpy.nan)
        # handle ids that are coded as missing (i.e., -1)
        var_summary['GeneID'] = var_summary['GeneID'].replace(-1, numpy.nan)
        var_summary['RS# (dbSNP)'] = var_summary['RS# (dbSNP)'].replace(-1, numpy.nan)
        # convert date format
        var_summary['LastEvaluated'] = var_summary['LastEvaluated'].str.replace('None', '')
        var_summary['LastEvaluated'] = pandas.to_datetime(var_summary['LastEvaluated'])
        var_summary['LastEvaluated'] = var_summary['LastEvaluated'].dt.strftime('%B %d, %Y')
        var_summary['LastEvaluated'] = var_summary['LastEvaluated'].replace('', numpy.nan)
        # rename variables
        var_summary.rename(columns={'#AlleleID': 'AlleleID', 'nsv/esv (dbVar)': 'nsv', 'Name': 'VariantName'},
                           inplace=True)
        # update variable types
        var_summary['GeneID'] = var_summary['GeneID'].astype('Int64')
        var_summary['RS# (dbSNP)'] = var_summary['RS# (dbSNP)'].astype('Int64')

        # subset df to process multiple assembly entries
        var_summary_update_assemb = var_summary.copy()
        var_summary_update_assemb = var_summary_update_assemb[['VariationID', 'Assembly', 'ChromosomeAccession',
                                                               'Chromosome', 'Start', 'Stop', 'ReferenceAllele',
                                                               'AlternateAllele', 'Cytogenetic',
                                                               'PositionVCF']].drop_duplicates()
        # identify columns to process
        assemb_cols = ['ChromosomeAccession', 'Chromosome', 'Start', 'Stop', 'ReferenceAllele',
                       'AlternateAllele', 'Cytogenetic', 'PositionVCF', 'ReferenceAlleleVCF', 'AlternateAlleleVCF']
        # group data by variant
        df = var_summary_update_assemb.fillna('None')
        df = df.groupby('VariationID').apply(
            lambda g: str(g.drop(['VariationID'], axis=1).to_dict('records'))).to_dict()
        # convert to Pandas DataFrame
        df_items = df.items()
        temp_df = pandas.DataFrame({'VariationID': [x[0] for x in df_items], 'Assembly': [x[1] for x in df_items]})
        # join temp df with original data
        var_summary_assemb = var_summary.copy().drop(assemb_cols + ['Assembly'], axis=1)
        var_summary_update = var_summary_assemb.merge(temp_df, on='VariationID', how='left')
        var_summary_update.drop_duplicates(inplace=True)  # drop duplicates

        # process and clean up phenotype identifiers
        var_summary_update['Phenotype'] = var_summary_update['PhenotypeIDS'].str.replace('|', ';').str.replace(',', ';')
        var_summary_update['OtherIDs'] = var_summary_update['OtherIDs'].str.replace(';', '|').str.replace(',', '|')
        # remove unneeded variables
        drop_list = ['PhenotypeList', 'PhenotypeIDS']
        var_summary_update = var_summary_update.drop(drop_list, axis=1).drop_duplicates()
        # replace NaN with 'None'
        var_summary_update['Phenotype'] = var_summary_update['Phenotype'].fillna('None')
        # reformat phenotypeIDS and trim leading whitespace from unnested columns
        var_summary_update['Phenotype'] = var_summary_update['Phenotype'].apply(
            lambda x: ';'.join(set(x for x in ['MONDO:' + i.split(':')[-1] if i.startswith('MONDO')
                                               else 'HP:' + i.split(':')[-1] if i.startswith('Human Phenotype')
                                               else 'ORPHA:' + i.split(':')[-1] if i.startswith('Orphanet')
                                               else 'None' if i.endswith(' conditions')
                                               else i for i in x.split(';')] if x != 'None')))
        var_summary_update.drop_duplicates(inplace=True)  # drop duplicates

        return var_summary_update

    def _processes_var_citation_data(self) -> pandas.DataFrame:
        """Data from ClinVar (var_citations) is downloaded and the file is cleaned to handle missing data, unneeded
        variables are removed, and a new citation field is created.

        Returns:
            var_citations: A Pandas DataFrame containing processed clinvar data.
        """

        var_citations = self.reads_gcs_bucket_data_to_df(f_name='var_citations.txt', delm='\t', head=0)

        # replace "na" and "-" with NaN
        var_citations = var_citations.replace('na', numpy.nan); var_citations = var_citations.replace('-', numpy.nan)
        # combine citation information
        var_citations['Citation'] = var_citations['citation_source'] + ':' + var_citations['citation_id']
        # remove unneeded variables
        drop_list = ['citation_source', 'citation_id']
        var_citations = var_citations.drop(drop_list, axis=1).drop_duplicates()
        # group data by citations
        var_citations = var_citations.groupby('VariationID').Citation.agg([('Citation', '|'.join)]).reset_index()
        var_citations = var_citations.drop_duplicates().sort_values(by=['VariationID'])

        return var_citations

    def _processes_allele_gene_data(self) -> pandas.DataFrame:
        """Data from ClinVar (allele_gene) is downloaded and the file is cleaned to handle missing data, unneeded
        variables are removed, and the file is reduced to only contain a subset of relevant variables.

        Returns:
            allele_gene: A Pandas DataFrame containing processed clinvar data.
        """

        allele_gene = self.reads_gcs_bucket_data_to_df(f_name='allele_gene.txt', delm='\t', head=0)

        # replace "na" and "-" with NaN
        allele_gene = allele_gene.replace('na', numpy.nan)
        allele_gene = allele_gene.replace('-', numpy.nan)
        # handle gene ids that may be coded as -1
        allele_gene['GeneID'] = allele_gene['GeneID'].replace(-1, numpy.nan)
        # rename variables
        allele_gene.rename(columns={'#AlleleID': 'AlleleID', 'Symbol': 'GeneSymbol', 'Name': 'GeneName'}, inplace=True)
        # update variable types
        allele_gene['GeneID'] = allele_gene['GeneID'].astype('Int64')

        return allele_gene

    def processes_clinvar_data(self) -> None:
        """Processes ClinVar data by performing light tidying and filtering and then outputs data needed to create
        mappings between genes, variants, and phenotypes.

        Returns:
            None.
        """

        log_str = 'Generating ClinVar Cross-Mapping Data'; print(log_str); logger.info(log_str)

        # obtain processed data sets
        var_summary_update = self._processes_variant_summary_data()
        var_citations = self._processes_var_citation_data()
        allele_gene = self._processes_allele_gene_data()

        # merge var_summary and var_citation data
        merge_cols = list(set(var_summary_update.columns).intersection(set(var_citations.columns)))
        var_summary_merged = var_summary_update.merge(var_citations, on=merge_cols, how='left')
        # added allele_gene data
        merge_cols = list(set(var_summary_merged.columns).intersection(set(allele_gene.columns)))
        var_summary_merged = var_summary_merged.merge(allele_gene, on=merge_cols, how='left')
        var_summary_merged['GenesPerAlleleID'] = var_summary_merged['GenesPerAlleleID'].astype('Int64')
        # reduce data set to extract variant gene edges
        var_summary_merged_gene = var_summary_merged.copy()
        var_summary_merged_gene = var_summary_merged_gene[[
            'VariationID', 'AlleleID', 'RS# (dbSNP)', 'Type', 'VariantName', 'OtherIDs', 'GeneID', 'GeneSymbol',
            'GeneName', 'GenesPerAlleleID', 'Assembly', 'Category', 'Guidelines', 'TestedInGTR', 'RCVaccession',
            'LastEvaluated', 'ReviewStatus', 'ClinicalSignificance', 'ClinSigSimple', 'Origin', 'OriginSimple',
            'Source', 'SubmitterCategories', 'NumberSubmitters', 'Citation']]
        var_summary_merged_gene.drop_duplicates(inplace=True)
        var_summary_merged_gene = var_summary_merged_gene.dropna(subset=['GeneID'])
        var_summary_merged_gene['GeneID'] = 'NCBIGene_' + var_summary_merged_gene['GeneID'].astype(str)
        var_summary_merged_gene['VariationID'] = 'clinvar_' + var_summary_merged_gene['VariationID'].astype(str)
        filename = 'CLINVAR_VARIANT_GENE_EDGES.txt'
        var_summary_merged_gene.to_csv(self.temp_dir + '/' + filename, sep='\t', encoding='utf-8', index=False)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename)
        # reduce data set to extract variant-disease/phenotype edges
        var_summary_merged_disease = var_summary_merged.copy()
        var_summary_merged_disease = var_summary_merged_disease[[
            'VariationID', 'RS# (dbSNP)', 'Type', 'VariantName', 'RCVaccession', 'LastEvaluated', 'ReviewStatus',
            'ClinicalSignificance', 'ClinSigSimple', 'NumberSubmitters', 'SubmitterCategories', 'Guidelines',
            'GeneID', 'TestedInGTR', 'Origin', 'OriginSimple', 'Assembly', 'Phenotype', 'Citation', 'OtherIDs']]
        var_summary_merged_disease.drop_duplicates(inplace=True)
        # expand results by disease identifier, remove phenotype rows with None, and drop duplicates
        cols = ['Phenotype']
        for col in tqdm(cols): var_summary_merged_disease = var_summary_merged_disease.assign(
            **{col: var_summary_merged_disease[col].str.split(';')}).explode(col)
        var_summary_merged_disease = var_summary_merged_disease[var_summary_merged_disease['Phenotype'] != 'None']
        var_summary_merged_disease.drop_duplicates(inplace=True)
        var_summary_merged_disease['VariationID'] = 'clinvar_' + var_summary_merged_disease['VariationID'].astype(str)
        filename = 'CLINVAR_VARIANT_DISEASE_PHENOTYPE_EDGES.txt'
        var_summary_merged_gene.to_csv(self.temp_dir + '/' + filename, sep='\t', encoding='utf-8', index=False)
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
                status = line.split('\t')[1]; upt_id = line.split('\t')[0]; upt_entry = line.split('\t')[2]
                pr_id = 'PR_' + line.split('\t')[3].strip(';')
                # get cofactors
                if 'CHEBI' in line.split('\t')[4]:
                    for i in line.split('\t')[4].split(';'):
                        chebi = i.split('[')[-1].replace(']', '').replace(':', '_')
                        out1.write(pr_id + '\t' + chebi + '\t' + status + '\t' + upt_id + '\t' + upt_entry + '\n')
                # get catalysts
                if 'CHEBI' in line.split('\t')[5]:
                    for i in line.strip('\n').split('\t')[5].split(';'):
                        chebi = i.split('[')[-1].replace(']', '').replace(':', '_')
                        out2.write(pr_id + '\t' + chebi + '\t' + status + '\t' + upt_id + '\t' + upt_entry + '\n')
        # push data to gsc bucket
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename1)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, filename2)

        return None

    # def _creates_gene_metadata_dict(self) -> Dict:
    #     """Creates a dictionary to store labels, synonyms, and a description for each Entrez gene identifier present in
    #     the input data file.
    #
    #     Returns:
    #         gene_metadata_dict: A dict containing metadata that's keyed by Entrez gene identifier and whose values are
    #             dicts containing label, description, and synonym information. For example:
    #                 {{'http://www.ncbi.nlm.nih.gov/gene/1': {
    #                     'Label': 'A1BG',
    #                     'Description': "A1BG is 'protein-coding' and is located on chromosome 19 (19q13.43).",
    #                     'Synonym': 'HEL-S-163pA|A1B|ABG|HYST2477alpha-1B-glycoprotein|GAB'}, ...}
    #     """
    #
    #     log_str = 'Generating Metadata for Gene Identifiers'; print('\t- ' + log_str); logger.info(log_str)
    #
    #     f_name = 'Homo_sapiens.gene_info'
    #     x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
    #     data = pandas.read_csv(x, header=0, delimiter='\t', low_memory=False)
    #     data = data.loc[data['#tax_id'].apply(lambda i: i == 9606)]
    #     data.fillna('None', inplace=True); data.replace('-', 'None', inplace=True, regex=False)
    #     # create metadata
    #     genes, lab, desc, syn = [], [], [], []
    #     for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
    #         gene_id, sym, defn, gene_type = row['GeneID'], row['Symbol'], row['description'], row['type_of_gene']
    #         chrom, map_loc, s1, s2 = row['chromosome'], row['map_location'], row['Synonyms'], row['Other_designations']
    #         if gene_id != 'None':
    #             genes.append('http://www.ncbi.nlm.nih.gov/gene/' + str(gene_id))
    #             if sym != 'None' or sym != '': lab.append(sym)
    #             else: lab.append('Entrez_ID:' + gene_id)
    #             if 'None' not in [defn, gene_type, chrom, map_loc]:
    #                 desc_str = "{} has locus group '{}' and is located on chromosome {} ({})."
    #                 desc.append(desc_str.format(sym, gene_type, chrom, map_loc))
    #             else: desc.append("{} locus group '{}'.".format(sym, gene_type))
    #             if s1 != 'None' and s2 != 'None':
    #                 syn.append('|'.join(set([x for x in (s1 + s2).split('|') if x != 'None' or x != ''])))
    #             elif s1 != 'None': syn.append('|'.join(set([x for x in s1.split('|') if x != 'None' or x != ''])))
    #             elif s2 != 'None': syn.append('|'.join(set([x for x in s2.split('|') if x != 'None' or x != ''])))
    #             else: syn.append('None')
    #     # combine into new data frame then convert it to dictionary
    #     metadata = pandas.DataFrame(list(zip(genes, lab, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
    #     metadata = metadata.astype(str); metadata.drop_duplicates(subset='ID', inplace=True)
    #     metadata.set_index('ID', inplace=True); gene_metadata_dict = metadata.to_dict('index')
    #
    #     return gene_metadata_dict
    #
    # def _creates_transcript_metadata_dict(self) -> Dict:
    #     """Creates a dictionary to store labels, synonyms, and a description for each Entrez gene identifier present
    #     in the input data file.
    #
    #     Returns:
    #         rna_metadata_dict: A dict containing metadata that's keyed by Ensembl transcript identifier and whose values
    #             are a dict containing label, description, and synonym information. For example:
    #                 {'https://uswest.ensembl.org/Homo_sapiens/Transcript/Summary?t=ENST00000456328': {
    #                     'Label': 'DDX11L1-202',
    #                     'Description': "Transcript DDX11L1-202 is classified as type 'processed_transcript'.",
    #                     'Synonym': 'None'}, ...}
    #     """
    #
    #     log_str = 'Generating Metadata for Transcript Identifiers'; print('\t- ' + log_str); logger.info(log_str)
    #
    #     f_name = 'ensembl_identifier_data_cleaned.txt'
    #     x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
    #     dup_cols = ['transcript_stable_id', 'transcript_name', 'ensembl_transcript_type']
    #     data = pandas.read_csv(x, header=0, delimiter='\t', low_memory=False)
    #     data = data.loc[data['transcript_stable_id'].apply(lambda i: i != 'None')]
    #     data.drop(['ensembl_gene_id', 'symbol', 'protein_stable_id', 'uniprot_id', 'master_transcript_type',
    #                'entrez_id', 'ensembl_gene_type', 'master_gene_type', 'symbol'], axis=1, inplace=True)
    #     data.drop_duplicates(subset=dup_cols, keep='first', inplace=True); data.fillna('None', inplace=True)
    #     # create metadata
    #     rna, lab, desc, syn = [], [], [], []
    #     for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
    #         rna_id, ent_type, nme = row[dup_cols[0]], row[dup_cols[2]], row[dup_cols[1]]
    #         rna.append('https://uswest.ensembl.org/Homo_sapiens/Transcript/Summary?t=' + rna_id)
    #         if nme != 'None': lab.append(nme)
    #         else: lab.append('Ensembl_Transcript_ID:' + rna_id); nme = 'Ensembl_Transcript_ID:' + rna_id
    #         if ent_type != 'None': desc.append("Transcript {} is classified as type '{}'.".format(nme, ent_type))
    #         else: desc.append('None')
    #         syn.append('None')
    #     # combine into new data frame then convert it to dictionary
    #     metadata = pandas.DataFrame(list(zip(rna, lab, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
    #     metadata = metadata.astype(str); metadata.drop_duplicates(subset='ID', inplace=True)
    #     metadata.set_index('ID', inplace=True); rna_metadata_dict = metadata.to_dict('index')
    #
    #     return rna_metadata_dict
    #
    # def _creates_variant_metadata_dict(self) -> Dict:
    #     """Creates a dictionary to store labels, synonyms, and a description for each ClinVar variant identifier present
    #     in the input data file.
    #
    #     Returns:
    #         variant_metadata_dict: A dict containing metadata that's keyed by ClinVar variant identifier and whose
    #             values are a dict containing label, description, and synonym information. For example:
    #                 {{'https://www.ncbi.nlm.nih.gov/snp/rs141138948': {
    #                     'Label': 'NM_016042.4(EXOSC3):c.395A>C (p.Asp132Ala)',
    #                     'Description': "This variant is a germline single nucleotide variant on chromosome 9
    #                     (NC_000009.12, start:37783993/stop:37783993 positions,cytogenetic location:9p13.2) and
    #                     has clinical significance 'Pathogenic/Likely pathogenic'. This entry is for the GRCh38 and was
    #                     last reviewed on Sep 30, 2020 with review status 'criteria provided, multiple submitters,
    #                     no conflict'.", 'Synonym': 'None'}, ...}
    #     """
    #
    #     log_str = 'Generating Metadata for Variant IDs'; print('\t- ' + log_str); logger.info(log_str)
    #
    #     f_name = 'variant_summary.txt'
    #     x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
    #     data = pandas.read_csv(x, header=0, delimiter='\t', low_memory=False)
    #     data = data.loc[data['Assembly'].apply(lambda i: i == 'GRCh38')]
    #     data = data.loc[data['RS# (dbSNP)'].apply(lambda i: i != -1)]
    #     data = data[['#AlleleID', 'Type', 'Name', 'ClinicalSignificance', 'RS# (dbSNP)', 'Origin', 'Start', 'Stop',
    #                  'ChromosomeAccession', 'Chromosome', 'ReferenceAllele', 'Assembly', 'AlternateAllele',
    #                  'Cytogenetic', 'ReviewStatus', 'LastEvaluated']]
    #     data.replace('na', 'None', inplace=True); data.fillna('None', inplace=True)
    #     data.sort_values('LastEvaluated', ascending=False, inplace=True)
    #     data.drop_duplicates(subset='RS# (dbSNP)', keep='first', inplace=True)
    #     # create metadata
    #     var, label, desc, syn = [], [], [], []
    #     for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
    #         var_id, lab = row['RS# (dbSNP)'], row['Name']
    #         if var_id != 'None':
    #             var.append('https://www.ncbi.nlm.nih.gov/snp/rs' + str(var_id))
    #             if lab != 'None': label.append(lab)
    #             else: label.append('dbSNP_ID:rs' + str(var_id))
    #             sent = "This variant is a {} {} located on chromosome {} ({}, start:{}/stop:{} positions, " + \
    #                    "cytogenetic location:{}) and has clinical significance '{}'. " + \
    #                    "This entry is for the {} and was last reviewed on {} with review status '{}'."
    #             desc.append(
    #                 sent.format(row['Origin'].str.replace(';', '/'), row['Type'].replace(';', '/'), row['Chromosome'],
    #                             row['ChromosomeAccession'], row['Start'], row['Stop'], row['Cytogenetic'],
    #                             row['ClinicalSignificance'], row['Assembly'], row['LastEvaluated'],
    #                             row['ReviewStatus']).replace('None', 'UNKNOWN'))
    #             syn.append('None')
    #     # combine into new data frame then convert it to dictionary
    #     metadata = pandas.DataFrame(list(zip(var, label, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
    #     metadata.drop_duplicates(inplace=True); metadata = metadata.astype(str)
    #     metadata.set_index('ID', inplace=True); variant_metadata_dict = metadata.to_dict('index')
    #
    #     return variant_metadata_dict

    @staticmethod
    def _metadata_api_mapper(nodes: List[str]) -> pandas.DataFrame:
        """Takes a list of nodes and queries them, in chunks of 20, against the Reactome API.

        Args:
            nodes: A list of identifiers to obtain metadata information for.

        Returns:
            A pandas.DataFrame of metadata results.
        """

        ids, labels, desc, synonyms = [], [], [], []
        url = 'https://reactome.org/ContentService/data/query/ids'
        headers = {'accept': 'application/json', 'content-type': 'text/plain', }
        for request_ids in tqdm(list(chunks(nodes, 20))):
            data = ','.join(request_ids)
            results = requests.post(url=url, headers=headers, data=data, verify=False).json()
            if isinstance(results, List) or results['code'] != 404:
                for row in results:
                    ids.append(row['stId']); labels.append(row['displayName']); desc.append('None')
                    if row['displayName'] != row['name']: synonyms.append('|'.join(row['name']))
                    else: synonyms.append('None')

        # combine into new data frame
        column_names = ['ID', 'Label', 'Description', 'Synonym']
        metadata = pandas.DataFrame(list(zip(ids, labels, desc, synonyms)), columns=column_names)
        node_metadata_final = metadata.astype(str)

        return node_metadata_final

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
        data1 = pandas.read_csv(g, header=None, delimiter='\t', skiprows=4, low_memory=False)
        data1 = data1.loc[data1[12].apply(lambda x: x == 'taxon:9606')]
        data1[5] = data1[5].str.replace('REACTOME:', '', regex=True)
        # reactome CHEBI data
        f_name2 = 'ChEBI2Reactome_All_Levels.txt'
        h = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name2, self.temp_dir)
        data2 = pandas.read_csv(h, header=None, delimiter='\t', low_memory=False)
        data2 = data2.loc[data2[5].apply(lambda x: x == 'Homo sapiens')]
        # set unique node list
        nodes = list(set(data[0]) | set(data1[5]) | set(data2[1])); metadata = self._metadata_api_mapper(nodes)
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
        relation_metadata_dict = {}
        cls = [x for x in gets_ontology_classes(ro_graph) if '/RO_' in str(x)] + \
              [x for x in gets_object_properties(ro_graph) if '/RO_' in str(x)]
        master_synonyms = [x for x in ro_graph if 'synonym' in str(x[1]).lower() and isinstance(x[0], URIRef)]
        for x in tqdm(cls):
            cls_label = [x for x in ro_graph.objects(x, RDFS.label) if '@' not in n3(x) or '@en' in n3(x)]
            labels = str(cls_label[0]) if len(cls_label) > 0 else 'None'
            cls_syn = [str(i[2]) for i in master_synonyms if x == i[0]]
            synonym = str(cls_syn[0]) if len(cls_syn) > 0 else 'None'
            cls_desc = [x for x in ro_graph.objects(x, obo.IAO_0000115) if '@' not in n3(x) or '@en' in n3(x)]
            desc = '|'.join([str(cls_desc[0])]) if len(cls_desc) > 0 else 'None'
            relation_metadata_dict[str(x)] = {'Label': labels, 'Description': desc, 'Synonym': synonym}

        return relation_metadata_dict

    def _loads_mapping_data(self) -> Dict:
        """

        Returns:
             id_map_dict: A dictionary of Pandas DataFrame objects keyed by variable name.
        """

        id_map_dict: Dict = {
            'rna_map': pandas.read_csv(self.temp_dir + '/ENTREZ_GENE_ENSEMBL_TRANSCRIPT_MAP.txt',
                                       header=None, delimiter='\t', low_memory=False, usecols=[0, 1, 2, 4],
                                       names=['Entrez_Gene_IDs', 'Ensembl_Transcript_IDs', 'Entrez_Gene_Type',
                                              'Ensembl_Transcript_Type', 'Master_Gene_Type', 'Master_Transcript_Type',
                                              'Entrez_Gene_prefix']),
            'entrez_pro_map' : pandas.read_csv(self.temp_dir + '/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt',
                                               header=None, delimiter='\t', low_memory=False, usecols=[0, 1, 2, 4],
                                               names=['Gene_IDs', 'Protein_Ontology_IDs', 'Entrez_Gene_Type',
                                                      'Master_Gene_Type', 'Entrez_Gene_Prefix']),
            'string_pro_map': pandas.read_csv(self.temp_dir + '/STRING_PRO_ONTOLOGY_MAP.txt',
                                              header=None, delimiter='\t', low_memory=False, usecols=[0, 1],
                                              names=['STRING_IDs', 'Protein_Ontology_IDs']),
            'uniprot_pro_map': pandas.read_csv(self.temp_dir + '/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt',
                                               header=None, delimiter='\t', low_memory=False, usecols=[0, 1],
                                               names=['Uniprot_Accession_IDs', 'Protein_Ontology_IDs']),
            'uniprot_entrez_data': pandas.read_csv(self.temp_dir + '/UNIPROT_ACCESSION_ENTREZ_GENE_MAP.txt',
                                                   header=None, delimiter='\t', low_memory=False, usecols=[0, 1, 2, 3],
                                                   names=['Uniprot_Accession_IDs', 'Entrez_Gene_IDs',
                                                          'master_gene_type', 'gene_type_update']),
            'mesh_chebi_map': pandas.read_csv(self.temp_dir + '/MESH_CHEBI_MAP.txt', header=None,
                                              names=['MESH_ID', 'CHEBI_ID'], delimiter='\t'),
            'disease_maps': pandas.read_csv(self.temp_dir + '/DISEASE_MONDO_MAP.txt', header=None,
                                            names=['Disease_IDs', 'MONDO_IDs'], delimiter='\t'),
            'phenotype_maps': pandas.read_csv(self.temp_dir + '/PHENOTYPE_HPO_MAP.txt', header=None,
                                              names=['Disease_IDs', 'HP_IDs'], delimiter='\t')
        }

        return id_map_dict

    def _creates_genomic_metadata_dict(self) -> Dict:
        """Process a genomic metadata dictionary created in the prior steps in order to assist with creating a master
        metadata file for all nodes that are a genomic entity (i.e., genes, transcripts, or proteins).

        Returns:
             genomic_metadata: A nested dictionary of genomic metadata keyed by NCBIGene, ensembl, and Protein Ontology
                identifiers.
        """

        filepath = self.temp_dir + '/Merged_gene_rna_protein_identifiers.pkl'
        max_bytes = 2**31 - 1; input_size = os.path.getsize(filepath); bytes_in = bytearray(0)
        with open(filepath, 'rb') as f_in:
            for _ in range(0, input_size, max_bytes):
                bytes_in += f_in.read(max_bytes)
        reformatted_mapped_identifiers = pickle.loads(bytes_in)

        # clean up data for use with master metadata
        genomic_metadata = dict()
        for key, value in tqdm(reformatted_mapped_identifiers.items()):
            old_prefix = '_'.join(key.split('_')[0:-1]); idx = key.split('_')[-1]; pass_var = True; new_prefix = None
            if old_prefix == 'entrez_id': new_prefix = 'NCBIGene'
            elif old_prefix in ['ensembl_gene_id', 'protein_stable_id', 'transcript_stable_id']: new_prefix = 'ensembl'
            elif old_prefix == 'pro_id_PR': new_prefix = 'PR'
            else: pass_var = False
            if pass_var and new_prefix is not None:
                updated_key = new_prefix + '_' + idx; master_metadata_dict = {updated_key: {}}
                for x in value:
                    i, j = '_'.join(x.split('_')[0:-1]), x.split('_')[-1]
                    if 'type' in i: continue
                    elif i == 'entrez_id': new_i = 'NCBIGene'; j = new_i + '_' + j
                    elif i == 'ensembl_gene_id': new_i = 'ensembl gene'; j = 'ensembl_' + j
                    elif i == 'protein_stable_id': new_i = 'ensembl protein'; j = 'ensembl_' + j
                    elif i == 'transcript_stable_id': new_i = 'ensembl transcript'; j = 'ensembl_' + j
                    elif i == 'pro_id_PR': new_i = 'PR'; j = new_i + '_' + j
                    elif i == 'hgnc_id': new_i = 'HGNC_ID'; j = new_i + '_' + j
                    elif i == 'uniprot_id': new_i = 'uniprot'; j = new_i + '_' + j
                    elif i == 'symbol': new_i = 'GeneSymbol'; j = new_i + '_' + j
                    else:
                        if i == 'synonyms': new_i = 'Synonyms'
                        elif i == 'name': new_i = 'Label'
                        elif i == 'Other_designations': new_i = 'Synonyms'; j = j.split('|')
                        else: new_i = i
                    if new_i in master_metadata_dict[updated_key].keys():
                        if isinstance(j, list): master_metadata_dict[updated_key][new_i] += j
                        else: master_metadata_dict[updated_key][new_i] += [j]
                    else: master_metadata_dict[updated_key][new_i] = [j]
                genomic_metadata[updated_key] = master_metadata_dict

        return genomic_metadata

    def _processes_ctd_gene_inx_data(self, master, g_dict, mesh_chebi_map, rna_map, entrez_pro_map) -> Dict:
        """This function processes the CTD_chem_gene_ixns.tsv file and obtains the following node and edge metadata:
            Nodes:
                - ChemicalID: A string containing the concept's MESH identifier.
                - CasRN: A string containing a CAS Registry Number.
                - ChemicalName: A string containing the concept's synonym.
                - Organism: A string containing the name of an organism.
                - GeneSymbol: A string containing the concept's gene symbol.
            Relations:
                - Interaction: A string describing a chemical-gene/protein/rna interaction.
                - InteractionActions: A "|"-delimited list of the actions that underlie an interaction.
                - PubMedIDs: |'-delimited list of PubMed identifiers that do not include a prefix.

        Args:
            master: The master metadata dictionary keyed by nodes and relations.
            g_dict: A nested dictionary of genomic metadata keyed by NCBIGene, ensembl, and Protein Ontology IDs.
            mesh_chebi_map: A Pandas DataFrame that contains MeSH-CHEBI identifier mappings.
            rna_map: A Pandas DataFrame that contains Entrez Gene-Ensembl Transcript identifier mappings.
            entrez_pro_map: A Pandas DataFrame that contains Entrez Gene-Protein Ontology identifier mappings.

        Returns:
            master_dict: The master metadata dictionary keyed by nodes and relations.
        """

        # download and process data
        url = 'http://ctdbase.org/reports/CTD_chem_gene_ixns.tsv.gz'; f_name = self.temp_dir + '/CTD_chem_gene_ixns.tsv'
        if not os.path.exists(f_name): data_downloader(url, f_name)
        df = pandas.read_csv(f_name, header=0, delimiter='\t', skiprows=27)
        df = df[df['# ChemicalName'] != '#']; df = df[df['OrganismID'] == 9606]; df = df[df['PubMedIDs'] != numpy.nan]
        df['ChemicalID'] = 'MESH:' + df['ChemicalID']
        df['GeneID'] = df['GeneID'].astype('Int64'); df['OrganismID'] = df['OrganismID'].astype('Int64')
        # merge identifier maps
        df = df.merge(mesh_chebi_map, left_on='ChemicalID', right_on='MESH_ID')
        df = df.merge(rna_map, left_on='GeneID', right_on='Entrez_Gene_IDs')
        df = df.merge(entrez_pro_map, left_on='GeneID', right_on='Gene_IDs')

        for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
            chebi = row['CHEBI_ID'].rstrip(); n_key = None; genomic_info = None; r_key = None; form = None
            chemical_name = row['# ChemicalName']; chemical_id = row['ChemicalID'].rstrip(); casrn = row['CasRN']
            evidence = [{'CTD_Interaction': row['Interaction'], 'CTD_InteractionActions': row['InteractionActions'],
                         'CTD_PubMedIDs': row['PubMedIDs']}]
            g_info = None; relation_key = '{}-{}'.format(chebi, n_key); edge_type = None
            if row['GeneForms'] == 'gene':
                n_key = row['Entrez_Gene_prefix'].rstrip(); edge_type = 'chemical-gene'; form = row['GeneForms']
                if n_key in g_dict.keys(): g_info = g_dict[n_key]
            if row['GeneForms'] == 'protein':
                n_key = row['Protein_Ontology_IDs'].rstrip(); edge_type = 'chemical-protein'; form = row['GeneForms']
                if n_key in g_dict.keys(): g_info = g_dict[n_key]
            if row['GeneForms'] == 'rna':
                n_key = row['Ensembl_Transcript_IDs'].rstrip(); edge_type = 'chemical-rna'; form = row['GeneForms']
                if n_key in g_dict.keys(): g_info = g_dict[n_key]
            if form is not None:
                # add node data to dictionary
                if chebi in master['nodes'].keys():
                    if 'CTD_ChemicalName' in master['nodes'][chebi].keys():
                        master['nodes'][chebi]['CTD_ChemicalName'] |= {chemical_name}
                    else: master['nodes'][chebi]['CTD_ChemicalName'] = {chemical_name}
                    if 'CTD_ChemicalID' in master['nodes'][chebi].keys():
                        master['nodes'][chebi]['ChemicalID'] |= {chemical_id}
                    else: master['nodes'][chebi]['ChemicalID'] = {chemical_id}
                    if 'CTD_CasRN' in master['nodes'][chebi].keys(): master['nodes'][chebi]['CTD_CasRN'] |= {casrn}
                    else: master['nodes'][chebi]['CTD_CasRN'] = {casrn}
                else:
                    master['nodes'][n_key] = {'CTD_GeneForms': form}
                    if g_info is not None: master['nodes'][n_key]['genomic_data'] = {n_key: g_info}
                    master['nodes'][chebi] = {}
                    master['nodes'][chebi]['ChemicalID'] = {chemical_id}
                    master['nodes'][chebi]['CTD_CasRN'] = {casrn}
                    master['nodes'][chebi]['CTD_ChemicalName'] = {chemical_name}
                    # add relation data to dictionary
                if r_key in master['relations'][edge_type].keys():
                    if 'CTD_Evidence' in master['relations'][edge_type][r_key].keys():
                        master['relations'][edge_type][r_key]['CTD_Evidence'] += [evidence]
                    else: master['relations'][edge_type][r_key]['CTD_Evidence'] = [evidence]
                else:
                    master['relations'][edge_type][r_key] = {}
                    master['relations'][edge_type][r_key]['CTD_Evidence'] = [evidence]

        return master



    def creates_metadata_dict(self) -> None:
        """Creates a single large metadata dictionary that is keyed by nodes and relations and contains a variety of
        metadata. See the following file for additional details:
        https://github.com/callahantiff/PheKnowLator/tree/master/resources/pheknowlator_source_metadata.xlsx.

        Returns:
            None.
        """

        log_str = 'Creating Master Metadata Dictionary for Non-Ontology Entities'; print(log_str); logger.info(log_str)

        # load identifier mapping data
        id_map = self._loads_mapping_data()
        rna_map = id_map['rna_map']; entrez_pro_map = id_map['entrez_pro_map']; disease_maps = id_map['disease_maps']
        string_pro_map = id_map['string_pro_map']; uniprot_pro_map = id_map['uniprot_pro_map']
        uniprot_entrez_data = id_map['uniprot_entrez_data']; mesh_chebi_map = id_map['mesh_chebi_map']
        phenotype_maps = id_map['phenotype_maps']

        # create dictionary
        master_dict = {'nodes': {}, 'relations': {}}

        # obtain metadata dictionaries
        genomic = self._creates_genomic_metadata_dict()
        master_dict = self._processes_ctd_gene_inx_data(master_dict, genomic, mesh_chebi_map, rna_map, entrez_pro_map)




        # create single dictionary of
        master_metadata_dictionary = {'nodes': {**self._creates_gene_metadata_dict(),
                                                **self._creates_transcript_metadata_dict(),
                                                **self._creates_variant_metadata_dict(),
                                                **self._creates_pathway_metadata_dict()},
                                      'relations': self._creates_relations_metadata_dict()}

        # verify metadata strings are properly formatted
        temp_copy = master_metadata_dictionary.copy(); master_metadata_dictionary = dict()
        for key, value in tqdm(temp_copy.items()):
            master_metadata_dictionary[key] = {}
            for ent_key, ent_value in value.items():
                updated_inner_dict = {k: re.sub('\s\s+', ' ', v.replace('\n', ' '))
                                      if v is not None else v for k, v in ent_value.items()}
                master_metadata_dictionary[key][ent_key] = updated_inner_dict
        del temp_copy

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
        log_str = 'STEP 5: SEQUENCE ONTOLOGY + PATHWAY ID MAP'; print('\n' + log_str); logger.info(log_str)
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
        self.creates_metadata_dict()
        uploads_data_to_gcs_bucket(self.bucket, self.log_location, log_dir, log)

        return None
