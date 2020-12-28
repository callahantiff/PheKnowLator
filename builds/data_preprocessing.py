#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import cloudstorage as gcs
import datetime
import glob
import ijson
import itertools
import networkx
import numpy
import os
import pandas
import pickle
import requests
import sys

from collections import Counter
from functools import reduce
from owlready2 import subprocess
from rdflib import Graph, Namespace, URIRef, BNode, Literal
from rdflib.namespace import OWL, RDF, RDFS
from reactome2py import content
from tqdm import tqdm

# import script containing helper functions
from pkt_kg.utils import *  # tests written for called methods in pkt_kg/utils/data_utils.py


class DataPreprocessing(object):
    """Class provides a container for the data preprocessing methods

    Attributes:
        gcs_org_bucket: A string specifying the location of the original_data directory for a specific build.
        gcs_processed_data: A string specifying the location of the original_data directory for a specific build.

    """

    def __init__(self, gcs_org_bucket: str, gcs_processed_data: str) -> None:

        self.genomic_type_mapper = pickle.load(open('builds/genomic_typing.pkl', 'rb'))

        # set-up Google Cloud Storage (GCS) bucket
        # TODO: FIGURE THIS OUT - AUTH VIA GITHUB SECRET??
        self.bucket_data_files = XXX  # get list of all files in bucket locations
        self.original_data_bucket = gcs_org_bucket
        self.preprocessed_data = gcs_processed_data

        # create temp directory to use locally for writing data GCS data to
        os.mkdir('builds/temp')

    def writes_data_to_gcs_bucket(self, filename: str, data: pd.DataFrame) -> None:
        """Takes a filename and a data object and writes the data in that object to the Google Cloud Storage bucket
        specified in the filename.

        Args:
            filename: A string containing the name of file to write to a Google Cloud Storage bucket.
            data: A Pandas DataFrame object containing data.

        Returns:
            None.
        """

        # TODO: write func that pushes data to a specific GCS bucket

        file_location = self.preprocessed_data + '/' + filename

        return None

    def reads_gcs_bucket_data_to_df(self, file_loc: str, delm: str, skip: int = 0,
                                    header: Optional[int, List] = None) -> pd.DataFrame:
        """Takes a Google Cloud Storage bucket and a filename and reads the data into a Pandas DataFrame.

        Args:
            file_loc: A string containing the name of file that exists in a Google Cloud Storage bucket.
            delm: A string specifying a file delimiter.
            skip: An integer specifying the number of rows to skip when reading in the data.
            header: An integer specifying the header row or None specifying no header or a list when passing in the
                header names.

        Returns:
             data: A Pandas DataFrame object containing data read from a Google Cloud Storage bucket.
        """

        # TODO: FIGURE OUT HOW YOU WANT TO DO THIS
        file_location = glob.glob(self.original_data_bucket + '/' + filename)[0]
        if header is None or isinstance(header, int):
            data = pandas.read_csv(file_loc, header=header, delimiter=delm, skiprows=skip, low_memory=False)
        else:
            data = pandas.read_csv(file_loc, header=None, names=header, delm=delimiter, skiprows=skip, low_memory=False)

        return data

    def preprocess_hgnc_data(self) -> pd.DataFrame:
        """Processes HGNC data in order to prepare it for combination with other gene identifier data sources. The
        reformatting performed on the data includes removing unnecessary columns and reformatting column values to a
        common set of terms that will be applied universally to all gene and protein identifier data sources.

        Return:
            explode_df_hgnc: A Pandas DataFrame containing processed and filtered data.
        """

        # read in data and prepare it for processing
        hgnc = self.reads_gcs_bucket_data_to_df('hgnc_complete_set.txt', '\t', 0, 0)
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
        hgnc['synonyms'] = hgnc['alias_symbol'] + '|' + hgnc['alias_name']

        # explode nested data and reformat values in preparation for combining it with other gene identifiers
        explode_df_hgnc = explodes_data(hgnc.copy(), ['ensembl_gene_id', 'uniprot_id', 'symbol', 'name'], '|')
        # reformat hgnc gene type
        for val in genomic_type_mapper['hgnc_gene_type'].keys():
            explode_df_hgnc['hgnc_gene_type'].replace(val, genomic_type_mapper['hgnc_gene_type'][val], inplace=True)
        # reformat master hgnc gene type
        explode_df_hgnc['master_gene_type'] = explode_df_hgnc['hgnc_gene_type']
        master_dict = genomic_type_mapper['hgnc_master_gene_type']
        for val in master_dict.keys():
            explode_df_hgnc['master_gene_type'].replace(val, master_dict[val], inplace=True)

        # post-process reformatted data
        explode_df_hgnc.drop(['alias_symbol', 'alias_name'], axis=1, inplace=True)  # remove original gene type column
        explode_df_hgnc.drop_duplicates(subset=None, keep='first', inplace=True)

        return explode_df_hgnc

    def preprocess_ensembl_data(self) -> pd.DataFrame:
        """Processes Ensembl data in order to prepare it for combination with other gene identifier data sources. The
        reformatting performed on the data includes removing unnecessary columns and reformatting column values to a
        common set of terms that will be applied universally to all gene and protein identifier data sources.

        Return:
            ensembl_geneset: A Pandas DataFrame containing processed and filtered data.
        """

        # read in data and prepare it for processing
        ensembl_geneset, cleaned_col = self.reads_gcs_bucket_data_to_df('Homo_sapiens.GRCh38.*.gtf', '\t', 5, 0), []
        data_cols = ['gene_id', 'transcript_id', 'gene_name', 'gene_biotype', 'transcript_name', 'transcript_biotype']
        for data_list in list(ensembl_geneset[8]):
            data_list = data_list if not data_list.endswith(';') else data_list[:-1]
            temp_data = [data_list.split('; ')[[x.split(' ')[0] for x in data_list.split('; ')].index(col)]
                         if col in data_list else col + ' None' for col in data_cols]
            cleaned_col.append(temp_data)
        ensembl_geneset.fillna('None', inplace=True)
        # update primary column values
        ensembl_geneset['ensembl_gene_id'] = [x[0].split(' ')[-1].replace('"', '') for x in cleaned_col]
        ensembl_geneset['transcript_stable_id'] = [x[1].split(' ')[-1].replace('"', '') for x in cleaned_col]
        ensembl_geneset['symbol'] = [x[2].split(' ')[-1].replace('"', '') for x in cleaned_col]
        ensembl_geneset['ensembl_gene_type'] = [x[3].split(' ')[-1].replace('"', '') for x in cleaned_col]
        ensembl_geneset['transcript_name'] = [x[4].split(' ')[-1].replace('"', '') for x in cleaned_col]
        ensembl_geneset['ensembl_transcript_type'] = [x[5].split(' ')[-1].replace('"', '') for x in cleaned_col]

        # reformat ensembl gene type
        gene_dict = genomic_type_mapper['ensembl_gene_type']
        for val in gene_dict.keys():
            ensembl_geneset['ensembl_gene_type'].replace(val, gene_dict[val], inplace=True)
        # reformat master gene type
        ensembl_geneset['master_gene_type'] = ensembl_geneset['ensembl_gene_type']
        gene_dict = genomic_type_mapper['ensembl_master_gene_type']
        for val in gene_dict.keys():
            ensembl_geneset['master_gene_type'].replace(val, gene_dict[val], inplace=True)
        # reformat master transcript type
        ensembl_geneset['master_transcript_type'] = ensembl_geneset['ensembl_transcript_type']
        trans_dict = genomic_type_mapper['ensembl_master_transcript_type']
        for val in trans_dict.keys():
            ensembl_geneset['master_transcript_type'].replace(val, trans_dict[val], inplace=True)

        # post-process reformatted data
        ensembl_geneset.drop(list(range(9)), axis=1, inplace=True)
        ensembl_geneset.drop_duplicates(subset=None, keep='first', inplace=True)

        return ensembl_geneset

    def merges_ensembl_mapping_data(self) -> pd.DataFrame:
        """Processes Ensembl uniprot and entrez mapping data in order to prepare it for combination with other gene
        identifier data sources. After merging the annotation together the main gene data is merged with the
        annotation data.

        Return:
            ensembl: A Pandas DataFrame containing processed and filtered data that has been merged with
                additional annotation  mapping data from uniprot and entrez.
        """

        drop_cols = 'db_name', 'info_type', 'source_identity', 'xref_identity', 'linkage_type'
        # uniprot data
        ensembl_uniprot = self.reads_gcs_bucket_data_to_pandas_df('Homo_sapiens.GRCh38.*.uniprot.tsv', '\t', 0, 0)
        ensembl_uniprot.rename(columns={'xref': 'uniprot_id', 'gene_stable_id': 'ensembl_gene_id'}, inplace=True)
        ensembl_uniprot.replace('-', 'None', inplace=True)
        ensembl_uniprot.fillna('None', inplace=True)
        ensembl_uniprot.drop([drop_cols], axis=1, inplace=True)
        ensembl_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)
        # entrez data
        ensembl_entrez = self.reads_gcs_bucket_data_to_pandas_df('Homo_sapiens.GRCh38.*.entrez.tsv', '\t', 0, 0)
        ensembl_entrez.rename(columns={'xref': 'entrez_id', 'gene_stable_id': 'ensembl_gene_id'}, inplace=True)
        ensembl_entrez = ensembl_entrez.loc[ensembl_entrez['db_name'].apply(lambda x: x == 'EntrezGene')]
        ensembl_entrez.replace('-', 'None', inplace=True)
        ensembl_entrez.fillna('None', inplace=True)
        ensembl_entrez.drop([drop_cols], axis=1, inplace=True)
        ensembl_entrez.drop_duplicates(subset=None, keep='first', inplace=True)

        # merge annotation data
        mrglist = ['ensembl_gene_id', 'transcript_stable_id', 'protein_stable_id']
        ensembl_annot = pandas.merge(ensembl_uniprot, ensembl_entrez, left_on=mrglist, right_on=mrglist, how='outer')
        ensembl_annot.fillna('None', inplace=True)
        # merge annotation and gene data
        mrglist, ensembl_geneset = ['ensembl_gene_id', 'transcript_stable_id'], self.preprocess_ensembl_uniprot()
        ensembl = pandas.merge(ensembl_geneset, ensembl_annot, left_on=mrg_list, right_on=mrg_list, how='outer')
        ensembl.fillna('None', inplace=True)
        ensembl.replace('NA', 'None', inplace=True, regex=False)

        self.writes_data_to_gcs_bucket('ensembl_identifier_data_cleaned.txt', self.ensembl)

        return ensembl

    def preprocess_uniprot_data(self) -> pd.DataFrame:
        """Processes Uniprot data in order to prepare it for combination with other gene identifier data sources. The
        reformatting performed on the data includes removing unnecessary columns and reformatting column values to a
        common set of terms that will be applied universally to all gene and protein identifier data sources.

        Return:
            explode_df_uniprot: A Pandas DataFrame containing processed and filtered data.
        """

        # read in data and prepare it for processing
        uniprot = self.reads_gcs_bucket_data_to_df('uniprot_identifier_mapping.tab', '\t', 0, 0)
        uniprot.fillna('None', inplace=True)
        uniprot.rename(columns={'Entry': 'uniprot_id', 'Cross-reference (GeneID)': 'entrez_id',
                                'Ensembl transcript': 'transcript_stable_id', 'Cross-reference (HGNC)': 'hgnc_id',
                                'Gene names  (synonym )': 'synonyms', 'Gene names  (primary )': 'symbol'}, inplace=True)
        uniprot['synonyms'] = uniprot['synonyms'].apply(lambda x: '|'.join(x.split()) if x.isupper() else x)

        # explode nested data
        explode_df_uniprot = explodes_data(uniprot.copy(), ['transcript_stable_id', 'entrez_id', 'hgnc_id'], ';')
        explode_df_uniprot = explodes_data(explode_df_uniprot.copy(), ['symbol'], '|')

        # post-process reformatted data
        explode_df_uniprot['transcript_stable_id'].replace('\s.*', '', inplace=True, regex=True)  # strip uniprot names
        explode_df_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)

        return explode_df_uniprot

    def preprocess_ncbi_data(self) -> pd.DataFrame:
        """Processes NCBI Gene data in order to prepare it for combination with other gene identifier data sources.
        The reformatting performed on the data includes removing unnecessary columns and reformatting column values to a
        common set of terms that will be applied universally to all gene and protein identifier data sources.

        Return:
            explode_df_ncbi_gene: A Pandas DataFrame containing processed and filtered data.
        """

        # read in data and prepare it for processing
        ncbi_gene = self.reads_gcs_bucket_data_to_df('Homo_sapiens.gene_info', '\t', 5, 0)
        ncbi_gene = ncbi_gene.loc[ncbi_gene['#tax_id'].apply(lambda x: x == 9606)]  # remove non-human rows
        ncbi_gene.replace('-', 'None', inplace=True)
        ncbi_gene.rename(columns={'GeneID': 'entrez_id', 'Symbol': 'symbol', 'Synonyms': 'synonyms'}, inplace=True)
        # combine symbol columns into single column
        ncbi_gene['symbol'] = ncbi_gene['Symbol_from_nomenclature_authority'] + '|' + ncbi_gene['symbol']
        ncbi_gene['name'] = ncbi_gene['Full_name_from_nomenclature_authority'] + '|' + ncbi_gene['description']

        # explode nested data
        explode_df_ncbi_gene = explodes_data(ncbi_gene.copy(), ['symbol', 'name'], '|')
        explode_df_ncbi_gene['entrez_id'] = explode_df_ncbi_gene['entrez_id'].astype(str)

        # reformat entrez gene type
        explode_df_ncbi_gene['entrez_gene_type'] = explode_df_ncbi_gene['type_of_gene']
        gene_dict = genomic_type_mapper['entrez_gene_type']
        for val in gene_dict.keys():
            explode_df_ncbi_gene['entrez_gene_type'].replace(val, gene_dict[val], inplace=True)
        # reformat master gene type
        explode_df_ncbi_gene['master_gene_type'] = explode_df_ncbi_gene['entrez_gene_type']
        gene_dict = genomic_type_mapper['master_gene_type']
        for val in gene_dict.keys():
            explode_df_ncbi_gene['master_gene_type'].replace(val, gene_dict[val], inplace=True)

        # post-process reformatted data
        explode_df_ncbi_gene.drop(['type_of_gene', 'dbXrefs', 'description', 'Nomenclature_status', 'Modification_date',
                                   'LocusTag', '#tax_id', 'Full_name_from_nomenclature_authority', 'Feature_type',
                                   'Symbol_from_nomenclature_authority'], axis=1, inplace=True)
        explode_df_ncbi_gene.drop_duplicates(subset=None, keep='first', inplace=True)

        return explode_df_ncbi_gene

    def preprocess_protein_ontology_mapping_data(self) -> pd.DataFrame:
        """Processes PRotein Ontology identifier mapping data in order to prepare it for combination with other gene
        identifier data sources.

        Return:
            pro_mapping: A Pandas DataFrame containing processed and filtered data.
        """

        pro_mapping = self.reads_gcs_bucket_data_to_df('promapping.txt', '\t', 5, ['pro_id', 'Entry', 'pro_mapping'])
        # remove rows without 'UniProtKB'
        pro_mapping = pro_mapping.loc[pro_mapping['Entry'].apply(lambda x: x.startswith('UniProtKB:'))]
        pro_mapping['pro_id'].replace('PR:', 'PR_', inplace=True, regex=True)  # replace PR: with PR_
        pro_mapping['Entry'].replace('(^\w*\:)', '', inplace=True, regex=True)  # remove id type which appear before ':'
        pro_mapping.rename(columns={'Entry': 'uniprot_id'}, inplace=True)
        pro_mapping.drop(['pro_mapping'], axis=1, inplace=True)
        pro_mapping.drop_duplicates(subset=None, keep='first', inplace=True)

        return pro_mapping

    def merges_genomic_identifier_data(self) -> pd.DataFrame:
        """Merges HGNC, Ensembl, Uniprot, and PRotein Ontology identifiers into a single Pandas DataFrame.

        Return:
            merged_data: A Pandas DataFrame of merged genomic identifier information.
        """

        # hgnc + ensembl
        merge_cols = ['ensembl_gene_id', 'entrez_id', 'uniprot_id', 'master_gene_type', 'symbol']
        ensembl_hgnc = pandas.merge(self.merges_ensembl_mapping_data(), self.preprocess_hgnc_data(),
                                    left_on=merge_cols, right_on=merge_cols, how='outer')
        ensembl_hgnc.fillna('None', inplace=True)
        ensembl_hgnc.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc + uniprot
        merge_cols = ['entrez_id', 'hgnc_id', 'uniprot_id', 'transcript_stable_id', 'symbol', 'synonyms']
        ensembl_hgnc_uniprot = pandas.merge(ensembl_hgnc, self.preprocess_uniprot_data(),
                                            left_on=merge_cols, right_on=merge_cols, how='outer')
        ensembl_hgnc_uniprot.fillna('None', inplace=True)
        ensembl_hgnc_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc_uniprot + Homo_sapiens.gene_info
        merge_cols = ['entrez_id', 'master_gene_type', 'symbol', 'synonyms', 'name', 'map_location']
        ensembl_hgnc_uniprot_ncbi = pandas.merge(ensembl_hgnc_uniprot, self.preprocess_ncbi_data(),
                                                 left_on=merge_cols, right_on=merge_cols, how='outer')
        ensembl_hgnc_uniprot_ncbi.fillna('None', inplace=True)
        ensembl_hgnc_uniprot_ncbi.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc_uniprot_ncbi + promapping.txt
        merged_data = pandas.merge(ensembl_hgnc_uniprot_ncbi, self.preprocess_protein_ontology_mapping_data(),
                                   left_on='uniprot_id', right_on='uniprot_id', how='outer')
        merged_data.fillna('None', inplace=True)
        merged_data.drop_duplicates(subset=None, keep='first', inplace=True)

        return merged_data

    def fixes_genomic_symbols(self) -> pd.DataFrame:
        """Takes a Pandas DataFrame of genomic identifier data and fixes gene symbol information.

        Returns:
            merged_data_clean: A Pandas DataFrame with fix genomic symbols.
        """

        clean_dates, merged_data = [], self.merges_genomic_identifier_data()
        for x in tqdm(list(merged_data['symbol'])):
            if '-' in x and len(x.split('-')[0]) < 3 and len(x.split('-')[1]) == 3:
                clean_dates.append(x.split('-')[1].upper() + x.split('-')[0])
            else:
                clean_dates.append(x)
        merged_data['symbol'] = clean_dates

        # make sure that all gene and transcript type columns have none recoded to unknown or not protein-coding
        merged_data['hgnc_gene_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data['ensembl_gene_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data['entrez_gene_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data['master_gene_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data['master_transcript_type'].replace('None', 'not protein-coding', inplace=True, regex=False)
        merged_data['ensembl_transcript_type'].replace('None', 'unknown', inplace=True, regex=False)
        merged_data_clean = merged_data.drop_duplicates(subset=None, keep='first')

        self.writes_data_to_gcs_bucket('Merged_Human_Ensembl_Entrez_HGNC_Uniprot_Identifiers.txt', merged_data_clean)

        return merged_data_clean

    def creates_master_identifier_genomic_map(self) -> Tuple:
        """Takes a Pandas Dataframe of merged genomic identifiers and expands them to create a complete mapping between
        the identifiers. A master dictionary is built, where the keys are ensembl_gene_id, transcript_stable_id,
        symbol, protein_stable_id, uniprot_id, entrez_id, hgnc_id, and pro_id identifiers and values are the list of
        identifiers that match to each identifier. It's important to note that there are several labeling identifiers
        (i.e. name, chromosome, map_location, Other_designations, synonyms, transcript_name, *_gene_types, and
        trasnscript_type_update), which will only be mapped when clustered against one of the primary identifier types
        (i.e. the keys described above).

        Returns:
            master_dict: A dictionary with the formatting described above.
            ids: A list of identifiers.
            stop_point: An integer used to sort ids.
        """

        df, stop_point = self.fixes_genomic_symbols(), 8
        # get all permutations of identifiers (e.g. ['entrez_id', 'ensembl_gene_id'])
        ids = ['ensembl_gene_id', 'transcript_stable_id', 'protein_stable_id', 'uniprot_id', 'entrez_id', 'hgnc_id',
               'pro_id', 'symbol', 'synonyms', 'ensembl_gene_type', 'transcript_name', 'ensembl_transcript_type',
               'master_gene_type', 'master_transcript_type', 'hgnc_gene_type', 'name', 'map_location', 'chromosome',
               'Other_designations', 'entrez_gene_type']
        # get list of data types that ignores subjects of a permutation pair that are metadata
        identifier_list = [x for x in list(itertools.permutations(ids, 2)) if x[0] not in ids[stop_point:]]

        master_dict = {}
        for ids in tqdm(identifier_list):
            x = {k: [ids[1] + '_' + x for x in set(g[ids[1]].tolist()) if x != 'None'] for k, g in df.groupby(ids[0])}
            for key in x.keys():
                if ids[0] + '_' + key in master_dict.keys(): master_dict[ids[0] + '_' + key] += x[key]
                else: master_dict[ids[0] + '_' + key] = x[key]

        return master_dict, ids, stop_point

    def formats_master_genomic_identifier_map(self) -> Dict:
        """Takes a dictionary of genomic identifier information and finalizes its formatting in order

        Returns:
            reformatted_mapped_ids: A dictionary containing genomic identifier information which is keyed by genomic
                identifier types and where values are lists of all other genomic identifiers that map to that key.
        """

        gene_var, transcript_var, reformatted_mapped_ids = 'master_gene_type', 'master_transcript_type', {}
        master, ids, stop = self.condenses_master_genomic_identifer_map()
        for ident in tqdm(master.keys()):
            id_info, type_updates = set(), []
            for i in master[ident]:  # get all identifying information for all linked identifiers
                if not any(x for x in ids[stop:] if i.startswith(x)) and i in master.keys(): id_info |= set(master[i])
                else: continue
            genes = [x.split('_')[-1] for x in id_info if x.startswith(gene_type_var)]
            trans = [x.split('_')[-1] for x in id_info if x.startswith(transcript_type_var)]
            for types in [genes if len(genes) > 0 else ['None'], trans if len(trans) > 0 else ['None']]:
                if 'protein-coding' in set(types): type_updates.append('protein-coding')
                else: type_updates.append('not protein-coding')
            # update identifier set information
            identifier_info = [x for x in id_info if not x.startswith(gene_var) and not x.startswith(transcript_var)]
            identifier_info += ['gene_type_update_' + type_updates[0], 'transcript_type_update_' + type_updates[1]]
            reformatted_mapped_ids[ident] = identifier_info

        # save results for output > 4GB requires special approach: https://stackoverflow.com/questions/42653386
        filepath = 'Merged_gene_rna_protein_identifiers.pkl'
        # defensive way to write pickle.write, allowing for very large files on all platforms
        # TODO: figure out how to write this type of data to the GCS bucket
        with open(filepath, 'wb') as f_out:
            for idx in range(0, sys.getsizeof(pickle.dumps(reformatted_mapped_ids)), 2 ** 31 - 1):
                f_out.write(pickle.dumps(reformatted_mapped_ids)[idx:idx + (2 ** 31 - 1)])

        return reformatted_mapped_ids

