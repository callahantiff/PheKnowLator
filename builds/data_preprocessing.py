#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import fnmatch
import itertools
import networkx
import os
import pandas as pd
import pickle

from collections import Counter
from google.cloud import storage
from rdflib import Graph, URIRef
from reactome2py import content
from tqdm import tqdm
from typing import List

# import script containing helper functions
from pkt_kg.utils import *  # tests written for called methods in pkt_kg/utils/data_utils.py


class DataPreprocessing(object):
    """Class provides a container for the data preprocessing methods, original housed in the Data_Preprocessing.ipynb
    Jupyter Notebook. See notebook (https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb) for
    more detailed descriptions of each processed data source and the rationale behind the different filtering and
    processing approaches.

    Attributes:
        gcs_bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        org_data: A string specifying the location of the original_data directory for a specific build.
        processed_data: A string specifying the location of the original_data directory for a specific build.
        temp_dir: A string specifying a temporary directory to use while processing data locally.

    Raises:
        ValueError: when trying to download a non-existent file from the GCS original_data dir of the current build
    """

    def __init__(self, gcs_bucket: storage.bucket.Bucket, org_data: str, processed_data: str, temp_dir: str) -> None:

        self.genomic_type_mapper = pickle.load(open('builds/genomic_typing.pkl', 'rb'))
        self.bucket: storage.bucket.Bucket = gcs_bucket
        self.original_data: str = org_data
        self.processed_data: str = processed_data
        self.temp_dir = temp_dir

    def uploads_data_to_gcs_bucket(self, file_loc: str) -> None:
        """Takes a file name and pushes the data referenced by the filename object and stored locally in that object to
        a Google Cloud Storage bucket.

        Args:
            file_loc: A string containing the name of file to write to a Google Cloud Storage bucket.

        Returns:
            None.
        """

        blob = self.bucket.blob(self.processed_data + file_loc)
        blob.upload_from_filename(self.temp_dir + '/' + file_loc)
        print('Uploaded {} to GCS bucket: {}'.format(file_loc, self.processed_data))

        return None

    def downloads_data_from_gcs_bucket(self, file_loc: str) -> str:
        """Takes a filename and a data object and writes the data in that object to the Google Cloud Storage bucket
        specified in the filename.

        Args:
            file_loc: A string containing the name of file to write to a Google Cloud Storage bucket.

        Returns:
            data_file: A string containing the local filepath for a file downloaded from a GSC bucket.

        Raises:
            ValueError: when trying to download a non-existent file from the GCS original_data dir of the current build.
        """

        try:
            _files = [_.name for _ in self.bucket.list_blobs(prefix=self.original_data)]
            matched_file = fnmatch.filter(_files, '*/' + file_loc)[0]  # poor man's glob
            self.bucket.blob(matched_file).download_to_filename(self.temp_dir + '/' + matched_file.split('/')[-1])
            data_file = self.temp_dir + '/' + matched_file.split('/')[-1]
        except IndexError:
            raise ValueError('Cannot find {} in the GCS original_data directory of the current build'.format(file_loc))

        return data_file

    def reads_gcs_bucket_data_to_df(self, file_loc: str, delm: str, skip: int = 0, head: Optional[int, List] = None,
                                    sht: Optional[int, str] = None) -> pd.DataFrame:
        """Takes a Google Cloud Storage bucket and a filename and downloads to the data to a local temp directory.
        Once downloaded, the file is read into a Pandas DataFrame.

        Args:
            file_loc: A string containing the name of file that exists in a Google Cloud Storage bucket.
            delm: A string specifying a file delimiter.
            skip: An integer specifying the number of rows to skip when reading in the data.
            head: An integer specifying the header row, None for no header or a list of header names.
            sht: Used for reading xlsx files. If not None, an integer or string specifying which sheet to read in.

        Returns:
             df: A Pandas DataFrame object containing data read from a Google Cloud Storage bucket.
        """

        dat = self.downloads_data_from_gcs_bucket(file_loc)

        if not isinstance(head, List):
            if sht is not None: df = pd.read_excel(dat, sheet_name=sht, header=head, skiprows=skip, engine='openpyxl')
            else: df = pd.read_csv(dat, header=head, delimiter=delm, skiprows=skip, low_memory=0)
        else:
            if sht is not None:
                df = pd.read_excel(dat, sheet_name=sht, header=None, names=head, skiprows=skip, engine='openpyxl')
            else:
                df = pd.read_csv(data_file, header=None, names=head, delm=delm, skiprows=skip, low_memory=0)

        return df

    def _preprocess_hgnc_data(self) -> pd.DataFrame:
        """Processes HGNC data in order to prepare it for combination with other gene identifier data sources. Data
        needs to be lightly cleaned before it can be merged with other data. This light cleaning includes renaming
        columns, replacing NaN with None, updating data types (i.e. making all columns type str), and unnesting '|'-
        delimited data. The final step is to update the gene_type variable such that each of the variable values is
        re-grouped to be protein-coding, other or ncRNA.

        Returns:
            explode_df_hgnc: A Pandas DataFrame containing processed and filtered data.
        """

        # read in data and prepare it for processing
        hgnc = self.reads_gcs_bucket_data_to_df('hgnc_complete_set.txt', '\t', 0, 0, None)
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

    def _preprocess_ensembl_data(self) -> pd.DataFrame:
        """Processes Ensembl data in order to prepare it for combination with other gene identifier data sources. Data
        needs to be reformatted in order for it to be able to be merged with the other gene, RNA, and protein identifier
        data. To do this, we iterate over each row of the data and extract the fields shown below in column_names,
        making each of these extracted fields their own column. The final step is to update the gene_type variable such
        that each of the variable values is re-grouped to be protein-coding, other or ncRNA.

        Returns:
            ensembl_geneset: A Pandas DataFrame containing processed and filtered data.
        """

        # read in data and prepare it for processing
        ensembl_geneset = self.reads_gcs_bucket_data_to_df('Homo_sapiens.GRCh38.*.gtf', '\t', 5, 0, None)
        cleaned_col = []
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
        annotation data. The cleaned Ensembl data is saved so that it can be used when generating node metadata for
        transcript identifiers.

        Returns:
            ensembl: A Pandas DataFrame containing processed and filtered data that has been merged with
                additional annotation  mapping data from uniprot and entrez.
        """

        drop_cols = 'db_name', 'info_type', 'source_identity', 'xref_identity', 'linkage_type'
        # uniprot data
        ensembl_uniprot = self.reads_gcs_bucket_data_to_pandas_df('Homo_sapiens.GRCh38.*.uniprot.tsv', '\t', 0, 0, None)
        ensembl_uniprot.rename(columns={'xref': 'uniprot_id', 'gene_stable_id': 'ensembl_gene_id'}, inplace=True)
        ensembl_uniprot.replace('-', 'None', inplace=True)
        ensembl_uniprot.fillna('None', inplace=True)
        ensembl_uniprot.drop([drop_cols], axis=1, inplace=True)
        # entrez data
        ensembl_entrez = self.reads_gcs_bucket_data_to_pandas_df('Homo_sapiens.GRCh38.*.entrez.tsv', '\t', 0, 0)
        ensembl_entrez.rename(columns={'xref': 'entrez_id', 'gene_stable_id': 'ensembl_gene_id'}, inplace=True)
        ensembl_entrez = ensembl_entrez.loc[ensembl_entrez['db_name'].apply(lambda x: x == 'EntrezGene')]
        ensembl_entrez.replace('-', 'None', inplace=True)
        ensembl_entrez.fillna('None', inplace=True)
        ensembl_entrez.drop([drop_cols], axis=1, inplace=True)
        # merge annotation data
        mrglist = ['ensembl_gene_id', 'transcript_stable_id', 'protein_stable_id']
        ensembl_annot = pandas.merge(ensembl_uniprot, ensembl_entrez, left_on=mrglist, right_on=mrglist, how='outer')
        ensembl_annot.fillna('None', inplace=True)
        # merge annotation and gene data
        ensembl_geneset = self._preprocess_ensembl_uniprot()
        ensembl = pandas.merge(ensembl_geneset, ensembl_annot, left_on=mrglist[0:2], right_on=mrglist[0:2], how='outer')
        ensembl.fillna('None', inplace=True)
        ensembl.replace('NA', 'None', inplace=True, regex=False)
        ensembl.drop_duplicates(subset=None, keep='first', inplace=True)

        # save data
        filename = 'ensembl_identifier_data_cleaned.txt'
        ensembl.to_csv(self.temp_dir + '/' + filename, header=True, sep='\t', index=False)
        self.uploads_data_to_gcs_bucket(filename)

        return ensembl

    def _preprocess_uniprot_data(self) -> pd.DataFrame:
        """Processes Uniprot data in order to prepare it for combination with other gene identifier data sources. The
        reformatting performed on the data includes removing unnecessary columns and reformatting column values to a
        common set of terms that will be applied universally to all gene and protein identifier data sources.

        Returns:
            explode_df_uniprot: A Pandas DataFrame containing processed and filtered data.
        """

        uniprot = self.reads_gcs_bucket_data_to_df('uniprot_identifier_mapping.tab', '\t', 0, 0, None)
        uniprot.fillna('None', inplace=True)
        uniprot.rename(columns={'Entry': 'uniprot_id', 'Cross-reference (GeneID)': 'entrez_id',
                                'Ensembl transcript': 'transcript_stable_id', 'Cross-reference (HGNC)': 'hgnc_id',
                                'Gene names  (synonym )': 'synonyms', 'Gene names  (primary )': 'symbol'}, inplace=True)
        uniprot['synonyms'] = uniprot['synonyms'].apply(lambda x: '|'.join(x.split()) if x.isupper() else x)

        # explode nested data and perform light reformatting
        explode_df_uniprot = explodes_data(uniprot.copy(), ['transcript_stable_id', 'entrez_id', 'hgnc_id'], ';')
        explode_df_uniprot = explodes_data(explode_df_uniprot.copy(), ['symbol'], '|')
        explode_df_uniprot['transcript_stable_id'].replace('\s.*', '', inplace=True, regex=True)  # strip uniprot names
        explode_df_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)

        return explode_df_uniprot

    def _preprocess_ncbi_data(self) -> pd.DataFrame:
        """Processes NCBI Gene data in order to prepare it for combination with other gene identifier data sources.
        Data needs to be lightly cleaned before it can be merged with other data. This light cleaning includes renaming
        columns, replacing NaN with None, updating data types (i.e. making all columns type str), and unnesting '|'-
        delimited data. Then, the gene_type variable is cleaned such that each of the variable's values are re-grouped
        to be protein-coding, other or ncRNA.

        Returns:
            explode_df_ncbi_gene: A Pandas DataFrame containing processed and filtered data.
        """

        # read in data and prepare it for processing
        ncbi_gene = self.reads_gcs_bucket_data_to_df('Homo_sapiens.gene_info', '\t', 5, 0, None)
        ncbi_gene = ncbi_gene.loc[ncbi_gene['#tax_id'].apply(lambda x: x == 9606)]  # remove non-human rows
        ncbi_gene.replace('-', 'None', inplace=True)
        ncbi_gene.rename(columns={'GeneID': 'entrez_id', 'Symbol': 'symbol', 'Synonyms': 'synonyms'}, inplace=True)
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

    def _preprocess_protein_ontology_mapping_data(self) -> pd.DataFrame:
        """Processes PRotein Ontology identifier mapping data in order to prepare it for combination with other gene
        identifier data sources.

        Returns:
            pro_mapping: A Pandas DataFrame containing processed and filtered data.
        """

        col_names = ['pro_id', 'Entry', 'pro_mapping']
        pro_mapping = self.reads_gcs_bucket_data_to_df('promapping.txt', '\t', 5, col_names, None)
        pro_mapping = pro_mapping.loc[pro_mapping['Entry'].apply(lambda x: x.startswith('UniProtKB:'))]
        pro_mapping['pro_id'].replace('PR:', 'PR_', inplace=True, regex=True)  # replace PR: with PR_
        pro_mapping['Entry'].replace('(^\w*\:)', '', inplace=True, regex=True)  # remove id type which appear before ':'
        pro_mapping.rename(columns={'Entry': 'uniprot_id'}, inplace=True)
        pro_mapping.drop(['pro_mapping'], axis=1, inplace=True)
        pro_mapping.drop_duplicates(subset=None, keep='first', inplace=True)

        return pro_mapping

    def merges_genomic_identifier_data(self) -> pd.DataFrame:
        """Merges HGNC, Ensembl, Uniprot, and PRotein Ontology identifiers into a single Pandas DataFrame.

        Returns:
            merged_data: A Pandas DataFrame of merged genomic identifier information.
        """

        # hgnc + ensembl
        merge_cols = ['ensembl_gene_id', 'entrez_id', 'uniprot_id', 'master_gene_type', 'symbol']
        ensembl_hgnc = pandas.merge(self.merges_ensembl_mapping_data(), self._preprocess_hgnc_data(),
                                    left_on=merge_cols, right_on=merge_cols, how='outer')
        ensembl_hgnc.fillna('None', inplace=True)
        ensembl_hgnc.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc + uniprot
        merge_cols = ['entrez_id', 'hgnc_id', 'uniprot_id', 'transcript_stable_id', 'symbol', 'synonyms']
        ensembl_hgnc_uniprot = pandas.merge(ensembl_hgnc, self._preprocess_uniprot_data(),
                                            left_on=merge_cols, right_on=merge_cols, how='outer')
        ensembl_hgnc_uniprot.fillna('None', inplace=True)
        ensembl_hgnc_uniprot.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc_uniprot + Homo_sapiens.gene_info
        merge_cols = ['entrez_id', 'master_gene_type', 'symbol', 'synonyms', 'name', 'map_location']
        ensembl_hgnc_uniprot_ncbi = pandas.merge(ensembl_hgnc_uniprot, self._preprocess_ncbi_data(),
                                                 left_on=merge_cols, right_on=merge_cols, how='outer')
        ensembl_hgnc_uniprot_ncbi.fillna('None', inplace=True)
        ensembl_hgnc_uniprot_ncbi.drop_duplicates(subset=None, keep='first', inplace=True)
        # ensembl_hgnc_uniprot_ncbi + promapping.txt
        merged_data = pandas.merge(ensembl_hgnc_uniprot_ncbi, self._preprocess_protein_ontology_mapping_data(),
                                   left_on='uniprot_id', right_on='uniprot_id', how='outer')
        merged_data.fillna('None', inplace=True)
        merged_data.drop_duplicates(subset=None, keep='first', inplace=True)

        return merged_data

    def _fixes_genomic_symbols(self) -> pd.DataFrame:
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

        return merged_data_clean

    def _harmonizes_genomic_mapping_data(self) -> Tuple:
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

        df, stop_point = self._fixes_genomic_symbols(), 8
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
                if ids[0] + '_' + key in master_dict.keys():
                    master_dict[ids[0] + '_' + key] += x[key]
                else:
                    master_dict[ids[0] + '_' + key] = x[key]

        return master_dict, ids, stop_point

    def creates_master_genomic_identifier_map(self) -> Dict:
        """Takes a dictionary of genomic identifier information and finalizes its formatting in order

        Returns:
            reformatted_mapped_ids: A dictionary containing genomic identifier information which is keyed by genomic
                identifier types and where values are lists of all other genomic identifiers that map to that key.
        """

        gene_var, transcript_var, reformatted_mapped_ids = 'master_gene_type', 'master_transcript_type', {}
        master, ids, stop = self._harmonizes_genomic_mapping_data()
        for ident in tqdm(master.keys()):
            id_info, type_updates = set(), []
            for i in master[ident]:  # get all identifying information for all linked identifiers
                if not any(x for x in ids[stop:] if i.startswith(x)) and i in master.keys():
                    id_info |= set(master[i])
                else:
                    continue
            genes = [x.split('_')[-1] for x in id_info if x.startswith(gene_type_var)]
            trans = [x.split('_')[-1] for x in id_info if x.startswith(transcript_type_var)]
            for types in [genes if len(genes) > 0 else ['None'], trans if len(trans) > 0 else ['None']]:
                if 'protein-coding' in set(types):
                    type_updates.append('protein-coding')
                else:
                    type_updates.append('not protein-coding')
            # update identifier set information
            identifier_info = [x for x in id_info if not x.startswith(gene_var) and not x.startswith(transcript_var)]
            identifier_info += ['gene_type_update_' + type_updates[0], 'transcript_type_update_' + type_updates[1]]
            reformatted_mapped_ids[ident] = identifier_info

        # save results for output > 4GB requires special approach: https://stackoverflow.com/questions/42653386
        filename = 'Merged_gene_rna_protein_identifiers.pkl'
        with open(self.temp_dir + '/' + filename, 'wb') as f_out:
            for idx in range(0, sys.getsizeof(pickle.dumps(reformatted_mapped_ids)), 2 ** 31 - 1):
                f_out.write(pickle.dumps(reformatted_mapped_ids)[idx:idx + (2 ** 31 - 1)])
        self.uploads_data_to_gcs_bucket(filename)

        return reformatted_mapped_ids

    def generates_specific_genomic_maps(self) -> None:
        """Method takes a list of information needed to create mappings between specific sets of genomic identifiers.

        Returns:
            None.
        """

        reformatted_mapped_ids = self.creates_master_genomic_identifier_map()
        gene_sets = [
            ['/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt', 'ensembl_gene_id', 'entrez_id', 'gene_type_update', False, False],
            ['/ENSEMBL_TRANSCRIPT_PROTEIN_ONTOLOGY_MAP.txt', 'transcript_stable_id', 'pro_id',
             'transcript_type_update', False, True],
            ['/ENTREZ_GENE_ENSEMBL_TRANSCRIPT_MAP.txt', 'entrez_id', 'transcript_stable_id', 'transcript_type_update',
             False, False],
            ['/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt', 'entrez_id', 'pro_id', 'gene_type_update', False, True],
            ['/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt', 'symbol', 'transcript_stable_id', 'transcript_type_update',
             False, False],
            ['/STRING_PRO_ONTOLOGY_MAP.txt', 'protein_stable_id', 'pro_id', None, False, True],
            ['/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt', 'uniprot_id', 'pro_id', None, False, True]
        ]

        for x in gene_sets:
            genomic_id_mapper(reformatted_mapped_ids, self.temp_dir + x[0], x[1], x[2], x[3], x[4], x[5])
            self.uploads_data_to_gcs_bucket(x[0])

        return None

    def _processes_mesh_data(self) -> pd.DataFrame:
        """Parses MeSH data converting it from n-triples format into Pandas DataFrame that can be merged with ChEBI
        data.

        Returns:
            df: A Pandas Data Frame containing three columns: CODES (i.e. 'MESH' identifiers), STRINGS (i.e. string
                labels or synonyms), and TYPES (i.e. a string signifying if the STRING column entry is a 'NAME' or
                'SYNONYM').
        """

        mesh = [x.split('> ') for x in tqdm(open(self.downloads_data_from_gcs_bucket('mesh2021.nt'), 'r').readlines())]
        mesh_dict, dat = {}, []
        for row in tqdm(mesh):
            dbxref, label, msh_type = None, None, None
            s, p, o = 'MESH_' + row[0].split('/')[-1], row[1].split('#')[-1], row[2]
            if p == 'preferredConcept' or p == 'concept': dbxref = 'MESH_' + o.split('/')[-1]
            if 'label' in p.lower(): label = o.split('"')[1]
            if 'type' in p.lower(): msh_type = o.split('#')[1]
            if s in mesh_dict.keys():
                if dbxref is not None: mesh_dict[s]['dbxref'].add(dbxref)
                if label is not None: mesh_dict[s]['label'].add(label)
                if msh_type is not None: mesh_dict[s]['type'].add(msh_type)
            else:
                mesh_dict[s] = {'dbxref': set(), 'label': set(), 'type': set(), 'synonym': set()}
                if dbxref is not None: mesh_dict[s]['dbxref'] = {dbxref}
                if label is not None: mesh_dict[s]['label'] = {label}
                if msh_type is not None: mesh_dict[s]['type'] = {msh_type}
        # fine tune dictionary
        for key in tqdm(mesh_dict.keys()):
            if 'SCR_Chemical' in mesh_dict[key]['type'] or 'TopicalDescriptor' in mesh_dict[key]['type']:
                for i in mesh_dict[key]['dbxref']:
                    if len(mesh_dict[key]['dbxref']) > 0 and i in mesh_dict.keys():
                        mesh_dict[key]['synonym'] |= mesh_dict[i]['label']
        dict_keys = [x for x in mesh_dict.keys()
                     if x.strip('MESH_')[0] not in ['C', 'D'] or ('.' in x or 'Q' in x or len(x.strip('MESH_')) < 5)]
        for key in tqdm(dict_keys):
            del mesh_dict[key]

        # convert to pandas DataFrame
        for key, value in mesh_dict.items():
            dat += [[key, list(value['label'])[0], 'NAME']]
            if len(value['synonym']) > 0:
                for i in value['synonym']:
                    dat += [[key, i, 'SYNONYM']]
        df = pandas.DataFrame({'CODE': [x[0] for x in dat], 'TYPE': [x[2] for x in dat], 'STRING': [x[1] for x in dat]})
        # lowercase all strings and remove white space and punctuation
        df['STRING'] = df['STRING'].str.lower()
        df['STRING'] = df['STRING'].str.replace('[^\w]', '')

        return df

    def _processes_chebi_data(self) -> pd.DataFrame:
        """Parses ChEBI data into a Pandas DataFrame that can be merged with MeSH data.

        Returns:
            chebi_filtered: A Pandas DataFrame containing three columns: CODES (i.e. 'CHEBI' identifiers), STRINGS (i.e.
                string labels or synonyms), and TYPES (i.e. a string signifying if the STRING column entry is a
                'NAME' or 'SYNONYM').
        """

        chebi = self.reads_gcs_bucket_data_to_df('names.tsv', '\t', 0, 0, None)

        # preprocess data
        chebi_filtered = chebi[['COMPOUND_ID', 'TYPE', 'NAME']]
        chebi_filtered.drop_duplicates(subset=None, keep='first', inplace=True)
        chebi_filtered.columns = ['CODE', 'TYPE', 'STRING']

        # append CHEBI to the number in each code
        chebi_filtered['CODE'] = chebi_filtered['CODE'].apply(lambda x: "{}{}".format('CHEBI_', x))

        # lowercase all strings and remove white space and punctuation
        chebi_filtered['STRING'] = chebi_filtered['STRING'].str.lower()
        chebi_filtered['STRING'] = chebi_filtered['STRING'].str.replace('[^\w]', '')

        return chebi_filtered

    def maps_chebi_to_mesh(self) -> None:
        """

        Return:
            None.
        """

        chebi_filtered = self._processes_mesh_data()
        mesh_filtered = self._processes_chebi_data()

        # merge data
        chem_merge = pandas.merge(chebi_filtered[['STRING', 'CODE']],
                                  mesh_filtered[['STRING', 'CODE']], on='STRING', how='inner')

        # filter results
        mesh_edges = set()
        for idx, row in chem_merge.drop_duplicates().iterrows():
            mesh, chebi = row['CODE_y'], row['CODE_x']
            syns = [x for x in mesh_dict[mesh]['dbxref'] if 'C' in x or 'D' in x]
            mesh_edges.add(tuple([mesh, chebi]))
            if len(syns) > 0:
                for x in syns:
                    mesh_edges.add(tuple([x, chebi]))

        # write results and push data to gcs bucket
        filename = 'MESH_CHEBI_MAP.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out:
            for pair in mesh_edges:
                out.write(pair[0] + '\t' + pair[1] + '\n')
        self.uploads_data_to_gcs_bucket(filename)

        return None

    def _preprocess_disease_mapping_data(self) -> Tuple[Dict, Dict]:
        """Method processes MONDO and HPO ontology data in order to create a dictionary that aligns HPO and MONDO
        concepts with other types of disease terminology identifiers. This is done by obtaining database
        cross-references (dbxrefs) for each ontology and then combining the results into a single large dictionary
        keyed by dbxrefs with MONDO and HP identifiers as values.

        Returns:
            mondo_dict: A dict where disease identifiers mapped to mondo are keys and mondo identifiers are values.
            hp_dict: A dict where disease identifiers mapped to hpo are keys and mondo identifiers are values.
        """

        # mondo data
        mondo_graph = Graph().parse(self.downloads_data_from_gcs_bucket('mondo_with_imports.owl'))
        dbxref_res = gets_ontology_class_dbxrefs(mondo_graph)[0]
        mondo_dict = {str(k).lower().split('/')[-1]: {str(v).split('/')[-1].replace('_', ':')}
                      for k, v in dbxref_res.items() if 'MONDO' in str(v)}

        # hp data
        hp_graph = Graph().parse(self.downloads_data_from_gcs_bucket('hp_with_imports.owl'))
        dbxref_res = gets_ontology_class_dbxrefs(hp_graph)[0]
        hp_dict = {str(k).lower().split('/')[-1]: {str(v).split('/')[-1].replace('_', ':')}
                   for k, v in dbxref_res.items() if 'HP' in str(v)}

        return mondo_dict, hp_dict

    def creates_disease_identifier_mappings(self) -> None:
        """Creates Human Phenotype Ontology (HPO) and MonDO Disease Ontology (MONDO) dbxRef maps and then uses them
        with the DisGEeNET UMLS disease mappings to create a master mapping between all disease identifiers to HPO
        and MONDO.

        Returns:
            None.
        """

        mondo_dict, hp_dict = self._preprocess_disease_mapping_data()
        data = self.reads_gcs_bucket_data_to_df('disease_mappings.tsv', '\t', 0, 0, None)
        data['vocabulary'], data['diseaseId'] = data['vocabulary'].str.lower(), data['diseaseId'].str.lower()
        data['vocabulary'] = ['doid' if x == 'do' else 'ordoid' if x == 'ordo' else x for x in data['vocabulary']]

        # get all CUIs found with HPO and MONDO
        ont_dict, disease_data_keep = {}, data.query('vocabulary == "hpo" | vocabulary == "mondo"')
        for idx, row in tqdm(disease_data_keep.iterrows(), total=disease_data_keep.shape[0]):
            if row['vocabulary'] == 'mondo':
                key, value = 'umls:' + row['diseaseId'], 'MONDO:' + row['code']
            else:
                key, value = 'umls:' + row['diseaseId'], row['code']
            if key in ont_dict.keys():
                ont_dict[key] |= {value}
            else:
                ont_dict[key] = {value}
        for key in tqdm(ont_dict.keys()):  # add ontology mappings from MONDO and HPO
            if key in mondo_dict.keys(): ont_dict[key] = set(list(ont_dict[key]) + list(mondo_dict[key]))
            if key in hp_dict.keys(): ont_dict[key] = set(list(ont_dict[key]) + list(hp_dict[key]))
        # get all rows for HPO/MONDO CUIs
        disease_dict, disease_data_other = {}, data[data.diseaseId.isin(disease_data_keep['diseaseId'])]
        for idx, row in tqdm(disease_data_other.iterrows(), total=disease_data_other.shape[0]):
            vocab, ids, code = row['vocabulary'], row['diseaseId'], row['code']
            if vocab == 'mondo' or vocab == 'hpo':
                key, value = 'umls:' + ids.lower(), code
                if key in disease_dict.keys():
                    disease_dict[key] |= {value}
                else:
                    disease_dict[key] = {value}
            else:
                if 'mondo' not in code or 'hp' not in code:
                    if ':' not in code:
                        key, value = vocab + ':' + code, ont_dict['umls:' + ids]
                    else:
                        key, value = code, ont_dict['umls:' + ids]
                    if key in disease_dict.keys():
                        disease_dict[key] |= value
                    else:
                        disease_dict[key] = value

        # save data and push to GCS bucket
        file1, file2 = 'DISEASE_MONDO_MAP.txt', 'PHENOTYPE_HPO_MAP.txt'
        with open(self.temp_dir + '/' + file1, 'w') as out1, open(self.temp_dir + '/' + file2, 'w') as out2:
            for k, v in tqdm({**disease_dict, **mondo_dict, **hp_dict}.items()):
                if any(x for x in v if x.startswith('MONDO')):
                    for idx in [x.replace(':', '_') for x in v if 'MONDO' in x]:
                        out1.write(k.upper() + '\t' + idx + '\n')
                if any(x for x in v if x.startswith('HP')):
                    for idx in [x.replace(':', '_') for x in v if 'HP' in x]:
                        out2.write(k.upper() + '\t' + idx + '\n')
        self.uploads_data_to_gcs_bucket(file1)
        self.uploads_data_to_gcs_bucket(file2)

        return None

    def extracts_hpa_tissue_information(self) -> pd.DataFrame:
        """Method reads in Human Protein Atlas (HPA) data and saves the columns, which contain the anatomical
        entities (i.e. cell types, cell lines, tissues, and fluids) that need manual alignment to ontologies. These
        data are not necessarily needed for every build, only when updating the ontology alignment mappings.

        Returns:
             hpa: A Pandas DataFrame object containing tissue data.
        """

        hpa = self.reads_gcs_bucket_data_to_df('proteinatlas_search.tsv', '\t', 0, 0, None)
        hpa.fillna('None', inplace=True)

        # write results
        with open(self.temp_dir + '/HPA_tissues.txt', 'w') as outfile:
            for x in tqdm(list(hpa.columns)):
                if x.endswith('[NX]'): outfile.write(x.split('RNA - ')[-1].split(' [NX]')[:-1][0] + '\n')
        self.uploads_data_to_gcs_bucket('HPA_tissues.txt')

        return hpa

    def _hpa_gtex_ontology_alignment(self) -> None:
        """Processes data to align Human Protein Atlas (HPA) and Genotype-Tissue Expression Project (GTEx) data to the
        Uber-Anatomy (UBERON), Cell Ontology (CL), and the Cell Line Ontology (CLO). The processed data is then
        written to a txt file and pushed to GCS.

        Returns:
            None.
        """

        mapping_data = self.reads_gcs_bucket_data_to_df('zooma_tissue_cell_mapping_04JAN2020.xlsx', '\t', 0, 0,
                                                        'Concept_Mapping - 04JAN2020')
        mapping_data.fillna('NA', inplace=True)

        # write data to useful format for pheknowlator and push to gcs
        filename = 'HPA_GTEx_TISSUE_CELL_MAP.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out:
            for idx, row in tqdm(mapping_data.iterrows(), total=mapping_data.shape[0]):
                if row['UBERON'] != 'NA': out.write(str(row['TERM']).strip() + '\t' + str(row['UBERON']).strip() + '\n')
                if row['CL ID'] != 'NA': out.write(str(row['TERM']).strip() + '\t' + str(row['CL ID']).strip() + '\n')
                if row['CLO ID'] != 'NA': out.write(str(row['TERM']).strip() + '\t' + str(row['CLO ID']).strip() + '\n')
        self.uploads_data_to_gcs_bucket(filename)

        return None

    def processes_hpa_gtex_data(self) -> None:
        """Method processes and combines gene expression experiment results from the Human protein Atlas (HPA) and the
        Genotype-Tissue Expression Project (GTEx). Additional details provided below on how each source are processed.
            - HPA: The HPA data is reformatted so all tissue, cell, cell lines, and fluid types are stored as a nested
              list. The anatomy type is specified as an item in the list according to its type.
            - GTEx: All protein-coding genes that appear in the HPA data set are removed. Then, only those non-coding
              genes with a median expression >= 1.0 are maintained. GTEx data are formatted such the anatomical
              entities are stored as columns and genes stored as rows, thus the expression filtering step is
              performed while also reformatting the file resulting in a nested list.

        Returns:
            None.
        """

        hpa = self._extracts_hpa_tissue_information()
        gtex = self.reads_gcs_bucket_data_to_df('GTEx_Analysis_*_RNASeQC*_gene_median_tpm.gct', '\t', 2, 0, None)
        gtex.fillna('None', inplace=True)
        gtex['Name'].replace('(\..*)', '', inplace=True, regex=True)

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
        with open(processed_data_location + '/' + filename, 'w') as out:
            for x in hpa_results + gtex_results:
                out.write(x[0] + '\t' + x[1] + '\t' + x[2] + '\t' + x[3] + '\t' + x[4] + '\t' + x[5] + '\n')
        self.uploads_data_to_gcs_bucket(filename)

        return None

    def _preprocess_pathway_mapping_data(self) -> Dict:
        """Method processes the Pathway Ontology (PW) data in order to create a dictionary that aligns PW concepts
        with other types of pathway identifiers.

        Returns:
             id_mappings: A dictionary containing mappings between the PW and other relevant ontologies.
        """

        pw_graph = Graph().parse(self.downloads_data_from_gcs_bucket('pw_with_imports.owl'))
        dbxref_res = gets_ontology_class_dbxrefs(pw_graph)[0]
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
            reactome: A dictionary mapping different pathway identifiers to the Pathway Ontology.
        """

        reactome_pathways = self.reads_gcs_bucket_data_to_df('ReactomePathways.txt', '\t', 0, None, None)
        reactome_pathways = reactome_pathways.loc[reactome_pathways[2].apply(lambda x: x == 'Homo sapiens')]
        reactome = {x: {'PW_0000001'} for x in set(list(reactome_pathways[0]))}

        # reactome - GO associations
        reactome_pathways2 = self.reads_gcs_bucket_data_to_df('gene_association.reactome', '\t', 1, None, None)
        reactome_pathways2 = reactome_pathways2.loc[reactome_pathways2[12].apply(lambda x: x == 'taxon:9606')]
        reactome.update({x.split(':')[-1]: {'PW_0000001'} for x in set(list(reactome_pathways2[5]))})

        # reactome CHEBI
        reactome_pathways3 = self.reads_gcs_bucket_data_to_df('ChEBI2Reactome_All_Levels.txt', '\t', 0, None, None)
        reactome_pathways3 = reactome_pathways3.loc[reactome_pathways3[5].apply(lambda x: x == 'Homo sapiens')]
        reactome.update({x: {'PW_0000001'} for x in set(list(reactome_pathways3[1]))})

        return reactome

    def _processes_compath_pathway_data(self, reactome: Dict, pw_dict: Dict) -> Dict:
        """Processes compath pathway mappings data, extending the input reactome dictionary by extending it to add
        mappings from KEGG to reactome and the Pathway Ontology (PW).

        Args:
            reactome: A dictionary mapping different pathway identifiers to the Pathway Ontology.
            pw_dict: A dictionary containing dbxref mappings to PW identifiers.

        Returns:
             reactome: An extended dictionary mapping different pathway identifiers to the Pathway Ontology.
        """

        compath = self.reads_gcs_bucket_data_to_df('compath_canonical_pathway_mappings.txt', '\t', 0, None, None)
        compath.fillna('None', inplace=True)
        for idx, row in tqdm(compath.iterrows(), total=compath.shape[0]):
            if row[6] == 'kegg' and 'kegg:' + row[5].strip('path:hsa') in pw_dict.keys() and row[2] == 'reactome':
                for x in pw_dict['kegg:' + row[5].strip('path:hsa')]:
                    if row[1] in reactome.keys():
                        reactome[row[1]] |= {x.split('/')[-1]}
                    else:
                        reactome[row[1]] = {x.split('/')[-1]}
            if (row[2] == 'kegg' and 'kegg:' + row[1].strip('path:hsa') in pw_dict.keys()) and row[6] == 'reactome':
                for x in pw_dict['kegg:' + row[1].strip('path:hsa')]:
                    if row[5] in reactome.keys():
                        reactome[row[5]] |= {x.split('/')[-1]}
                    else:
                        reactome[row[5]] = {x.split('/')[-1]}

        return reactome

    def _processes_kegg_pathway_data(self, reactome: Dict, pw_dict: Dict) -> Dict:
        """Processes KEGG-Reactome  data, extending the input reactome dictionary by extending it to add mappings from
        KEGG to reactome and the Pathway Ontology (PW).

        Args:
            reactome: A dictionary mapping different pathway identifiers to the Pathway Ontology.
            pw_dict: A dictionary containing dbxref mappings to PW identifiers.

        Returns:
             reactome: An extended dictionary mapping different pathway identifiers to the Pathway Ontology.
        """

        kegg_reactome_map = self.reads_gcs_bucket_data_to_df('kegg_reactome.csv', ',', 0, None, None)
        src, tar, tar_ids, src_ids = 'Source Resource', 'Target Resource', 'Target ID', 'Source ID'

        for idx, row in tqdm(kegg_reactome_map.iterrows(), total=kegg_reactome_map.shape[0]):
            if row[src] == 'reactome' and 'kegg:' + row[tar_ids].strip('path:hsa') in pw_dict.keys():
                for x in pw_dict['kegg:' + row[tar_ids].strip('path:hsa')]:
                    if row[src_ids] in reactome.keys():
                        reactome[row[src_ids]] |= {x.split('/')[-1]}
                    else:
                        reactome[row[src_ids]] = {x.split('/')[-1]}
            if row[tar] == 'reactome' and 'kegg:' + row[src].strip('path:hsa') in pw_dict.keys():
                for x in pw_dict['kegg:' + row[src_ids].strip('path:hsa')]:
                    if row[tar_ids] in reactome.keys():
                        reactome[row[tar_ids]] |= {x.split('/')[-1]}
                    else:
                        reactome[row[tar_ids]] = {x.split('/')[-1]}

        return reactome

    @staticmethod
    def _queries_reactome_api(reactome: Dict) -> Dict:
        """Runs a set of reactome identifiers against the reactome API in order to obtain mappings to the Gene
        Ontology, specifically to Biological Processes.

        Args:
            reactome: A dictionary mapping different pathway identifiers to the Pathway Ontology.

        Returns:
             reactome: An extended dictionary mapping different pathway identifiers to the Pathway Ontology.
        """

        print('Querying Reactome API to Obtain Gene Ontology Mappings')

        for request_ids in tqdm(list(chunks(list(reactome.keys()), 20))):
            result, key = content.query_ids(ids=','.join(request_ids)), 'goBiologicalProcess'
            if result is not None:
                for res in result:
                    if key in res.keys():
                        if res['stId'] in reactome.keys():
                            reactome[res['stId']] |= {'GO_' + res[key]['accession']}
                        else:
                            reactome[res['stId']] = {'GO_' + res[key]['accession']}

        return reactome

    def _creates_pathway_identifier_mappings(self) -> Dict:
        """Processes the canonical pathways and other kegg-reactome pathway mapping files from the ComPath
        Ecosystem in order to create the following identifier mappings: Reactome Pathway Identifiers ➞ KEGG Pathway
        Identifiers ➞ Pathway Ontology Identifiers.

        Returns:
            reactome: A dictionary mapping different types of pathway identifiers to sequence ontology classes.
        """

        pw_dict = self._preprocess_disease_mapping_data()
        reactome = self._processes_reactome_data()
        compath_reactome = self._processes_compath_pathway_data(reactome, pw_dict)
        kegg_reactome = self._processes_kegg_pathway_data(compath_reactome, pw_dict)
        reactome = self._queries_reactome_api(kegg_reactome)

        # write data
        filename = 'REACTOME_PW_GO_MAPPINGS.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out:
            for key in tqdm(reactome.keys()):
                for x in reactome[key]:
                    if x.startswith('PW') or x.startswith('GO'): out.write(key + '\t' + x + '\n')
        self.uploads_data_to_gcs_bucket(filename)

        return reactome

    def _preprocesses_gene_types(self, genomic_map: Dict) -> Dict:
        """Creates mappings between bio types for different gene identifiers to sequence ontology classes.

        Args:
            genomic_map: A dictionary containing mappings between gene identifier types and Sequence Ontology
                identifiers.

        Returns:
            sequence_map: A dictionary containing genomic identifiers as keys and Sequence Ontology classes as values.
        """

        gene_ids = pickle.load(open(self.temp_dir + '/Merged_gene_rna_protein_identifiers.pkl', 'rb'), encoding='bytes')
        sequence_map = {}
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
                if len(ensembl) > 0:
                    gene_type = genomic_map[ensembl[0].replace('ensembl_gene_type_', '') + '_Gene']
                elif len(hgnc) > 0:
                    gene_type = genomic_map[hgnc[0].replace('hgnc_gene_type_', '') + '_Gene']
                elif len(entrez) > 0:
                    gene_type = genomic_map[entrez[0].replace('entrez_gene_type_', '') + '_Gene']
                else:
                    gene_type = 'SO_0000704'
                # update sequence map
                if id_clean in sequence_map.keys():
                    sequence_map[id_clean] += [gene_type]
                else:
                    sequence_map[id_clean] = [gene_type]

        return sequence_map

    def _preprocesses_transcript_types(self, genomic_map: Dict, sequence_map: Dict) -> Dict:
        """Creates mappings between bio types for different transcript identifiers to sequence ontology classes.

        Args:
            genomic_map: A dictionary containing mappings between transcript identifier types and Sequence Ontology
                identifiers.
            sequence_map: A dict containing genomic identifiers as keys and Sequence Ontology classes as values.

        Returns:
            sequence_map: A dict containing genomic identifiers as keys and Sequence Ontology classes as values.
        """

        trans_data = self.reads_gcs_bucket_data_to_df('ensembl_identifier_data_cleaned.txt', '\t', 0, 0, None)
        trans, trans_id = {}, 'transcript_stable_id'
        for idx, row in tqdm(trans_data.iterrows(), total=trans_data.shape[0]):
            if row[trans_id] != 'None':
                if row[trans_id].replace('transcript_stable_id_', '') in trans.keys():
                    trans[row[trans_id].replace('transcript_stable_id_', '')] += [row['ensembl_transcript_type']]
                else:
                    trans[row[trans_id].replace('transcript_stable_id_', '')] = [row['ensembl_transcript_type']]
        # update SO map dictionary
        for ids in tqdm(transcripts.keys()):
            if trans[ids][0] == 'protein_coding':
                trans_type = genomic_map['protein-coding_Transcript']
            elif trans[ids][0] == 'misc_RNA':
                trans_type = genomic_map['miscRNA_Transcript']
            else:
                trans_type = genomic_map[list(set(trans[ids]))[0] + '_Transcript']
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

        variant_data, var = self.reads_gcs_bucket_data_to_df('variant_summary.txt', '\t', 0, 0, None), {}
        for idx, row in tqdm(variant_data.iterrows(), total=variant_data.shape[0]):
            if row['Assembly'] == 'GRCh38' and row['RS# (dbSNP)'] != -1:
                if 'rs' + str(row['RS# (dbSNP)']) in var.keys(): var['rs' + str(row['RS# (dbSNP)'])] |= {row['Type']}
                else: var['rs' + str(row['RS# (dbSNP)'])] = {row['Type']}
        # update SO map dictionary
        for identifier in tqdm(variants.keys()):
            for typ in variants[identifier]:
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

        mapping_data = self.reads_gcs_bucket_data_to_df('genomic_sequence_ontology_mappings.xlsx', '\t', 0, 0,
                                                        'GenomicType_SO_Map_09Mar2020')
        genomic_type_so_map = {}
        for idx, row in tqdm(mapping_data.iterrows(), total=mapping_data.shape[0]):
            genomic_type_so_map[row['source_*_type'] + '_' + row['Genomic']] = row['SO ID']

        # add genes, transcripts, and variants
        genomic_sequence_map = self._preprocesses_gene_types(genomic_type_so_map)
        trans_sequence_map = self._preprocesses_transcript_types(genomic_type_so_map, genomic_sequence_map)
        sequence_map = self._preprocesses_variant_types(genomic_type_so_map, trans_sequence_map)

        # output data
        filename = 'SO_GENE_TRANSCRIPT_VARIANT_TYPE_MAPPING.txt'
        with open(self.temp_dir + '/' + filename, 'w') as outfile:
            for key in tqdm(sequence_map.keys()):
                for map_type in sequence_map[key]:
                    outfile.write(key + '\t' + map_type + '\n')
        self.uploads_data_to_gcs_bucket(filename)

        return sequence_map

    def combines_pathway_and_sequence_ontology_dictionaries(self) -> None:
        """Combines the Pathway Ontology and Sequence Ontology dictionaries into a dictionary.

        Returns:
            None.
        """

        sequence_map = self._creates_sequence_identifier_mappings()
        reactome_map = self._creates_pathway_identifier_mappings()

        # combine genomic and pathway maps + iterate over pathway lists and combine them into a single dictionary
        sequence_map.update(reactome_map)
        subclass_mapping = {}
        for key in tqdm(sequence_map.keys()):
            subclass_mapping[key] = sequence_map[key]

        # save file and push to gcs
        filename = 'subclass_construction_map.pkl'
        pickle.dump(subclass_mapping, open(self.temp_dir + '/' + filename, 'wb'), protocol=4)
        self.uploads_data_to_gcs_bucket(filename)

        return None

    def processes_relation_ontology_data(self) -> None:
        """Processes the Relations Ontology (RO) in order to obtain all ObjectProperties and their inverse relations.
        Additionally, the method writes out a file of all of the labels for all relations.

        Returns:
             None.
        """

        ro_graph = Graph().parse(self.downloads_data_from_gcs_bucket('ro_with_imports.owl'))
        labs = {str(x[2]).lower(): str(x[0]) for x in ro_graph if '/RO_' in str(x[0]) and 'label' in str(x[1]).lower()}

        # identify relations and their inverses
        write_dir, filename1 = './resources/construction_approach/', 'INVERSE_RELATIONS.txt'
        with open(self.temp_dir + '/' + filename, 'w') as out1:
            out1.write('Relation' + '\t' + 'Inverse_Relation' + '\n')
            for s, p, o in ro_graph:
                if 'owl#inverseOf' in str(p) and ('RO' in str(s) and 'RO' in str(o)):
                    out1.write(str(s.split('/')[-1]) + '\t' + str(o.split('/')[-1]) + '\n')
                    out1.write(str(o.split('/')[-1]) + '\t' + str(s.split('/')[-1]) + '\n')
        self.uploads_data_to_gcs_bucket(filename1)

        # identify relation labels
        filename2 = 'INVERSE_RELATIONS.txt'
        with open(self.temp_dir + '/' + filename2, 'w') as out1:
            out1.write('Relation' + '\t' + 'Label' + '\n')
            for k, v in labs.items():
                out1.write(str(k).split('/')[-1] + '\t' + str(v) + '\n')
        self.uploads_data_to_gcs_bucket(filename2)

        return None

    def processes_clinvar_data(self) -> None:
        """Processes ClinVar data by performing light tidying and filtering.

        Returns:
            None.
        """

        clinvar_data = self.reads_gcs_bucket_data_to_df('variant_summary.txt', '\t', 0, 0, None)
        clinvar_data.fillna('None', inplace=True)

        # explode nested data
        explode_df_clinvar = explodes_data(clinvar_data.copy(), ['PhenotypeIDS'], ';')
        explode_df_clinvar = explodes_data(explode_df_clinvar.copy(), ['PhenotypeIDS'], ',')
        explode_df_clinvar['PhenotypeIDS'].replace('Orphanet:ORPHA', 'ORPHA:', inplace=True, regex=True)
        explode_df_clinvar['PhenotypeIDS'].replace('Human Phenotype Ontology:HP:', 'HP_', inplace=True, regex=True)

        # write data
        filename = 'CLINVAR_VARIANT_GENE_DISEASE_PHENOTYPE_EDGES.txt'
        explode_df_clinvar.to_csv(self.temp_dir + '/' + filename, header=True, sep='\t', encoding='utf-8', index=False)
        self.uploads_data_to_gcs_bucket(filename)

        return None

    def processes_cofactor_catalyst_data(self) -> None:
        """Processes uniprot-cofactor-catalyst.tab file from the Uniprot Knowledge Base in order to enable the building
        of protein-cofactor and protein-catalyst edges.

        Returns:
            None.
        """

        data = open(self.downloads_data_from_gcs_bucket('uniprot-cofactor-catalyst.tab')).readlines()

        # reformat data and write data
        filename1, filename2 = 'UNIPROT_PROTEIN_COFACTOR.txt', 'UNIPROT_PROTEIN_CATALYST.txt'
        with open(self.temp_dir + '/' + filename1, 'w') as out1, open(self.temp_dir + '/' + filename2, 'w') as out2:
            for line in tqdm(data):
                # get cofactors
                if 'CHEBI' in line.split('\t')[4]:
                    for i in line.split('\t')[4].split(';'):
                        chebi = i.split('[')[-1].replace(']', '').replace(':', '_')
                        out1.write('PR_' + line.split('\t')[3].strip(';') + '\t' + chebi + '\n')
                # get catalysts
                if 'CHEBI' in line.split('\t')[5]:
                    for j in line.split('\t')[5].split(';'):
                        chebi = j.split('[')[-1].replace(']', '').replace(':', '_')
                        out2.write('PR_' + line.split('\t')[3].strip(';') + '\t' + chebi + '\n')
        # push data to gsc bucket
        self.uploads_data_to_gcs_bucket(filename1)
        self.uploads_data_to_gcs_bucket(filename2)

        return None

    def _creates_gene_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each Entrez gene identifier present
        in the input data file.

        Returns:
            gene_metadata_dict: A dictionary containing metadata that's keyed by Entrez gene identifier and whose
                values is a dictionary containing label, description, and synonym information. For example:
                {{'1': {
                    'Label': 'A1BG',
                    'Description': "A1BG has locus group 'protein-coding' and is located on chromosome 19 (19q13.43).",
                    'Synonym': 'HEL-S-163pA|A1B|ABG|HYST2477alpha-1B-glycoprotein|GAB'}, ...}
        """

        data = self.reads_gcs_bucket_data_to_df('Homo_sapiens.gene_info', '\t', 0, 0, None)
        data = data.loc[data['#tax_id'].apply(lambda x: x == 9606)]
        data.fillna('None', inplace=True)
        data.replace('-', 'None', inplace=True, regex=False)

        # create metadata
        genes, lab, desc, syn = [], [], [], []
        for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
            gene_id, sym, defn, gene_type = row['GeneID'], row['Symbol'], row['description'], row['type_of_gene']
            chrom, map_loc, s1, s2 = row['chromosome'], row['map_location'], row['Synonyms'], row['Other_designations']
            if gene_id != 'None':
                genes.append(gene_id)
                if sym != 'None' or sym != '': lab.append(sym)
                else: lab.append('Entrez_ID:' + gene_id)
                if 'None' not in [defn, gene_type, chrom, map_loc]:
                    desc_str = "{} has locus group '{}' and is located on chromosome {} ({})."
                    desc.append(desc_str.format(sym, gene_type, chrom, map_loc))
                else:
                    desc.append("{} locus group '{}'.".format(sym, gene_type))
                if s1 != 'None' and s2 != 'None':
                    syn.append('|'.join(set([x for x in (s1 + s2).split('|') if x != 'None' or x != ''])))
                elif s1 != 'None': syn.append('|'.join(set([x for x in s1.split('|') if x != 'None' or x != ''])))
                elif s2 != 'None': syn.append('|'.join(set([x for x in s2.split('|') if x != 'None' or x != ''])))
                else: syn.append('None')

        # combine into new data frame
        metadata = pandas.DataFrame(list(zip(genes, lab, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
        metadata = metadata.astype(str)
        metadata.drop_duplicates(subset='ID', keep='first', inplace=True)
        # convert df to dictionary
        metadata.set_index('ID', inplace=True)
        gene_metadata_dict = metadata.to_dict('index')

        return gene_metadata_dict

    def _creates_transcript_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each Entrez gene identifier present
        in the input data file.

        Returns:
            rna_metadata_dict: A dictionary containing metadata that's keyed by Ensembl transcript identifier and whose
                values is a dictionary containing label, description, and synonym information. For example:
                {'ENST00000456328': {
                    'Label': 'DDX11L1-202',
                    'Description': "Transcript DDX11L1-202 is classified as type 'processed_transcript'.",
                    'Synonym': 'None'}, ...}
        """

        data = self.reads_gcs_bucket_data_to_df('ensembl_identifier_data_cleaned.txt', '\t', 0, 0, None)
        data = data.loc[data['transcript_stable_id'].apply(lambda x: x != 'None')]
        data.drop(['ensembl_gene_id', 'symbol', 'protein_stable_id', 'uniprot_id', 'master_transcript_type',
                   'entrez_id', 'ensembl_gene_type', 'master_gene_type', 'symbol'], axis=1, inplace=True)
        data.drop_duplicates(subset=['transcript_stable_id', 'transcript_name', 'ensembl_transcript_type'],
                             keep='first', inplace=True)
        data.fillna('None', inplace=True)

        # create metadata
        rna, lab, desc, syn = [], [], [], []
        for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
            rna_id, ent_type, nme = row['transcript_stable_id'], row['ensembl_transcript_type'], row['transcript_name']
            rna.append(rna_id)
            if nme != 'None': lab.append(nme)
            else:
                lab.append('Ensembl_Transcript_ID:' + rna_id)
                nme = 'Ensembl_Transcript_ID:' + rna_id
            if ent_type != 'None': desc.append("Transcript {} is classified as type '{}'.".format(nme, ent_type))
            else: desc.append('None')
            syn.append('None')

        # combine into new data frame
        metadata = pandas.DataFrame(list(zip(rna, lab, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
        metadata = metadata.astype(str)
        metadata.drop_duplicates(subset='ID', keep='first', inplace=True)
        # convert df to dictionary
        metadata.set_index('ID', inplace=True)
        rna_metadata_dict = metadata.to_dict('index')

        return rna_metadata_dict

    def _creates_variant_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each ClinVar variant identifier present
        in the input data file.

        Returns:
            variant_metadata_dict: A dictionary containing metadata that's keyed by ClinVar variant identifier and whose
                values is a dictionary containing label, description, and synonym information. For example:
                {{'rs141138948': {
                    'Label': 'NM_016042.4(EXOSC3):c.395A>C (p.Asp132Ala)',
                    'Description': "This variant is a germline;inherited;paternal;unknown single nucleotide variant on
                    chromosome 9 (NC_000009.12, start:37783993/stop:37783993 positions,cytogenetic location:9p13.2) and
                    has clinical significance 'Pathogenic/Likely pathogenic'. This entry is for the GRCh38 and was last
                    reviewed on Sep 30, 2020 with review status 'criteria provided, multiple submitters, no conflict'.",
                    'Synonym': 'None'}, ...}
        """

        data = self.reads_gcs_bucket_data_to_df('variant_summary.txt', '\t', 0, 0, None)
        data = data.loc[var_data['Assembly'].apply(lambda x: x == 'GRCh38')]
        data = data.loc[data['RS# (dbSNP)'].apply(lambda x: x != -1)]
        data = data[['#AlleleID', 'Type', 'Name', 'ClinicalSignificance', 'RS# (dbSNP)', 'Origin', 'Start', 'Stop',
                     'ChromosomeAccession', 'Chromosome',  'ReferenceAllele', 'Assembly', 'AlternateAllele',
                     'Cytogenetic', 'ReviewStatus', 'LastEvaluated']]
        var_metadata.replace('na', 'None', inplace=True)
        var_metadata.fillna('None', inplace=True)
        data.sort_values('LastEvaluated', ascending=False, inplace=True)
        data.drop_duplicates(subset='RS# (dbSNP)', keep='first', inplace=True)

        # create metadata
        variant, label, desc, syn = [], [], [], []
        for idx, row in tqdm(data.iterrows(), total=data.shape[0]):
            var_id, lab = row['RS# (dbSNP)'], row['Name']
            if var_id != 'None':
                variant.append('rs' + str(var_id))
                if lab != 'None': label.append(lab)
                else: label.append('dbSNP_ID:rs' + str(var_id))
                sent = "This variant is a {} {} on chromosome {} ({}, start:{}/stop:{} positions," + \
                       "cytogenetic location:{}) and has clinical significance '{}'." + \
                       "This entry is for the {} and was last reviewed on {} with review status '{}'."
                desc.append(
                    sent.format(row['Origin'], row['Type'], row['Chromosome'], row['ChromosomeAccession'], row['Start'],
                                row['Stop'], row['Cytogenetic'], row['ClinicalSignificance'], row['Assembly'],
                                row['LastEvaluated'], row['ReviewStatus']).replace('None', 'UNKNOWN'))
                syn.append('None')

        # combine into new data frame
        metadata = pandas.DataFrame(list(zip(var, label, desc, syn)), columns=['ID', 'Label', 'Description', 'Synonym'])
        metadata.drop_duplicates(subset=None, keep='first', inplace=True)
        metadata = metadata.astype(str)
        # convert df to dictionary
        metadata.set_index('ID', inplace=True)
        variant_metadata_dict = metadata.to_dict('index')

        return variant_metadata_dict

    def _creates_pathway_metadata_dict(self) -> Dict:
        """Creates a dictionary to store labels, synonyms, and a description for each Reactome Pathway identifier
        present in the input data file.

        Returns:
            pathway_metadata_dict: A dictionary containing metadata that's keyed by Reactome Pathway identifier and
            whose values is a dictionary containing label, description, and synonym information. For example:
                {{'R-HSA-157858': {
                    'Label': 'Gap junction trafficking and regulation',
                    'Description': 'None',
                    'Synonym': 'Gap junction trafficking and regulation'}}, ...}
        """

        data = self.reads_gcs_bucket_data_to_df('ReactomePathways.txt', '\t', 0, None, None)
        data = data.loc[data[2].apply(lambda x: x == 'Homo sapiens')]

        # get metadata
        nodes = set(list(data[0]))
        metadata = metadata_api_mapper(list(nodes))

        # convert df to dictionary
        metadata.set_index('ID', inplace=True)
        pathway_metadata_dict = metadata.to_dict('index')

        return pathway_metadata_dict

    def creates_non_ontology_class_metadata_dict(self) -> None:
        """Combines the gene metadata, transcript metadata, variant metadata, and pathway metadata dictionaries into a
        single large metadata dictionary. See the specific functions used below for examples of the output.

        Returns:
            master_metadata_dictionary: A dictionary keyed by Entrez gene identifier, Ensembl transcript identifier,
                and variant identifier with a dictionary as values containing labels, descriptions, and synonyms.
        """

        # create single dictionary of
        master_metadata_dictionary = {**self._creates_gene_metadata_dict(),
                                      **self._creates_transcript_metadata_dict(),
                                      **self._creates_variant_metadata_dict(),
                                      **self._creates_pathway_metadata_dict()}

        # save data and push to gcs bucket
        filename = 'node_metadata_dict.pkl'
        pickle.dump(master_metadata_dictionary, open(self.temp_dir + '/' + filename, 'wb'), protocol=4)
        self.uploads_data_to_gcs_bucket(filename)

        return master_metadata_dictionary

    def preprocesses_build_data(self) -> None:
        """Main method for class which calls orchestrates the complete data preprocessing workflow. All data is
        preprocessed, written locally, and pushed to the processed_data Google Cloud Storage bucket for the current
        build.

        Returns:
            None.
        """



        return None
