#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import pickle
import re
import shutil

from tqdm import tqdm

try:
    from pkt_kg.utils import data_downloader
except ModuleNotFoundError:
    import sys, os.path
    app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
    print(app_dir)
    sys.path.append(app_dir)
    from pkt_kg.utils import data_downloader


def main():

    # create a temp directory creating the mapping dictionary -- assumes that the 'data' directory already exists
    write_location1 = 'data/temp/'
    write_location2 = 'data/'
    if not os.path.exists(write_location1): os.mkdir(write_location1)
    print('Creating temporary directory: {}'.format(write_location1))

    # download the omop2obo mapping files from zenodo
    # omop2obo: https://github.com/callahantiff/OMOP2OBO
    # this is a temporary workaround -- users should check the Zenodo cite to make sure they have the latest files
    print('Downloading mapping data')
    data_urls = [
        'https://zenodo.org/record/6949688/files/OMOP2OBO_V1_Condition_Occurrence_Mapping_Oct2020.xlsx?download=1',
        'https://zenodo.org/record/6949696/files/OMOP2OBO_V1_Drug_Exposure_Mapping_Oct2020.xlsx?download=1'
    ]

    for url in data_urls:
        file_name = re.sub(r'\?.*', '', url.split('/')[-1])
        if not os.path.exists(write_location1 + file_name):
            data_downloader(url, write_location1, file_name)

    # read in the data files -- assumes only condition occurrence (hp and mondo) and drug exposure (chebi)
    print('Reading in mapping data from OMOP2OBO')
    # condition occurrence mappings
    print('Condition Occurrence Mappings')
    cond = 'OMOP2OBO_V1_Condition_Occurrence_Mapping_Oct2020.xlsx'
    co_hp = pd.read_excel(write_location1 + cond, sep=',', header=0, sheet_name="OMOP2OBO_HPO_Mapping_Results")
    co_mondo = pd.read_excel(write_location1 + cond, sep=',', header=0,
                             sheet_name="OMOP2OBO_Mondo_Mapping_Results")
    # drug exposure mappings
    print('Drug Exposure Mappings')
    drg = 'OMOP2OBO_V1_Drug_Exposure_Mapping_Oct2020.xlsx'
    de_chebi = pd.read_excel(write_location1 + drg, sep=',', header=0, sheet_name="OMOP2OBO_ChEBI_Mapping_Results")

    # create mapping dictionary -- in the future this part could be extended to allow for matches to ancestor concepts
    print('Creating concept mapping dictionary')
    mapping_dict = {}
    dfs = [co_hp, co_mondo, de_chebi]
    obos = ['HP', 'MONDO', 'CHEBI']

    for i in range(len(obos)):
        ont = obos[i]
        map_df = dfs[i][['CONCEPT_ID', 'CONCEPT_NAME', 'CONCEPT_CODE', 'ONTOLOGY_LOGIC', 'ONTOLOGY_URI', 'ONTOLOGY_LABEL']]
        for idx, row in tqdm(map_df.iterrows(), total=map_df.shape[0]):
            if ont in row['ONTOLOGY_URI']:
                concept_id = row['CONCEPT_ID']; concept_name = row['CONCEPT_NAME']; concept_code = row['CONCEPT_CODE']
                map_logic = row['ONTOLOGY_LOGIC']; ont_uri = row['ONTOLOGY_URI']; ont_label = row['ONTOLOGY_LABEL']
                if concept_id in mapping_dict.keys():
                    mapped_info = {ont: {'mapping_logic': map_logic, 'id': ont_uri, 'label': ont_label}}
                    mapping_dict[concept_id]['mapping(s)'].update(mapped_info)
                else:
                    mapping_dict[concept_id] = {
                        'concept_name': concept_name,
                        'mapping(s)': {ont: {'mapping_logic': map_logic, 'id': ont_uri, 'label': ont_label}}
                    }

    # write dictionary to data directory where other kg data are stored
    print('writing mapping dictionary to: {}'.format(write_location2))
    with open(write_location2 + 'omop2obo_mapping_dict.pkl', 'wb') as handle:
        pickle.dump(mapping_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # clean up directory -- delete temp directory
    print('Removing temp directory: {}'.format(write_location1))
    shutil.rmtree(write_location1)


if __name__ == '__main__':
    main()
