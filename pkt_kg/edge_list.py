#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import csv
import json
import pandas as pd  # type: ignore
import re

from difflib import SequenceMatcher
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, IO, List, Optional, TextIO, Tuple, Union

# TODO:
#  (1) using eval() to handle filtering of downloaded data, should consider replacing this in a future release.
#  (2) modify data_reader to stream/chunk large data files


class CreatesEdgeList(object):
    """Class creates edge lists based off data type.

    The class takes as input a string that represents the type of data being stored and a list of lists, where each item
    in the list is a file path/name of a data source. With these inputs, the class reads and processes the data and then
    assembles it into a dictionary where the keys are tuples storing an entity identifier and a relation that connects
    the entity to another entity, which is stored as a value. The class returns a dictionary, keyed by data source, that
    contains a dictionary of edge relations.

    Attributes:
        data_files: A list that contains the full file path and name of each downloaded data source.
        source_file: A string containing the filepath to resource information.
    """

    def __init__(self, data_files: Dict[str, str], source_file: str) -> None:

        self.data_files = data_files
        self.source_file = source_file
        self.source_info: Dict[str, Dict[str, Any]] = dict()

        with open(source_file, 'r') as source_file_data:
            for row in source_file_data.read().splitlines():
                cols = ['"{}"'.format(x.strip()) for x in list(csv.reader([row], delimiter='|', quotechar='"'))[0]]
                key = cols[0].strip('"').strip("'")
                self.source_info[key] = {}
                self.source_info[key]['source_labels'] = cols[1].strip('"').strip("'")
                self.source_info[key]['data_type'] = cols[2].strip('"').strip("'")
                self.source_info[key]['edge_relation'] = cols[3].strip('"').strip("'")
                self.source_info[key]['uri'] = (cols[4].strip('"').strip("'"), cols[5].strip('"').strip("'"))
                self.source_info[key]['delimiter'] = cols[6].strip('"').strip("'")
                self.source_info[key]['column_idx'] = cols[7].strip('"').strip("'")
                self.source_info[key]['identifier_maps'] = cols[8].strip('"').strip("'")
                self.source_info[key]['evidence_criteria'] = cols[9].strip('"').strip("'")
                self.source_info[key]['filter_criteria'] = cols[10].strip('"').strip("'")
                self.source_info[key]['edge_list'] = []

        source_file_data.close()

    @staticmethod
    def identify_header(file_path: str, delimiter: str, skip_rows: List[int]) -> Optional[int]:
        """Compares the similarity of the first line of a Pandas DataFrame to the column headers when read in with and
        without a header to determine whether or not the data frame should be built with a header or not. This
        function was modified from a Stack Overflow post: https://stackoverflow.com/a/40193509

        Args:
            file_path: A filepath to a data file.
            delimiter: A character specifying how the rows of the data are delimited.
            skip_rows: A list of indices to skip when reading in the data.

        Returns:
            - 0, if the data should be read in with a header else None.
        """

        df_with_header = pd.read_csv(file_path, header='infer', nrows=1, delimiter=delimiter, skiprows=skip_rows)
        df_without_header = pd.read_csv(file_path, header=None, nrows=1, delimiter=delimiter, skiprows=skip_rows)

        # calculate similarity between header and first row
        with_header_test = SequenceMatcher(None, '|'.join([str(x) for x in list(df_with_header.iloc[0])]),
                                           '|'.join([str(x) for x in list(df_with_header)])).ratio()
        without_header_test = SequenceMatcher(None, '|'.join([str(x) for x in list(df_without_header.iloc[0])]),
                                              '|'.join([str(x) for x in list(df_without_header)])).ratio()
        # determine if header should be used
        if abs(with_header_test-without_header_test) < 0.5: return 0
        else: return None

    def data_reader(self, file_path: str, delimiter: str = 't') -> pd.DataFrame:
        """Takes a filepath pointing to data source and reads it into a Pandas DataFrame using information in the file
        and line splitter variables.

        Args:
            file_path: A Filepath to data.
            delimiter: A Character used to split rows into columns.

        Return:
            A Pandas DataFrame containing the data from the data_filepath.

        Raises:
            Exception: If the Pandas DataFrame does not contain at least 2 columns and more than 10 rows.
        """

        with open(file_path, 'r') as input_data_r:  # type: IO[Any]
            data = input_data_r.read().splitlines()
        input_data_r.close()
        # try:
        #     with open(file_path, 'r') as input_data_r:  # type: IO[Any]
        #         data = input_data_r.read().splitlines()
        #     input_data_r.close()
        # except ValueError:
        #     with open(file_path, 'rb') as input_data_rb:  # type: IO[Any]
        #         data = input_data_rb.read().decode('utf-8').splitlines()  # decode bytes to strings
        #     input_data_rb.close()

        # clean up data to only keep valid rows (rows that are not empty space or metadata)
        splitter = '\t' if 't' in delimiter else r"\s+" if '' in delimiter else delimiter
        if delimiter == '' or delimiter == ' ':
            skip = [row for row in range(0, len(data)) if delimiter not in data[row]]
        else:
            skip = [row for row in range(0, len(data)) if splitter not in data[row]]
        # determine if file contains a header
        header = self.identify_header(file_path, splitter, skip)
        edge_data = pd.read_csv(file_path, header=header, delimiter=splitter, low_memory=False, skiprows=skip)
        # clean environment and return verified data
        del data, skip
        return edge_data.fillna('None', inplace=False)

    @staticmethod
    def filter_fixer(criteria):
        """Processes empty strings by converting them to None.

        Args:
            criteria: A '::' delimited string; each delimited item is a set of filtering or evidence criteria.

        Returns:
            A string where empty strings have been replaced with "None".
        """

        if '(' in criteria:
            return criteria
        else:
            # replace space with empty string and then replace empty strings at end of criteria with 'None'
            no_spaces = re.sub(r"\'\s+|\"\s+", '', criteria)
            fix_string = ';'.join([re.sub('^(?![\\s\\S])', x, 'None') if x == '' else x for x in no_spaces.split(';')])

            return fix_string

    def filter_data(self, edge_data: pd.DataFrame, filter_criteria: str, evidence_criteria: str) -> pd.DataFrame:
        """Applies a set of filtering and/or evidence criteria to specific columns in a Pandas DataFrame and returns a
        filtered data frame.

        Args:
            edge_data: A Pandas DataFrame.
            filter_criteria: A '::' delimited string; each delimited item is a set of filtering criteria.
            evidence_criteria: A '::' delimited string; each delimited item is a set of mapping criteria.

        Returns:
            edge_data: A filtered Pandas DataFrame.

        Raises:
            Exception: If the Pandas DataFrame does not contain at least 2 columns and more than 10 rows.
        """

        if filter_criteria == 'None' and evidence_criteria == 'None':
            return edge_data
        else:  # fix known errors when filtering empty cells
            map_filter_criteria = self.filter_fixer(filter_criteria) + '::' + self.filter_fixer(evidence_criteria)
            for crit in [x for x in map_filter_criteria.split('::') if x != 'None']:
                # check if argument is to deduplicate data
                if crit.split(';')[1] == 'dedup':
                    sort_col = list(edge_data)[int(crit.split(';')[0].split('-')[0])]
                    filter_col = list(edge_data)[int(crit.split(';')[0].split('-')[1])]
                    sort_dir = [True if crit.split(';')[-1].lower() == 'asc' else False][0]
                    edge_data.sort_values(sort_col, ascending=sort_dir, inplace=True)
                    edge_data.drop_duplicates(subset=filter_col, keep='first', inplace=True)
                else:
                    col = list(edge_data)[int(crit.split(';')[0])]
                    try:
                        if type(float(crit.split(';')[2])) is float or type(int(crit.split(';')[2])) is int:
                            edge_data[col] = edge_data[col].apply(lambda x: 0 if x == 'None' else x)
                            if type(float(crit.split(';')[2])) is float: edge_data[col] = edge_data[col].astype(float)
                            else: edge_data[col] = edge_data[col].astype(int)
                            exp = '{} {} {}'.format('x', crit.split(';')[1], crit.split(';')[2])
                    except ValueError:
                        if crit.split(';')[2] == '' and '(' in crit.split(';')[1]:
                            exp = '{}{}'.format('x', crit.split(';')[1])
                        elif '(' in crit.split(';')[2] or '[' in crit.split(';')[2]:
                            exp = '{} {} {}'.format('x', crit.split(';')[1], crit.split(';')[2].replace("'", ''))
                        else:
                            exp = '{} {} "{}"'.format('x', crit.split(';')[1], crit.split(';')[2].replace("'", ''))
                    edge_data = edge_data.loc[edge_data[col].apply(lambda x: eval(exp))]

            return edge_data

    @staticmethod
    def data_reducer(cols: str, edge_data: pd.DataFrame) -> pd.DataFrame:
        """Reduces a Pandas DataFrame to the 2 columns specified by resource_info.txt. Prior to returning the data, the
        function checks the data type of each column in the reduced Pandas DataFrame to make sure that neither column is
        of type float.

        Args:
            cols: A ';'-delimited string containing column indices (e.g. 0;3 - which maps to columns 0 and 3).
            edge_data: A Pandas DataFrame.

        Returns:
            A Pandas DataFrame that consists of the two columns provided by the 'col' variable.
        """

        edge_data = edge_data[[list(edge_data)[int(cols.split(';')[0])], list(edge_data)[int(cols.split(';')[1])]]]
        edge_data = edge_data.drop_duplicates(subset=None, keep='first', inplace=False)
        # make sure neither column is float
        for x in list(edge_data):
            if 'float' in str(edge_data[x].dtype): edge_data[x] = edge_data[x].astype(int)

        return edge_data

    @staticmethod
    def label_formatter(edge_data: pd.DataFrame, label_criteria: str) -> pd.DataFrame:
        """Applies criteria to reformat edge data labels.

        Args:
            edge_data: A Pandas DataFrame containing a column for each node in the edge
            label_criteria: A ';' delimited string containing 3 arguments:
                1 - string splitter
                2 - string to append to subject node
                3 - string to append to object node

        Returns:
            edge_data: A Pandas DataFrame with updated value labels.
        """

        cut = label_criteria.split(';')[0]
        for col in range(0, len(label_criteria.split(';')[1:])):
            formatter, col_to_check = label_criteria.split(';')[col + 1], edge_data[list(edge_data)[col]].astype(str)
            if (cut == '' and formatter != '') or not any(i for i in list(col_to_check) if cut in i):
                edge_data[list(edge_data)[col]] = edge_data[list(edge_data)[col]].apply(lambda x: formatter + str(x))
            elif cut != '' and formatter != '':
                edge_data[list(edge_data)[col]].replace('(^.*{})'.format(cut), formatter, inplace=True, regex=True)
            elif cut != '' and formatter == '':
                edge_data[list(edge_data)[col]].replace('(^.*{})'.format(cut), formatter, inplace=True, regex=True)
            else:
                pass

        return edge_data

    def data_merger(self, node: int, mapping_data: str, edge_data: pd.DataFrame) -> List[Union[str, pd.DataFrame]]:
        """Processes a string that contains instructions for mapping a column in the edge_data Pandas DataFrame. This
        function assumes that the mapping data pointed to contains two columns: (1) identifier in edge_data to be
        mapped and (2) the desired identifier to map to. If one of the columns does not need to be mapped to an
        identifier then the original node's column is used for the final merge.

        Args:
            node: A column integer.
            mapping_data: A ';' delimited string containing information on identifier mapping data. Each item
                contains an index of an edge_data column and a filepath to an identifier mapping data set:
                    '0:./filepath/mapping_data_0.txt;1:./filepath/mapping_data_1.txt'
            edge_data: A Pandas DataFrame row containing two columns of identifiers.

        Returns:
            A nested list containing:
                1 - column that needs mapping
                2 - a Pandas DataFrame containing the merged data
        """

        # check if node needs to be mapped to an outside data source
        if str(node) in re.sub('(?:(?!:)\\D)*', '', mapping_data).split(':'):  # MAPPING TO OUTSIDE DATA SOURCE
            node2map = list(edge_data)[node]
            try:
                map_data = self.data_reader(mapping_data.split(';')[node].split(':')[1]).astype(str)
            except IndexError:
                map_data = self.data_reader(mapping_data.split(';')[0].split(':')[1]).astype(str)
            # process mapping data
            map_col = list(map_data)[0]
            col_to_map = str(node2map) + '_' + str(map_col) + '_mapped'
            map_data.rename(columns={list(map_data)[1]: str(col_to_map)}, inplace=True)
            try:
                merged_data = pd.merge(edge_data, map_data, left_on=node2map, right_on=map_col, how='inner')
            except ValueError:
                # update map_data merge col to match edge_data merge col type
                edge_data[node2map], map_data[map_col] = edge_data[node2map].astype(str), map_data[map_col].astype(str)
                merged_data = pd.merge(edge_data, map_data, left_on=node2map, right_on=map_col, how='inner')
            # drop all columns but merge key and value columns
            merged_data = merged_data[[list(edge_data)[0], list(edge_data)[1], col_to_map]]
        else:   # NOT MAPPING TO OUTSIDE DATA SOURCE
            col_to_map = str(list(edge_data)[node]) + '_mapped'
            edge_data[col_to_map] = edge_data[[list(edge_data)[node]]]
            merged_data = edge_data[[list(edge_data)[0], list(edge_data)[1], col_to_map]]

        return [col_to_map, merged_data]

    def process_mapping_data(self, mapping_data: str, edge_data: pd.DataFrame) -> Tuple[Tuple[Any, Any], ...]:
        """Merges two mapped Pandas DataFrames into a single DataFrame. After merging the DataFrames, the function
        removes all columns except the the mapped columns and removes any duplicate rows.

        Args:
            mapping_data: A ';' delimited string containing information on identifier mapping data. Each item
                contains an index of an edge_data column and a filepath to an identifier mapping data set:
                    '0:./filepath/mapping_data_0.txt;1:./filepath/mapping_data_1.txt'
            edge_data: A Pandas DataFrame row containing two columns of identifiers.

        Returns:
            A tuple of tuples, where each tuple contains a mapped identifier from each node column in the edge_data
            Pandas DataFrame. For example: [['CHEBI_24505', 'R-HSA-1006173'], ['CHEBI_28879', 'R-HSA-1006173']]
        """

        if mapping_data == 'None':
            edge_data = edge_data.astype(str)
            return tuple(zip(list(edge_data[list(edge_data)[0]]), list(edge_data[list(edge_data)[1]])))
        else:
            # merge edge data with referenced mapping data
            maps = [self.data_merger(node, mapping_data, edge_data) for node in range(2)]
            # merge mapping data merge result DataFrames
            merged_cols = list(set(maps[0][1]).intersection(set(maps[1][1])))
            merged_data = pd.merge(maps[0][1].astype(str),  # type: ignore
                                   maps[1][1].astype(str),  # type: ignore
                                   left_on=merged_cols, right_on=merged_cols, how='inner')
            keep_cols = [x for x in merged_data.columns if 'mapped' in str(x)]  # remove unwanted columns
            merged_data = merged_data[keep_cols].drop_duplicates(subset=None, keep='first', inplace=False)

            return tuple(zip(list(merged_data[maps[0][0]]), list(merged_data[maps[1][0]])))

    def gets_entity_namespaces(self) -> None:
        """Identifies namespaces for all non-ontology entities. This is achieved by adding an entity_namespace key to
        the source_info dictionary, which contains a sub-dictionary keyed by edge entity with it's associated URL as
        the value. For example:
            source_info['entity_namespaces']: {'gene': 'http://www.ncbi.nlm.nih.gov/gene/',
                                               'pathway': 'https://reactome.org/content/detail/',
                                               'rna': 'https://uswest.ensembl.org/Homo_sapiens/Transcript/Summary?t=',
                                               'variant': 'https://www.ncbi.nlm.nih.gov/snp/'}

        Returns:
            None.
        """

        self.source_info['entity_namespaces'] = {}
        for key in [x for x in self.source_info.keys() if x != 'entity_namespaces']:
            data_type, uri = self.source_info[key]['data_type'].split('-'), self.source_info[key]['uri']
            if data_type != ['class', 'class']:
                entities = [key.split('-')[data_type.index(x)] for x in data_type if x == 'entity']
                namespaces = [uri[data_type.index(x)] for x in data_type if x == 'entity']
                for x in zip(entities, namespaces):
                    self.source_info['entity_namespaces'][x[0]] = x[1]

        return None

    def creates_knowledge_graph_edges(self) -> None:
        """Generates edge lists for each edge type in an input dictionary. In order to generate the edge list,
        the function performs six steps: (1) read in data; (2) apply filtering and evidence criteria; (3) reduce data
        to specific columns, remove duplicates, and ensure proper formatting of column data; (4) update node column
        values; (5) rename nodes; and (6) map identifiers.

        Returns:
            source_info: A dictionary that contains all of the master information for each edge type resource. For
                example: {'chemical-complex': {'source_labels': ';;', 'data_type': 'class-entity',
                                               'edge_relation': 'RO_0002436', 'uri': ['http://ex/', 'https://ex/'],
                                               'delimiter': 't', 'column_idx': '0;1', 'identifier_maps': 'None',
                                               'evidence_criteria': 'None', 'filter_criteria': 'None',
                                               'edge_list': [['CHEBI_24505', 'R-HSA-1006173'], ...]}}
        """

        for edge_type in tqdm(self.source_info.keys()):
            print('\n\n### Processing Edge: {}'.format(edge_type))

            # STEP 1: Read Data
            print('*** Reading Edge Data ***')
            edge_data = self.data_reader(self.data_files[edge_type], self.source_info[edge_type][
                'delimiter'])

            # STEP 2: Apply Filtering and Evidence Criteria
            print('*** Applying Filtering and/or Mapping Criteria to Edge Data ***')
            edge_data = self.filter_data(edge_data,
                                         self.source_info[edge_type]['filter_criteria'],
                                         self.source_info[edge_type]['evidence_criteria'])

            # STEP 3: reduce data to specific columns, remove duplicates, and ensure proper formatting of column data
            edge_data = self.data_reducer(self.source_info[edge_type]['column_idx'], edge_data)

            # STEP 4: Update Node Column Values
            print('*** Reformatting Node Values ***')
            edge_data = self.label_formatter(edge_data, self.source_info[edge_type]['source_labels'])

            # STEP 5: Rename Nodes
            edge_data.rename(columns={list(edge_data)[0]: str(list(edge_data)[0]) + '-' + edge_type.split('-')[0],
                                      list(edge_data)[1]: str(list(edge_data)[1]) + '-' + edge_type.split('-')[1]},
                             inplace=True)

            # STEP 6: Map Identifiers
            print('*** Performing Identifier Mapping ***')
            mapped_data = self.process_mapping_data(self.source_info[edge_type]['identifier_maps'], edge_data)
            self.source_info[edge_type]['edge_list'] = [edge for edge in mapped_data if 'None' not in edge]

            # print Edge Statistics
            unique_edges = [list(y) for y in set([tuple(x) for x in self.source_info[edge_type]['edge_list']])]
            print('\nPROCESSED EDGE: {}'.format(edge_type))
            print('{}: Unique Node Count = {}'.format(edge_type.split('-')[0], len(set([x[0] for x in unique_edges]))))
            print('{}: Unique Node Count = {}'.format(edge_type.split('-')[1], len(set([x[1] for x in unique_edges]))))
            print('Total Unique Edge Count: {}'.format(len(unique_edges)))
            print('Total Unique Edge Count + Inverses: {}'.format(len(unique_edges)*2))

        # add source entity namespaces
        self.gets_entity_namespaces()

        # save a copy of the final master edge list
        with open('/'.join(self.source_file.split('/')[:-1]) + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(self.source_info, filepath)

        return None
