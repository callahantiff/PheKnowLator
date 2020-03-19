#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import csv
import json
import pandas  # type: ignore
import re

from difflib import SequenceMatcher
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Optional, TextIO, Tuple, Union

# TODO: using eval() to handle filtering of downloaded data, should consider replacing this in a future release.


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
        source_info: A nested dictionary that contains information about each edge-type. Keys are the edge-type (e.g.
            'chemical-gene' and values are a dictionary with keys for all of the information provided in the
            resource_info.txt file, which is used to process and generate the data. Additionally, this information
            also includes the type of edge (e.g. class, instance or subclass) and a nested edge list. For additional
            information and an example, see the creates_knowledge_graph_edges() method.

    """

    def __init__(self, data_files: Dict[str, str], source_file: str) -> None:

        self.data_files = data_files
        self.source_file = source_file

        # convert edge data to a dictionary
        self.source_info: Dict[str, Dict[str, Any]] = dict()
        source_file_data: TextIO = open(source_file)

        for row in source_file_data.read().split('\n'):
            cols = ['"{}"'.format(x.strip()) for x in list(csv.reader([row], delimiter='|', quotechar='"'))[0]]
            self.source_info[cols[0].strip('"').strip("'")] = {}
            self.source_info[cols[0].strip('"').strip("'")]['source_labels'] = cols[1].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['data_type'] = cols[2].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['edge_relation'] = cols[3].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['uri'] = (cols[4].strip('"').strip("'"),
                                                                      cols[5].strip('"').strip("'"))
            self.source_info[cols[0].strip('"').strip("'")]['row_splitter'] = cols[6].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['column_splitter'] = cols[7].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['column_idx'] = cols[8].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['identifier_maps'] = cols[9].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['evidence_criteria'] = cols[10].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['filter_criteria'] = cols[11].strip('"').strip("'")
            self.source_info[cols[0].strip('"').strip("'")]['edge_list'] = []

        source_file_data.close()

    @staticmethod
    def identify_header(file_path: str, file_delimiter: str) -> Optional[int]:
        """Compares the similarity of the first line of a Pandas DataFrame to the column headers when read in with and
        without a header to determine whether or not the data frame should be built with a header or not. This
        function was modified from a Stack Overflow post:
        https://stackoverflow.com/questions/40193388/how-to-check-if-a-csv-has-a-header-using-python/40193509

        Args:
            file_path: A filepath to a data file.
            file_delimiter: A character specifying how the rows of the data are delimited.

        Returns:
            - 0, if the data should be read in with a header.
            - None, if the data should be read in without a header.
        """

        # read in data
        df_with_header = pandas.read_csv(file_path, header='infer', nrows=1, delimiter=file_delimiter)
        df_without_header = pandas.read_csv(file_path, header=None, nrows=1, delimiter=file_delimiter)

        # calculate similarity between header and first row
        with_header_test = SequenceMatcher(None, '|'.join([str(x) for x in list(df_with_header.iloc[0])]),
                                           '|'.join([str(x) for x in list(df_with_header)])).ratio()

        without_header_test = SequenceMatcher(None, '|'.join([str(x) for x in list(df_without_header.iloc[0])]),
                                              '|'.join([str(x) for x in list(df_without_header)])).ratio()

        # determine if header should be used
        if abs(with_header_test-without_header_test) < 0.05:
            return 0
        elif with_header_test >= without_header_test:
            return None
        else:
            return None

    def data_reader(self, file_path: str, file_splitter: str = '\n', line_splitter: str = 't') -> pandas.DataFrame:
        """Takes a filepath pointing to data source and reads it into a Pandas DataFrame using information in the
        file and line splitter variables.

        Args:
            file_path: A Filepath to data.
            file_splitter: A Character to split data, used when a file contains metadata information.
            line_splitter: A Character used to split rows into columns.

        Return:
            A Pandas DataFrame containing the data from the data_filepath.

        Raises:
            Exception: If the Pandas DataFrame does not contain at least 2 columns and more than 10 rows.
        """

        # identify line splitter
        splitter = '\t' if 't' in line_splitter else " " if ' ' in line_splitter else line_splitter

        if '!' in file_splitter or '#' in file_splitter:
            # ASSUMPTION: assumes data is read in without a header
            with open(file_path) as file_data:
                decoded_data = file_data.read().split(file_splitter)[-1]
            file_data.close()
            edge_data = pandas.DataFrame([x.split(splitter) for x in decoded_data.split('\n') if x != ''])
        else:
            # determine if file contains a header
            header = self.identify_header(file_path, splitter)
            edge_data = pandas.read_csv(file_path, header=header, delimiter=splitter, low_memory=False)

        if len(list(edge_data)) >= 2 and len(edge_data) > 10:
            return edge_data.dropna(inplace=False)
        else:
            raise ValueError('ERROR: Data could not be properly read in')

    @staticmethod
    def filter_data(edge_data: pandas.DataFrame, filter_criteria: str, evidence_criteria: str) -> pandas.DataFrame:
        """Applies a set of filtering and/or evidence criteria to specific columns in a Pandas DataFrame and returns a
        filtered data frame.

        Args:
            edge_data: A Pandas DataFrame.
            filter_criteria: A '::' delimited string; each delimited item is a set of filtering criteria.
            evidence_criteria: A '::' delimited string; each delimited item is a set of mapping criteria.

        Returns:
            edge_data: A filtered Pandas DataFrame.

        Raises:
            Exception: If the size of the Pandas DataFrame is the same before and after applying evidence and/or
                filtering criteria.
            Exception: If the Pandas DataFrame does not contain at least 2 columns and more than 10 rows.
        """

        if filter_criteria == 'None' and evidence_criteria == 'None':
            return edge_data
        else:
            map_filter_criteria = filter_criteria + '::' + evidence_criteria
            edge_data_filt = None

            for crit in [x for x in map_filter_criteria.split('::') if x != 'None']:
                # check if argument is to deduplicate data
                if crit.split(';')[1] == 'dedup':
                    sort_col = list(edge_data)[int(crit.split(';')[0].split('-')[0])]
                    filter_col = list(edge_data)[int(crit.split(';')[0].split('-')[1])]

                    # sort data
                    sort_dir = [True if crit.split(';')[-1].lower() == 'asc' else False][0]
                    edge_data.sort_values(sort_col, ascending=sort_dir, inplace=True)
                    edge_data.drop_duplicates(subset=filter_col, keep='first', inplace=True)
                else:
                    col = list(edge_data)[int(crit.split(';')[0])]
                    try:
                        if type(float(crit.split(';')[2])) is float or type(int(crit.split(';')[2])) is int:
                            if type(float(crit.split(';')[2])) is float:
                                edge_data[col] = edge_data[col].astype(float)
                            else:
                                edge_data[col] = edge_data[col].astype(int)

                            exp = '{} {} {}'.format('x', crit.split(';')[1], crit.split(';')[2])

                    except ValueError:
                        if crit.split(';')[2] == '' and '(' in crit.split(';')[1]:
                            exp = '{}{}'.format('x', crit.split(';')[1])
                        elif '(' in crit.split(';')[2] or '[' in crit.split(';')[2]:
                            exp = '{} {} {}'.format('x', crit.split(';')[1], crit.split(';')[2].replace("'", ''))
                        else:
                            exp = '{} {} "{}"'.format('x', crit.split(';')[1], crit.split(';')[2].replace("'", ''))

                    edge_data_filt = edge_data.loc[edge_data[col].apply(lambda x: eval(exp))]

            if len(edge_data) == len(edge_data_filt):
                raise Exception('ERROR: Filtering and/or Evidence criteria were not applied')
            elif len(list(edge_data_filt)) >= 2 and len(edge_data_filt) >= 1:
                return edge_data_filt
            else:
                raise Exception('ERROR: Data could not be properly read in')

    @staticmethod
    def data_reducer(cols: str, edge_data: pandas.DataFrame) -> pandas.DataFrame:
        """Reduces a Pandas DataFrame to the 2 columns specified by resource_info.txt. Prior to returning the data, the
        function checks the data type of each column in the reduced Pandas DataFrame to make sure that neither column is
        of type float.

        Args:
            cols: A ';'-delimited string containing column indices (e.g. 0;3 - which maps to columns 0 and 3).
            edge_data: A Pandas DataFrame.

        Returns:
            A Pandas.DataFrame that consists of the two columns provided by the 'col' variable.
        """

        edge_data = edge_data[[list(edge_data)[int(cols.split(';')[0])], list(edge_data)[int(cols.split(';')[1])]]]
        edge_data = edge_data.drop_duplicates(subset=None, keep='first', inplace=False)

        # make sure neither column is float
        for x in list(edge_data):
            if 'float' in str(edge_data[x].dtype):
                edge_data[x] = edge_data[x].astype(int)

        return edge_data

    @staticmethod
    def label_formatter(edge_data: pandas.DataFrame, label_criteria: str) -> pandas.DataFrame:
        """Applies criteria to reformat edge data labels.

        Args:
            edge_data: A Pandas DataFrame containing a column for each node in the edge
            label_criteria: A ';' delimited string containing 3 arguments:
                1 - string splitter
                2 - string to append to subject node
                3 - string to append to object node

        Returns:
            edge_data: A Pandas.DataFrame with updated value labels.
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

    def data_merger(self, node: int, mapping_data: str, edge_data: pandas.DataFrame) ->\
            List[Union[str, pandas.DataFrame]]:
        """Processes a string that contains instructions for mapping a column in the edge_data Pandas DataFrame.

        This function assumes that the mapping data pointed to contains two columns: (1) identifier in edge_data to be
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
        if str(node) in re.sub('(?:(?!:)\\D)*', '', mapping_data).split(':'):
            node2map = list(edge_data)[node]

            # MAPPING TO OUTSIDE DATA SOURCE
            try:
                map_data = self.data_reader(mapping_data.split(';')[node].split(':')[1])
            except IndexError:
                map_data = self.data_reader(mapping_data.split(';')[0].split(':')[1])

            # process mapping data
            map_col = list(map_data)[0]
            col_to_map = str(node2map) + '_' + str(map_col) + '_mapped'
            map_data.rename(columns={list(map_data)[1]: str(col_to_map)}, inplace=True)

            try:
                merged_data = pandas.merge(edge_data, map_data, left_on=node2map, right_on=map_col, how='inner')
            except ValueError:
                # update map_data merge col to match edge_data merge col type
                edge_data[node2map], map_data[map_col] = edge_data[node2map].astype(str), map_data[map_col].astype(str)
                merged_data = pandas.merge(edge_data, map_data, left_on=node2map, right_on=map_col, how='inner')

            # drop all columns but merge key and value columns
            merged_data = merged_data[[list(edge_data)[0], list(edge_data)[1], col_to_map]]

        # NOT MAPPING TO OUTSIDE DATA SOURCE
        else:
            col_to_map = str(list(edge_data)[node]) + '_mapped'
            edge_data[col_to_map] = edge_data[[list(edge_data)[node]]]
            merged_data = edge_data[[list(edge_data)[0], list(edge_data)[1], col_to_map]]

        return [col_to_map, merged_data]

    def process_mapping_data(self, mapping_data: str, edge_data: pandas.DataFrame) -> Tuple[Tuple[Any, Any], ...]:
        """Merges two mapped Pandas DataFrames into a single DataFrame. After merging the DataFrames, the function
        removes all columns except the the mapped columns and removes any duplicate rows.

        Args:
            mapping_data: A ';' delimited string containing information on identifier mapping data. Each item
                contains an index of an edge_data column and a filepath to an identifier mapping data set:
                    '0:./filepath/mapping_data_0.txt;1:./filepath/mapping_data_1.txt'
            edge_data: A Pandas DataFrame row containing two columns of identifiers.

        Returns:
            A tuple of tuples, where each tuple contains a mapped identifier from each node column in the edge_data
            Pandas DataFrame. For example:
                [['CHEBI_24505', 'R-HSA-1006173'], ['CHEBI_28879', 'R-HSA-1006173'], ['CHEBI_59888', 'R-HSA-1013011']]
        """

        if mapping_data == 'None':
            edge_data = edge_data.astype(str)
            return tuple(zip(list(edge_data[list(edge_data)[0]]), list(edge_data[list(edge_data)[1]])))
        else:
            # merge edge data with referenced mapping data
            maps = [self.data_merger(node, mapping_data, edge_data) for node in range(2)]

            # merge mapping data merge result DataFrames
            merged_cols = list(set(maps[0][1]).intersection(set(maps[1][1])))
            merged_data = pandas.merge(maps[0][1], maps[1][1], left_on=merged_cols, right_on=merged_cols, how='inner')

            # remove unwanted columns
            keep_cols = [x for x in merged_data.columns if 'mapped' in str(x)]
            merged_data = merged_data[keep_cols].drop_duplicates(subset=None, keep='first', inplace=False)

            # make sure that both columns are type string
            merged_data = merged_data.astype(str)

            return tuple(zip(list(merged_data[maps[0][0]]), list(merged_data[maps[1][0]])))

    def creates_knowledge_graph_edges(self) -> None:
        """Generates edge lists for each edge type in an input dictionary. In order to generate the edge list,
        the function performs six steps: (1) read in data; (2) apply filtering and evidence criteria; (3) reduce data
        to specific columns, remove duplicates, and ensure proper formatting of column data; (4) update node column
        values; (5) rename nodes; and (6) map identifiers.

        Returns:
            source_info: A dictionary that contains all of the master information for each edge type resource. For
                example: {'chemical-complex': {'source_labels': ';;',
                                               'data_type': 'class-subclass',
                                               'edge_relation': 'RO_0002436',
                                               'uri': ['http://purl.obolibrary.org/obo/',
                                                       'https://reactome.org/content/detail/'],
                                               'row_splitter': 'n',
                                               'column_splitter': 't',
                                               'column_idx': '0;1',
                                               'identifier_maps': 'None',
                                               'evidence_criteria': 'None',
                                               'filter_criteria': 'None',
                                               'edge_list': [['CHEBI_24505', 'R-HSA-1006173'], ...]},
                        }
        """

        for edge_type in tqdm(self.source_info.keys()):
            print('\n### Processing Edge: {}'.format(edge_type))

            # STEP 1: read in data
            print('*** Reading Edge Data ***')
            edge_data = self.data_reader(self.data_files[edge_type],
                                         self.source_info[edge_type]['row_splitter'],
                                         self.source_info[edge_type]['column_splitter'])

            # STEP 2: apply filtering and evidence criteria
            print('*** Applying Filtering and/or Mapping Criteria to Edge Data ***')
            edge_data = self.filter_data(edge_data,
                                         self.source_info[edge_type]['filter_criteria'],
                                         self.source_info[edge_type]['evidence_criteria'])

            # STEP 3: reduce data to specific columns, remove duplicates, and ensure proper formatting of column data
            edge_data = self.data_reducer(self.source_info[edge_type]['column_idx'], edge_data)

            # STEP 4: update node column values
            print('*** Reformatting Node Values ***')
            edge_data = self.label_formatter(edge_data, self.source_info[edge_type]['source_labels'])

            # STEP 5: rename nodes
            edge_data.rename(columns={list(edge_data)[0]: str(list(edge_data)[0]) + '-' + edge_type.split('-')[0],
                                      list(edge_data)[1]: str(list(edge_data)[1]) + '-' + edge_type.split('-')[1]},
                             inplace=True)

            # STEP 6: map identifiers
            print('*** Performing Identifier Mapping ***')
            mapped_data = self.process_mapping_data(self.source_info[edge_type]['identifier_maps'], edge_data)
            self.source_info[edge_type]['edge_list'] = mapped_data

            # print edge statistics
            unique_edges = [list(y) for y in set([tuple(x) for x in self.source_info[edge_type]['edge_list']])]
            print('\nPROCESSED EDGE: {}'.format(edge_type))
            print('Total Unique Edge Count: {}'.format(len(unique_edges)))
            print('{}: Unique Node Count = {}'.format(edge_type.split('-')[0], len(set([x[0] for x in unique_edges]))))
            print('{}: Unique Node Count = {}'.format(edge_type.split('-')[1], len(set([x[1] for x in unique_edges]))))
            print('\n\n')

        # save a copy of the final master edge list
        with open('/'.join(self.source_file.split('/')[:-1]) + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(self.source_info, filepath)

        return None
