#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import csv
import json
import pandas
import re

from tqdm import tqdm

# TODO: currently using eval() to handle filtering of downloaded data, should consider replacing this in a future
#  release.


class EdgeList(object):
    """Class creates edge lists based off data type.

    The class takes as input a string that represents the type of data being stored and a list of lists, where each item
    in the list is a file path/name of a data source. With these inputs the class reads and processes the data and then
    assembles it into a dictionary where the keys are tuples storing an entity identifier and a relation that
    connects the entity to another entity, which is stored as a value. The class returns a dictionary, keyed by data
    source, that contains a dictionary of edge relations

    Args:
        data_files: a list that contains the full file path and name of each downloaded data source.
        source_file: a string containing the filepath to resource information.

    """

    def __init__(self, data_files: dict, source_file: str):

        self.data_files = data_files
        self.source_file = source_file

        # convert edge data to a dictionary
        self.source_info = dict()

        for row in open(source_file).read().split('\n'):
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

    @staticmethod
    def identify_header(path: str, sep: str, rows: int = 5, threshold: float = 0.9):
        """Compares the similarity of a Pandas DataFrame read in with and without a header to determine whether or
        not the data frame should be built with a header or not. This function was modified from a Stack Overflow
        post: https://stackoverflow.com/questions/40193388/how-to-check-if-a-csv-has-a-header-using-python/40193509

        Args:
            path: A filepath to a data file.
            sep: A character specifying how the data is delimited.
            rows: The number of rows to compare.
            threshold: A threshold used to determine the similarity between the data with and without the header.

        Returns:
            if the similarity is grater than the threshold, then None is returned, otherwise 0 is returned

        """

        df_with_header = pandas.read_csv(path, header='infer', nrows=rows, delimiter=sep)
        df_without_header = pandas.read_csv(path, header=None, nrows=rows, delimiter=sep)

        # detect similarity
        sim = (df_with_header.dtypes.values == df_without_header.dtypes.values).mean()

        if sim > threshold:
            return None

        else:
            return 0

    def data_reader(self, data_filepath: str, file_splitter: str = '\n', line_splitter: str = 't'):
        """Takes a filepath pointing to data source and reads it into a Pandas DataFrame using information in the
        file and line splitter variables.

        Args:
            data_filepath: Filepath to data.
            file_splitter: Character to split data, used when a file contains metadata information.
            line_splitter: Character used to split rows into columns.

        Return:
            A Pandas DataFrame containing the data from the data_filepath.

        Raises:
            An exception is raised if the Pandas DataFrame contains at least 2 columns and more than 10 rows.

        """

        # identify line splitter
        splitter = '\t' if 't' in line_splitter else " " if ' ' in line_splitter else line_splitter

        # read in data
        if '!' in file_splitter or '#' in file_splitter:
            # ASSUMPTION: assumes data is read in without a header
            decoded_data = open(data_filepath).read().split(file_splitter)[-1]
            edge_data = pandas.DataFrame([x.split(splitter) for x in decoded_data.split('\n') if x != ''])

        else:
            # determine if file contains a header
            header = self.identify_header(data_filepath, splitter, 5, 0.9)
            edge_data = pandas.read_csv(data_filepath, header=header, delimiter=splitter, low_memory=False)

        # CHECK - verify data
        if len(list(edge_data)) >= 2 and len(edge_data) > 10:
            return edge_data.dropna(inplace=False)

        else:
            raise Exception('ERROR: Data could not be properly read in')

    @staticmethod
    def filter_data(edge_data: pandas.DataFrame, filter_criteria: str, evidence_criteria: str):
        """Applies a set of filtering and/or evidence criteria to specific columns in a Pandas DataFrame and returns a
        filtered DataFrame.

        Args:
            edge_data: A Pandas DataFrame.
            filter_criteria: A '::' delimited string; each delimited item is a set of filtering criteria.
            evidence_criteria: A '::' delimited string; each delimited item is a set of mapping criteria.

        Returns:
            A filtered Pandas DataFrame.

        Raises:
            An exception is raised if the Pandas DataFrame contains at least 2 columns and more than 10 rows.

        """

        if filter_criteria == 'None' and evidence_criteria == 'None':
            return edge_data

        else:
            map_filter_criteria = filter_criteria + '::' + evidence_criteria

            for crit in [x for x in map_filter_criteria.split('::') if x != 'None']:
                col = list(edge_data)[int(crit.split(';')[0])]

                try:
                    if type(float(crit.split(';')[2])) is float or type(int(crit.split(';')[2])) is int:
                        if type(float(crit.split(';')[2])) is float:
                            edge_data[col] = edge_data[col].astype(float)

                        else:
                            edge_data[col] = edge_data[col].astype(int)

                        statement = '{} {} {}'.format('x', crit.split(';')[1], crit.split(';')[2])

                except ValueError:
                    if crit.split(';')[2] == '' and '(' in crit.split(';')[1]:
                        statement = '{}{}'.format('x', crit.split(';')[1])

                    elif '(' in crit.split(';')[2] or '[' in crit.split(';')[2]:
                        statement = '{} {} {}'.format('x', crit.split(';')[1], crit.split(';')[2].replace("'", ''))

                    else:
                        statement = '{} {} "{}"'.format('x', crit.split(';')[1], crit.split(';')[2].replace("'", ''))

                edge_data = edge_data.loc[edge_data[col].apply(lambda x: eval(statement))]

            # CHECK - verify data
            if len(list(edge_data)) >= 2 and len(edge_data) > 10:
                return edge_data

            else:
                raise Exception('ERROR: Data could not be properly read in')

    @staticmethod
    def label_formatter(edge_data: pandas.DataFrame, label_criteria: str):
        """Applies criteria to reformat edge data labels.

        Args:
            edge_data: A data frame containing a column for each node in the edge
            label_criteria: A ';' delimited string containing 3 arguments: (1) string splitter, (2) string to append
                to subject node, and (3) string to append to object node

        Returns:
            edge_data:

        """

        cut = label_criteria.split(';')[0]

        for col in range(0, len(label_criteria.split(';')[1:])):
            formatter, col_to_check = label_criteria.split(';')[col + 1], edge_data[list(edge_data)[col]].astype(str)

            if (cut == '' and formatter != '') or not any(i for i in list(col_to_check) if cut in i):
                edge_data[list(edge_data)[col]] = edge_data[list(edge_data)[col]].apply(lambda x: formatter + str(x))

            elif cut != '' and formatter != '':
                edge_data[list(edge_data)[col]].replace('(^.*{})'.format(cut), formatter, inplace=True, regex=True)

            else:
                pass

        return edge_data

    def data_merger(self, node: int, mapping_data: str, edge_data: pandas.DataFrame):
        """Processes a string that contains instructions for mapping both columns in the edge_data Pandas DataFrame.
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
            A nested list - [0] node of edge type, [1] edge_data column that needs mapping, and [2]: a Pandas
            DataFrame containing the data to be mapped.

        Raises:
            An exception if when indexing into a list that does not contain object at index.
            An exception is raised when trying to merge data on columns of different data types.

        """

        # check if node needs to be mapped to an outside data source
        if str(node) in re.sub('(?:(?!:)\D)*', '', mapping_data).split(':'):
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

                # merge data
                merged_data = pandas.merge(edge_data, map_data, left_on=node2map, right_on=map_col, how='inner')

            # drop all columns but merge key and value columns
            merged_data = merged_data[[list(edge_data)[0], list(edge_data)[1], col_to_map]]

        # NOT MAPPING TO OUTSIDE DATA SOURCE
        else:
            col_to_map = str(list(edge_data)[node]) + '_mapped'
            edge_data[col_to_map] = edge_data[[list(edge_data)[node]]]
            merged_data = edge_data[[list(edge_data)[0], list(edge_data)[1], col_to_map]]

        return [col_to_map, merged_data]

    def process_mapping_data(self, mapping_data: str, edge_data: pandas.DataFrame):
        """Merges two mapped Pandas DataFrames into a single DataFrame. After merging the DataFrames, the function
        removes all columns except the the mapped columns and removes any duplicate rows.

        Args:
            mapping_data: A ';' delimited string containing information on identifier mapping data. Each item
                contains an index of an edge_data column and a filepath to an identifier mapping data set:
                    '0:./filepath/mapping_data_0.txt;1:./filepath/mapping_data_1.txt'
            edge_data: A Pandas DataFrame row containing two columns of identifiers.

        Returns:
            A list of tuples, where each tuple contains a mapped identifier from each node column in the edge_data
            Pandas DataFrame.

        """

        if mapping_data == 'None':
            return tuple(zip(list(edge_data[list(edge_data)[0]]), list(edge_data[list(edge_data)[1]])))

        else:
            # merge edge data with referenced mapping data
            maps = [self.data_merger(node, mapping_data, edge_data) for node in range(2)]

            # merge mapping data merge result DataFrames
            merged_cols = list(set(maps[0][1]).intersection(set(maps[1][1])))
            merged_data = pandas.merge(maps[0][1], maps[1][1], left_on=merged_cols, right_on=merged_cols, how='inner')

            # remove un-wanted columns
            keep_cols = [x for x in merged_data.columns if 'mapped' in str(x)]
            merged_data = merged_data[keep_cols].drop_duplicates(subset=None, keep='first', inplace=False)

            return tuple(zip(list(merged_data[maps[0][0]]), list(merged_data[maps[1][0]])))

    def creates_knowledge_graph_edges(self):
        """Generates edge lists for each edge type in an input dictionary.

        Returns:
            A dictionary that contains all of the master information for each edge type resource.

        """

        for edge_type in tqdm(self.source_info.keys()):
            print('\n### Processing Edge: {}'.format(edge_type))

            # step 1: read in, filter data, and label data
            print('*** Reading Edge Data ***')
            edge_data = self.data_reader(self.data_files[edge_type],
                                         self.source_info[edge_type]['row_splitter'],
                                         self.source_info[edge_type]['column_splitter'])

            # apply filtering and evidence criteria
            print('*** Applying Filtering and/or Mapping Criteria to Edge Data ***')
            edge_data = self.filter_data(edge_data,
                                         self.source_info[edge_type]['filter_criteria'],
                                         self.source_info[edge_type]['evidence_criteria'])

            # reduce data to specific edge columns and remove duplicates
            cols = self.source_info[edge_type]['column_idx']
            edge_data = edge_data[[list(edge_data)[int(cols.split(';')[0])], list(edge_data)[int(cols.split(';')[1])]]]
            edge_data = edge_data.drop_duplicates(subset=None, keep='first', inplace=False)

            # update node column values
            print('*** Reformatting Node Values ***')
            edge_data = self.label_formatter(edge_data, self.source_info[edge_type]['source_labels'])

            # relabel nodes
            edge_data.rename(columns={list(edge_data)[0]: str(list(edge_data)[0]) + '-' + edge_type.split('-')[0],
                                      list(edge_data)[1]: str(list(edge_data)[1]) + '-' + edge_type.split('-')[1]},
                             inplace=True)

            # map identifiers
            print('*** Performing Identifier Mapping ***')
            mapped_data = self.process_mapping_data(self.source_info[edge_type]['identifier_maps'], edge_data)

            # add edge and node data to master dictionary
            self.source_info[edge_type]['edge_list'] = mapped_data

            # print edge statistics
            s, o = edge_type.split('-')[0], edge_type.split('-')[1]
            n0 = len(set([x[0] for x in self.source_info[edge_type]['edge_list']]))
            n1 = len(set([x[1] for x in self.source_info[edge_type]['edge_list']]))
            link = len(self.source_info[edge_type]['edge_list'])

            print('=== Processed: {0} ({1} edges; {2}s:{3}; {4}s:{5}) ==='.format(edge_type, link, s, n0, o, n1))

        # save a copy of the final master edge list
        with open('/'.join(self.source_file.split('/')[:-1]) + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(self.source_info, filepath)

        return None
