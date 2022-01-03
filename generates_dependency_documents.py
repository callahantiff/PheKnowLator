#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import os
import os.path

from typing import Dict, Tuple


# TODO: (1) Need to add checks to ensure that user input is correct + currently only works for ontology and class data

class DocumentationMaker(object):
    """Has functionality to interact with a user and gather the information needed in order to prepare the input
    documents needed to run the PheKnowLator program. For more information on the dependency documents, please see
    the following Wiki: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies.

    If successfully run, the class will write the following three documents to the ./resources directory:
        1. resource_info.txt
        2. ontology_source_list.txt
        3. edge_source_list.txt

    Attributes:
        edge_count: An integer specifying the number of edges to create.
        write_location: A string containing a filename. Defaults to './resources'.

    Raises:
        ValueError: If edge_count is not an integer.
    """

    def __init__(self, edge_count: int, write_location: str = './resources') -> None:

        # check edge count
        if not isinstance(edge_count, int):
            raise ValueError('edge_count must be an integer (i.e. "1" not "one").')
        else:
            self.edge_count = edge_count

        # make sure that the specified location to write data exists
        if os.path.exists(write_location):
            self.write_location = write_location
        else:
            print('Creating {} directory.'.format(write_location))
            os.mkdir('./resources/')
            self.write_location = write_location

    def information_getter(self) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
        """Creates three dictionaries from information provided by a user. The three dictionaries store information
        for each of the two required documents.

        Returns:
            A list of two dictionaries:
                1. Information needed to create the resource_info.txt file.
                2. Information needed to create the ontology_source_list.txt file.
                3. Information needed to create the edge_source_list.txt file.
        """

        # store input
        resource_data, ont_data, edge_data = {}, {}, {}

        # get edge information
        for edge in range(self.edge_count):
            print('\n' + '#' * 40)
            print('GATHERING INFORMATION FOR EDGE: {count}/{total}'.format(count=edge, total=self.edge_count))
            print('#' * 40)

            edge_type = input('Please enter the edge type (e.g. "gene-protein", "disease-chemical"): ')
            print('\n')

            ont = input('Is one or both of the nodes in edge an ontology? Please enter "one" or "both": ')
            if ont == 'one':
                ont_edge = input('Enter node name for ontology (e.g. "go"): ')
                ont_data[ont_edge] = input('Provide an owl or obo URL for this ontology: ')
            else:
                for _ in range(2):
                    ont_edge = input('Enter node name for ontology (e.g. "go"): ')
                    ont_data[ont_edge] = input('Provide an owl or obo URL for this ontology: ')

            print('\n')

            delimiter = input('Provide the character used to split each row into columns (e.g. "t" or ","): ')
            print('\n')

            col_idx = input('Provide the column index for each node in the input data, separated by ";" (e.g. "0;3"): ')
            print('\n')

            id_maps = input('Provide identifier mapping information for each node: col:./filepath '
                            '(col = edge[node_idx], filepath = mapping data location)\n'
                            '  - If both nodes require mapping, separate each set of mapping information by a ";" '
                            '(e.g. "0:./filepath;1:./filepath")\n'
                            '  - If none of the nodes require mapping, please enter "None"\nProvide mapping '
                            'information now: ') or 'None'
            print('\n')
            evi_crit = input('Provide evidence column information needed to filter edges: (e.g. keep all rows from the '
                             '3rd column that contain "IEA": "4;==;IEA")\n'
                             '  - If there are multiple evidence columns, separate each set of information by "::" '
                             '(e.g. "4;!=;IEA::6;>;0")\n'
                             '  - If there are no evidence columns, please enter "None"\nProvide evidence information '
                             'now: ') or 'None'
            print('\n')
            filt_crit = input('Provide filtering column information needed to filter edges: (e.g. keep all rows '
                              'starting with "9606." from the 2nd column: 3;.startswith("9606.");'
                              '\n  - If there are multiple evidence columns, separate each set of information by "::" '
                              '(e.g. "3;.startswith("STRING");::7;==;Homo sapiens")\n'
                              '  - If there are no evidence columns, please enter "None"\nProvide evidence '
                              'information now: ') or 'None'
            print('\n')
            edge_relation = input('Provide the Relation Ontology property class used to connect the nodes (e.g. '
                                  '"RO_0000056"): ')
            print('\n')

            identifier_prefix_information = input('Source Identifier Formatting (i.e., GO:12838340, when we need '
                                                  'GO_12838340).\n\nProvide the following 3 items:\n(1) Character to '
                                                  'split existing CURIE (e.g., ":" in GO:1283834);\n(2) New subject '
                                                  'prefix to replace existing one (e.g. "GO_");\n(3) New object '
                                                  'prefix to replace existing one (e.g., GO_).\n\nEnter each item '
                                                  'separated by ";". If the existing prefix is correct, press "enter": '
                                                  ') or ";;"')
            print('\n')

            # add edge data to dictionary
            resource_data[edge_type] = '{0}|{1}|{2}|{3}|{4}|{5}|{6}'.format(identifier_prefix_information,
                                                                            edge_relation, delimiter, col_idx,
                                                                            id_maps, evi_crit, filt_crit)

            # get edge data sources
            edge_data[edge_type] = input('Provide a URL or file path to data used to create this edge: ')

        return resource_data, ont_data, edge_data

    def writes_out_document(self, data: Dict[str, str], delimiter: str, filename: str) -> None:
        """Function takes a dictionary of file information and writes it to a user-provided location.

        Args:
            data: A dictionary of edge data.
            delimiter: A string containing a character to use as the row delimiter.
            filename: A string containing a file path.

        Returns:
            None
        """

        with open(self.write_location + '/' + filename, 'w') as outfile:
            for edge, values in data.items():
                outfile.write(edge + delimiter + values + '\n')
        outfile.close()

        return None


def main():
    # print initial message for user
    print('\n\n' + '***' * 50)
    print('INPUT DOCUMENT BUILDER\n\nThis program will help you generate the input documentation needed to run '
          'PheKnowLator by asking specific information about each edge type in the knowledge graph.\nIt will help '
          'you create three documents:\n\t\t(1) resource_info.txt\n\t\t(2) ontology_source_info.txt\n\t\t(3) '
          'edge_source_info.txt\nAn example of the data this program expects to find within each of these '
          'documents is shown below:\n\n(1) resource_info.txt: This document represents each edge type as a single '
          '"|" delimited string and contains a total of 9 items:\n\t(1) edge_type: A string label for an edge '
          '(node1-node2). The label matches what is used in the edge_source_list.txt and ontology_source_list.txt files'
          '\n\t(2) identifier_prefix_nformation: A ";"-separated string used to update a prefix-identifier pair '
          '(e.g., GO;GO). The first and second items contain BioRegistry prefixes. If one of the\n\t\texisting '
          'prefixes is correct leave its spot empty and if both are correct, type ";". All prefixes should be the '
          'preferred prefix from the BioRegistry\n\t\t(https://bioregistry.io/registry/);\n\t(3) relation: A Relation '
          'Ontology (http://www.obofoundry.org/ontology/ro.html) CURIE (e.g., RO_0000056)\n\t(4) delimiter: A '
          'character used to split rows from an input data source into columns (e.g., "t" for '
          'tab-delimited data or "," for comma-delimited data);\n\t(5) column_indexes: Two-column indexes separated by '
          '";" (e.g., "0;4" for the first and third columns in the input data source);\n\t(5) IdentifierMaps: A string '
          'of mapping information for each node in an edge. For example, the string "2:mapping_file_1.txt;'
          '4:mapping_file_2.txt" means that the first node require\n\t\tdata contained in the 2nd column of the '
          '"mapping_file_1.txt" and the second node requires data from the 4th column in the "mapping_file_2.txt" '
          'file;\n\t(6) evidence_criteria: Evidence criteria that can be used to filter an input data source (e.g., '
          'scores above a certain cut-off). An evidence set is composed of 3 pieces of ";"\n\t\t-separated '
          'information. Multiple filtering sets can be passed, where each set is separated by "::". Consider the '
          'following example: "4;!=;IEA::8;<;0.0001"):\n\t\t\t1. The index of the column to apply the evidence '
          'criteria to (e.g., "4" and "8" in the example above)\n\t\t\t2. The operator (i.e., "==", "!=", "<", '
          '">", "<=", ">=", "in", ".startswith()", ".endswith()") to use when filtering (e.g., "!=" and "<" in the '
          'example above).\n\t\t\t3.The value (i.e., "int", "float", "str", "list") to filter on (e.g., "IEA" and '
          '"0.0001" in the example above);\n\t(7) filtering_criteria: Criteria that can be used to filter an input '
          'data source (e.g., human proteins). An evidence set is composed of 3 pieces of ";"-separated information.'
          '\n\t\tMultiple filtering sets can be passed as demonstrated by the example above, where each set is '
          'separated by "::". Consider the following example: "5;==;P::7;==;9606"):\n\t\t\t1. The index of the '
          'column to apply the evidence criteria to (e.g., "5" and "7" in the example above)\n\t\t\t2. The operator '
          '(i.e., "==", "!=", "<", ">", "<=", ">=", "in", ".startswith()", ".endswith()") to use when filtering '
          '(e.g., "==" and "==" in the example above)\n\t\t\t3. The value (i.e., "int", "float", "str", "list") to '
          'filter on (e.g., "P" and "9606" in the example above).\n\n\tAn example line from the resource_info.txt file '
          'is shown below:\n\t\tchemical-gene|;MESH_;|RO_0002434|#|t|1;4|0:./resources/data_maps/'
          'MESH_CHEBI_MAP.txt|None|7;==;9606\n\n(2) ontology_source_info.txt: This document contains a "|"-delimited '
          'line for each ontology source used, for example:\n\t"chemical|http://purl.obolibrary.org/obo/chebi.owl"'
          '\n\t"gene|http://purl.obolibrary.org/obo/so.owl"\n\n(3) edge_source_info.txt: This document contains a '
          '"|"-delimited line for each edge data source, for example:\n\t"chemical-gene|'
          'http://ctdbase.org/reports/CTD_chem_gene_ixns.tsv.gz"\n\nIf you would like more information on the '
          'dependency documents need to run PheKnowLator, please visit the following Wiki page:\n'
          'https://github.com/callahantiff/PheKnowLator/wiki/Dependencies.')
    print('***' * 60 + '\n')

    # initialize class
    edge_count = int(input('EDGE COUNT: Enter the number of edge types to create: '))
    edge_maker = DocumentationMaker(edge_count)

    # run method to obtain edge data information
    edge_data = edge_maker.information_getter()

    # write out resource info data
    print('***' * 12 + '\nWRITING REQUIRED INPUT DOCUMENTATION\n' + '***' * 12)
    edge_maker.writes_out_document(edge_data[0], '|', 'resource_info.txt')

    # write out ontology data
    edge_maker.writes_out_document(edge_data[1], '|', 'ontology_source_list.txt')

    # write out edge data
    edge_maker.writes_out_document(edge_data[2], '|', 'edge_source_list.txt')


if __name__ == '__main__':
    main()
