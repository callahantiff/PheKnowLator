#!/usr/bin/env python
# -*- coding: utf-8 -*-


class DocumentationMaker(object):
    """Has functionality to interact with a user and gather the information needed in order to prepare the three
    input documents needed to run the PheKnowLator program.

    If successfully run, the class will write the following three documents to the ./resources directory:
        1. resource_info.txt
        2. class_source_list.txt
        3. instance_source_list.txt

    Attributes:
        edge_count: an integer specifying the number of edges to create.
        write_location: A string containing a filename.

    """

    def __init__(self, edge_count: int, write_location: str = './resources/'):
        self.edge_count = edge_count
        self.write_location = write_location

    def information_getter(self):
        """Creates three dictionaries from information provided by a user. The three dictionaries store information
        for each of the three required documents.

        Returns:
            A list of three dictionaries:
                1. Information needed to create the resource_info.txt file
                2. Information needed to create the class_source_list.txt file
                3. Information needed to create the instance_source_list.txt file

        """

        # store input
        edge_data, class_data, instance_data = {}, {}, {}

        # get edge information
        for edge in range(self.edge_count):
            print('\n' + '#' * 40)
            print('GATHERING INFORMATION FOR EDGE: {count}/{total}'.format(count=edge + 1, total=self.edge_count + 1))
            print('#' * 40)

            edge_name = input('Please enter the edge type (e.g. gene-protein): ')
            print('\n')
            data_type = input('Provide the data types for each node in the edge (e.g. gene-drug --> class-instance): ')
            print('\n')
            row_splitter = input('Provide the character used to split input text into rows: ')
            print('\n')
            col_splitter = input('Provide the character used to split each row into columns: ')
            print('\n')
            col_idx = input('Provide the column index for each node in the input data, separated by ";" (e.g. 0;3): ')
            print('\n')
            id_maps = input('Provide identifier mapping information for each node: col:./filepath '
                            '(col = edge[node_idx], filepath = mapping data location)\n'
                            '  - If both nodes require mapping, separate each set of mapping information by a ";" '
                            '(e.g. 0:./filepath;1:./filepath)\n'
                            '  - If none of the nodes require mapping, please enter "None"\nProvide mapping '
                            'information now: ') or 'None'
            print('\n')
            evi_crit = input('Provide evidence column information needed to filter edges: (e.g. keep all rows from the '
                             '3rd column that contain "IEA": 4;==;IEA)\n'
                             '  - If there are multiple evidence columns, separate each set of information by "::" '
                             '(e.g. 4;!=;IEA::6;>;0)\n'
                             '  - If there are no evidence columns, please enter "None"\nProvide evidence information '
                             'now: ') or 'None'
            print('\n')
            filt_crit = input('Provide filtering column information needed to filter edges: (e.g. keep all rows '
                              'starting with "9606." from the 2nd column: 3;.startswith("9606.");'
                              '\n  - If there are multiple evidence columns, separate each set of information by "::" '
                              '(e.g. 3;.startswith("STRING");::7;==;"Homo sapiens")\n'
                              '  - If there are no evidence columns, please enter "None"\nProvide evidence '
                              'information now: ') or 'None'
            print('\n')
            edge_relation = input('Provide the Relation Ontology property class used to connect the nodes: ')
            print('\n')
            subj_uri = input('Provide the Universal Resource Identifier that will be connected to the subject node: ')
            print('\n')
            obj_uri = input('Provide the Universal Resource Identifier that will be connected to the object node: ')
            print('\n')
            source_label = input('Source Identifier Formatting (i.e. GO:12838340, when we need '
                                 'GO_12838340).\n\nProvide the following 3 items:\n(1) Character to split existing'
                                 'source labels (e.g. : in GO:1283834);\n(2) Label to use (or replace existing label) '
                                 'for subject node (e.g. GO_);\n(3) Label to use (or replace existing label) for object'
                                 ' node (e.g. GO_).\n\nEnter each item separated by ";". If the existing label is '
                                 'correct, press "enter": ') or ';;;'
            print('\n')

            # add edge data to dictionary
            edge_data[edge_name] = '{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}'.format(source_label, data_type,
                                                                                         edge_relation, subj_uri,
                                                                                         obj_uri, row_splitter,
                                                                                         col_splitter, col_idx, id_maps,
                                                                                         evi_crit, filt_crit)

            # get edge data sources
            if 'class' in data_type:
                class_data[edge_name] = input('Provide a URL or file path to data used to create this edge: ')
            else:
                instance_data[edge_name] = input('Provide a URL or file path to data used to create this edge: ')

        return edge_data, class_data, instance_data

    def writes_out_document(self, data: dict, delimiter: str, filename: str):
        """Function takes a dictionary of file information and writes it to a user-provided location.

        Args:
            data: a dictionary of edge data.
            delimiter: a string containing a character to use as the row delimiter.
            filename: a string containing a file path.

        Returns:
            None

        """

        with open(self.write_location + filename, 'w') as outfile:
            for edge, values in data.items():
                outfile.write(edge + delimiter + values + '\n')

        outfile.close()

        return None


def main():

    # print initial message for user
    print('\n\n' + '***' * 20)
    print('INPUT DOCUMENT BUILDER\n\nThis program will help you generate the input documentation\nneeded to run '
          'PheKnowLator by asking specific information\nabout each edge type in the knowledge graph.')
    print('***' * 20 + '\n')

    # initialize class
    edge_count = int(input('Enter the number of edge types to create: '))
    edge_maker = DocumentationMaker(edge_count)

    # get edge data
    edge_data = edge_maker.information_getter()

    # write out resource info data
    print('***' * 12 + '\nWRITING REQUIRED INPUT DOCUMENTATION\n' + '***' * 12)
    edge_maker.writes_out_document(edge_data[0], '|', 'TEST_resource_info.txt')

    # write out class data
    edge_maker.writes_out_document(edge_data[1], ', ', 'TEST_class_source_list.txt')

    # write out instance data
    edge_maker.writes_out_document(edge_data[2], ', ', 'TEST__instance_source_list.txt')


if __name__ == '__main__':
    main()
