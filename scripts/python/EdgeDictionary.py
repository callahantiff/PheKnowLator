#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import codecs
import csv
import re
import urllib

from copy import deepcopy
from rdflib import Graph
from tqdm import tqdm
from urllib.parse import urlencode
from urllib.request import urlopen


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
        data_files (dict): a list that contains the full file path and name of each downloaded data source.
        source_file (str): A string containing the filepath to resource information.

    """

    def __init__(self, data_files, source_file):

        self.data_files = data_files
        self.source_file = source_file

        # convert to dictionary
        self.source_info = dict()

        for line in open(source_file).read().split('\n'):
            line_data = ['"{}"'.format(x.strip()) for x in list(csv.reader([line], delimiter='|', quotechar='"'))[0]]

            self.source_info[line_data[0].strip('"').strip("'")] = {}
            self.source_info[line_data[0].strip('"').strip("'")]['source_labels'] = line_data[1].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['data_type'] = line_data[2].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['relation'] = line_data[3].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['uri'] = (line_data[4].strip('"').strip("'"),
                                                                           line_data[5].strip('"').strip("'"))
            self.source_info[line_data[0].strip('"').strip("'")]['file_splitter'] = line_data[6].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['line_splitter'] = line_data[7].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['columns'] = line_data[8].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['map'] = line_data[9].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['evidence'] = line_data[10].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['filter'] = line_data[11].strip('"').strip("'")
            self.source_info[line_data[0].strip('"').strip("'")]['edge_list'] = []

    @staticmethod
    def filters_data(edge_data, splitter, data_filter):
        """Function takes a list of data and then applies a user-input filter to process the data.

        Args:
            edge_data (list): A list of data results.
            splitter (str): A string containing a character to split a row data into columns.
            data_filter (str): A string containing information that is used to filter results.

        Return:
            A list of filtered results.

        """

        filtered_data = []

        for line in edge_data:
            split_line = ['"{}"'.format(x) for x in list(csv.reader([line], delimiter=splitter, quotechar='"'))[0]]

            if len(split_line) > 1 and '.' not in data_filter:
                statement = '{} {} "{}"'.format(split_line[int(data_filter.split(';')[0])],
                                                data_filter.split(';')[1],
                                                data_filter.split(';')[2])
                if eval(statement):
                    filtered_data.append(line)

            elif len(split_line) > 1 and '.' in data_filter:
                statement = '{}{}'.format(split_line[int(data_filter.split(';')[0])], data_filter.split(';')[1])

                if eval(statement):
                    filtered_data.append(line)

        return filtered_data

    @staticmethod
    def formats_column_labels(line_data, columns, src_label):
        """Function takes a single row of data input as a list and then extracts and re-labels specific columns from
        that row.

        Args:
            line_data (list): A list representing a single row of data.
            columns (str): A string containing the columns to extract from the line of data (e.g. "1;2").
            src_label (str): A string which contains source labels to append to extracted columns.

        Returns:
            A list of extracted and processed edges.

        """

        # format labels
        src_split = src_label.split(';')[0] if src_label.split(';')[0] != '' else None
        source1 = src_label.split(';')[1] if src_label.split(';')[1] != '' else src_label.split(';')[1]
        source2 = src_label.split(';')[2] if src_label.split(';')[2] != '' else src_label.split(';')[2]

        # update data using formatted labels
        edge1 = source1 + re.sub(r'9606.|' + source1, '', line_data[int(columns.split(';')[0])].split(src_split)[-1])
        edge2 = source2 + re.sub(r'9606.|' + source2, '', line_data[int(columns.split(';')[1])].split(src_split)[-1])

        return edge1.replace(':', '_'), edge2.replace(':', '_')

    def processes_edge_data(self, data, file_split, line_split, columns, evidence, data_filter, src_label):
        """Function process a data set and uses the user input to generate a nested list where each nested list
        represents an edge.

        Args:
            data (str): A string containing a filepath for a data set.
            file_split (str): A string containing a character to split a string into rows of data.
            line_split (str): A string containing a character to split a row data into columns.
            columns (str): A string containing the columns to extract from the line of data (e.g. "1;2").
            evidence (str): A string containing information that is used to filter results.
            data_filter (str): A string containing information that is used to filter results.
            src_label (str): A string which contains source labels to append to extracted columns.

        Returns:
            A list, where each list.

        Raises:
            An exception is raised if after processing the edges, no data is returned.
        """

        edges = []
        splitter = '\t' if 't' in line_split else " " if ' ' in line_split else line_split

        # read in data with properly file splitter
        if '!' in file_split or '#' in file_split:
            decoded_data = codecs.decode(open(data).read().split(file_split)[-1], 'unicode_escape')
            edge_data = decoded_data.split('\n')[1:]

        else:
            decoded_data = codecs.decode(open(data).read(), 'unicode_escape')
            split = '\n' if 'n' in file_split else ' '
            edge_data = decoded_data.split(split)[1:]

        # perform filtering
        if 'None' not in data_filter:
            edge_data = self.filters_data(edge_data, splitter, data_filter)

        # perform evidence filtering
        if 'None' not in evidence:
            edge_data = self.filters_data(edge_data, splitter, evidence)

        # filter to specific columns
        for line in edge_data:
            if len(line) > 1:
                # re-format and clean up quoted-text
                line = ['"{}"'.format(x) for x in list(csv.reader([line], delimiter=splitter, quotechar='"'))[0]]
                line_data = [x.strip('"').strip("'") for x in line]

                # format labels
                labeled_edges = self.formats_column_labels(line_data, columns, src_label)

                edges.append(['_'.join(list(filter(None, labeled_edges[0].split('_')))),
                              '_'.join(list(filter(None, labeled_edges[1].split('_'))))])

        # check that there is data
        if len(edges) <= 1:
            raise Exception('ERROR: Something went wrong when processing data')

        else:
            return edges


    @staticmethod
    def queries_ontologies(data_file):
        """Takes a string representing a path/file name to an ontology. The function uses the RDFLib library
        and creates a graph. The graph is then queried to return all classes and their database cross-references. The
        function returns a list of query results

        Args:
            data_file (str): A filepath pointing to an ontology saved in an '.owl' file.

         Returns:
            A dictionary of results returned from mapping identifiers.

        Raises:
            An exception is raised if the generated dictionary does not have the same number of entries as the
            ontology graph.
         """

        # read in ontology as graph
        graph = Graph()
        graph.parse(data_file)

        # query graph to get all cross-referenced sources
        results = graph.query(
            """SELECT DISTINCT ?source ?c
               WHERE {
                  ?c rdf:type owl:Class .
                  ?c oboInOwl:hasDbXref ?source .}
               """, initNs={"rdf": 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                            "owl": 'http://www.w3.org/2002/07/owl#',
                            "oboInOwl": 'http://www.geneontology.org/formats/oboInOwl#'})

        # convert results to dictionary
        ont_results = {}
        for res in list(results):
            if str(res[0]).split(':')[-1] in ont_results.keys():
                ont_results[str(res[0]).split(':')[-1]].append(str(res[1]))

            else:
                ont_results[str(res[0]).split(':')[-1]] = [str(res[1])]

        # CHECK - all URLs returned an data file
        if len(list(ont_results)) == 0:
            raise Exception('ERROR: Query returned no data')
        else:
            return ont_results

    @staticmethod
    def queries_uniprot_api(data_file):
        """Searches a list of entities against the Uniprot API (uniprot.org/help/api_idmapping).

        Args:
            data_file (str): A filepath containing data to map.

        Returns:
            A dictionary of results returned from mapping identifiers.

        Raises:
            An exception is raised if the generated dictionary does not have the same number of rows as the what was
            returned by the API.
        """
        proteins = list(set([x.split('\\t')[1] for x in open(data_file).read().split('!')[-1].split('\\n')[1:]]))
        params = {'from': 'ACC+ID', 'to': 'P_ENTREZGENEID', 'format': 'tab', 'query': ' '.join(proteins)}

        data = urllib.parse.urlencode(params).encode('utf-8')
        requests = urllib.request.Request('https://www.uniprot.org/uploadlists/', data)
        results = urllib.request.urlopen(requests).read().decode('utf-8')

        # convert results to dictionary
        api_results = {}
        for res in results.split('\n')[1:-1]:
            res_row = res.split('\t')
            if str(res_row[0]) in api_results.keys():
                api_results[str(res_row[0])].append('http://purl.uniprot.org/geneid/' + str(res_row[1]))

            else:
                api_results[str(res_row[0])] = ['http://purl.uniprot.org/geneid/' + str(res_row[1])]

        # CHECK - all URLs returned an data file
        if len(api_results) == 0:
            raise Exception('ERROR: API returned no data')
        else:
            return api_results

    @staticmethod
    def queries_txt_file(data_file):
        """Reads in a file containing two columns of data and converts it to a dictionary.

        Args:
            data_file (str): A filepath containing data to map.

        Return:
            A dictionary of results returned from mapping identifiers.

        Raises:
            An exception is raised if the generated dictionary does not have the same number of rows as the input file.
        """

        results = open(data_file).readlines()

        # convert results to dictionary
        file_results = {}

        for res in results[:-1]:
            key = str(res.split('\t')[0].split('_')[-1])

            if key in file_results.keys():
                file_results[key].append(str(res.split('\t')[1].strip('\n')))

            else:
                file_results[key] = [str(res.split('\t')[1].strip('\n'))]

        # CHECK - all URLs returned an data file
        if len(results) == 0:
            raise Exception('ERROR: dictionary missing results')

        else:
            return file_results

    def gets_edge_information(self, map_source, loc, edge_type):
        """Uses information about an edge type to identify the appropriate source to map.

        NOTE. Since CHEBI does not have dbXRef mappings to MESH, that the current code has logic to point to the
        MESH-CHEBI_MAP.txt if the edge type contains 'chemical'. If CHEBI adds MESH then this logic can change.

        Args:
            map_source (str): A string naming the mapping source to use for mapping.
            loc (int): An integer representing an index.
            edge_type: A string naming the type of edge.

        Returns:
            A dictionary of mapped identifiers.
        """

        # specify mapping task dictionary
        map_task = {'dbxref': self.queries_ontologies, 'goa': self.queries_uniprot_api, 'txt': self.queries_txt_file}

        try:
            edge = edge_type.split('-')[loc]
            data_loc = map_source if edge == 'chemical' else self.data_files[edge]

        except KeyError:
            data_loc = map_source

        return map_task[[key for key in map_task.keys() if key in map_source][0]](data_loc)

    def maps_identifiers(self, edges, edge_type, edge_loc, map_source):
        """Maps identifiers in an edge list to a specified resource.

        Args:
            edges (list): A nested list where each list represents an edge.
            edge_type (str): A string naming the type of edge.
            edge_loc (list): A list of strings that represent integers.å
            map_source (list): A string naming the mapping source to use for mapping.

        Returns:
            A dictionary containing information on all of the processed edge types.

        Raises:
            An exception is raised if after mapping tthe identifiers, no data is returned.
        """

        map2 = updated_edges = []

        # copy edge list - done bc we are using 'pop' to remove edges
        edge_data = deepcopy(edges)

        # get dictionary of mapped identifiers
        if len(edge_loc) > 1:
            map1 = self.gets_edge_information(map_source[int(edge_loc[0])], int(edge_loc[0]), edge_type)
            map2 = self.gets_edge_information(map_source[int(edge_loc[1])], int(edge_loc[1]), edge_type)

        else:
            map1 = self.gets_edge_information(map_source[0], int(edge_loc[0]), edge_type)

        # create edge lists
        for edge in edge_data:
            if len(edge_loc) > 1:
                map_edge1 = edge[0].split('_')[-1]
                map_edge2 = edge[1].split('_')[-1]
                mapped1 = map1[map_edge1.split('_')[-1]] if map_edge1.split('_')[-1] in map1.keys() else ''
                mapped2 = map2[map_edge2.split('_')[-1]] if map_edge2.split('_')[-1] in map2.keys() else ''

            else:
                map_edge1 = edge.pop(int(edge_loc[0])).split('_')[-1]
                map_edge2 = edge[0]
                mapped1 = map1[map_edge1.split('_')[-1]] if map_edge1.split('_')[-1] in map1.keys() else ''
                mapped2 = [map_edge2]

            for mapped_i in mapped1:
                for mapped_j in mapped2:
                    cleaned_edge = [None, None]
                    cleaned_edge[int(edge_loc[0])] = mapped_i.split('/')[-1]
                    cleaned_edge[abs(1 - int(edge_loc[0]))] = mapped_j.split('/')[-1]

                    # add updated edge
                    updated_edges.append(cleaned_edge)

        # check that there is data
        if len(updated_edges) <= 1:
            raise Exception('ERROR: Something went wrong when mapping identifiers')

        else:
            return updated_edges

    def creates_knowledge_graph_edges(self):
        """Generates edge lists for each edge type in an input dictionary.

        Returns:
            A dictionary that contains all of the master information for each edge type resource.

        """

        for edge_type in tqdm(self.source_info.keys()):

            print('\n\n' + '=' * 50)
            print('Processing Edge: {0}'.format(edge_type))
            print('=' * 50 + '\n')

            # edge_type = 'pathway-gobp'

            # step 1: read in, process, and filter data
            print('Cleaning Edges')

            clean_data = self.processes_edge_data(self.data_files[edge_type],
                                                  self.source_info[edge_type]['file_splitter'],
                                                  self.source_info[edge_type]['line_splitter'],
                                                  self.source_info[edge_type]['columns'],
                                                  self.source_info[edge_type]['evidence'],
                                                  self.source_info[edge_type]['filter'],
                                                  self.source_info[edge_type]['source_labels'])

            # step 2: map identifiers + add proper source labels
            print('Mapping Identifiers and Updating Edge List\n')

            if self.source_info[edge_type]['map'] == 'None':
                self.source_info[edge_type]['edge_list'] = clean_data

            else:
                edge_loc = [i for j in self.source_info[edge_type]['map'].split(';') for i in j.split(':')][0::2]
                map_source = [i for j in self.source_info[edge_type]['map'].split(';') for i in j.split(':')][1::2]
                mapped_data = self.maps_identifiers(clean_data, edge_type, edge_loc, map_source)

                # add results back to dict
                self.source_info[edge_type]['edge_list'] = mapped_data

        return None
