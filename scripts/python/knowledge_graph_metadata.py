#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import datetime
import glob
import json
import os
import pandas
import re
import subprocess

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS
from tqdm import tqdm
from typing import Dict, List, Optional, Union


class Metadata(object):
    """Class helps manage knowledge graph metadata.

    Attributes:
        kg_version: A string that contains the version of the knowledge graph build.
        node_metadata_flag: A string indicating whether or not node metadata should be added to the knowledge graph.
        write_location: A filepath to the knowledge graph directory (e.g. './resources/knowledge_graphs).
        full_kg: The subdirectory and name of the knowledge graph (e.g.'/relations_only/KG.owl').
        node_data: A filepath to a directory called 'node_data' containing a file for each instance node.
        node_dict: A nested dictionary storing metadata for the nodes in the edge_dict. Where the outer key is a node
            identifier and each inner key is an identifier type keyed by the identifier type with the value being the
            corresponding metadata for that identifier type. For example:
                {'6469': {'Label': 'SHH',
                          'Description': 'Sonic Hedgehog Signaling Molecule is a protein-coding gene that is located on
                                          chromosome 7 (map_location: 7q36.3).',
                          'Synonym': 'HHG1|HLP3|HPE3|MCOPCB5|SMMCI|ShhNC|TPT|TPTPS|sonic hedgehog protein'
                          },
                }
    """

    def __init__(self, kg_version: str, flag: str, write_location: str, kg_location: str, node_data: Optional[str],
                 node_dict: Dict[str, Dict[str, str]]) -> None:

        self.kg_version = kg_version
        self.node_metadata_flag = flag
        self.write_location = write_location
        self.full_kg = kg_location
        self.node_data = node_data
        self.node_dict = node_dict

    def node_metadata_processor(self) -> None:
        """Processes a directory of node data sets by reading in each data set and then converting the read in data
        into a dictionary, which is then added to the class attribute node_dict. This dictionary stores the "ID"
        column of each data frame as the keys and all other columns as values. For example:
            {'variant-gene': {
                397508135: {'Label': 'NM_000492.3(CFTR):c.*80T>G',
                            'Description': 'This variant is a germline single nucleotide variant that results when a T
                                allele is changed to G on chromosome 7 (NC_000007.14, start:117667188/stop:117667188
                                positions, cytogenetic location:7q31.2) and has clinical significance not provided.
                                This entry is for the GRCh38 and was last reviewed on - with review status "no
                                assertion provided".'
                            }
                              }
            }

        Assumptions:
            1 - Each edge contains a dictionary.
            2 - Each edge's dictionary is keyed by "ID" and contains at least 1 of the following: "Label",
                "Description", and "Symbol".

        Returns:
            None.
        """

        if self.node_data:
            # create list where first item is edge type and the second item is the df
            dfs = [[re.sub('.*/', '', re.sub('((_[^/]*)_.*$)', '', x)),
                    pandas.read_csv(x, header=0, delimiter='\t')] for x in self.node_data]

            # convert each data frame to dictionary, using the "ID" column as the index
            for i in range(0, len(dfs)):
                df_processed = dfs[i][1].astype(str)
                df_processed.drop_duplicates(keep='first', inplace=True)
                df_processed.set_index('ID', inplace=True)
                df_dict = df_processed.to_dict('index')

                # add data frame to master node metadata dictionary
                self.node_dict[dfs[i][0]] = df_dict

    def creates_node_metadata(self, node: str, edge_type: str, url: str, graph: Graph) -> Graph:
        """Given a node in the knowledge graph, if the node has metadata information, new edges are created to add
        the metadata to the knowledge graph. Metadata that is added includes: labels, descriptions, and synonyms.

        Note. Metadata edges will only be added to the knowledge graph if node_metadata_flag='yes'.

        Args:
            node: A node identifier (e.g. 'HP_0003269', 'rs765907815').
            edge_type: A string which specifies the edge type (e.g. chemical-gene).
            url: The node's url needed to form a complete uri (e.g. http://purl.obolibrary.org/obo/), which is
                specified in the resource_info.txt document.
            graph: An rdflib graph containing knowledge graph data.

        Returns:
            graph: An rdflib graph with updated metadata edges.
        """

        # set namespace arguments
        obo = Namespace('http://purl.obolibrary.org/obo/')
        oboinowl = Namespace('http://www.geneontology.org/formats/oboInOwl#')

        # create metadata dictionary
        metadata = self.node_dict[edge_type][node]

        if self.node_metadata_flag == 'yes':
            if 'Label' in metadata.keys() and metadata['Label'] != 'None':
                graph.add((URIRef(url + str(node)), RDFS.label, Literal(metadata['Label'])))
            if 'Description' in metadata.keys() and metadata['Description'] != 'None':
                graph.add((URIRef(url + str(node)), URIRef(obo + 'IAO_0000115'), Literal(metadata['Description'])))
            if 'Synonym' in metadata.keys() and metadata['Synonym'] != 'None':
                for syn in metadata['Synonym'].split('|'):
                    graph.add((URIRef(url + str(node)), URIRef(oboinowl + 'hasExactSynonym'), Literal(syn)))
        else:
            pass

        return graph

    def adds_ontology_annotations(self, filename: str, graph: Graph) -> Graph:
        """Updates the ontology annotation information for an input knowledge graph or ontology.

        Args:
            filename: A string containing the name of a knowledge graph.
            graph: An rdflib graph object.

        Returns:
            graph: An rdflib graph object with edited ontology annotations.
        """

        print('\n*** Adding Ontology Annotation ***')

        # set namespaces
        oboinowl = Namespace('http://www.geneontology.org/formats/oboInOwl#')
        owl = Namespace('http://www.w3.org/2002/07/owl#')

        # set annotation variables
        authors = 'Authors: Tiffany J. Callahan, William A. Baumgartner, Ignacio Tripodi, Adrianne L. Stefanski'
        date = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')

        # convert filename to permanent url
        parsed_filename = '_'.join(filename.lower().split('/')[-1].split('_')[2:])
        url = 'https://pheknowlator.com/pheknowlator_' + parsed_filename

        # query ontology to obtain existing ontology annotations
        results = graph.query(
            """SELECT DISTINCT ?o ?p ?s
                WHERE {
                    ?o rdf:type owl:Ontology .
                    ?o ?p ?s . }
            """, initNs={'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                         'owl': 'http://www.w3.org/2002/07/owl#'})

        # iterate over annotations and remove existing annotations
        for res in results:
            graph.remove(res)

        # add new annotations
        graph.add((URIRef(url + '.owl'), RDF.type, URIRef(owl + 'Ontology')))
        graph.add((URIRef(url + '.owl'), URIRef(oboinowl + 'default-namespace'), Literal(filename)))
        graph.add((URIRef(url + '.owl'), URIRef(owl + 'versionIRI'), URIRef(url + '/wiki/' + self.kg_version)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal('PheKnowLator Release version ' + self.kg_version)))
        graph.add((URIRef(url + '.owl'), URIRef(oboinowl + 'date'), Literal(date)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal(authors)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal('For more information please visit: ' + url)))

        return graph

    @staticmethod
    def ontology_file_formatter(graph_location: str) -> None:
        """Reformat an .owl file to be consistent with the formatting used by the OWL API. To do this, an ontology
        referenced by graph_location is read in and output to the same location via the OWLTools API.

        Args:
            graph_location: A string naming the location of an ontology.

        Returns:
            None.

        Raises:
            TypeError: If something other than an .owl file is passed to function.
            TypeError: If the input file contains no data.
        """

        print('\n*** Applying OWL API Formatting to Knowledge Graph OWL File ***')

        # check input owl file
        if '.owl' not in graph_location:
            raise TypeError('ERROR: The provided file is not type .owl')
        elif os.stat(graph_location).st_size == 0:
            raise TypeError('ERROR: input file: {} is empty'.format(graph_location))
        else:
            try:
                subprocess.check_call(['./resources/lib/owltools', graph_location, '-o', graph_location])
            except subprocess.CalledProcessError as error:
                print(error.output)

        return None

    def removes_annotation_assertions(self) -> None:
        """Utilizes OWLTools to remove annotation assertions. The '--remove-annotation-assertions' method in OWLTools
        removes annotation assertions to make a pure logic subset', which reduces the overall size of the knowledge
        graph, while still being compatible with a reasoner.

        Returns:
            None.
        """

        # remove annotation assertions
        try:
            subprocess.check_call(['./resources/lib/owltools',
                                   self.write_location + self.full_kg,
                                   '--remove-annotation-assertions',
                                   '-o',
                                   self.write_location + self.full_kg[:-4] + '_NoAnnotationAssertions.owl'])
        except subprocess.CalledProcessError as error:
            print(error.output)

        return None

    @staticmethod
    def adds_annotation_assertions(graph: Graph, filename: str) -> Graph:
        """Adds edges removed from the knowledge graph when annotation assertions were removed. First, the knowledge
        graph that was created prior to removing annotation assertions is read in. Then, the function iterates over the
        closed knowledge graph files and adds any edges that are present in the list, but missing from the closed
        knowledge graph.

        Args:
            graph: An rdflib graph object.
            filename: A string containing the filepath to the annotation assertions edge list.

        Returns:
            graph: An rdflib graph object that includes annotation assertions.
        """

        # read in list of removed annotation assertions
        assertions = Graph()
        assertions.parse(filename)

        # iterate over removed assertions and add them back to closed graph
        for edge in tqdm(assertions):
            graph.add(edge)

        return graph

    def adds_node_metadata(self, graph: Graph, edge_dict: Dict[str, List[Union[str, List[Union[str, List[str]]]]]])\
            -> Graph:
        """Iterates over nodes in each edge in the edge_dict, by edge_type. If the node has metadata available in the
        the node_data dictionary, then it is added to the knowledge graph.

        Args:
            graph: An rdflib graph object.
            edge_dict: A nested dictionary storing the master edge list for the knowledge graph. Where the outer key is
            an edge-type (e.g. gene-cell) and each inner key contains a dictionary storing details from the
            resource_info.txt input document. For example:
            {'chemical-complex': {'source_labels': ';;',
                                  'data_type': 'class-instance',
                                  'edge_relation': 'RO_0002436',
                                  'uri': ['http://purl.obolibrary.org/obo/', 'https://reactome.org/content/detail/'],
                                  'row_splitter': 'n',
                                  'column_splitter': 't',
                                  'column_idx': '0;1',
                                  'identifier_maps': 'None',
                                  'evidence_criteria': 'None',
                                  'filter_criteria': 'None',
                                  'edge_list': [['CHEBI_24505', 'R-HSA-1006173'], ...]
                                  },
            }

        Returns:
            graph: An rdflib graph object that includes node metadata.
        """

        for edge_type in edge_dict.keys():
            if 'instance' not in edge_dict[edge_type]['data_type']:
                pass
            else:
                node_idx = [x for x in range(2) if edge_dict[edge_type]['data_type'].split('-')[x] == 'instance']

                for edge in tqdm(edge_dict[edge_type]['edge_list']):
                    if self.node_dict and edge_type in self.node_dict.keys():
                        if len(node_idx) == 2:
                            subj, obj = edge.split('-')[0], edge.split('-')[1]

                            if subj and obj in self.node_dict[edge_type].keys():
                                subj_uri = str(edge_dict[edge_type]['uri'][0])
                                obj_uri = str(edge_dict[edge_type]['uri'][1])
                                self.creates_node_metadata(subj, edge_type, subj_uri, graph)
                                self.creates_node_metadata(obj, edge_type, obj_uri, graph)
                        else:
                            subj = edge.split('-')[node_idx]

                            if subj in self.node_dict[edge_type].keys():
                                node_uri = str(edge_dict[edge_type]['uri'][0])
                                self.creates_node_metadata(subj, edge_type, node_uri, graph)

        # print kg statistics
        edges = len(set(list(graph)))
        nodes = len(set([str(node) for edge in list(graph) for node in edge[0::2]]))
        print('\nThe KG with node data contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        return graph

    def extracts_class_metadata(self, graph: Graph) -> None:
        """Functions queries the knowledge graph to obtain labels, definitions/descriptions, and synonyms for
        classes. This information is then added to the existing self.node_dict dictionary under the key of 'classes'.
        Each metadata type is saved as a dictionary key with the actual string stored as the value. The metadata types
        are packaged as a dictionary which is stored as the value to the node identifier as the key.

        Args:
            graph: An rdflib graph object.

        Returns:
            None.
        """

        self.node_dict['classes'] = {}

        # query knowledge graph to obtain metadata
        results = graph.query(
            """SELECT DISTINCT ?class ?class_label ?class_definition ?class_syn
                   WHERE {
                      ?class rdf:type owl:Class .
                      ?class rdfs:label ?class_label .
                      ?class obo:IAO_0000115 ?class_definition .
                      ?class oboinowl:hasExactSynonym ?class_syn .}
                   """, initNs={'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                                'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                                'owl': 'http://www.w3.org/2002/07/owl#',
                                'obo': 'http://purl.obolibrary.org/obo/',
                                'oboinowl': 'http://www.geneontology.org/formats/oboInOwl#'})

        # convert results to dictionary
        for result in tqdm(results):
            node = str(result[0]).split('/')[-1]

            if node in self.node_dict['classes'].keys():
                self.node_dict['classes'][node]['Synonym'].append(str(result[3]))
            else:
                self.node_dict['classes'][node] = {}
                self.node_dict['classes'][node]['Label'] = str(result[1])
                self.node_dict['classes'][node]['Description'] = str(result[2])
                self.node_dict['classes'][node]['Synonym'] = [str(result[3])]

        return None

    def maps_node_labels_to_integers(self, graph: Graph, output_triple_integers: str, output_loc: str) -> None:
        """Loops over the knowledge graph in order to create three different types of files:
            - Integers: a tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
            subject, predicate, object). The subject, predicate, and object identifiers have been mapped to integers.
            - Identifiers: a tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
            subject, predicate, object). Both the subject and object identifiers have not been mapped to integers.
            - Identifier-Integer Map: a `.json` file containing a dictionary where the keys are node identifiers and
            the values are integers.

        Args:
            graph: an rdflib graph object.
            output_triple_integers: the name and file path to write out results.
            output_loc: the name and file path to write out results.

        Returns:
            None.

        Raises:
            ValueError: If the length of the graph is not the same as the number of extracted triples.
        """

        # create dictionary for mapping and list to write edges to
        node_map, output_triples, node_counter = {}, [], 0

        # build graph from input file and set counter
        out_ints = open(self.write_location + output_triple_integers, 'w')
        label_location = self.write_location + '_'.join(output_triple_integers.split('_')[:-1]) + '_Identifiers.txt'
        out_labs = open(label_location, 'w')

        # write file headers
        out_ints.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
        out_labs.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')

        for edge in tqdm(graph):
            if str(edge[0]) not in node_map:
                node_counter += 1
                node_map[str(edge[0])] = node_counter
            if str(edge[1]) not in node_map:
                node_counter += 1
                node_map[str(edge[1])] = node_counter
            if str(edge[2]) not in node_map:
                node_counter += 1
                node_map[str(edge[2])] = node_counter

            # convert edge labels to ints
            subj, pred, obj = str(edge[0]), str(edge[1]), str(edge[2])
            output_triples.append([node_map[subj], node_map[pred], node_map[obj]])
            out_ints.write('%d' % node_map[subj] + '\t' + '%d' % node_map[pred] + '\t' + '%d' % node_map[obj] + '\n')
            out_labs.write(subj + '\t' + pred + '\t' + obj + '\n')

        out_ints.close(), out_labs.close()

        # CHECK - verify we get the number of edges that we would expect to get
        if len(graph) != len(output_triples):
            raise ValueError('ERROR: The number of triples is incorrect!')
        else:
            json.dump(node_map, open(self.write_location + '/' + output_loc, 'w'))

        return None

    def output_knowledge_graph_metadata(self, graph: Graph) -> None:
        """Loops over the self.node_dict dictionary and writes out the data to a file locally. The data is stored as
        a tab-delimited '.txt' file with four columns: (1) node identifier; (2) node label; (3) node description or
        definition; and (4) node synonym.

        Args:
            graph: A rdflib graph object.

        Returns:
            None.
        """

        # add metadata for nodes that are data type class to self.node_dict
        self.extracts_class_metadata(graph)

        with open(self.write_location + self.full_kg[:-6] + 'NodeLabels.txt', 'w') as outfile:
            # write file header
            outfile.write('node_id' + '\t' + 'label' + '\t' + 'description/definition' + '\t' + 'synonym' + '\n')

            for edge_type in tqdm(self.node_dict.keys()):
                for node in self.node_dict[edge_type]:
                    node_id = node
                    label = self.node_dict[edge_type][node]['Label']
                    desc = self.node_dict[edge_type][node]['Description']
                    syn_list = self.node_dict[edge_type][node]['Synonym']

                    if isinstance(syn_list, list) and len(syn_list) > 1:
                        syn = '|'.join(syn_list)
                    elif isinstance(syn_list, list) and len(syn_list) == 1:
                        syn = syn_list[0]
                    else:
                        syn = syn_list

                    outfile.write(node_id + '\t' + label + '\t' + desc + '\t' + syn + '\n')

        outfile.close()

        return None
