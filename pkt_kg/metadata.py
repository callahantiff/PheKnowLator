#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import datetime
import glob
import json
import os
import os.path
import pandas  # type: ignore
import re
import subprocess

from rdflib import Graph, Literal, Namespace, URIRef   # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Dict, List, Optional, Set, Union

# set environmental variables
oboinowl = Namespace('http://www.geneontology.org/formats/oboInOwl#')
obo = Namespace('http://purl.obolibrary.org/obo/')


class Metadata(object):
    """Class helps manage knowledge graph metadata.

    Attributes:
        kg_version: A string that contains the version of the knowledge graph build.
        write_location: A filepath to the knowledge graph directory (e.g. './resources/knowledge_graphs).
        kg_location: The subdirectory and name of the knowledge graph (e.g.'/relations_only/KG.owl').
        node_data: A filepath to a directory called 'node_data' containing a file for each instance/subclass node.
        node_dict: A nested dictionary storing metadata for the nodes in the edge_dict. Where the outer key is a node
            identifier and each inner key is an identifier type keyed by the identifier type with the value being the
            corresponding metadata for that identifier type. For example:
                {'6469': {'Label': 'SHH',
                          'Description': 'Sonic Hedgehog Signaling Molecule is a protein-coding gene that is located on
                                          chromosome 7 (map_location: 7q36.3).',
                          'Synonym': 'HHG1|HLP3|HPE3|MCOPCB5|SMMCI|ShhNC|TPT|TPTPS|sonic hedgehog protein'}}
    """

    def __init__(self, kg_version: str, write_location: str, kg_location: str, node_data: Optional[List],
                 node_dict: Optional[Dict]) -> None:

        self.kg_version = kg_version
        self.write_location = write_location
        self.full_kg = kg_location
        self.node_data = node_data
        self.node_dict = node_dict

    def node_metadata_processor(self) -> None:
        """Processes a directory of node data sets by reading in each data set and then converting the read in data
        into a dictionary, which is then added to the class attribute node_dict. This dictionary stores the "ID"
        column of each data frame as the keys and all other columns as values. For example:
            {'variant-gene': {397508135: {'Label': 'NM_000492.3(CFTR):c.*80T>G',
                            'Description': 'This variant is a germline single nucleotide variant that results when a T
                                allele is changed to G on chromosome 7 (NC_000007.14, start:117667188/stop:117667188
                                positions, cytogenetic location:7q31.2) and has clinical significance not provided.
                                This entry is for the GRCh38 and was last reviewed on - with review status "no
                                assertion provided".'}}}

        Assumptions:
            1 - Each edge contains a dictionary.
            2 - Each edge's dictionary is keyed by "ID" and contains at least 1 of the following: "Label",
                "Description", and "Symbol".

        Returns:
            None.
        """

        if self.node_data:
            print('Loading and Processing Node Metadata')
            # create list where first item is edge type and the second item is the df
            dfs = [[re.sub('.*/', '', re.sub('((_[^/]*)_.*$)', '', x)),
                    pandas.read_csv(x, header=0, delimiter='\t')] for x in self.node_data]
            # convert each data frame to dictionary, using the 'ID' column as the index
            for i in range(0, len(dfs)):
                df_processed = dfs[i][1].astype(str)
                df_processed.drop_duplicates(keep='first', inplace=True)
                df_processed.set_index('ID', inplace=True)
                df_dict = df_processed.to_dict('index')
                # add data frame to master node metadata dictionary
                self.node_dict[dfs[i][0]] = df_dict  # type: ignore

        return None

    def creates_node_metadata(self, node: str, edge_type: str, url: str, graph: Graph) -> Graph:
        """Given a node in the knowledge graph, if the node has metadata information, new edges are created to add
        the metadata to the knowledge graph. Metadata that is added includes: labels, descriptions, and synonyms.

        Args:
            node: A node identifier (e.g. 'HP_0003269', 'rs765907815').
            edge_type: A string which specifies the edge type (e.g. chemical-gene).
            url: The node's url needed to form a complete uri (e.g. http://purl.obolibrary.org/obo/), which is
                specified in the resource_info.txt document.
            graph: An rdflib graph containing knowledge graph data.

        Returns:
            graph: An rdflib graph with updated metadata edges.
        """

        if self.node_dict:
            metadata = self.node_dict[edge_type][node]
            if 'Label' in metadata.keys() and metadata['Label'] != 'None':
                graph.add((URIRef(url + str(node)), RDFS.label, Literal(metadata['Label'])))
            if 'Description' in metadata.keys() and metadata['Description'] != 'None':
                graph.add((URIRef(url + str(node)), URIRef(obo + 'IAO_0000115'), Literal(metadata['Description'])))
            if 'Synonym' in metadata.keys() and metadata['Synonym'] != 'None':
                for syn in metadata['Synonym'].split('|'):
                    graph.add((URIRef(url + str(node)), URIRef(oboinowl + 'hasExactSynonym'), Literal(syn)))
            return graph
        else:
            return graph

    def adds_node_metadata(self, graph: Graph, edge_dict: Optional[Dict]) -> Graph:
        """Iterates over nodes in each edge in the edge_dict, by edge_type. If the node has metadata available in the
        the node_data dictionary, then it is added to the knowledge graph.

        Args:
            graph: An RDFLib graph object.
            edge_dict: A nested dictionary storing the master edge list for the knowledge graph. Where the outer key is
            an edge-type (e.g. gene-cell) and each inner key contains a dictionary storing details from the
            resource_info.txt input document. For example:
            {'chemical-complex': {'source_labels': ';;', 'data_type': 'class-instance', 'edge_relation': 'RO_0002436',
                                  'uri': ['http://ex', 'https://ex'], 'row_splitter': 'n', 'column_splitter': 't',
                                  'column_idx': '0;1', 'identifier_maps': 'None', 'evidence_criteria': 'None',
                                  'filter_criteria': 'None', 'edge_list': [['CHEBI_24505', 'R-HSA-1006173'], ...]}}

        Returns:
            graph: An rdflib graph object that includes node metadata.
        """

        print('\nAdding Non-Ontology Data Metadata to Knowledge Graph')

        if edge_dict:
            for edge_type in tqdm(edge_dict.keys()):
                edge_data_type = edge_dict[edge_type]['data_type']
                if edge_data_type.split('-')[0] == 'class' and edge_data_type.split('-')[1] == 'class':
                    pass
                else:
                    print('Processing ontology terms of {} edge type'.format(edge_data_type))
                    node_idx = [x for x in range(2) if edge_data_type.split('-')[x] == 'entity']
                    for edge in tqdm(edge_dict[edge_type]['edge_list']):
                        if self.node_dict and edge_type in self.node_dict.keys():
                            if len(node_idx) == 2:
                                subj, obj = edge[0], edge[1]
                                if subj in self.node_dict[edge_type].keys() and obj in self.node_dict[edge_type].keys():
                                    subj_uri = str(edge_dict[edge_type]['uri'][0])
                                    obj_uri = str(edge_dict[edge_type]['uri'][1])
                                    graph = self.creates_node_metadata(subj, edge_type, subj_uri, graph)
                                    graph = self.creates_node_metadata(obj, edge_type, obj_uri, graph)
                            else:
                                inst_node = edge[node_idx[0]]
                                if inst_node in self.node_dict[edge_type].keys():
                                    node_uri = str(edge_dict[edge_type]['uri'][0])
                                    graph = self.creates_node_metadata(inst_node, edge_type, node_uri, graph)

            return graph

    def removes_annotation_assertions(self, owltools_location: str = os.path.abspath('./pkt_kg/libs/owltools')) -> None:
        """Utilizes OWLTools to remove annotation assertions. The '--remove-annotation-assertions' method in OWLTools
        removes annotation assertions to make a pure logic subset', which reduces the overall size of the knowledge
        graph, while still being compatible with a reasoner.

        Note. This method is usually only applied to partial builds.

        Args:
            owltools_location: A string pointing to the location of the owl tools library.

        Returns:
            None.
        """

        # remove annotation assertions
        try:
            subprocess.check_call([owltools_location, self.write_location + self.full_kg,
                                   '--remove-annotation-assertions',
                                   '-o', self.write_location + self.full_kg[:-4] + '_NoAnnotationAssertions.owl'])
        except subprocess.CalledProcessError as error:
            print(error.output)

        return None

    @staticmethod
    def adds_annotation_assertions(graph: Graph, filename: str) -> Graph:
        """Adds edges removed from the knowledge graph when annotation assertions were removed. First, the knowledge
        graph that was created prior to removing annotation assertions is read in. Then, the function iterates over the
        closed knowledge graph files and adds any edges that are present in the list, but missing from the closed
        knowledge graph.

        Note. This method is usually only applied to post-closure builds.

        Args:
            graph: An rdflib graph object.
            filename: A string containing the filepath to the full knowledge graph that includes annotation assertions.

        Returns:
            graph: An rdflib graph object that includes annotation assertions.
        """

        assertions = Graph().parse(filename)
        for edge in tqdm(assertions):
            graph.add(edge)

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

        print('\nExtracting Class Metadata')

        if self.node_dict:
            self.node_dict['classes'] = {}
            results = graph.query(
                """SELECT DISTINCT ?class ?class_label ?class_definition ?class_syn
                       WHERE {
                          ?class rdf:type owl:Class .
                          optional{?class rdfs:label ?class_label .}
                          optional{?class obo:IAO_0000115 ?class_definition .}
                          optional{?class oboinowl:hasExactSynonym ?class_syn .}}
                       """, initNs={'rdf': RDF, 'rdfs': RDFS, 'owl': OWL, 'obo': obo, 'oboinowl': oboinowl})

            # convert results to dictionary
            for result in tqdm(results):
                node = str(result[0]).split('/')[-1]
                if node in self.node_dict['classes'].keys():
                    self.node_dict['classes'][node]['Label'].append(str(result[1]) if result[1] else 'None')
                    self.node_dict['classes'][node]['Description'].append(str(result[2]) if result[2] else 'None')
                    self.node_dict['classes'][node]['Synonym'].append(str(result[3]) if result[3] else 'None')
                else:
                    self.node_dict['classes'][node] = {}
                    self.node_dict['classes'][node]['Label'] = [str(result[1]) if result[1] else 'None']
                    self.node_dict['classes'][node]['Description'] = [str(result[2]) if result[2] else 'None']
                    self.node_dict['classes'][node]['Synonym'] = [str(result[3]) if result[3] else 'None']

        return None

    def output_knowledge_graph_metadata(self, graph: Graph) -> None:
        """Loops over the self.node_dict dictionary and writes out the data to a file locally. The data is stored as
        a tab-delimited '.txt' file with four columns: (1) node identifier; (2) node label; (3) node description or
        definition; and (4) node synonym.

        NOTE. Not every node in the knowledge class will have metadata. There are some non-ontology nodes that are
        added (e.g. Ensembl transcript identifiers) that at the time of adding did not include labels, synonyms,
        or definitions. While these nodes have valid metadata through their original provider, this data may not have
        been available for download and thus would not have been added to the node_dict.

        Args:
            graph: A rdflib graph object.

        Returns:
            None.
        """

        # add metadata for nodes that are data type class to self.node_dict
        self.extracts_class_metadata(graph)
        node_tracker: Set = set()  # creat set to ensure nodes are not written out multiple times

        if self.node_dict:
            print('\nWriting Class Metadata')
            with open(self.write_location + self.full_kg[:-6] + 'NodeLabels.txt', 'w', encoding='utf-8') as outfile:
                outfile.write('node_id' + '\t' + 'label' + '\t' + 'description/definition' + '\t' + 'synonym' + '\n')
                for edge_type in tqdm(self.node_dict.keys()):
                    for node in self.node_dict[edge_type]:
                        if node not in node_tracker:
                            node_tracker |= {node}  # increment node tracker
                            # labels
                            label_list = self.node_dict[edge_type][node]['Label']
                            if isinstance(label_list, list) and len(label_list) > 1: label = '|'.join(set(label_list))
                            elif isinstance(label_list, list) and len(label_list) == 1: label = label_list[0]
                            else: label = label_list
                            # descriptions
                            desc_list = self.node_dict[edge_type][node]['Description']
                            if isinstance(desc_list, list) and len(desc_list) > 1: desc = '|'.join(set(desc_list))
                            elif isinstance(desc_list, list) and len(desc_list) == 1: desc = desc_list[0]
                            else: desc = desc_list
                            # synonyms
                            syn_list = self.node_dict[edge_type][node]['Synonym']
                            if isinstance(syn_list, list) and len(syn_list) > 1: syn = '|'.join(set(syn_list))
                            elif isinstance(syn_list, list) and len(syn_list) == 1: syn = syn_list[0]
                            else: syn = syn_list
                            try:
                                outfile.write(node + '\t' + label + '\t' + desc + '\t' + syn + '\n')
                            except UnicodeEncodeError:
                                outfile.write(node + '\t' +
                                              label.encode('utf-8').decode() + '\t' +
                                              desc.encode('utf-8').decode() + '\t' +
                                              syn.encode('utf-8').decode() + '\n')
            outfile.close()

        return None

    def adds_ontology_annotations(self, filename: str, graph: Graph) -> Graph:
        """Updates the ontology annotation information for an input knowledge graph or ontology.

        Args:
            filename: A string containing the name of a knowledge graph.
            graph: An rdflib graph object.

        Returns:
            graph: An rdflib graph object with edited ontology annotations.
        """

        print('\n*** Adding Ontology Annotations ***')

        # set annotation variables
        authors = 'Authors: Tiffany J. Callahan, William A. Baumgartner, Ignacio Tripodi, Adrianne L. Stefanski'
        date = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        # convert filename to permanent url
        parsed_filename = '_'.join(filename.lower().split('/')[-1].split('_')[2:])
        url = 'https://pheknowlator.com/pheknowlator_' + parsed_filename
        pkt_url = 'https://github.com/callahantiff/PheKnowLator'

        # query ontology to obtain existing ontology annotations and remove them
        results = graph.query(
            """SELECT DISTINCT ?o ?p ?s
                WHERE {
                    ?o rdf:type owl:Ontology .
                    ?o ?p ?s . }
            """, initNs={'rdf': RDF, 'owl': OWL})
        for res in results:
            graph.remove(res)

        # add new annotations
        graph.add((URIRef(url + '.owl'), RDF.type, URIRef(OWL + 'Ontology')))
        graph.add((URIRef(url + '.owl'), URIRef(oboinowl + 'default-namespace'), Literal(filename)))
        graph.add((URIRef(url + '.owl'), URIRef(OWL + 'versionIRI'), URIRef(pkt_url + '/wiki/' + self.kg_version)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal('PheKnowLator Release version ' + self.kg_version)))
        graph.add((URIRef(url + '.owl'), URIRef(oboinowl + 'date'), Literal(date)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal(authors)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal('For more information visit: ' + pkt_url)))

        return graph
