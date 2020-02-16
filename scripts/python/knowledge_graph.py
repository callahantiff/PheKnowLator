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
import uuid

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS
from tqdm import tqdm


class KGBuilder(object):
    """Class creates a semantic knowledge graph.

    The class implements several methods, which are designed to construct a semantic knowledge graph given a
    dictionary of pre-processed knowledge graph edges, a directory of ontology relation data, and a directory of node
    metadata. The class is designed to facilitate three types of knowledge graph builds:
        1 - Full: Runs all build steps in the algorithm.
        2 - Partial: Runs all of the build steps in the algorithm through adding the class-class, instance-class,
            class-instance, and instance-instance edges. Designed for those wanting to run a reasoner on a pure logic
            subset of the knowledge graph.
        3 - Post-Closure: Assumes that a reasoner was run over a knowledge graph and that the remaining build steps
            should be applied to a closed knowledge graph.

    Attributes:
        kg_version: A string that contains the version of the knowledge graph build.
        build: A string that indicates what kind of build.
        write_location: A file path used for writing knowledge graph data.
        edge_data: A path to a file that references a dictionary of edge list tuples used to build the knowledge graph.
        node_data: A filepath to a directory called 'node_data' containing a file for each instance node.
        relations_data: A filepath to a directory called 'relations_data' containing the relations data.
        remove_owl_semantics: A string indicating whether edges containing owl semantics should be removed.
        edge_dict: A nested dictionary storing the master edge list for building the knowledge graph. Where the outer
            key is an edge-type (e.g. gene-cell) and each inner key contains a dictionary storing details from the
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
        node_dict: A nested dictionary storing metadata for the nodes in the edge_dict. Where the outer key is a node
            identifier and each inner key is an identifier type keyed by the identifier type with the value being the
            corresponding metadata for that identifier type. For example:
                {'6469': {'Label': 'SHH',
                          'Description': 'Sonic Hedgehog Signaling Molecule is a protein-coding gene that is
                                          located on chromosome 7 (map_location: 7q36.3).',
                          'Synonym': 'HHG1|HLP3|HPE3|MCOPCB5|SMMCI|ShhNC|TPT|TPTPS|sonic hedgehog protein'
                          },
                }
        relations_dict: A dictionary storing the relation identifiers and labels. An example
            {'RO_0002616': 'related via evidence or inference to', 'RO_0002442': 'mutualistically interacts with}
        inverse_relations_dict: A dictionary storing relations ids and their inverse relation ids. For example:
            {'RO_0000056': 'RO_0000057', 'RO_0000079': 'RO_0000085'}
        merged_ont_kg: A string containing the filename of the knowledge graph that only contains the merged ontologies.
        full_kg: A string containing the filename for the full knowledge graph.
        ontologies: A list of file paths to an .owl file containing ontology data.
        kg_uuid_map: A dictionary storing the mapping between a class and its instance. The keys are the original class
             uri and the values are the hashed uuid of the uri needed to create an instance of the class. For example:
             {'http://purl.obolibrary.org/obo/CHEBI_24505':
              'https://github.com/callahantiff/PheKnowLator/obo/ext/d1552fc9-a91b-414a-86cb-885f8c4822e7'}
        graph: An rdflib graph object which stores the knowledge graph.
    """

    def __init__(self, kg_version: str, build: str, write_location: str, edge_data: str = None, node_data: dict = None,
                 relations_data: dict = None, remove_owl_semantics: str = None):

        # set build type
        if build.lower() not in ['full', 'partial', 'post-closure']:
            raise ValueError('ERROR: build must be "full", "partial", or "post-closure"')
        else:
            self.build = build.lower()

        # set build version
        if kg_version is None:
            raise ValueError('ERROR: kg_version must not contain a valid version e.g. v.2.0.0')
        else:
            self.kg_version = kg_version

        # set write location
        if write_location is None:
            raise ValueError('ERROR: write_location must not contain a valid filepath')
        else:
            self.write_location = write_location

        # read in knowledge graph edge list
        if edge_data is None:
            raise ValueError('ERROR: edge_data must not contain a valid filepath')
        elif os.stat(edge_data).st_size == 0:
            raise Exception('FILE ERROR: input file: {} is empty'.format(edge_data))
        else:
            self.edge_dict = json.load(open(edge_data, 'r'))

        # set file names for writing data
        header = '/PheKnowLator_' + self.build + '_'
        self.merged_ont_kg = '/PheKnowLator_MergedOntologies.owl'

        # read in relations data
        if relations_data.lower() not in ['yes', 'no']:
            raise ValueError('ERROR: relations_data must be "no" or "yes"')
        else:
            if relations_data.lower() == 'yes':
                if len(glob.glob('*/relations_data/*.txt', recursive=True)) == 0:
                    raise Exception('ERROR: the relations_data directory is empty')
                else:
                    self.relations_data = glob.glob('*/relations_data/*.txt', recursive=True)
                    self.relations_dict = dict()
                    self.inverse_relations_dict = dict()
                    kg_rel = '/inverse_relations' + header + 'InverseRelations_'
            else:
                self.relations_data, self.relations_dict, self.inverse_relations_dict = None, None, None
                kg_rel = '/relations_only' + header

        # read in node metadata
        if node_data.lower() not in ['yes', 'no']:
            raise ValueError('ERROR: node_data must be "no" or "yes"')
        else:
            if node_data.lower() == 'yes':
                if len(glob.glob('*/node_data/*.txt', recursive=True)) == 0:
                    raise Exception('ERROR: the node_data directory is empty')
                else:
                    self.node_data = glob.glob('*/node_data/*.txt', recursive=True)
                    self.node_dict = dict()

                    if self.build == 'post-closure':
                        kg_node = kg_rel + 'Closed_'
                    else:
                        kg_node = kg_rel + 'NotClosed_'
            else:
                self.node_data, self.node_dict = None, None

                if self.build == 'post-closure':
                    kg_node = kg_rel + 'NoNodeMetadata_Closed_'
                else:
                    kg_node = kg_rel + 'NoNodeMetadata_NotClosed_'

        # set owl semantics removal attribute
        if remove_owl_semantics.lower() not in ['yes', 'no']:
            raise ValueError('ERROR: remove_owl_semantics must be "no" or "yes"')
        else:
            if remove_owl_semantics.lower() == 'yes' and self.build != 'partial':
                self.full_kg = kg_node + 'NoOWLSemantics_KG.owl'
                self.remove_owl_semantics = 'yes'
            else:
                self.full_kg = kg_node + 'OWLSemantics_KG.owl'
                self.remove_owl_semantics = None

        # read in ontologies data
        self.ontologies: list = glob.glob('*/ontologies/*.owl')

        # set knowledge graph build files
        self.kg_uuid_map: dict = dict()
        self.graph: Graph = Graph()

    def sets_up_environment(self):
        """Sets-up the environment by checking for the existence of the following directories and if either do not
        exist, it creates them:
            - 'knowledge_graphs' directory in the `resources` directory
            - 'relations_only' directory in `resources/knowledge_graphs` directory, if performing a full or partial
               build and not adding inverse relations
            - 'inverse_relations' directory in `resources/knowledge_graphs` directory, if performing a full or partial
               build and creating inverse relations

        Returns:
            None.
        """

        if self.write_location not in glob.glob('./resources/**'): os.mkdir(self.write_location)

        if self.build in ['full', 'partial']:
            if isinstance(self.relations_data, list):
                if self.write_location + '/inverse_relations' not in glob.glob('./resources/**/**'):
                    os.mkdir(self.write_location + '/inverse_relations')
            else:
                if self.write_location + '/relations_only' not in glob.glob('./resources/**/**'):
                    os.mkdir(self.write_location + '/relations_only')

        return None

    def node_metadata_processor(self):
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

    def reverse_relation_processor(self):
        """Reads in data to a Pandas DataFrame from a user-provided filepath. The pandas DataFrame is then converted
        to a specific dictionary depending on whether it contains inverse relation data or relation data identifiers
        and labels. This distinction is derived from the filename (e.g. resources/relations_data/INVERSE_RELATIONS.txt).

        Returns:
            None.
        """

        if self.relations_data:
            for data in self.relations_data:
                df = pandas.read_csv(data, header=0, delimiter='\t')
                df.drop_duplicates(keep='first', inplace=True)
                df.set_index(list(df)[0], inplace=True)

                if 'inverse' in data.lower():
                    self.inverse_relations_dict = df.to_dict('index')
                else:
                    self.relations_dict = df.to_dict('index')

    def merges_ontologies(self):
        """Using the OWLTools API, each ontology listed in in the ontologies attribute is recursively merged with into
        a master merged ontology file and saved locally to the provided file path via the merged_ontology attribute.
        The function assumes that the file is written to the directory specified by the write_location attribute.

        Returns:
            None.
        """

        if not self.ontologies:
            self.graph.parse(self.write_location + self.merged_ont_kg)

            edges = len(set(list(self.graph)))
            nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
            print('\nThe merged ontology KG contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        else:
            if self.write_location + self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
                ont1, ont2 = self.ontologies.pop(), self.write_location + self.merged_ont_kg
            else:
                ont1, ont2 = self.ontologies.pop(), self.ontologies.pop()

            # merge ontologies
            print('\nMerging Ontologies: {ont1}, {ont2}\n'.format(ont1=ont1.split('/')[-1], ont2=ont2.split('/')[-1]))

            try:
                subprocess.check_call(['./resources/lib/owltools', str(ont1), str(ont2),
                                       '--merge-support-ontologies',
                                       '-o', self.write_location + self.merged_ont_kg])

            except subprocess.CalledProcessError as error:
                print(error.output)

            return self.merges_ontologies()

    def creates_node_metadata(self, node: str, edge_type: str, url: str):
        """Given a node in the knowledge graph, if the node has metadata information, new edges are created to add
        the metadata to the knowledge graph. Metadata that is added includes: labels, descriptions, and synonyms.

        Note. If running a "partial" build and node_data='yes', the algorithm will not add the metadata edges to the
        KG. This is because 'partial' builds are intended to be run in situations when the generated knowledge graph
        will be run through a reasoner and thus, the goal is to only include the edges necessary to close the graph.
        Node metadata can be added back to the KG by running the algorithm again with the build set to "post-closure".

        Args:
            node: A node identifier (e.g. 'HP_0003269', 'rs765907815').
            edge_type: A string which specifies the edge type (e.g. chemical-gene).
            url: The node's url needed to form a complete uri (e.g. http://purl.obolibrary.org/obo/), which is
                specified in the resource_info.txt document.

        Returns:
            None.
        """

        # set namespace arguments
        obo = Namespace('http://purl.obolibrary.org/obo/')
        oboinowl = Namespace('http://www.geneontology.org/formats/oboInOwl#')

        # create metadata dictionary
        metadata = self.node_dict[edge_type][node]

        if self.build != 'partial':
            if 'Label' in metadata.keys() and metadata['Label'] != 'None':
                self.graph.add((URIRef(url + str(node)), RDFS.label, Literal(metadata['Label'])))
            if 'Description' in metadata.keys() and metadata['Description'] != 'None':
                self.graph.add((URIRef(url + str(node)), URIRef(obo + 'IAO_0000115'), Literal(metadata['Description'])))
            if 'Synonym' in metadata.keys() and metadata['Synonym'] != 'None':
                for syn in metadata['Synonym'].split('|'):
                    self.graph.add((URIRef(url + str(node)), URIRef(oboinowl + 'hasExactSynonym'), Literal(syn)))
        else:
            pass

        return None

    def checks_for_inverse_relations(self, relation: str, edge_list: list):
        """Checks a relation to determine whether or not edges for an inverse relation should be created and added to
        the knowledge graph.

        Args:
            relation: A string that contains the relation assigned to edge in resource_info.txt (e.g. 'RO_0000056').
            edge_list: A list of tuples, where each tuple contains a knowledge graph edge. For example:
                    [['CHEBI_24505', 'R-HSA-1006173'], ['CHEBI_28879', 'R-HSA-1006173']]

        Returns:
            A string containing an ontology identifier (e.g. "RO_0000056). The value depends on a set of conditions:
                - inverse relation, if the stored relation has an inverse relation in inverse_relations
                - current relation, if the stored relation string includes 'interact' and there is an equal count of
                  each node type (i.e., this is checking for symmetry in interaction-based edge types)
                - None, assuming the prior listed conditions are not met
        """

        if self.relations_data:
            if relation in self.inverse_relations_dict.keys():
                return self.inverse_relations_dict[relation]['Inverse_Relation']
            elif relation in self.relations_dict.keys() and 'interact' in self.relations_dict[relation]['Label']:
                if len(set([x[0] for x in edge_list])) != len(set([x[1] for x in edge_list])):
                    return relation
            else:
                return None

    def adds_ontology_annotations(self, filename: str, graph: Graph):
        """Updates the ontology annotation information for an input knowledge graph or ontology.

        Args:
            filename: A string containing the name of a knowledge graph.
            graph: An RDFLib graph object.

        Returns:
            graph: An RDFLib graph object with edited ontology annotations.
        """

        print('\n*** ADDING ONTOLOGY ANNOTATIONS ***')

        # set namespaces
        oboinowl = Namespace('http://www.geneontology.org/formats/oboInOwl#')
        owl = Namespace('http://www.w3.org/2002/07/owl#')

        # set annotation variables
        authors = 'Authors: Tiffany J. Callahan, William A. Baumgartner, Ignacio Tripodi, Adrianne L. Stefanski'
        date = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')

        # convert filename to permanent url
        parsed_filename = '_'.join(filename.lower().split('/')[-1].split('_')[2:]).replace('_nodemetadata', '')
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
    def ontology_file_formatter(graph_location: str):
        """Reformat an .owl file to be consistent with the formatting used by the OWL API. To do this, an ontology
        referenced by graph_location is read in and output to the same location via the OWLTools API.

        Args:
            graph_location: A string naming the location of an ontology.

        Returns:
            None.

        Raises:
            An exception is raised if something other than an .owl file is passed to function.
            An exception is raised if the input file contains no data.
            An exception is raised if OWLTools API is unable to process the command line call.
        """

        print('\n*** REFORMATTING KNOWLEDGE GRAPH FILE FORMAT ***')

        # check input owl file
        if '.owl' not in graph_location:
            raise Exception('ERROR: The provided file is not type .owl')
        elif os.stat(graph_location).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(graph_location))
        else:
            try:
                subprocess.check_call(['./resources/lib/owltools', graph_location, '-o', graph_location])
            except subprocess.CalledProcessError as error:
                print(error.output)

        return None

    def creates_instance_instance_data_edges(self, edge_type: str):
        """Adds edges that contain nodes that are both of type instance to a knowledge graph. The typing of each node
        in an edge is specified in the resource_info.txt document. For each edge, the function will add metadata for
        nodes that are part of the edge and of type instance. The function will also check whether or not edges for
        inverse relations should also be created.

        Note. If node_dict is not None (i.e. metadata was provided via setting the node_dict='yes'), the function will
        only add edges to the knowledge graph that have metadata information (i.e. edges where each node has at least a
        label in the node_dict metadata dictionary).

        Args:
            edge_type: A string that contains the edge type being processed (i.e. chemical-gene).

        Returns:
            edge_counts: A list of the edges that were added to the knowledge graph.
        """

        edge_counts = []
        obo = Namespace('http://purl.obolibrary.org/obo/')

        # check for inverse relations
        if self.relations_data:
            inverse_relation = self.checks_for_inverse_relations(self.edge_dict[edge_type]['edge_relation'],
                                                                 self.edge_dict[edge_type]['edge_list'])
        else:
            inverse_relation = None

        for edge in tqdm(self.edge_dict[edge_type]['edge_list']):
            # adds node metadata information if present
            if self.node_dict:
                if edge_type in self.node_dict.keys():
                    if edge[0] in self.node_dict[edge_type].keys() and edge[1] in self.node_dict[edge_type].keys():
                        self.creates_node_metadata(edge[0], edge_type, str(self.edge_dict[edge_type]['uri'][0]))
                        self.creates_node_metadata(edge[1], edge_type, str(self.edge_dict[edge_type]['uri'][1]))

                        # add primary edge
                        self.graph.add((URIRef(str(self.edge_dict[edge_type]['uri'][0] + edge[0])),
                                        URIRef(str(obo + self.edge_dict[edge_type]['edge_relation'])),
                                        URIRef(str(self.edge_dict[edge_type]['uri'][1] + edge[1]))))

                        # add inverse relation
                        if inverse_relation:
                            self.graph.add((URIRef(str(self.edge_dict[edge_type]['uri'][1] + edge[1])),
                                            URIRef(str(obo + inverse_relation)),
                                            URIRef(str(self.edge_dict[edge_type]['uri'][0] + edge[0]))))

                        edge_counts.append(edge)

                # when no node metadata has been provided
                else:
                    self.graph.add((URIRef(str(self.edge_dict[edge_type]['uri'][0] + edge[0])),
                                    URIRef(str(obo + self.edge_dict[edge_type]['edge_relation'])),
                                    URIRef(str(self.edge_dict[edge_type]['uri'][1] + edge[1]))))

                    if inverse_relation:
                        self.graph.add((URIRef(str(self.edge_dict[edge_type]['uri'][1] + edge[1])),
                                        URIRef(str(obo + inverse_relation)),
                                        URIRef(str(self.edge_dict[edge_type]['uri'][0] + edge[0]))))

                    edge_counts.append(edge)

        return edge_counts

    def creates_class_class_data_edges(self, edge_type: str):
        """Adds edges that contain nodes that are both of type class to a knowledge graph. The typing of each node
        in an edge is specified in the resource_info.txt document. While adding each edge, the function will check
        whether or not edges for inverse relations should also be
        created.

        Note. Edges that contain nodes that are both of type class will include no metadata. This is because class data
        is derived from ontologies, which already contain metadata.

        Args:
            edge_type: A string that contains the edge type being processed (i.e. chemical-gene).

        Returns:
            edge_counts: A list of the edges that were added to the knowledge graph.
        """

        edge_counts = []

        # define namespaces
        obo = Namespace('http://purl.obolibrary.org/obo/')

        # check for inverse relations
        if self.relations_data:
            inverse_relation = self.checks_for_inverse_relations(self.edge_dict[edge_type]['edge_relation'],
                                                                 self.edge_dict[edge_type]['edge_list'])
        else:
            inverse_relation = None

        for edge in tqdm(self.edge_dict[edge_type]['edge_list']):
            # add primary edge
            self.graph.add((URIRef(str(self.edge_dict[edge_type]['uri'][0] + edge[0])),
                            URIRef(str(obo + self.edge_dict[edge_type]['edge_relation'])),
                            URIRef(str(self.edge_dict[edge_type]['uri'][1] + edge[1]))))

            # add inverse relation
            if inverse_relation:
                self.graph.add((URIRef(str(self.edge_dict[edge_type]['uri'][1] + edge[1])),
                                URIRef(str(obo + inverse_relation)),
                                URIRef(str(self.edge_dict[edge_type]['uri'][0] + edge[0]))))

            edge_counts.append(edge)

        return edge_counts

    def creates_instance_class_data_edges(self, edge_type: str):
        """Adds edges that contain one node of type instance and one node of type class to a knowledge graph. The
        typing of each node in an edge is specified in the resource_info.txt document. While adding each edge,
        the function will add metadata for nodes that are part of the edge and of type instance. The function will
        also check whether or not edges for inverse relations should also be created.

        Note. If node_dict is not None (i.e. metadata was provided via setting the node_dict='yes'), the function will
        only add edges to the knowledge graph that have metadata information (i.e. edges where each node has at least a
        label in the node_dict metadata dictionary).

        Args:
            edge_type: A string that contains the edge type being processed (i.e. chemical-gene).

        Returns:
            edge_counts: A list of the edges that were added to the knowledge graph.
        """

        edge_counts = []

        # define namespaces
        pheknowlator = Namespace('https://github.com/callahantiff/PheKnowLator/obo/ext/')
        obo = Namespace('http://purl.obolibrary.org/obo/')

        # get edge type
        cls = self.edge_dict[edge_type]['data_type'].split('-').index('class')
        inst = self.edge_dict[edge_type]['data_type'].split('-').index('instance')

        # check for inverse relation
        if self.relations_data:
            inverse_relation = self.checks_for_inverse_relations(self.edge_dict[edge_type]['edge_relation'],
                                                                 self.edge_dict[edge_type]['edge_list'])
        else:
            inverse_relation = None

        # add uuid for class-instance to dictionary - but check if one has been created first
        for edge in tqdm(self.edge_dict[edge_type]['edge_list']):
            # create a uuid for instance of the class
            if self.edge_dict[edge_type]['uri'][cls] + edge[cls] in self.kg_uuid_map.keys():
                ont_class_iri = self.kg_uuid_map[str(self.edge_dict[edge_type]['uri'][cls] + edge[cls])]
            else:
                ont_class_iri = str(pheknowlator) + str(uuid.uuid4())
                self.kg_uuid_map[str(self.edge_dict[edge_type]['uri'][cls] + edge[cls])] = ont_class_iri

            # adds node metadata information if present
            if self.node_dict:
                if edge_type in self.node_dict.keys() and edge[inst] in self.node_dict[edge_type].keys():
                    self.creates_node_metadata(edge[inst], edge_type, str(self.edge_dict[edge_type]['uri'][inst]))

                    # add instance of class
                    self.graph.add((URIRef(ont_class_iri),
                                    RDF.type,
                                    URIRef(str(self.edge_dict[edge_type]['uri'][cls] + edge[cls]))))

                    # add edge between instance of class and instance
                    self.graph.add((URIRef(ont_class_iri),
                                    URIRef(str(obo + self.edge_dict[edge_type]['edge_relation'])),
                                    URIRef(str(self.edge_dict[edge_type]['uri'][inst] + str(edge[inst])))))

                    # add inverse relations
                    if inverse_relation:
                        self.graph.add((URIRef(str(self.edge_dict[edge_type]['uri'][inst] + str(edge[inst]))),
                                        URIRef(str(obo + inverse_relation)),
                                        URIRef(ont_class_iri)))

                    edge_counts.append(edge)

            else:  # when no node metadata has been provided
                # add instance of class
                self.graph.add((URIRef(ont_class_iri),
                                RDF.type,
                                URIRef(str(self.edge_dict[edge_type]['uri'][cls] + edge[cls]))))

                # add relation between instance of class and instance
                self.graph.add((URIRef(ont_class_iri),
                                URIRef(str(obo + self.edge_dict[edge_type]['edge_relation'])),
                                URIRef(str(self.edge_dict[edge_type]['uri'][inst] + str(edge[inst])))))

                # add inverse relations
                if inverse_relation:
                    self.graph.add((URIRef(str(self.edge_dict[edge_type]['uri'][inst] + str(edge[inst]))),
                                    URIRef(str(obo + inverse_relation)),
                                    URIRef(ont_class_iri)))

                edge_counts.append(edge)

        return edge_counts

    def creates_knowledge_graph_edges(self):
        """Takes a nested dictionary of edge lists and adds them to an existing knowledge graph. The function
        performs different tasks in order to add the edges according to whether or not the edge is of type
        instance-instance, class-class, class-instance/instance-class.

        Edges are added by their edge_type (e.g. chemical-gene) and after the edges for a given edge-type have been
        added to the knowledge graph, some simple statistics are printed to include the number of unique nodes and
        edges. Once the knowledge graph is complete, it is written out to the write_location directory to an .owl
        file specified by the output_loc input variable. Additionally, the kg_uuid_map dictionary that stores the
        mapping between each class and its instance is lso written out to the same location and filename with
        '_ClassInstanceMap.json' appended to the end.

        Returns:
            None.
        """

        # loop over classes to create instances
        for edge_type in self.edge_dict.keys():
            if self.edge_dict[edge_type]['data_type'] == 'instance-instance':
                print('\n*** EDGE: {} - Creating Instance-Instance Edges ***'.format(edge_type))
                edge_results = self.creates_instance_instance_data_edges(edge_type)
            elif self.edge_dict[edge_type]['data_type'] == 'class-class':
                print('\n*** EDGE: {} - Creating Class-Class Edges ***'.format(edge_type))
                edge_results = self.creates_class_class_data_edges(edge_type)
            else:
                print('\n*** EDGE: {} - Creating Class-Instances Edges ***'.format(edge_type))
                edge_results = self.creates_instance_class_data_edges(edge_type)

            # print edge-type statistics
            print('Unique Edges: {}'.format(len([list(x) for x in set([tuple(y) for y in edge_results])])))
            print('Unique {}: {}'.format(edge_type.split('-')[0], len(set([x[0] for x in edge_results]))))
            print('Unique {}: {}'.format(edge_type.split('-')[1], len(set([x[1] for x in edge_results]))))

            # # print statistics on kg (comment out when not testing)
            # edges = len(set(list(self.graph)))
            # nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
            # print('\nThe KG contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        # print statistics on kg
        edges = len(set(list(self.graph)))
        nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
        print('\nThe KG contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        # add ontology annotations
        if 'partial' not in self.full_kg.split('/')[-1]:
            self.graph = self.adds_ontology_annotations(self.full_kg.split('/')[-1], self.graph)

        # serialize graph
        self.graph.serialize(destination=self.write_location + self.full_kg, format='xml')

        # apply OWL API formatting to file
        self.ontology_file_formatter(self.write_location + self.full_kg)

        # write class-instance uuid mapping dictionary to file
        json.dump(self.kg_uuid_map, open(self.write_location + self.full_kg[:-7] + '_ClassInstanceMap.json', 'w'))

        return None

    def removes_annotation_assertions(self):
        """Utilizes OWLTools to remove annotation assertions. The '--remove-annotation-assertions' method in OWLTools
        removes annotation assertions to make a pure logic subset', which reduces the overall size of the knowledge
        graph, but still makes able to be reasoned against. To ease the process adding the annotation assertions back
        after running a reasoner, the edges from the original knowledge graph are written to a text file prior to
        removing the annotations.

        Returns:
            None.
        """

        # write knowledge graph edges locally
        with open(self.write_location + self.full_kg[:-4] + '_edgelist.txt', 'w') as outfile:
            for edge in tqdm(self.graph):
                outfile.write(str(edge) + '\n')

        outfile.close()

        # self.graph.serialize(destination=self.write_location + self.full_kg, format='ntriples')

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

    def adds_annotation_assertions(self):
        """Adds edges removed from the knowledge graph when annotation assertions were removed. First, the edge list
        from the knowledge graph that was created prior to removing annotation assertions is read in. Then, the function
        iterates over the closed knowledge graph files and adds any edges that are present in the list, but missing
        from the closed knowledge graph.

        Returns:
            None.
        """

        # read in annotation assertions from full graph
        assertions = open(self.write_location + self.full_kg[:-4] + '_edgelist.txt', 'r').readlines()

        # reformat/clean edges - important to speed up the process of checking for them in the knowledge graph
        assertion_edges = []
        for edge in tqdm(assertions):
            triples = []

            for triple_part in [x for x in edge.strip('\n').split('rdflib') if 'term' in x]:
                if 'URIRef' in triple_part:
                    triples.append(URIRef(re.sub("(.*\(')|('\).*)", '', triple_part)))
                elif 'Literal' in triple_part:
                    triples.append(Literal(re.sub("(.*\(')|('\).*)", '', triple_part)))
                else:
                    triples.append(BNode(re.sub("(.*\(')|('\).*)", '', triple_part)))

            assertion_edges.append(tuple(triples))

        # iterate over closed knowledge graph and add back missing annotation assertions
        for edge in tqdm(self.graph):
            if edge not in assertions:
                self.add(edge)

        return None

    def adds_node_metadata(self):
        """Iterates over nodes in each edge in the edge_dict, by edge_type. If the node has metadata available in the
        the node_data dictionary, then it is added to the knowledge graph.

        Returns:
            None.
        """

        for edge_type in self.edge_dict.keys():
            if 'instance' not in self.edge_dict[edge_type]['data_type']:
                pass
            else:
                print('Adding Node Metadata for {} Edges\n'.format(edge_type))
                node_idx = [x for x in range(2) if self.edge_dict[edge_type]['data_type'].split('-')[x] == 'instance']

                for edge in tqdm(self.edge_dict[edge_type]['edge_list']):
                    if self.node_dict and edge_type in self.node_dict.keys():
                        if len(node_idx) == 2:
                            subj, obj = edge.split('-')[0], edge.split('-')[1]

                            if subj and obj in self.node_dict[edge_type].keys():
                                self.creates_node_metadata(subj, edge_type, str(self.edge_dict[edge_type]['uri'][0]))
                                self.creates_node_metadata(obj, edge_type, str(self.edge_dict[edge_type]['uri'][0]))
                        else:
                            subj = edge.split('-')[node_idx]

                            if subj in self.node_dict[edge_type].keys():
                                self.creates_node_metadata(subj, edge_type, str(self.edge_dict[edge_type]['uri'][0]))

        # print kg statistics
        edges = len(set(list(self.graph)))
        nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
        print('\nThe KG with node data contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        return None

    def maps_node_labels_to_integers(self, graph: Graph, output_triple_integers: str, output_loc):
        """Loops over the knowledge graph in order to create three different types of files:
            - Integers: a tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
            subject, predicate, object). The subject, predicate, and object identifiers have been mapped to integers.
            - Identifiers: a tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
            subject, predicate, object). Both the subject and object identifiers have not been mapped to integers.
            - Identifier-Integer Map: a `.json` file containing a dictionary where the keys are node identifiers and
            the values are integers.

        Args:
            graph: an RDFlib graph object.
            output_triple_integers: the name and file path to write out results.
            output_loc: the name and file path to write out results.

        Returns:
            None.

        Raises:
            An exception is raised if the length of the graph is not the same as the number of extracted triples.
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
            raise Exception('ERROR: The number of triples is incorrect!')
        else:
            json.dump(node_map, open(self.write_location + '/' + output_loc, 'w'))

        return None

    def extracts_class_metadata(self):
        """Functions queries the knowledge graph to obtain labels, definitions/descriptions, and synonyms for
        classes. This information is then added to the existing self.node_dict dictionary under the key of 'classes'.
        Each metadata type is saved as a dictionary key with the actual string stored as the value. The metadata types
        are packaged as a dictionary which is stored as the value to the node identifier as the key.

        Returns:
            None.
        """

        self.node_dict['classes'] = {}
        # edge_list = [x for y in [self.edge_dict[x]['edge_list'] for x in self.edge_dict.keys()] for x in y]

        # query knowledge graph to obtain metadata
        results = self.graph.query(
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

    def output_knowledge_graph_metadata(self):
        """Loops over the self.node_dict dictionary and writes out the data to a file locally. The data is stored as
        a tab-delimited '.txt' file with four columns: (1) node identifier; (2) node label; (3) node description or
        definition; and (4) node synonym.

        Returns:
            None.
        """

        # add metadata for nodes that are data type class to self.node_dict
        self.extracts_class_metadata()

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

    def construct_knowledge_graph(self):
        """Builds a knowledge graph through the following steps: (0) setting up environment - making sure that the
        directories to write data to exist; (1) Processing node and relations data - if the user has indicated that
        each of these data types is needed, the system process each source setting class attributes self.node_dict,
        self.relations_dict, and self.inverse_relations_dict; (2) Merges ontology data (class data); (3) Add edges
        from master edge list to merged ontologies. During this process, if specified by the user inverse relations
        and node meta data are also added; (4) Remove edges containing OWL semantics (if specified by the user);
        (5) outputs integer and node identifier-labeled edge lists; (6) Outputs a dictionary of all node
        identifiers and their label metadata (i.e. labels, descriptions/definitions, and synonyms); and (7) writes a
        tab-delimited '.txt' file containing the node id, label, description/definition, and synonyms for each node
        in the knowledge graph.

        The knowledge graph build is completed differently depending on the build type that the user requested. The
        build types that a user can request includes: "full", "partial", and "post-closure". The steps for each
        build, referenced by their number above (i.e. (2)) are listed below:
            - Full Build: Steps 0-7
            - Partial Build: Steps 0-4
            - Post-Closure Build: Steps 4-7

        Returns:
            None.

        Raises:
            - An exception is raised when the build type is
        """

        # STEP 0: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 1: PROCESS NODE METADATA AND RELATIONS DATA
        print('*** Loading Support Data ***')
        self.node_metadata_processor()
        self.reverse_relation_processor()

        if self.build == 'post-closure':
            print('\n*** Starting Knowledge Graph Build: post-closure ***')

            # STEP 3: LOAD CLOSED KNOWLEDGE GRAPH
            closed_kg_location = input('Provide the relative filepath to the location of the closed knowledge graph: ')
            uuid_map_location = input('Provide the relative filepath to the location of the class-instance UUID Map: ')

            # check uuid input file
            if '.json' not in uuid_map_location:
                raise Exception('FILE TYPE ERROR: The provided file is not type .json')
            elif os.stat(uuid_map_location).st_size == 0:
                raise Exception('ERROR: input file: {} is empty'.format(uuid_map_location))
            else:
                # load closed knowledge graph
                print('*** Knowledge Graph Class-Instance UUID Map ***')
                self.kg_uuid_map = json.load(open(uuid_map_location, 'r'))

            # check input owl file
            if '.owl' not in closed_kg_location:
                raise Exception('FILE TYPE ERROR: The provided file is not type .owl')
            elif os.stat(closed_kg_location).st_size == 0:
                raise Exception('ERROR: input file: {} is empty'.format(closed_kg_location))
            else:
                # load closed knowledge graph
                print('*** Loading Merged Ontologies ***')
                self.graph = Graph()
                self.graph.parse(closed_kg_location)

                # print stats
                edges = len(set(list(self.graph)))
                nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
                print('\nThe closed ontology contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

            # STEP 4: ADD NODE DATA
            print('*** Adding Node Metadata ***')
            self.adds_node_metadata()

            # STEP 5: REMOVE OWL SEMANTICS FROM KNOWLEDGE GRAPH
            if self.remove_owl_semantics:
                print('*** Removing Metadata Nodes ***')
                cleaned_kg = self.removes_edges_with_owl_semantics()
            else:
                cleaned_kg = self.graph

            # STEP 6: WRITE OUT KNOWLEDGE GRAPH EDGE LISTS
            print('*** Writing Knowledge Graph Edge Lists ***')
            self.maps_node_labels_to_integers(graph=cleaned_kg,
                                              output_triple_integers=self.cleaned_kg[:-6] + '_Triples_Integers.txt',
                                              output_loc=self.cleaned_kg[:-6] + '_Triples_Integer_Labels_Map.json')

            # STEP 7: WRITE OUT NODE LABELS, DEFINITIONS, AND SYNONYMS
            if self.node_dict:
                print('*** Writing Knowledge Graph Labels, Definitions/Descriptions, and Synonyms ***')
                self.output_knowledge_graph_metadata()

        elif self.build == 'partial':
            print('\n*** Starting Knowledge Graph Build: PARTIAL ***')

            # STEP 2: MERGE ONTOLOGIES
            if self.write_location + self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
                print('*** Loading Merged Ontologies ***')
                self.graph.parse(self.write_location + self.merged_ont_kg)

                # print stats
                edges = len(set(list(self.graph)))
                nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
                print('\nThe merged ontology contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))
            else:
                if len(self.ontologies) == 0:
                    raise Exception('ERROR: the ontologies directory: {} is empty'.format(
                        self.write_location + '/' + self.ontologies))
                else:
                    print('*** Merging Ontology Data ***')
                    self.merges_ontologies()

            # STEP 3: ADD EDGE DATA TO KNOWLEDGE GRAPH
            # create temporary directory to store partial builds
            temp_dir = '/'.join((self.write_location + self.full_kg).split('/')[:4])

            if temp_dir + '/partial_build' not in glob.glob(self.write_location + '/**/**'):
                os.mkdir(temp_dir + '/partial_build')

            # update path to write data to
            self.full_kg = '/'.join(self.full_kg.split('/')[:2] + ['partial_build'] + self.full_kg.split('/')[2:])

            # build knowledge graph
            print('*** Building Knowledge Graph Edges ***')
            self.creates_knowledge_graph_edges()

            # STEP 4: REMOVE ANNOTATION ASSERTIONS
            print('*** Removing Annotation Assertions ***')
            self.removes_annotation_assertions()

        else:
            print('\n*** Starting Knowledge Graph Build: FULL ***')

            # STEP 2: MERGE ONTOLOGIES
            if self.write_location + self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
                print('*** Loading Merged Ontologies ***')
                self.graph.parse(self.write_location + self.merged_ont_kg)

                # print stats
                edges = len(set(list(self.graph)))
                nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
                print('\nThe merged ontology contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))
            else:
                if len(self.ontologies) == 0:
                    raise Exception('ERROR: the ontologies directory: {} is empty'.format(
                        self.write_location + '/' + self.ontologies))
                else:
                    print('*** Merging Ontology Data ***')
                    self.merges_ontologies()

            # STEP 3: ADD EDGE DATA TO KNOWLEDGE GRAPH
            print('*** Building Knowledge Graph Edges ***')
            self.creates_knowledge_graph_edges()

            # STEP 4: REMOVE OWL SEMANTICS FROM KNOWLEDGE GRAPH
            if self.remove_owl_semantics:
                print('*** Removing Metadata Nodes ***')
                cleaned_kg = self.removes_edges_with_owl_semantics()
            else:
                cleaned_kg = self.graph

            # STEP 5: WRITE OUT KNOWLEDGE GRAPH EDGE LISTS
            print('*** Writing Knowledge Graph Edge Lists ***')
            self.maps_node_labels_to_integers(graph=cleaned_kg,
                                              output_triple_integers=self.full_kg[:-6] + 'Triples_Integers.txt',
                                              output_loc=self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

            # STEP 6: WRITE OUT NODE LABELS, DEFINITIONS, AND SYNONYMS
            if self.node_dict:
                print('*** Writing Knowledge Graph Labels, Definitions/Descriptions, and Synonyms ***')
                self.output_knowledge_graph_metadata()

        return None
