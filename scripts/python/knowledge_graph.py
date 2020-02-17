#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import json
import os
import pandas
import subprocess
import uuid

from abc import ABCMeta, abstractmethod
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS
from tqdm import tqdm
from typing import Callable

from scripts.python.knowledge_graph_metadata import Metadata
# from scripts.python.knowledge_graph_metadata import OWLNETS


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

    __metaclass__ = ABCMeta

    def __init__(self, kg_version: str, write_location: str, edge_data: str = None, node_data: dict = None,
                 relations_data: dict = None, remove_owl_semantics: str = None):

        # set build type
        self.build = self.gets_build_type().lower().split()[0]

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

    def creates_instance_instance_data_edges(self, edge_type: str, creates_node_metadata_func: Callable):
        """Adds edges that contain nodes that are both of type instance to a knowledge graph. The typing of each node
        in an edge is specified in the resource_info.txt document. For each edge, the function will add metadata for
        nodes that are part of the edge and of type instance. The function will also check whether or not edges for
        inverse relations should also be created.

        Note. If node_dict is not None (i.e. metadata was provided via setting the node_dict='yes'), the function will
        only add edges to the knowledge graph that have metadata information (i.e. edges where each node has at least a
        label in the node_dict metadata dictionary).

        Args:
            edge_type: A string that contains the edge type being processed (i.e. chemical-gene).
            creates_node_metadata_func: A function that adds metadata (i.e. labels, descriptions, and synonyms) for
                class-instance and instance-instance edges.

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
            if self.node_dict:  # adds node metadata information if present
                if edge_type in self.node_dict.keys():
                    if edge[0] in self.node_dict[edge_type].keys() and edge[1] in self.node_dict[edge_type].keys():
                        subj_uri = str(self.edge_dict[edge_type]['uri'][0])
                        obj_uri = str(self.edge_dict[edge_type]['uri'][1])

                        self.graph = creates_node_metadata_func(edge[0], edge_type, subj_uri, self.graph)
                        self.graph = creates_node_metadata_func(edge[1], edge_type, obj_uri, self.graph)

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

                else:  # when no node metadata has been provided
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

    def creates_instance_class_data_edges(self, edge_type: str, creates_node_metadata_func: Callable):
        """Adds edges that contain one node of type instance and one node of type class to a knowledge graph. The
        typing of each node in an edge is specified in the resource_info.txt document. While adding each edge,
        the function will add metadata for nodes that are part of the edge and of type instance. The function will
        also check whether or not edges for inverse relations should also be created.

        Note. If node_dict is not None (i.e. metadata was provided via setting the node_dict='yes'), the function will
        only add edges to the knowledge graph that have metadata information (i.e. edges where each node has at least a
        label in the node_dict metadata dictionary).

        Args:
            edge_type: A string that contains the edge type being processed (i.e. chemical-gene).
            creates_node_metadata_func: A function that adds metadata (i.e. labels, descriptions, and synonyms) for
                class-instance and instance-instance edges.

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

            if self.node_dict:  # adds node metadata information if present
                if edge_type in self.node_dict.keys() and edge[inst] in self.node_dict[edge_type].keys():
                    node_uri = str(self.edge_dict[edge_type]['uri'][inst])
                    self.graph = creates_node_metadata_func(edge[inst], edge_type, node_uri, self.graph)

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

    def creates_knowledge_graph_edges(self, creates_node_metadata_func: Callable, ontology_annotator_func: Callable,
                                      ontology_file_formatter_func: Callable):
        """Takes a nested dictionary of edge lists and adds them to an existing knowledge graph. The function
        performs different tasks in order to add the edges according to whether or not the edge is of type
        instance-instance, class-class, class-instance/instance-class.

        Edges are added by their edge_type (e.g. chemical-gene) and after the edges for a given edge-type have been
        added to the knowledge graph, some simple statistics are printed to include the number of unique nodes and
        edges. Once the knowledge graph is complete, it is written out to the write_location directory to an .owl
        file specified by the output_loc input variable. Additionally, the kg_uuid_map dictionary that stores the
        mapping between each class and its instance is lso written out to the same location and filename with
        '_ClassInstanceMap.json' appended to the end.

        Args:
            creates_node_metadata_func: A function that adds metadata (i.e. labels, descriptions, and synonyms) for
                class-instance and instance-instance edges.
            ontology_annotator_func:
            ontology_file_formatter_func:

        Returns:
            None.
        """

        # loop over classes to create instances
        for edge_type in self.edge_dict.keys():
            if self.edge_dict[edge_type]['data_type'] == 'instance-instance':
                print('\n*** EDGE: {} - Creating Instance-Instance Edges ***'.format(edge_type))
                edge_results = self.creates_instance_instance_data_edges(creates_node_metadata_func, edge_type)
            elif self.edge_dict[edge_type]['data_type'] == 'class-class':
                print('\n*** EDGE: {} - Creating Class-Class Edges ***'.format(edge_type))
                edge_results = self.creates_class_class_data_edges(edge_type)
            else:
                print('\n*** EDGE: {} - Creating Class-Instances Edges ***'.format(edge_type))
                edge_results = self.creates_instance_class_data_edges(creates_node_metadata_func, edge_type)

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
        self.graph = ontology_annotator_func(self.full_kg.split('/')[-1], self.graph)

        # serialize graph
        self.graph.serialize(destination=self.write_location + self.full_kg, format='xml')

        # apply OWL API formatting to file
        ontology_file_formatter_func(self.write_location + self.full_kg)

        # write class-instance uuid mapping dictionary to file
        json.dump(self.kg_uuid_map, open(self.write_location + self.full_kg[:-7] + '_ClassInstanceMap.json', 'w'))

        return None

    def construct_knowledge_graph(self):
        """Builds a knowledge graph. The knowledge graph build is completed differently depending on the build type
        that the user requested. The build types that a user can request includes: "full", "partial", or "post-closure".

        The knowledge graph is built through the following steps: (0) setting up environment - making sure that the
        directories to write data to exist; (1) Processing node and relations data - if the user has indicated that
        each of these data types is needed; (2) Merges ontology data (class data); (3) Add edges from master edge list
        to merged ontologies. During this process, if specified by the user inverse relations and node meta data are
        also added; (4) Remove edges containing OWL semantics (if specified by the user); (5) outputs integer and node
        identifier-labeled edge lists, a dictionary of all node identifiers and their label metadata (i.e. labels,
        descriptions/definitions, and synonyms), and write a tab-delimited '.txt' file containing the node id, label,
        description/definition, and synonyms for each node in the knowledge graph.

        Returns:
            None.
        """

        pass

    @abstractmethod
    def gets_build_type(self):
        """"A string representing the type of knowledge graph build."""

        pass


class PartialBuild(KGBuilder):

    def gets_build_type(self):
        """"A string representing the type of knowledge graph build."""

        return 'Partial Build'

    def construct_knowledge_graph(self):
        """Builds a partial knowledge graph. A partial knowledge graph build is recommended when one intends to build a
        knowledge graph and intends to run a reasoner over it.

        The knowledge graph is built through the following steps: (0) setting up environment - making sure that the
        directories to write data to exist; (1) Processing node and relations data - if the user has indicated that
        each of these data types is needed, the system process each source setting class attributes self.node_dict,
        self.relations_dict, and self.inverse_relations_dict; (2) Merges ontology data (class data); (3) Add edges from
        master edge list to merged ontologies. During this process, if specified by the user inverse relations and node
        metadata are also added; and (4) Remove edges containing annotation assertions.

        Returns:
            None.

        Raises:
            An exception is raised if the ontologies directory is empty.
        """

        print('\n### Starting Knowledge Graph Build: PARTIAL ###')

        # STEP 0: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 1: PROCESS NODE METADATA AND RELATIONS DATA
        print('*** Loading Support Data ***')

        # load relations data
        self.reverse_relation_processor()

        # load and process node metadata
        kg_metadata = Metadata(self.kg_version,
                               self.gets_build_type,
                               self.write_location,
                               self.full_kg,
                               self.node_data,
                               self.node_dict)

        kg_metadata.node_metadata_processor()

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
        kg_metadata.full_kg = self.full_kg

        # build knowledge graph
        print('*** Building Knowledge Graph Edges ***')
        self.creates_knowledge_graph_edges(kg_metadata.creates_node_metadata,
                                           kg_metadata.adds_ontology_annotations,
                                           kg_metadata.ontology_file_formatter)

        # STEP 4: REMOVE ANNOTATION ASSERTIONS
        print('*** Removing Annotation Assertions ***')
        kg_metadata.removes_annotation_assertions(self.graph)

        return None


class PostClosureBuild(KGBuilder):

    def gets_build_type(self):
        """"A string representing the type of knowledge graph being built."""

        return 'Post-Closure Build'

    def construct_knowledge_graph(self):
        """Builds a post-closure knowledge graph. A post-closure knowledge graph build is recommended when one has
        previously performed a "partial" knowledge graph build and then ran a reasoner over the partially built
        knowledge graph. This build type inputs the closed partially built knowledge graph and completes the build
        process.

        The knowledge graph through the following steps: (0) setting up environment - making sure that the
        directories to write data to exist; (1) load closed knowledge graph and class-instance UUID map; (2) add node
        metadata; (3) Remove edges containing OWL semantics (if specified by the user); (4) outputs integer and node
        identifier-labeled edge lists; (3) Outputs a dictionary of all node identifiers and their label metadata (
        i.e. labels, descriptions/definitions, and synonyms) and write a tab-delimited '.txt' file containing the
        node id, label, description/definition, and synonyms for each node in the knowledge graph.

        Returns:
            None.

        Raises:
            - An error is returned if the class-instance UUID Map file type is not json.
            - An error is raised if the class-instance UUID Map file is empty.
            - An error is returned if the knowledge graph file type is not owl.
            - An error is raised if the knowledge graph file is empty.
        """

        print('\n### Starting Knowledge Graph Build: post-closure ###')

        # STEP 0: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 1: PROCESS NODE METADATA AND RELATIONS DATA
        print('*** Loading Support Data ***')

        # load relations data
        self.reverse_relation_processor()

        # load and process node metadata
        kg_metadata = Metadata(self.kg_version,
                               self.gets_build_type,
                               self.write_location,
                               self.full_kg,
                               self.node_data,
                               self.node_dict)

        kg_metadata.node_metadata_processor()

        # STEP 1: LOAD CLOSED KNOWLEDGE GRAPH
        uuid_location = input('Provide the relative filepath to the class-instance UUID Map: ')
        closed_kg_location = input('Provide the relative filepath to the closed knowledge graph: ')
        annotation_assertions = input('Provide the relative filepath to the annotation assertions edge list: ')

        # check uuid input file
        if '.json' not in uuid_location:
            raise Exception('ERROR: The provided file is not type .json')
        elif os.stat(uuid_location).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(uuid_location))
        else:
            # load closed knowledge graph
            print('*** Loading Knowledge Graph Class-Instance UUID Map ***')
            self.kg_uuid_map = json.load(open(uuid_location, 'r'))

            # move file out of 'partial build' directory
            os.rename(uuid_location, uuid_location.replace('partial_', '').replace('build/', '').replace('Not', ''))

        # check input owl file
        if '.owl' not in closed_kg_location:
            raise Exception('FILE TYPE ERROR: The provided file is not type .owl')
        elif os.stat(closed_kg_location).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(closed_kg_location))
        else:
            # load closed knowledge graph
            print('*** Loading Closed Knowledge Graph ***')
            self.graph = Graph()
            self.graph.parse(closed_kg_location)

            # print stats
            edges = len(set(list(self.graph)))
            nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
            print('\nThe closed ontology contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        # STEP 2: ADD NODE METADATA
        # check annotation assertion edges input file
        if '.txt' not in annotation_assertions:
            raise Exception('ERROR: The provided file is not type .txt')
        elif os.stat(annotation_assertions).st_size == 0:
            raise Exception('ERROR: input file: {} is empty'.format(annotation_assertions))
        else:
            # load closed knowledge graph
            print('*** Loading Annotation Assertions Edge List ***')
            annotation_assertions_file = json.load(open(annotation_assertions, 'r'))
            self.graph = kg_metadata.adds_annotation_assertions(self.graph, annotation_assertions_file)

        print('*** Adding Node Metadata ***')
        self.graph = kg_metadata.adds_node_metadata(self.graph, self.edge_dict)

        # STEP 3: REMOVE OWL SEMANTICS FROM KNOWLEDGE GRAPH
        # if self.remove_owl_semantics:
        #     print('*** Removing Metadata Nodes ***')
        #     cleaned_kg = self.removes_edges_with_owl_semantics()
        # else:
        #     cleaned_kg = self.graph
        cleaned_kg = self.graph

        # STEP 4: WRITE OUT KNOWLEDGE GRAPH EDGE LISTS
        print('*** Writing Knowledge Graph Edge Lists ***')

        # output knowledge graph edge lists
        kg_metadata.maps_node_labels_to_integers(cleaned_kg,
                                                 self.cleaned_kg[:-6] + '_Triples_Integers.txt',
                                                 self.cleaned_kg[:-6] + '_Triples_Integer_Labels_Map.json')

        # output metadata
        if self.node_dict:
            print('*** Writing Knowledge Graph Labels, Definitions/Descriptions, and Synonyms ***')
            kg_metadata.output_knowledge_graph_metadata(cleaned_kg)

        # remove partial build temp directory
        os.remove(glob.glob(self.write_location + '/'.join(self.full_kg.split('/')[0:2]) + '/partial_build')[0])

        return None


class FullBuild(KGBuilder):

    def gets_build_type(self):
        """"A string representing the type of knowledge graph being built."""

        return 'Full Build'

    def construct_knowledge_graph(self):
        """Builds a full knowledge graph. Please note that the process to build this version of the knowledge graph
        does not include running a reasoner.

        The knowledge graph through the following steps: (0) setting up environment - making sure that the directories
        to write data to exist; (1) Processing node and relations data - if the user has indicated that each of these
        data types is needed; (2) Merges ontology data (class data); (3) Add edges from master edge list to merged
        ontologies. During this process, if specified by the user inverse relations and node meta data are also added;
        (4) Remove edges containing OWL semantics (if specified by the user); (5) outputs integer and node identifier-
        labeled edge lists, a dictionary of all node identifiers and their label metadata (i.e. labels, descriptions/
        definitions, and synonyms), and write a tab-delimited '.txt' file containing the node id, label, description/
        definition, and synonyms for each node in the knowledge graph.

        Returns:
            None.

        Raises:
            - An error is raised if the ontology directory is empty.
        """

        print('\n### Starting Knowledge Graph Build: FULL ###')

        # STEP 0: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 1: PROCESS NODE METADATA AND RELATIONS DATA
        print('*** Loading Support Data ***')

        # load relations data
        self.reverse_relation_processor()

        # load and process node metadata
        kg_metadata = Metadata(self.kg_version,
                               self.gets_build_type,
                               self.write_location,
                               self.full_kg,
                               self.node_data,
                               self.node_dict)

        kg_metadata.node_metadata_processor()

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
        self.creates_knowledge_graph_edges(kg_metadata.creates_node_metadata,
                                           kg_metadata.adds_ontology_annotations,
                                           kg_metadata.ontology_file_formatter)

        # STEP 4: REMOVE OWL SEMANTICS FROM KNOWLEDGE GRAPH
        # if self.remove_owl_semantics:
        #     print('*** Removing Metadata Nodes ***')
        #     cleaned_kg = self.removes_edges_with_owl_semantics()
        # else:
        #     cleaned_kg = self.graph
        cleaned_kg = self.graph

        # STEP 5: WRITE OUT KNOWLEDGE GRAPH EDGE LISTS
        print('*** Writing Knowledge Graph Edge Lists ***')

        # output knowledge graph edge lists
        kg_metadata.maps_node_labels_to_integers(cleaned_kg,
                                                 self.full_kg[:-6] + 'Triples_Integers.txt',
                                                 self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

        # output metadata
        if self.node_dict:
            print('*** Writing Knowledge Graph Labels, Definitions/Descriptions, and Synonyms ***')
            kg_metadata.output_knowledge_graph_metadata(cleaned_graph)

        return None
