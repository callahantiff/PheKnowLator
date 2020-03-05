#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import json
import networkx   # type: ignore
import os
import pandas   # type: ignore
import subprocess
import uuid

from abc import ABCMeta, abstractmethod
from rdflib import BNode, Graph, Literal, Namespace, URIRef  # type: ignore
from rdflib.namespace import RDF, RDFS  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Callable, Dict, List, Optional

from scripts.python.knowledge_graph_metadata import Metadata
from scripts.python.removes_owl_semantics import OWLNETS

# TODO: mypy throws errors for optional[dict] usage, this is an existing bug in mypy
# https://github.com/python/mypy/issues/4359


class KGBuilder(object):
    """Class creates a semantic knowledge graph.

    The class implements several methods, which are designed to construct a semantic knowledge graph given a
    dictionary of pre-processed knowledge graph edges, a directory of ontology relation data, and a directory of node
    metadata. The class is designed to facilitate three types of knowledge graph builds:
        (1) Full: Runs all build steps in the algorithm.
        (2) Partial: Runs all of the build steps in the algorithm through adding the class-class, instance-class,
            class-instance, and instance-instance edges. Designed for those wanting to run a reasoner on a pure logic
            subset of the knowledge graph.
        (3) Post-Closure: Assumes that a reasoner was run over a knowledge graph and that the remaining build steps
            should be applied to a closed knowledge graph.

    Attributes:
        kg_version: A string that contains the version of the knowledge graph build.
        build: A string that indicates what kind of build.
        write_location: A file path used for writing knowledge graph data.
        edge_data: A path to a file that references a dictionary of edge list tuples used to build the knowledge graph.
        node_data: A filepath to a directory called 'node_data' containing a file for each instance node.
        inverse_relations: A filepath to a directory called 'relations_data' containing the relations data.
        decode_owl_semantics: A string indicating whether edges containing owl semantics should be removed.
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
        full_kg: A string containing the filename for the full knowledge graph.
        merged_ont_kg: A string containing the filename of the knowledge graph that only contains the merged ontologies.
        ontologies: A list of file paths to an .owl file containing ontology data.
        kg_uuid_map: A dictionary storing the mapping between a class and its instance. The keys are the original class
             uri and the values are the hashed uuid of the uri needed to create an instance of the class. For example:
             {'http://purl.obolibrary.org/obo/CHEBI_24505':
              'https://github.com/callahantiff/PheKnowLator/obo/ext/d1552fc9-a91b-414a-86cb-885f8c4822e7'}
        graph: An rdflib graph object which stores the knowledge graph.
        nx_mdg: A networkx MultiDiGraph object which is only created if the user requests owl semantics be removed.

    Raises:
        ValueError: If the formatting of kg_version is incorrect (i.e. not "v.#.#.#").
        ValueError: If write_location does not contain a valid filepath.
        ValueError: If edge_data does not contain a valid filepath.
        IOError: If the edge_data file is empty.
        TypeError: If the edge_data contains no data.
        ValueError: If relations_data does not contain "yes" or "no".
        TypeError: If the relations_data directory does not contain any data files.
        ValueError: If node_data does not contain "yes" or "no".
        TypeError: If the node_data directory does not contain any data files.
        ValueError: If decode_owl_semantics does not contain "yes" or "no".
    """

    __metaclass__ = ABCMeta

    def __init__(self, kg_version: str, write_location: str, edge_data: Optional[str] = None,
                 node_data: Optional[str] = None, inverse_relations: Optional[str] = None,
                 decode_owl_semantics: Optional[str] = None) -> None:

        # initialize variables
        self.build: str = self.gets_build_type().lower().split()[0]
        self.edge_dict: Optional[Dict] = None
        self.inverse_relations: Optional[List] = None
        self.relations_dict: Optional[Dict] = None
        self.inverse_relations_dict: Optional[Dict] = None
        self.node_data: Optional[List] = None
        self.node_dict: Optional[Dict] = None
        self.decode_owl_semantics: Optional[str] = None

        # BUILD VARIABLES
        if kg_version is None:
            raise ValueError('ERROR: kg_version must not contain a valid version e.g. v.2.0.0')
        else:
            self.kg_version = kg_version

        # WRITE LOCATION
        if write_location is None:
            raise ValueError('ERROR: write_location must not contain a valid filepath')
        else:
            self.write_location = write_location

        # KNOWLEDGE GRAPH EDDE LIST
        if edge_data is None:
            raise ValueError('ERROR: edge_data must not contain a valid filepath')
        elif not os.path.exists(edge_data):
            raise IOError('The {} file does not exist!'.format(edge_data))
        elif os.stat(edge_data).st_size == 0:
            raise TypeError('FILE ERROR: input file: {} is empty'.format(edge_data))
        else:
            self.edge_dict = json.load(open(edge_data, 'r'))

        # RELATIONS DATA
        if inverse_relations and inverse_relations.lower() not in ['yes', 'no']:
            raise ValueError('ERROR: relations_data must be "no" or "yes"')
        else:
            if inverse_relations and inverse_relations.lower() == 'yes':
                if len(glob.glob('*/relations_data/*.txt', recursive=True)) == 0:
                    raise TypeError('ERROR: the relations_data directory is empty')
                else:
                    self.inverse_relations = glob.glob('*/relations_data/*.txt', recursive=True)
                    self.relations_dict, self.inverse_relations_dict = dict(), dict()
                    kg_rel = '/inverse_relations' + '/PheKnowLator_' + self.build + '_InverseRelations_'
            else:
                self.inverse_relations, self.relations_dict, self.inverse_relations_dict = None, None, None
                kg_rel = '/relations_only' + '/PheKnowLator_' + self.build + '_'

        # NODE METADATA
        if node_data and node_data.lower() not in ['yes', 'no']:
            raise ValueError('ERROR: node_data must be "no" or "yes"')
        else:
            if node_data and node_data.lower() == 'yes':
                if len(glob.glob('*/node_data/*.txt', recursive=True)) == 0:
                    raise TypeError('ERROR: the node_data directory is empty')
                else:
                    self.node_data, self.node_dict = glob.glob('*/node_data/*.txt', recursive=True), dict()
                    kg_node = kg_rel + 'Closed_' if self.build == 'post-closure' else kg_rel + 'NotClosed_'
            else:
                self.node_data, self.node_dict = None, None
                kg_node = kg_rel + 'NoMetadata_Closed_' if 'closure' in self.build else kg_rel + 'NoMetadata_NotClosed_'

        # OWL SEMANTICS
        if decode_owl_semantics and decode_owl_semantics.lower() not in ['yes', 'no']:
            raise ValueError('ERROR: decode_semantics must be "no" or "yes"')
        else:
            if decode_owl_semantics and decode_owl_semantics.lower() == 'yes' and self.build != 'partial':
                self.full_kg: str = kg_node + 'NoOWLSemantics_KG.owl'
                self.decode_owl_semantics = decode_owl_semantics
            else:
                self.full_kg = kg_node + 'OWLSemantics_KG.owl'
                self.decode_owl_semantics = None

        # set remaining attributes
        self.merged_ont_kg: str = '/PheKnowLator_MergedOntologies.owl'
        self.ontologies: List[str] = glob.glob('*/ontologies/*.owl')
        self.kg_uuid_map: Dict[str, str] = dict()
        self.graph: Graph = Graph()

    def sets_up_environment(self) -> None:
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
            if isinstance(self.inverse_relations, list):
                if self.write_location + '/inverse_relations' not in glob.glob('./resources/**/**'):
                    os.mkdir(self.write_location + '/inverse_relations')
            else:
                if self.write_location + '/relations_only' not in glob.glob('./resources/**/**'):
                    os.mkdir(self.write_location + '/relations_only')

        return None

    def reverse_relation_processor(self) -> None:
        """Reads in data to a Pandas DataFrame from a user-provided filepath. The pandas DataFrame is then converted
        to a specific dictionary depending on whether it contains inverse relation data or relation data identifiers
        and labels. This distinction is derived from the filename (e.g. resources/relations_data/INVERSE_RELATIONS.txt).

        Returns:
            None.
        """

        if self.inverse_relations:
            for data in self.inverse_relations:
                df = pandas.read_csv(data, header=0, delimiter='\t')
                df.drop_duplicates(keep='first', inplace=True)
                df.set_index(list(df)[0], inplace=True)

                if 'inverse' in data.lower():
                    self.inverse_relations_dict = df.to_dict('index')
                else:
                    self.relations_dict = df.to_dict('index')

        return None

    def merges_ontologies(self) -> None:
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

            print('\nMerging Ontologies: {ont1}, {ont2}\n'.format(ont1=ont1.split('/')[-1], ont2=ont2.split('/')[-1]))
            try:
                subprocess.check_call(['./resources/lib/owltools', str(ont1), str(ont2),
                                       '--merge-support-ontologies',
                                       '-o', self.write_location + self.merged_ont_kg])
            except subprocess.CalledProcessError as error:
                print(error.output)

            return self.merges_ontologies()

    def checks_for_inverse_relations(self, relation: str, edge_list: List[List[str]]) -> Optional[str]:
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

        if self.inverse_relations and relation in self.inverse_relations_dict.keys():
            return self.inverse_relations_dict[relation]['Inverse_Relation']
        elif self.inverse_relations and (relation in self.relations_dict.keys()):
            int_type = self.relations_dict[relation]['Label']

            if 'interact' in int_type and len(set([x[0] for x in edge_list])) != len(set([x[1] for x in edge_list])):
                return relation
            else:
                return None
        else:
            return None

    def creates_instance_instance_data_edges(self, edge_type: str, creates_node_metadata_func: Callable) -> List[str]:
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
        if self.inverse_relations:
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

    def creates_class_class_data_edges(self, edge_type: str) -> List[str]:
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
        if self.inverse_relations:
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

    def creates_instance_class_data_edges(self, edge_type: str, creates_node_metadata_func: Callable) -> List[str]:
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
        if self.inverse_relations:
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

    def creates_knowledge_graph_edges(self, creates_node_metadata_func: Callable, ontology_annotator_func: Callable) ->\
            None:
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
            ontology_annotator_func: A function that adds annotations to an existing ontology.

        Returns:
            None.
        """

        # loop over classes to create instances
        for edge_type in self.edge_dict.keys():
            if self.edge_dict[edge_type]['data_type'] == 'instance-instance':
                print('\nEDGE: {} - Creating Instance-Instance Edges ***'.format(edge_type))
                edge_results = self.creates_instance_instance_data_edges(edge_type, creates_node_metadata_func)
            elif self.edge_dict[edge_type]['data_type'] == 'class-class':
                print('\nEDGE: {} - Creating Class-Class Edges ***'.format(edge_type))
                edge_results = self.creates_class_class_data_edges(edge_type)
            else:
                print('\nEDGE: {} - Creating Class-Instances Edges ***'.format(edge_type))
                edge_results = self.creates_instance_class_data_edges(edge_type, creates_node_metadata_func)

            # print edge-type statistics
            print('\nUnique Edges: {}'.format(len([list(x) for x in set([tuple(y) for y in edge_results])])))
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
        self.ontology_file_formatter()

        # write class-instance uuid mapping dictionary to file
        json.dump(self.kg_uuid_map, open(self.write_location + self.full_kg[:-7] + '_ClassInstanceMap.json', 'w'))

        return None

    def ontology_file_formatter(self) -> None:
        """Reformat an .owl file to be consistent with the formatting used by the OWL API. To do this, an ontology
        referenced by graph_location is read in and output to the same location via the OWLTools API.

        Returns:
            None.

        Raises:
            TypeError: If something other than an .owl file is passed to function.
            IOError: If the graph_location file is empty.
            TypeError: If the input file contains no data.
        """

        print('\n*** Applying OWL API Formatting to Knowledge Graph OWL File ***')
        graph_write_location = self.write_location + self.full_kg

        # check input owl file
        if '.owl' not in graph_write_location:
            raise TypeError('ERROR: The provided file is not type .owl')
        elif not os.path.exists(graph_write_location):
            raise IOError('The {} file does not exist!'.format(graph_write_location))
        elif os.stat(graph_write_location).st_size == 0:
            raise TypeError('ERROR: input file: {} is empty'.format(graph_write_location))
        else:
            try:
                subprocess.check_call(['./resources/lib/owltools', graph_write_location, '-o', graph_write_location])
            except subprocess.CalledProcessError as error:
                print(error.output)

        return None

    def construct_knowledge_graph(self) -> None:
        """Builds a knowledge graph. The knowledge graph build is completed differently depending on the build type
        that the user requested. The build types that a user can request includes: "full", "partial", or "post-closure".

        The knowledge graph is built through the following steps:
            (1)  Set up environment.
            (2)  Process relation and inverse relation data.
            (3)  Process node metadata.
            (4)  Merge ontology data (class data).
            (5)  Add edges from master edge list to merged ontologies.
            (6)  Remove annotation assertions (if partial build).
            (7)  Add annotation assertions (if post-closure build).
            (8) Extract and write node metadata.
            (9) Decode OWL-encoded class and remove non-biologically meaningful semantics.
            (10) Output knowledge graph files and create edge lists.

        Returns:
            None.
        """

        pass

    def maps_node_ids_to_integers(self, output_triple_integers: str, output_triple_integers_map: str) -> None:
        """Loops over the knowledge graph in order to create three different types of files:
            - Integers: a tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
            subject, predicate, object). The subject, predicate, and object identifiers have been mapped to integers.
            - Identifiers: a tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
            subject, predicate, object). Both the subject and object identifiers have not been mapped to integers.
            - Identifier-Integer Map: a `.json` file containing a dictionary where the keys are node identifiers and
            the values are integers.

        Args:
            output_triple_integers: the name and file path to write out results.
            output_triple_integers_map: the name and file path to write out results.

        Returns:
            None.

        Raises:
            ValueError: If the length of the graph is not the same as the number of extracted triples.
        """

        # create dictionary for mapping and list to write edges to
        node_map, output_triples, node_counter = {}, 0, 0  # type: ignore
        graph_len = len(self.graph)

        # build graph from input file and set counter
        out_ints = open(self.write_location + output_triple_integers, 'w')
        out_ids = open(self.write_location + '_'.join(output_triple_integers.split('_')[:-1]) + '_Identifiers.txt', 'w')

        # write file headers
        out_ints.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
        out_ids.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')

        for edge in tqdm(self.graph):
            self.graph.remove(edge)

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
            out_ints.write('%d' % node_map[subj] + '\t' + '%d' % node_map[pred] + '\t' + '%d' % node_map[obj] + '\n')
            out_ids.write(subj + '\t' + pred + '\t' + obj + '\n')
            output_triples += 1

        out_ints.close(), out_ids.close()
        del self.graph

        # CHECK - verify we get the number of edges that we would expect to get
        if graph_len != output_triples:
            raise ValueError('ERROR: The number of triples is incorrect!')
        else:
            json.dump(node_map, open(self.write_location + '/' + output_triple_integers_map, 'w'))

        return None

    def converts_rdflib_to_networkx(self) -> None:
        """Converts an RDFLib.Graph object into a Networkx MultiDiGraph and pickles a copy locally.

        Returns:
            None.

        Raises:
            IOError: If the file referenced by filename does not exist.
        """

        print('\nConverting Knowledge Graph to MultiDiGraph')

        # read in knowledge graph if class graph attribute is not present
        try:
            graph = self.graph
        except (AttributeError, NameError):
            graph = Graph()
            graph.parse(self.write_location + self.full_kg)

        # convert graph to networkx object
        nx_mdg = networkx.MultiDiGraph()

        for s, p, o in tqdm(graph):
            graph.remove((s, p, o))
            nx_mdg.add_edge(s, o, **{'key': p})

        # pickle networkx graph
        print('\nPickling MultiDiGraph. For Large Networks Process Takes Several Minutes.')
        networkx.write_gpickle(nx_mdg, self.write_location + self.full_kg[:-4] + '_Networkx_MultiDiGraph.gpickle')

        # clean up environment
        del knowledge_graph, nx_mdg

        return None

    @abstractmethod
    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph build."""

        pass


class PartialBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph build."""

        return 'Partial Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a partial knowledge graph. A partial knowledge graph build is recommended when one intends to build a
        knowledge graph and intends to run a reasoner over it.

        The partial knowledge graph is built through the following steps:
            (1) Set up environment.
            (2) Process relation and inverse relation data.
            (3) Process node metadata.
            (4) Merge ontology data (class data).
            (5) Add edges from master edge list to merged ontologies.
            (6) Remove annotation assertions.

        Returns:
            None.

        Raises:
            TypeError: If the ontologies directory is empty.
        """

        print('\n### Starting Knowledge Graph Build: PARTIAL ###')

        # STEP 1: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 2: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 3: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(self.kg_version, 'no', self.write_location, self.full_kg, self.node_data, self.node_dict)
        metadata.node_metadata_processor()

        # STEP 4: MERGE ONTOLOGIES
        if self.write_location + self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            print('*** Loading Merged Ontologies ***')
            self.graph.parse(self.write_location + self.merged_ont_kg)

            # print stats
            edges = len(set(list(self.graph)))
            nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
            print('\nThe merged ontology contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))
        else:
            if len(self.ontologies) == 0:
                raise TypeError('ERROR: the ontologies directory: {} is empty'.format(
                    self.write_location + '/' + glob.glob('*/ontologies')[0]))
            else:
                print('*** Merging Ontology Data ***')
                self.merges_ontologies()

        # STEP 5: ADD MASTER EDGE DATA TO KNOWLEDGE GRAPH
        # create temporary directory to store partial builds
        temp_dir = '/'.join((self.write_location + self.full_kg).split('/')[:4])

        if temp_dir + '/partial_build' not in glob.glob(self.write_location + '/**/**'):
            os.mkdir(temp_dir + '/partial_build')

        # update path to write data to
        self.full_kg = '/'.join(self.full_kg.split('/')[:2] + ['partial_build'] + self.full_kg.split('/')[2:])
        metadata.full_kg = self.full_kg

        # build knowledge graph
        print('*** Building Knowledge Graph Edges ***')
        self.creates_knowledge_graph_edges(metadata.creates_node_metadata, metadata.adds_ontology_annotations)
        del self.graph, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 6: REMOVE ANNOTATION ASSERTIONS
        print('*** Removing Annotation Assertions ***')
        metadata.removes_annotation_assertions()
        del metadata

        return None


class PostClosureBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph being built."""

        return 'Post-Closure Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a post-closure knowledge graph. A post-closure knowledge graph build is recommended when one has
        previously performed a "partial" knowledge graph build and then ran a reasoner over the partially built
        knowledge graph. This build type inputs the closed partially built knowledge graph and completes the build
        process.

        The post-closure knowledge graph is built through the following steps:
            (1) Set up environment.
            (2) Process relation and inverse relation data.
            (3) Process node metadata.
            (4) Load closed knowledge graph data, knowledge graph with annotation assertions, and class-instance
                UUID-URI map.
            (5) Add annotation assertions.
            (6) Extract and write node metadata.
            (7) Decode OWL-encoded class and remove non-biologically meaningful semantics.
            (8) Output knowledge graph files and create edge lists

        Returns:
            None.

        Raises:
            TypeError: If the class-instance UUID Map file type is not json.
            IOError: If the class-instance UUID Map file does not exist.
            TypeError: If the class-instance UUID Map file is empty.
            TypeError: If the knowledge graph file type is not owl.
            IOError: If the knowledge graph file does not exist.
            TypeError: If the knowledge graph file is empty.
            TypeError: If the annotation assertion file type is not owl.
            IOError: If the annotation assertion file does not exist.
            TypeError: If the annotation assertion file is empty.
        """

        print('\n### Starting Knowledge Graph Build: post-closure ###')

        # STEP 1: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 2: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 3: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(self.kg_version, 'yes', self.write_location, self.full_kg, self.node_data, self.node_dict)
        metadata.node_metadata_processor()

        # STEP 4: LOAD CLOSED KNOWLEDGE GRAPH DATA
        uuid_location = input('Filepath to the class-instance UUID Map: ')
        annotation_assertions = input('Filepath to the knowledge graph with annotation assertions: ')
        closed_kg_location = input('Filepath to the closed knowledge graph: ')

        # check uuid input file
        if '.json' not in uuid_location:
            raise TypeError('ERROR: The provided file is not type .json')
        elif not os.path.exists(uuid_location):
            raise IOError('The {} file does not exist!'.format(uuid_location))
        elif os.stat(uuid_location).st_size == 0:
            raise TypeError('ERROR: input file: {} is empty'.format(uuid_location))
        else:
            # load closed knowledge graph
            print('*** Loading Knowledge Graph Class-Instance UUID Map ***')
            self.kg_uuid_map = json.load(open(uuid_location, 'r'))

            # move file out of 'partial build' directory
            os.rename(uuid_location, uuid_location.replace('partial_', '').replace('build/', '').replace('Not', ''))

        # check input owl file
        if '.owl' not in closed_kg_location:
            raise TypeError('ERROR: The provided file is not type .owl')
        elif not os.path.exists(closed_kg_location):
            raise IOError('The {} file does not exist!'.format(closed_kg_location))
        elif os.stat(closed_kg_location).st_size == 0:
            raise TypeError('ERROR: input file: {} is empty'.format(closed_kg_location))
        else:
            # load closed knowledge graph
            print('*** Loading Closed Knowledge Graph ***')
            self.graph = Graph()
            self.graph.parse(closed_kg_location)

            # print stats
            edges = len(set(list(self.graph)))
            nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
            print('\nThe closed ontology contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        # STEP 5: ADD ANNOTATION ASSERTIONS
        if '.txt' not in annotation_assertions:
            raise TypeError('ERROR: The provided file is not type .owl')
        elif not os.path.exists(annotation_assertions):
            raise IOError('The {} file does not exist!'.format(annotation_assertions))
        elif os.stat(annotation_assertions).st_size == 0:
            raise TypeError('ERROR: input file: {} is empty'.format(annotation_assertions))
        else:
            print('*** Loading Annotation Assertions Edge List ***')
            self.graph = metadata.adds_annotation_assertions(self.graph, annotation_assertions)

        # STEP 6: EXTRACT AND WRITE NODE METADATA
        print('*** Extracting and Writing Knowledge Graph Metadata ***')
        metadata.output_knowledge_graph_metadata(self.graph)
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 7: DECODE OWL SEMANTICS
        if self.decode_owl_semantics:
            print('*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
            owl_nets = OWLNETS(self.graph, self.kg_uuid_map, self.write_location, self.full_kg)
            self.graph = owl_nets.run_owl_nets()
            del owl_nets, self.kg_uuid_map

        # STEP 8: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        # output knowledge graph edge lists
        print('*** Writing Knowledge Graph Edge Lists ***')
        self.maps_node_ids_to_integers(self.full_kg[:-6] + 'Triples_Integers.txt',
                                       self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

        # convert graph into Networkx MultiDiGraph
        self.converts_rdflib_to_networkx()

        # remove partial build temp directory
        remove_partial_dir = input('\nDelete the "partial" directory?: (please type "yes" or "no")')

        if 'yes' in remove_partial_dir.lower():
            os.remove(glob.glob(self.write_location + '/'.join(self.full_kg.split('/')[0:2]) + '/partial_build')[0])

        return None


class FullBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph being built."""

        return 'Full Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a full knowledge graph. Please note that the process to build this version of the knowledge graph
        does not include running a reasoner.

        The full knowledge graph is built through the following steps:
            (1) Set up environment.
            (2) Process relation and inverse relation data.
            (3) Process node metadata.
            (4) Merge ontology data (class data).
            (5) Add edges from master edge list to merged ontologies.
            (6) Extract and write node metadata.
            (7) Decode OWL-encoded class and remove non-biologically meaningful semantics.
            (8) Output knowledge graph files and create edge lists.

        Returns:
            None.

        Raises:
            TypeError: If the ontology directory is empty.
        """

        print('\n### Starting Knowledge Graph Build: FULL ###')

        # STEP 1: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 2: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 3: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(self.kg_version, 'no', self.write_location, self.full_kg, self.node_data, self.node_dict)
        metadata.node_metadata_processor()

        # STEP 4: MERGE ONTOLOGIES
        if self.write_location + self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            print('*** Loading Merged Ontologies ***')
            self.graph.parse(self.write_location + self.merged_ont_kg)

            # print stats
            edges = len(set(list(self.graph)))
            nodes = len(set([str(node) for edge in list(self.graph) for node in edge[0::2]]))
            print('\nThe merged ontology contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))
        else:
            if len(self.ontologies) == 0:
                raise TypeError('ERROR: the ontologies directory: {} is empty'.format(
                    self.write_location + '/' + glob.glob('*/ontologies')[0]))
            else:
                print('*** Merging Ontology Data ***')
                self.merges_ontologies()

        # STEP 5: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        print('\n*** Building Knowledge Graph Edges ***')
        self.creates_knowledge_graph_edges(metadata.creates_node_metadata, metadata.adds_ontology_annotations)

        # STEP 6: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        metadata.output_knowledge_graph_metadata(self.graph)
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 7: DECODE OWL SEMANTICS
        if self.decode_owl_semantics:
            print('\n*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
            owl_nets = OWLNETS(self.graph, self.kg_uuid_map, self.write_location, self.full_kg)
            self.graph = owl_nets.run_owl_nets()
            del owl_nets, self.kg_uuid_map

        # STEP 8: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        # output knowledge graph edge lists
        print('\n*** Writing Knowledge Graph Edge Lists ***')
        self.maps_node_ids_to_integers(self.full_kg[:-6] + 'Triples_Integers.txt',
                                       self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

        # convert graph into Networkx MultiDiGraph
        self.converts_rdflib_to_networkx()

        return None
