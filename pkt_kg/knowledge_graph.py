#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import copy
import glob
import json
import networkx  # type: ignore
import os
import os.path
import pandas  # type: ignore
import pickle
import subprocess

from abc import ABCMeta, abstractmethod
from rdflib import Graph, Namespace, URIRef, BNode  # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Any, Callable, Dict, IO, List, Optional, Set, Tuple

from pkt_kg.construction_approaches import KGConstructionApproach
from pkt_kg.metadata import Metadata
from pkt_kg.owlnets import OwlNets
from pkt_kg.utils import *

# set global attributes
obo = Namespace('http://purl.obolibrary.org/obo/')


class KGBuilder(object):
    """Class creates a semantic knowledge graph (KG). The class is designed to facilitate two KG construction
    approaches and three build types.

        Two Construction Approaches: (1) Instance-based: Adds edge data that is not from an ontology by connecting
        each non-ontology data node to an instances of existing ontology class; and (2) Subclass-based: Adds edge data
        that is not from an ontology by connecting each non-ontology data node to an existing ontology class.

        Three Build Types: (1) Full: Runs all build steps in the algorithm; (2) Partial: Runs all of the build steps
        through adding new edges. Designed for running a reasoner; and (3) Post-Closure: Runs the remaining build steps
        over a closed knowledge graph.

    Attributes:
        build: A string that indicates what kind of build (i.e. partial, post-closure, or full).
        decode_owl: A string indicating whether edges containing owl semantics should be removed.
        edge_data: A path to a file that references a dictionary of edge list tuples used to build the knowledge graph.
        edge_dict: A nested master edge list dict. The outer key is an edge-type (e.g.go-gene) and inner key is a dict
            storing data from the resource_info.txt input document. For example:
            {'chemical-complex': {'source_labels': ';;', 'data_type': 'class-instance', 'edge_relation': 'RO_0002436',
             'uri': ['http://purl.obolibrary.org/obo/', 'https://reactome.org/content/detail/'], 'delimiter': 't',
             'column_idx': '0;1', 'identifier_maps': 'None', 'evidence_criteria': 'None', 'filter_criteria': 'None',
             'edge_list': [['CHEBI_24505', 'R-HSA-1006173'], ...] }, }
        graph: An rdflib graph object which stores the knowledge graph.
        inverse_relations: A filepath to a directory called 'relations_data' containing the relations data.
        inverse_relations_dict: A dict storing relations ids and their inverse relation ids. For example:
            {'RO_0000056': 'RO_0000057', 'RO_0000079': 'RO_0000085'}
        node_data: A filepath to a directory called 'node_data' containing a file for each instance node.
        node_dict: A nested node metadata dict. The outer key is a node id and each inner key is a node type with the
            inner-key value being the corresponding metadata for that node type. For example:
                {'6469': {'Label': 'SHH', 'Description': 'Sonic Hedgehog Signaling Molecule is a protein-coding gene
                 located on chromosome 7 (map_location: 7q36.3).', 'Synonym': 'HHG1|HLP3|HPE3|ShhNC|TPT'}}
        kg_metadata: A flag that indicates whether or not to add metadata to the knowledge graph.
        ont_classes: A list of all ontology classes in a graph.
        owl_tools: A string pointing to the location of the owl tools library.
        relations_dict: A dict storing the relation identifiers and labels. For example,
            {'RO_0002616': 'related via evidence or inference to', 'RO_0002442': 'mutualistically interacts with}
        kg_version: A string that contains the version of the knowledge graph build.
        write_location: A file path used for writing knowledge graph data.
        res_dir: A string pointing to the 'resources' directory.
        merged_ont_kg: A string containing the filename of the knowledge graph that only contains the merged ontologies.
        ontologies: A list of file paths to an .owl file containing ontology data.
        construction: A string that indicates what type of construction approach to use (i.e. instance or subclass).
        decode_owl: A string indicating whether edges containing owl semantics should be removed.
        full_kg: A string containing the filename for the full knowledge graph.
        nx_mdg: A networkx MultiDiGraph object which is only created if the user requests owl semantics be removed.

    Raises:
        ValueError: If the formatting of kg_version is incorrect (i.e. not "v.#.#.#").
        ValueError: If write_location, edge_data does not contain a valid filepath.
        OSError: If the ontologies, edge_data, subclass_dict files don't not exist.
        TypeError: If the edge_data and subclass_dict files contains no data.
        TypeError: If the relations_data, node_data, ontologies directories do not contain any data.
        TypeError: If construction, inverse_relations, node_data, and decode_owl are not strings.
        ValueError: If relations_data, node_data and decode_owl_semantics do not contain "yes" or "no".
        ValueError: If construction does not contain "instance" or "subclass".
    """

    __metaclass__ = ABCMeta

    def __init__(self, kg_version: str, write_location: str, construction: str, edge_data: str, kg_metadata_flag: str,
                 node_data: Optional[str] = None, inverse_relations: Optional[str] = None, decode_owl: Optional[str]
                 = None) -> None:

        self.build: str = self.gets_build_type().lower().split()[0]
        self.decode_owl: Optional[str] = None
        self.edge_dict: Dict = dict()
        self.graph: Graph = Graph()
        self.inverse_relations: Optional[List] = None
        self.inverse_relations_dict: Optional[Dict] = None
        self.node_data: Optional[List] = None
        self.node_dict: Optional[Dict] = None
        self.kg_metadata: str = kg_metadata_flag.lower()
        self.ont_classes: Set = set()
        self.obj_properties: Set = set()
        self.owl_tools = './pkt_kg/libs/owltools'
        self.relations_dict: Dict = dict()

        # BUILD TYPE
        if kg_version is None:
            raise ValueError('kg_version must contain a valid version e.g. v.2.0.0, not None')
        else:
            self.kg_version = kg_version

        # WRITE LOCATION
        if write_location is None:
            raise ValueError('write_location must contain a valid filepath, not None')
        else:
            self.write_location = os.path.relpath(write_location)
            self.res_dir = os.path.relpath('/'.join(self.write_location.split('/')[:-1]))

        # MERGED ONTOLOGY FILE PATH
        if len(glob.glob(self.res_dir + '/**/PheKnowLator_MergedOntologies*.owl')) > 0:
            self.merged_ont_kg: str = glob.glob(self.res_dir + '/**/PheKnowLator_MergedOntologies*.owl')[0]
        else:
            self.merged_ont_kg = self.write_location + '/PheKnowLator_MergedOntologies.owl'

        # FINDING ONTOLOGIES DATA DIRECTORY
        if not os.path.exists(self.merged_ont_kg):
            if not os.path.exists(self.res_dir + '/ontologies'):
                raise OSError("Can't find 'ontologies/' directory, this directory is a required input")
            elif len(glob.glob(self.res_dir + '/ontologies/*.owl')) == 0:
                raise TypeError('The ontologies directory is empty')
            else:
                self.ontologies: List[str] = glob.glob(self.res_dir + '/ontologies/*.owl')
        else:
            self.ontologies = list()

        # CONSTRUCTION APPROACH
        if construction and not isinstance(construction, str):
            raise TypeError('construction must be type string')
        elif construction.lower() not in ['subclass', 'instance']:
            raise ValueError('construction must be "instance" or "subclass"')
        else:
            self.construct_approach: str = construction.lower()

        # KNOWLEDGE GRAPH EDGE LIST
        if edge_data is None:
            raise ValueError('edge_data must not contain a valid filepath, not None')
        elif not os.path.exists(edge_data):
            raise OSError('The {} file does not exist!'.format(edge_data))
        elif os.stat(edge_data).st_size == 0:
            raise TypeError('The input file: {} is empty'.format(edge_data))
        else:
            with open(edge_data, 'r') as edge_filepath:
                self.edge_dict = json.load(edge_filepath)

        # RELATIONS DATA
        if inverse_relations and not isinstance(inverse_relations, str):
            raise TypeError('inverse_relations must be type string')
        elif inverse_relations and inverse_relations.lower() not in ['yes', 'no']:
            raise ValueError('relations_data must be "no" or "yes"')
        else:
            if inverse_relations and inverse_relations.lower() == 'yes':
                if len(glob.glob(self.res_dir + '/relations_data/*.txt')) == 0:
                    raise TypeError('The relations_data directory is empty, this is a required input')
                else:
                    self.inverse_relations = glob.glob(self.res_dir + '/relations_data/*.txt')
                    self.inverse_relations_dict = dict()
                    build = self.build.replace('-', '_')
                    kg_rel = '/inverse_relations' + '/PheKnowLator_' + build + '_InverseRelations_'
            else:
                self.inverse_relations, self.inverse_relations_dict = None, None
                kg_rel = '/relations_only' + '/PheKnowLator_' + self.build.replace('-', '_') + '_'

        # NODE METADATA
        if node_data and not isinstance(node_data, str):
            raise TypeError('node_data must be type string')
        elif node_data and node_data.lower() not in ['yes', 'no']:
            raise ValueError('node_data must be "no" or "yes"')
        else:
            if node_data and node_data.lower() == 'yes':
                if len(glob.glob(self.res_dir + '/node_data/*.txt')) == 0:
                    raise TypeError('The node_data directory is empty')
                else:
                    self.node_data, self.node_dict = glob.glob(self.res_dir + '/node_data/*.txt'), dict()
                    kg_node = kg_rel + 'Closed_' if self.build == 'post-closure' else kg_rel + 'NotClosed_'
            else:
                self.node_data, self.node_dict = None, None
                kg_node = kg_rel + 'NoMetadata_Closed_' if 'closure' in self.build else kg_rel + 'NoMetadata_NotClosed_'

        # OWL SEMANTICS
        if decode_owl and not isinstance(decode_owl, str):
            raise TypeError('decode_owl must be type string')
        elif decode_owl and decode_owl.lower() not in ['yes', 'no']:
            raise ValueError('decode_semantics must be "no" or "yes"')
        else:
            if decode_owl and decode_owl.lower() == 'yes' and self.build != 'partial':
                self.full_kg: str = kg_node + 'NoOWLSemantics_KG.owl'
                self.decode_owl = decode_owl
            else:
                self.full_kg = kg_node + 'OWLSemantics_KG.owl'
                self.decode_owl = None

    def sets_up_environment(self) -> None:
        """Sets-up the environment by checking for the existence and/or creating the following directories:
            - 'knowledge_graphs' directory in the `resources` directory
            - `resources/knowledge_graphs/relations_only`, if full or partial build without inverse relations
            - `resources/knowledge_graphs/inverse_relations`, if full or partial build with inverse relations

        Returns:
            None.
        """

        if isinstance(self.inverse_relations, list):
            if not os.path.isdir(self.write_location + '/inverse_relations'):
                os.mkdir(self.write_location + '/inverse_relations')
        else:
            if not os.path.isdir(self.write_location + '/relations_only'):
                os.mkdir(self.write_location + '/relations_only')

        return None

    def reverse_relation_processor(self) -> None:
        """Creates and converts a Pandas DataFrame to a specific dictionary depending on whether it contains
        inverse relation data or relation data identifiers and labels. Examples of each dictionary are provided below:
            relations_dict: {'RO_0002551': 'has skeleton', 'RO_0002442': 'mutualistically interacts with}
            inverse_relations_dict: {'RO_0000056': 'RO_0000057', 'RO_0000079': 'RO_0000085'}

        Returns:
            None.
        """

        if self.inverse_relations:
            print('Loading and Processing Relation Data')

            for data in self.inverse_relations:
                df = pandas.read_csv(data, header=0, delimiter='\t')
                df.drop_duplicates(keep='first', inplace=True)
                df.set_index(list(df)[0], inplace=True)

                if 'inverse' in data.lower():
                    self.inverse_relations_dict = df.to_dict('index')
                else:
                    self.relations_dict = df.to_dict('index')

        return None

    def verifies_object_property(self, object_property: URIRef) -> None:
        """Takes a string that contains an object property, representing a relation between two nodes, and adds it to
        the knowledge graph.

        Args:
            object_property: A string containing an obo ontology object property.

        Returns:
            None.

        Raises:
            TypeError: If the object_property is not type rdflib.term.URIRef
        """

        if not isinstance(object_property, URIRef):
            raise TypeError('object_property must be type rdflib.term.URIRef')
        else:
            if object_property not in self.obj_properties:
                self.graph.add((object_property, RDF.type, OWL.ObjectProperty))
                self.obj_properties = gets_object_properties(self.graph)  # refresh list of object properties
            else:
                pass

        return None

    def check_ontology_class_nodes(self, edge_info) -> bool:
        """Determines whether or not an edge is safe to add to the knowledge graph by making sure that any ontology
        class nodes are also present in the current list of classes from the merged ontologies knowledge graph.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                 'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                 'edges': ['CHEBI_81395', 'DOID_12858']}

        Returns:
            True - if the class node is already in the knowledge graph.
            False - if the edge contains at least 1 ontology class that is not present in the knowledge graph.
        """

        if edge_info['n1'] != 'class' and edge_info['n2'] != 'class':
            class_found = True
        elif edge_info['n1'] == 'class' and edge_info['n2'] == 'class':
            n1, n2 = URIRef(obo + edge_info['edges'][0]), URIRef(obo + edge_info['edges'][1])
            class_found = n1 in self.ont_classes and n2 in self.ont_classes
        else:
            class_found = URIRef(finds_node_type(edge_info)['cls1']) in self.ont_classes

        return class_found

    def checks_for_inverse_relations(self, relation: str, edge_list: List[List[str]]) -> Optional[str]:
        """Checks a relation to determine whether or not edges for an inverse relation should be created and added to
        the knowledge graph. The function also verifies that input relation and its inverse (if it exists) are both an
        existing owl:ObjectProperty in the knowledge graph.

        Args:
            relation: A string that contains the relation assigned to edge in resource_info.txt (e.g. 'RO_0000056').
            edge_list: A list of knowledge graph edges. For example: [["8837", "4283"], ["8837", "839"]]

        Returns:
            A string containing an ontology identifier (e.g. "RO_0000056). The value depends on a set of conditions:
                - inverse relation, if the stored relation has an inverse relation in inverse_relations
                - current relation, if the stored relation string includes 'interact' and there is an equal count of
                  each node type (i.e., this is checking for symmetry in interaction-based edge types)
                - None, assuming the prior listed conditions are not met
        """

        # check for inverse relations
        inverse_relation = None

        if self.inverse_relations:
            if self.inverse_relations_dict and relation in self.inverse_relations_dict.keys():
                self.verifies_object_property(URIRef(obo + self.inverse_relations_dict[relation]['Inverse_Relation']))
                inverse_relation = self.inverse_relations_dict[relation]['Inverse_Relation']
            elif relation in self.relations_dict.keys():
                if 'interact' in self.relations_dict[relation]['Label']:
                    if len(set([x[0] for x in edge_list])) != len(set([x[1] for x in edge_list])):
                        inverse_relation = relation
            else:
                pass

        return inverse_relation

    def creates_knowledge_graph_edges(self, node_metadata_func: Callable, ontology_annotator_func: Callable) -> None:
        """Takes a nested dictionary of edge lists and adds them to an existing knowledge graph by their edge_type (
        e.g. chemical-gene). Once the knowledge graph is complete, it is written out as an `.owl` file to the
        write_location  directory.

        Args:
            node_metadata_func: A function that adds metadata for non-ontology classes to a knowledge graph.
            ontology_annotator_func: A function that adds annotations to an existing ontology.

        Returns:
            None.
        """

        edge_builder = KGConstructionApproach(self.edge_dict, self.res_dir)  # initialize construction approaches

        for edge_type in self.edge_dict.keys():
            n1_type, n2_type = self.edge_dict[edge_type]['data_type'].split('-')
            uri = self.edge_dict[edge_type]['uri']
            edge_list = copy.deepcopy(self.edge_dict[edge_type]['edge_list'])
            edge_results: List = []

            # identify relations
            rel = self.edge_dict[edge_type]['edge_relation']
            self.verifies_object_property(URIRef(obo + rel))  # verify object property in knowledge graph
            invrel = self.checks_for_inverse_relations(rel, edge_list) if self.inverse_relations else None

            print('\nCreating {} ({}-{}) Edges ***'.format(edge_type.upper(), n1_type, n2_type))

            for edge in tqdm(edge_list):
                edge_info = {'n1': n1_type, 'n2': n2_type, 'rel': rel, 'inv_rel': invrel, 'uri': uri, 'edges': edge}

                if self.check_ontology_class_nodes(edge_info):  # verify edges - make sure ont class nodes are in KG
                    if self.construct_approach == 'subclass':
                        self.edge_dict, new_edges = edge_builder.subclass_constructor(edge_info, edge_type)
                        self.graph = adds_edges_to_graph(self.graph, new_edges)  # add new edges to graph
                        edge_results += new_edges  # update list of added edges
                    else:
                        self.edge_dict, new_edges = edge_builder.instance_constructor(edge_info, edge_type)
                        self.graph = adds_edges_to_graph(self.graph, new_edges)  # add new edges to graph
                        edge_results += new_edges  # update list of added edges
                else:
                    self.edge_dict[edge_type]['edge_list'].pop(self.edge_dict[edge_type]['edge_list'].index(edge))

            n1, n2, edges = edge_type.split('-')[0], edge_type.split('-')[1], self.edge_dict[edge_type]['edge_list']
            print('Total OWL Edges: {}'.format(len(set(edge_results))))
            print('Unique Non-OWL Edges: {}'.format(len(edges) * 2 if invrel else len(edges)))
            print('Unique {}: {}'.format(n1, len(set([x[0] for x in self.edge_dict[edge_type]['edge_list']]))))
            print('Unique {}: {}'.format(n2, len(set([x[1] for x in self.edge_dict[edge_type]['edge_list']]))))

        # output error logs
        if len(edge_builder.subclass_error.keys()) > 0:
            log_file = glob.glob(self.res_dir + '/construction*')[0] + '/subclass_map_missing_node_log.json'
            print('\nSome edge lists nodes were missing from subclass_dict, see log: {}'.format(log_file))
            outputs_dictionary_data(edge_builder.subclass_error, log_file)

        # add ontology metadata and annotations, serialize graph, and apply OWL API formatting to output
        if self.kg_metadata == 'yes': node_metadata_func(self.graph, self.edge_dict)
        self.graph = ontology_annotator_func(self.full_kg.split('/')[-1], self.graph)
        print('\nSerializing Knowledge Graph')
        self.graph.serialize(destination=self.write_location + self.full_kg, format='xml')
        ontology_file_formatter(self.write_location, self.full_kg, self.owl_tools)

        return None

    def construct_knowledge_graph(self) -> None:
        """Builds a knowledge graph. The knowledge graph build is completed differently depending on the build type
        that the user requested. The build types include: "full", "partial", or "post-closure". The knowledge graph
        is built through the following steps: (1) Set up environment; (2) Process relation/inverse relations; (3)
        Process node metadata; (4) Merge ontologies; (5) Add master edge list to merged ontologies; (6) Extract and
        write node metadata; (7) Decode OWL-encoded classes; and (8) Output knowledge graph files and create edge lists.

        Returns:
            None.
        """

        pass

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
        knowledge graph and intends to run a reasoner over it. The partial build includes the following steps: (1)
        Set up environment; (2) Process relation/inverse relation data; (3) Process node metadata; (4) Merge
        ontologies; and (5) Add master edge list to merged ontologies.

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
        metadata = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: metadata.node_metadata_processor()

        # STEP 4: MERGE ONTOLOGIES
        if self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            print('*** Loading Merged Ontologies ***')
            self.graph.parse(self.merged_ont_kg, format='xml')
        else:
            if len(self.ontologies) == 0:
                raise TypeError('ERROR: the ontologies directory: {} is empty'.format(
                    self.write_location + '/' + glob.glob('*/ontologies')[0]))
            else:
                print('*** Merging Ontology Data ***')
                merges_ontologies(self.ontologies,
                                  self.write_location, '/' + self.merged_ont_kg.split('/')[-1],
                                  self.owl_tools)

                # load the merged ontology
                self.graph.parse(self.merged_ont_kg, format='xml')

        # print statistics
        gets_ontology_statistics(self.merged_ont_kg, self.owl_tools)
        print('The Merged Core Ontology Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        # STEP 5: ADD MASTER EDGE DATA TO KNOWLEDGE GRAPH
        # create temporary directory to store partial builds and update path to write data to
        temp_dir = self.write_location + '/' + self.full_kg.split('/')[1] + '/partial_build'
        if temp_dir not in glob.glob(self.write_location + '/**/**'): os.mkdir(temp_dir)
        self.full_kg = '/'.join(self.full_kg.split('/')[:2] + ['partial_build'] + self.full_kg.split('/')[2:])
        metadata.full_kg = self.full_kg

        # build knowledge graph
        print('*** Building Knowledge Graph Edges ***')
        self.ont_classes = gets_ontology_classes(self.graph)
        self.obj_properties = gets_object_properties(self.graph)
        self.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # print statistics
        gets_ontology_statistics(self.write_location + self.full_kg, self.owl_tools)
        print('The Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        return None


class PostClosureBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph being built."""

        return 'Post-Closure Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a post-closure knowledge graph. This build is recommended when one has previously performed a
        "partial" knowledge graph build and then ran a reasoner over it. This build type inputs the closed partially
        built knowledge graph and completes the build process.

        The post-closure build utilizes the following steps: (1) Set up environment; (2) Process relation and inverse
        relation data; (3) Process node metadata; (4) Load closed knowledge graph; (5) Extract and write node metadata;
        (6) Decode OWL-encoded classes; and (7) Output knowledge graph files and create edge lists.

        Returns:
            None.

        Raises:
            OSError: If closed knowledge graph file does not exist.
            TypeError: If the closed knowledge graph file is empty.
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
        metadata = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: metadata.node_metadata_processor()

        # STEP 3: LOAD CLOSED KNOWLEDGE GRAPH
        closed_kg_location = glob.glob(self.write_location + '/'.join(self.full_kg.split('/')[0:2]) + '/*.owl')

        if len(closed_kg_location) == 0:
            raise OSError('The closed KG file does not exist!')
        elif os.stat(closed_kg_location[0]).st_size == 0:
            raise TypeError('input file: {} is empty'.format(closed_kg_location))
        else:
            print('*** Loading Closed Knowledge Graph ***')
            os.rename(closed_kg_location[0], self.write_location + self.full_kg)  # rename closed kg file
            self.graph = Graph()
            self.graph.parse(self.write_location + self.full_kg, format='xml')

            # print statistics
            gets_ontology_statistics(self.write_location + self.full_kg, self.owl_tools)
            print('The Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        # STEP 5: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        if self.node_data is not None: metadata.output_knowledge_graph_metadata(self.graph)

        # clean up environment
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 6: DECODE OWL SEMANTICS
        if self.decode_owl:
            print('*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
            owl_nets = OwlNets(self.construct_approach, self.graph, self.write_location, self.full_kg)
            self.graph = owl_nets.run_owl_nets()

            # reformat output and output statistics
            gets_ontology_statistics(self.write_location + self.full_kg, self.owl_tools)
            ontology_file_formatter(self.write_location, self.full_kg, self.owl_tools)
            print('The OWL-Decoded Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        # STEP 7: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        print('*** Writing Knowledge Graph Edge Lists ***')
        maps_node_ids_to_integers(self.graph, self.write_location,
                                  self.full_kg[:-6] + 'Triples_Integers.txt',
                                  self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

        converts_rdflib_to_networkx(self.write_location, self.full_kg, self.graph)

        return None


class FullBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph being built."""

        return 'Full Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a full knowledge graph. Please note that the process to build this version of the knowledge graph
        does not include running a reasoner. The full build includes the following steps: (1) Set up environment; (2)
        Process relation/inverse relations; (3) Process node metadata; (4) Merge ontologies; and (5) Add master edge
        list to merged ontologies; (6) Extract and write node metadata; (7) Decode OWL-encoded classes; and (8)
        Output knowledge graphs and create edge lists.

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
        metadata = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: metadata.node_metadata_processor()

        # STEP 4: MERGE ONTOLOGIES
        if self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            print('*** Loading Merged Ontologies ***')
            self.graph.parse(self.merged_ont_kg, format='xml')
        else:
            if len(self.ontologies) == 0:
                raise TypeError('The ontologies directory is empty')
            else:
                print('*** Merging Ontology Data ***')
                merges_ontologies(self.ontologies,
                                  self.write_location, '/' + self.merged_ont_kg.split('/')[-1],
                                  self.owl_tools)
                # load the merged ontology
                self.graph.parse(self.merged_ont_kg, format='xml')

        # print statistics
        gets_ontology_statistics(self.merged_ont_kg, self.owl_tools)
        print('The Merged Core Ontology Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        self.ont_classes = gets_ontology_classes(self.graph)
        self.obj_properties = gets_object_properties(self.graph)

        # STEP 5: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        print('\n*** Building Knowledge Graph Edges ***')
        self.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)
        gets_ontology_statistics(self.write_location + self.full_kg, self.owl_tools)
        print('The Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        # STEP 6: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        if self.node_data is not None or self.kg_metadata == 'yes': metadata.output_knowledge_graph_metadata(self.graph)

        # clean up environment
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 7: DECODE OWL SEMANTICS
        if self.decode_owl:
            print('\n*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
            owl_nets = OwlNets(self.construct_approach, self.graph, self.write_location, self.full_kg)
            self.graph = owl_nets.run_owl_nets()

            # reformat output and output statistics
            gets_ontology_statistics(self.write_location + self.full_kg, self.owl_tools)
            ontology_file_formatter(self.write_location, self.full_kg, self.owl_tools)
            print('The OWL-Decoded Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        # STEP 8: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        print('\n*** Writing Knowledge Graph Edge Lists ***')
        maps_node_ids_to_integers(self.graph, self.write_location,
                                  self.full_kg[:-6] + 'Triples_Integers.txt',
                                  self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

        converts_rdflib_to_networkx(self.write_location, self.full_kg, self.graph)

        return None
