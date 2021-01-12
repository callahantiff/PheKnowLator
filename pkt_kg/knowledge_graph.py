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

from pkt_kg.__version__ import __version__
from pkt_kg.construction_approaches import KGConstructionApproach
from pkt_kg.metadata import Metadata
from pkt_kg.owlnets import OwlNets
from pkt_kg.utils import *

# set global attributes
obo = Namespace('http://purl.obolibrary.org/obo/')


class KGBuilder(object):
    """Class creates a semantic knowledge graph (KG). The class is designed to facilitate two KG construction
    approaches and three build types. The class handles two types of construction approaches and three types of builds:
      - Two Construction Approaches: (1) Instance-based: Adds edge data that is not from an ontology by connecting
        each non-ontology data node to an instances of existing ontology class; and (2) Subclass-based: Adds edge data
        that is not from an ontology by connecting each non-ontology data node to an existing ontology class.
      - Three Build Types: (1) Full: Runs all build steps in the algorithm; (2) Partial: Runs all of the build steps
        through adding new edges. Designed for running a reasoner; and (3) Post-Closure: Runs the remaining build steps
        over a closed knowledge graph.

    Attributes:
        construction: A string that indicates what type of construction approach to use (i.e. instance or subclass).
        node_data: A filepath to a directory called 'node_data' containing a file for each instance node.
        inverse_relations: A filepath to a directory called 'relations_data' containing the relations data.
        decode_owl: A string indicating whether edges containing owl semantics should be removed.
        write_location: An optional string passed to specify the primary directory to write to.

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

    def __init__(self, construction: str, node_data: str, inverse_relations: str, decode_owl: str,
                 write_location: str = os.path.abspath('./resources/knowledge_graphs')) -> None:

        self.build: str = self.gets_build_type().lower().split()[0]
        self.graph: Graph = Graph()
        self.kg_version: str = 'v' + __version__
        self.obj_properties: Set = set()
        self.ont_classes: Set = set()
        self.owl_tools: str = './pkt_kg/libs/owltools'
        self.relations_dict: Dict = dict()
        self.write_location: str = write_location
        self.res_dir: str = os.path.abspath('/'.join(self.write_location.split('/')[:-1]))
        self.merged_ont_kg: str = self.write_location + '/PheKnowLator_MergedOntologies.owl'

        # ONTOLOGIES DATA DIRECTORY
        ontologies = glob.glob(self.res_dir + '/ontologies/*.owl')
        if not os.path.exists(self.res_dir + '/ontologies'): raise OSError("Can't find 'ontologies/' directory")
        elif len(ontologies) == 0: raise TypeError('The ontologies directory is empty')
        else: self.ontologies: List[str] = ontologies

        # CONSTRUCTION APPROACH
        const = construction.lower() if isinstance(construction, str) else str(construction).lower()
        if const not in ['subclass', 'instance']: raise ValueError('construction must be "instance" or "subclass"')
        else: self.construct_approach: str = const

        # GRAPH EDGE DATA
        edge_data = self.res_dir + '/Master_Edge_List_Dict.json'
        if not os.path.exists(edge_data): raise OSError('The {} file does not exist!'.format(edge_data))
        elif os.stat(edge_data).st_size == 0: raise TypeError('The input file: {} is empty'.format(edge_data))
        else:
            with(open(edge_data, 'r')) as _file: self.edge_dict: Dict = json.load(_file)

        # RELATIONS DATA
        inv, rel_dir = str(inverse_relations).lower(), glob.glob(self.res_dir + '/relations_data/*.txt')
        if inv not in ['yes', 'no']: raise ValueError('inverse_relations not "no" or "yes"')
        elif len(rel_dir) == 0: raise TypeError('relations_data directory is empty')
        elif inv == 'yes':
            self.inverse_relations: Optional[List] = rel_dir
            self.inverse_relations_dict: Optional[Dict] = {}
            rel = '_inverseRelations'
        else: self.inverse_relations, self.inverse_relations_dict, rel = None, None, '_relationsOnly'

        # NODE METADATA
        node_data, node_dir = str(node_data).lower(), glob.glob(self.res_dir + '/node_data/*.pkl')
        if node_data not in ['yes', 'no']: raise ValueError('node_data not "no" or "yes"')
        elif node_data == 'yes' and len(node_dir) == 0: raise TypeError('node_data directory is empty')
        elif node_data == 'yes' and len(node_dir) != 0:
            self.node_data: Optional[List] = node_dir
            self.node_dict: Optional[Dict] = dict()
        else: self.node_data, self.node_dict = None, None

        # OWL SEMANTICS
        decode_owl = str(decode_owl).lower()
        if decode_owl not in ['yes', 'no']: raise ValueError('decode_semantics not "no" or "yes"')
        elif decode_owl == 'yes':
            self.decode_owl: Optional[str] = decode_owl
            owl_kg = '_noOWL'
        else: self.decode_owl, owl_kg = None, '_OWL'

        # KG FILE NAME
        self.full_kg: str = '/PheKnowLator_' + self.kg_version + '_' + self.build + '_' + const + rel + owl_kg + '.owl'

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
                if 'inverse' in data.lower(): self.inverse_relations_dict = df.to_dict('index')
                else: self.relations_dict = df.to_dict('index')

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

        if edge_info['n1'] != 'class' and edge_info['n2'] != 'class': class_found = True
        elif edge_info['n1'] == 'class' and edge_info['n2'] == 'class':
            n1, n2 = URIRef(obo + edge_info['edges'][0]), URIRef(obo + edge_info['edges'][1])
            class_found = n1 in self.ont_classes and n2 in self.ont_classes
        else: class_found = URIRef(finds_node_type(edge_info)['cls1']) in self.ont_classes

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

    def gets_edge_statistics(self, edge_type: str, invrel: Optional[str], results: List) -> None:
        """Calculates the number of nodes and edges created from the build process.

        Args:
            edge_type: A string point to a specific edge type (e.g. 'chemical-disease).
            invrel: A string if there are inverse edges or None.
            results: A list of tuples representing the complete set of triples generated from the construction process.

        Returns:
            None
        """

        n1, n2, edges = edge_type.split('-')[0], edge_type.split('-')[1], self.edge_dict[edge_type]['edge_list']

        print('Total OWL Edges: {}'.format(len(set(results))))
        print('Unique Non-OWL Edges: {}'.format(len(edges) * 2 if invrel else len(edges)))
        print('Unique {}: {}'.format(n1, len(set([x[0] for x in self.edge_dict[edge_type]['edge_list']]))))
        print('Unique {}: {}'.format(n2, len(set([x[1] for x in self.edge_dict[edge_type]['edge_list']]))))

        return None

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

        edge_builder = KGConstructionApproach(kg.edge_dict, kg.res_dir)  # initialize construction approaches
        for edge_type in [x for x in kg.edge_dict.keys() if '-' in x]:
            n1_type, n2_type = kg.edge_dict[edge_type]['data_type'].split('-')
            uri, edge_list = kg.edge_dict[edge_type]['uri'], copy.deepcopy(kg.edge_dict[edge_type]['edge_list'])
            edge_results: List = []
            rel = kg.edge_dict[edge_type]['edge_relation']
            kg.verifies_object_property(URIRef(obo + rel))  # verify object property in knowledge graph
            invrel = kg.checks_for_inverse_relations(rel, edge_list) if kg.inverse_relations else None
            print('\nCreating {} ({}-{}) Edges ***'.format(edge_type.upper(), n1_type, n2_type))
            for edge in tqdm(edge_list):
                edge_info = {'n1': n1_type, 'n2': n2_type, 'rel': rel, 'inv_rel': invrel, 'uri': uri, 'edges': edge}
                meta = node_metadata_func([''.join(x) for x in list(zip(uri, edge))], [n1_type, n2_type])
                metadata_logic = [True if kg.node_data is None and meta is None
                                  or kg.node_data is not None and meta is not None else False][0]
                if kg.check_ontology_class_nodes(edge_info) and metadata_logic:  # make sure ont class nodes are in KG
                    if n1_type != 'class': updates_graph_namespace(n1_type, kg.graph, uri[0 + edge[0]])
                    if n2_type != 'class': updates_graph_namespace(n2_type, kg.graph, uri[1 + edge[1]])
                    if kg.construct_approach == 'subclass':
                        kg.edge_dict, new_edges = edge_builder.subclass_constructor(edge_info, edge_type)
                        edge_results += new_edges
                        if self.node_data and meta: self.graph = adds_edges_to_graph(self.graph, new_edges + meta)
                    else:
                        kg.edge_dict, new_edges = edge_builder.instance_constructor(edge_info, edge_type)
                        edge_results += new_edges
                        if self.node_data and meta: self.graph = adds_edges_to_graph(self.graph, new_edges)
                else: self.edge_dict[edge_type]['edge_list'].pop(self.edge_dict[edge_type]['edge_list'].index(edge))
            self.gets_edge_statistics(edge_type, invrel, edge_results)
        if len(edge_builder.subclass_error.keys()) > 0:  # output error logs
            log_file = glob.glob(self.res_dir + '/construction*')[0] + '/subclass_map_missing_node_log.json'
            print('\nSome edge lists nodes were missing from subclass_dict, see log: {}'.format(log_file))
            outputs_dictionary_data(edge_builder.subclass_error, log_file)
        self.graph = ontology_annotator_func(self.full_kg.split('/')[-1], self.graph)
        print('\nSerializing Knowledge Graph')
        full_kg_owl = self.full_kg.replace('noOWL', 'OWL') if self.decode_owl == 'yes' else self.full_kg
        self.graph.serialize(destination=self.write_location + full_kg_owl, format='xml')
        ontology_file_formatter(self.write_location, full_kg_owl, self.owl_tools)

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
        Process relation/inverse relation data; (2) Process node metadata; (3) Merge ontologies; and (4) Add master
        edge list to merged ontologies.

        Returns:
            None.

        Raises:
            TypeError: If the ontologies directory is empty.
        """

        print('\n### Starting Knowledge Graph Build: PARTIAL ###')

        # STEP 1: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 2: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: metadata.node_metadata_processor()

        # STEP 3: MERGE ONTOLOGIES
        if self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            print('*** Loading Merged Ontologies ***')
            self.graph.parse(self.merged_ont_kg, format='xml')
        else:
            print('*** Merging Ontology Data ***')
            merged_ontology_location = self.merged_ont_kg.split('/')[-1]
            merges_ontologies(self.ontologies, merged_ontology_location, self.owl_tools)
            self.graph.parse(self.merged_ont_kg, format='xml')  # load the merged ontology
        # print merged ontology statistics
        gets_ontology_statistics(self.merged_ont_kg, self.owl_tools)
        print('The Merged Core Ontology Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        # STEP 4: ADD MASTER EDGE DATA TO KNOWLEDGE GRAPH
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

        The post-closure build utilizes the following steps: (1) Process relation and inverse relation data; (2)
        Process node metadata; (3) Load closed knowledge graph; (4) Extract and write node metadata; (5) Decode
        OWL-encoded classes; and (6) Output knowledge graph files and create edge lists.

        Returns:
            None.

        Raises:
            OSError: If closed knowledge graph file does not exist.
            TypeError: If the closed knowledge graph file is empty.
        """

        print('\n### Starting Knowledge Graph Build: post-closure ###')

        # STEP 1: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 2: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: metadata.node_metadata_processor()

        # STEP 3: LOAD CLOSED KNOWLEDGE GRAPH
        closed_kg_location = glob.glob(self.write_location + '/*.owl')
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

        # STEP 4: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        if self.node_data: metadata.output_knowledge_graph_metadata(self.graph)
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict  # clean env

        # STEP 5: DECODE OWL SEMANTICS
        if self.decode_owl:
            print('*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
            owl_nets = OwlNets(self.construct_approach, self.graph, self.write_location, self.full_kg, self.owl_tools)
            self.graph = owl_nets.run_owl_nets()
        else:
            converts_rdflib_to_networkx(self.write_location, self.full_kg[:-4], self.graph)

        # STEP 6: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        print('*** Writing Knowledge Graph Edge Lists ***')
        maps_node_ids_to_integers(self.graph, self.write_location,
                                  self.full_kg[:-4] + '_Triples_Integers.txt',
                                  self.full_kg[:-4] + '_Triples_Integer_Identifier_Map.json')

        return None


class FullBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph being built."""

        return 'Full Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a full knowledge graph. Please note that the process to build this version of the knowledge graph
        does not include running a reasoner. The full build includes the following steps: (1) Process relation/inverse
        relations; (2) Merge ontologies; (3) Process node metadata; (4) Add master edge list to  merged ontologies; (5)
        Extract and write node metadata; (6) Decode OWL-encoded classes and (7) Output knowledge graphs and create
        edge lists.

        Returns:
            None.

        Raises:
            TypeError: If the ontology directory is empty.
        """

        print('\n### Starting Knowledge Graph Build: FULL ###')

        # STEP 1: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 2: MERGE ONTOLOGIES
        if self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            print('*** Loading Merged Ontologies ***')
            self.graph.parse(self.merged_ont_kg, format='xml')
        else:
            print('*** Merging Ontology Data ***')
            merged_ontology_location = self.merged_ont_kg.split('/')[-1]
            merges_ontologies(self.ontologies, merged_ontology_location, self.owl_tools)
            self.graph.parse(self.merged_ont_kg, format='xml')  # load the merged ontology
        # print statistics on the merged ontologies
        gets_ontology_statistics(self.merged_ont_kg, self.owl_tools)
        print('The Merged Core Ontology Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        # STEP 3: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(kg.kg_version, kg.write_location, kg.full_kg, kg.node_data, kg.node_dict)
        if self.node_data: metadata.node_metadata_processor(), metadata.extracts_class_metadata(kg.graph)
        self.node_dict = metadata.node_dict

        # STEP 4: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        print('\n*** Building Knowledge Graph Edges ***')
        kg.ont_classes = gets_ontology_classes(kg.graph)
        kg.obj_properties = gets_object_properties(kg.graph)
        self.creates_knowledge_graph_edges(metadata.creates_node_metadata, metadata.adds_ontology_annotations)
        gets_ontology_statistics(self.write_location + self.full_kg, self.owl_tools)
        print('The Knowledge Graph Contains: {} Triples'.format(len(self.graph)))

        # STEP 5: DECODE OWL SEMANTICS
        if self.decode_owl:
            print('\n*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
            owl_nets = OwlNets(self.graph, self.write_location, self.full_kg, self.construct_approach, self.owl_tools)
            self.graph = owl_nets.run_owl_nets()
        else: converts_rdflib_to_networkx(self.write_location, self.full_kg[:-4], self.graph)

        # STEP 6: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        print('\n*** Writing Knowledge Graph Edge Lists ***')
        node_int_map = maps_node_ids_to_integers(kg.graph, kg.write_location,
                                                 kg.full_kg[:-4] + '_Triples_Integers.txt',
                                                 kg.full_kg[:-4] + '_Triples_Integer_Identifier_Map.json')

        # STEP 7: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        if self.node_data: metadata.output_knowledge_graph_metadata(node_int_map)

        # clean environment
        del metadata, self.edge_dict, self.graph, self.inverse_relations_dict, self.node_dict, self.relations_dict

        return None
