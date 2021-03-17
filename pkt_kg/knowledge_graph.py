#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import copy
import glob
import json
import logging.config
import networkx  # type: ignore
import os
import os.path
import pandas  # type: ignore
import pickle
import shutil
import subprocess

from abc import ABCMeta, abstractmethod
from collections import Counter  # type: ignore
from rdflib import Graph, Namespace, URIRef, BNode  # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Any, Callable, Dict, IO, List, Optional, Set, Tuple, Union

from pkt_kg.__version__ import __version__
from pkt_kg.construction_approaches import KGConstructionApproach
from pkt_kg.metadata import Metadata
from pkt_kg.owlnets import OwlNets
from pkt_kg.utils import *

# set global attributes
obo = Namespace('http://purl.obolibrary.org/obo/')
# logging
log_dir, log, log_config = 'builds/logs', 'pkt_build_log.log', glob.glob('**/logging.ini', recursive=True)
try:
    if not os.path.exists(log_dir): os.mkdir(log_dir)
except FileNotFoundError:
    log_dir, log_config = '../builds/logs', glob.glob('../builds/logging.ini', recursive=True)
    if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


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
        node_data: A string containing "yes" or "no" indicating whether or not to add node data to the knowledge graph.
        inverse_relations: A string containing "yes" or "no" indicating whether or not to add inverse relations to the
            knowledge graph.
        decode_owl: A string containing "yes" or "no" indicating whether owl semantics should be removed.
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
        if not os.path.exists(self.res_dir + '/ontologies'):
            log_str = "Can't find 'ontologies/' directory"; logger.error("OSError: " + log_str); raise OSError(log_str)
        elif len(ontologies) == 0:
            log_str = 'Ontologies directory is empty'; logger.error('TypeError: ' + log_str); raise TypeError(log_str)
        else: self.ontologies: List[str] = ontologies

        # CONSTRUCTION APPROACH
        const = construction.lower() if isinstance(construction, str) else str(construction).lower()
        if const not in ['subclass', 'instance']:
            log_str = 'Construction must be "instance" or "subclass"'; logger.error('ValueError: ' + log_str)
            raise ValueError(log_str)
        else: self.construct_approach: str = const

        # GRAPH EDGE DATA
        edge_data = self.res_dir + '/Master_Edge_List_Dict.json'
        if not os.path.exists(edge_data):
            log_str = '{} file does not exist!'.format(edge_data); logger.error('OSError: ' + log_str)
            raise OSError(log_str)
        elif os.stat(edge_data).st_size == 0:
            log_str = 'The input file {} is empty'.format(edge_data); logger.error('TypeError: ' + log_str)
            raise TypeError(log_str)
        else:
            with(open(edge_data, 'r')) as _file: self.edge_dict: Dict = json.load(_file)

        # RELATIONS DATA
        inv, rel_dir = str(inverse_relations).lower(), glob.glob(self.res_dir + '/relations_data/*.txt')
        if inv not in ['yes', 'no']:
            log_str = 'inverse_relations must be "no" or "yes"'; logger.error('ValueError: ' + log_str)
            raise ValueError(log_str)
        elif len(rel_dir) == 0:
            log_str = 'relations_data directory is empty'; logger.error('TypeError: ' + log_str)
            raise TypeError(log_str)
        elif inv == 'yes':
            self.inverse_relations: Optional[List] = rel_dir; self.inverse_relations_dict: Optional[Dict] = {}
            rel = '_inverseRelations'
        else: self.inverse_relations, self.inverse_relations_dict, rel = None, None, '_relationsOnly'

        # NODE METADATA
        node_data, node_dir = str(node_data).lower(), glob.glob(self.res_dir + '/node_data/*.pkl')
        if node_data not in ['yes', 'no']:
            log_str = 'node_data not "no" or "yes"'; logger.error('ValueError: ' + log_str); raise ValueError(log_str)
        elif node_data == 'yes' and len(node_dir) == 0:
            log_str = 'node_data directory is empty'; logger.error('TypeError: ' + log_str); raise TypeError(log_str)
        elif node_data == 'yes' and len(node_dir) != 0:
            self.node_data: Optional[List] = node_dir; self.node_dict: Optional[Dict] = dict()
        else: self.node_data, self.node_dict = None, None

        # OWL SEMANTICS
        decode_owl = str(decode_owl).lower()
        if decode_owl not in ['yes', 'no']:
            log_str = 'decode_semantics not "no" or "yes"'; logger.error('ValueError: ' + log_str)
            raise ValueError(log_str)
        elif decode_owl == 'yes':
            self.decode_owl: Optional[str] = decode_owl; owl_kg = '_noOWL'
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

        if self.inverse_relations is not None:
            log_str = 'Loading and Processing Relation Data'; print(log_str); logging.info(log_str)
            for data in self.inverse_relations:
                with open(data, 'r') as f:
                    rel_data = [x.strip('\n').split('\t') for x in f.readlines()[1:]]  # assumes header
                    if 'inverse' in data.lower(): self.inverse_relations_dict = {x[0]: x[1] for x in rel_data}
                    else: self.relations_dict = {x[1].split('/')[-1]: x[0] for x in rel_data}

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
            log_str = 'object_property must be type rdflib.term.URIRef'; logger.error('TypeError: ' + log_str)
            raise TypeError(log_str)
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

    def checks_for_inverse_relations(self, relation: str, edge_list: Union[List, Set]) -> Optional[str]:
        """Checks a relation to determine whether or not edges for an inverse relation should be created and added to
        the knowledge graph. The function also verifies that input relation and its inverse (if it exists) are both an
        existing owl:ObjectProperty in the knowledge graph.

        Args:
            relation: A string that contains the relation assigned to edge in resource_info.txt (e.g. 'RO_0000056').
            edge_list: A list or set of knowledge graph edges. For example: {["8837", "4283"], ["8837", "839"]}

        Returns:
            inverse_rel: A string containing an ontology identifier (e.g. "RO_0000056) or None.
                The value depends on a set of conditions:
                    - inverse relation, if the stored relation has an inverse relation in inverse_relations
                    - current relation, if the stored relation string includes 'interact' and there is an equal count of
                      each node type (i.e., this is checking for symmetry in interaction-based edge types)
                    - None, assuming the prior listed conditions are not met
        """

        edge_list = set(tuple(x) for x in edge_list) if isinstance(edge_list, List) else edge_list
        if self.inverse_relations is not None:
            if self.inverse_relations_dict is not None and relation in self.inverse_relations_dict.keys():
                self.verifies_object_property(URIRef(obo + self.inverse_relations_dict[relation]))
                inverse_rel = self.inverse_relations_dict[relation]
            elif relation in self.relations_dict.keys() and 'interact' in self.relations_dict[relation]:
                inverse_rel = None if len([x for x in edge_list if x[-1::-1] not in edge_list]) == 0 else relation
            else: inverse_rel = None
        else: inverse_rel = None

        return inverse_rel

    @staticmethod
    def gets_edge_statistics(edge_type: str, results: Set, nodes: List) -> None:
        """Calculates the number of nodes and edges created from the build process.

        Args:
            edge_type: A string point to a specific edge type (e.g. 'chemical-disease).
            results: A set of tuples representing the complete set of triples generated from the construction process.
            nodes: A list of sets of tuples containing the raw node identifiers.

        Returns:
            None
        """

        n1, n2 = edge_type.split('-')[0], edge_type.split('-')[1]
        stats = [len(set(results)), len(nodes[0]), n1, len(nodes[1]), n2]
        stats_str = 'Edges: {}; Nodes: {} {}(s), {} {}(s)'.format(stats[0], stats[1], stats[2], stats[3], stats[4])
        print(stats_str); logger.info(stats_str)

        return None

    def creates_new_edges(self, node_metadata_func: Callable) -> None:
        """Takes a nested dictionary of edge lists and adds them to an existing knowledge graph by their edge_type (
        e.g. chemical-gene). Once the knowledge graph is complete, it is written out as an `.owl` file to the
        write_location  directory.

        Args:
            node_metadata_func: A function that adds metadata for non-ontology classes to a knowledge graph.

        Returns:
            None.
        """

        kg_owl = self.full_kg.replace('noOWL', 'OWL') if self.decode_owl == 'yes' else self.full_kg
        annot_loc, df = self.write_location + kg_owl[:-4] + '_AnnotationsOnly.nt', kg_owl[:-4] + '_LogicOnly.owl'
        kg_bld = KGConstructionApproach(self.res_dir); master_meta: Set = set()
        for edge_type in [x for x in self.edge_dict.keys() if x != 'entity_namespaces']:
            edge_list = self.edge_dict[edge_type]['edge_list']; del self.edge_dict[edge_type]['edge_list']
            s_type, o_type = self.edge_dict[edge_type]['data_type'].split('-'); node1, node2 = set(), set()
            rel, uri = self.edge_dict[edge_type]['edge_relation'], self.edge_dict[edge_type]['uri']
            invrel = self.checks_for_inverse_relations(rel, edge_list) if self.inverse_relations is not None else None
            p = 'Creating {} ({}-{}) Edges'.format(edge_type.upper(), s_type, o_type); print('\n' + p); logger.info(p)
            pbar = tqdm(total=len(edge_list)); res = set()
            while len(edge_list) > 0:
                pbar.update(1); edge = edge_list.pop(0)
                edge_info = {'n1': s_type, 'n2': o_type, 'rel': rel, 'inv_rel': invrel, 'uri': uri, 'edges': edge}
                meta = node_metadata_func(ent=[''.join(x) for x in list(zip(uri, edge))], e_type=[s_type, o_type])
                meta_logic = [True if (self.node_data is None and meta is None)
                              or [s_type, o_type] == ['class', 'class']
                              or (self.node_data is not None and meta is not None) else False][0]
                if self.check_ontology_class_nodes(edge_info) and meta_logic:  # ensure ont classes are in kg
                    if self.construct_approach == 'subclass': edges = kg_bld.subclass_constructor(edge_info, edge_type)
                    else: edges = kg_bld.instance_constructor(edge_info, edge_type)
                    self.graph = adds_edges_to_graph(self.graph, edges, False); res |= set(edges)
                    node1 |= {edge[0]}; node2 |= {edge[1]}
                    if meta is not None:
                        new_meta = {x for x in meta if x not in master_meta}; master_meta |= new_meta
                        if len(new_meta) > 0: appends_to_existing_file(new_meta, annot_loc, ' ')
            pbar.close(); self.gets_edge_statistics(edge_type, res, [node1, node2]); del [node1, node2]
        print('\nSerializing Knowledge Graph'); logger.info('Serializing Knowledge Graph')
        self.graph.serialize(self.write_location + df); ontology_file_formatter(self.write_location, df, self.owl_tools)
        if len(kg_bld.subclass_error.keys()) > 0:  # output error logs
            log_file = glob.glob(self.res_dir + '/construction*')[0] + '/subclass_map_log.json'
            logger.info('Some edge lists nodes were missing from the subclass_dict, see log: {}'.format(log_file))
            outputs_dictionary_data(kg_bld.subclass_error, log_file)

        return None

    def construct_knowledge_graph(self) -> None:
        """Builds a knowledge graph. The knowledge graph build is completed differently depending on the build type
        that the user requested. The build types include: "full", "partial", or "post-closure". The knowledge graph
        is built through the following steps: (1) Set up environment; (2) Process relation/inverse relations; (3)
        Create graph subsets; (4) Process node metadata; (5) Merge ontologies; (6) Add master edge list to merged
        ontologies; (7) Extract and write node metadata; (8) Decode OWL-encoded classes; and (8) Output knowledge
        graph files and create edge lists.

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
        Process relation/inverse relations; (2) Merge ontologies; (3) Process node metadata; (4) Create graph subsets;
        and (5) Add master edge list to merged ontologies.

        Returns:
            None.

        Raises:
            TypeError: If the ontologies directory is empty.
        """

        log_str = '### Starting Knowledge Graph Build: PARTIAL ###'; print('\n' + log_str)
        logger.info('*' * 10 + 'PKT STEP: CONSTRUCTING KNOWLEDGE GRAPH' + '*' * 10 + '\n' + log_str)

        # STEP 1: PROCESS RELATION AND INVERSE RELATION DATA
        log_str = '*** Loading Relations Data ***'; print(log_str); logger.info(log_str)
        self.reverse_relation_processor()

        # STEP 2: MERGE ONTOLOGIES
        if self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            log_str = '*** Loading Merged Ontologies ***'; print(log_str); logger.info(log_str)
            self.graph = Graph().parse(self.merged_ont_kg, format='xml')
        else:
            log_str = '*** Merging Ontology Data ***'; print(log_str); logger.info(log_str)
            merged_ontology_location = self.merged_ont_kg.split('/')[-1]
            merges_ontologies(self.ontologies, merged_ontology_location, self.owl_tools)
            self.graph.parse(self.merged_ont_kg, format='xml')  # load the merged ontology
        stats = derives_graph_statistics(self.graph); print(stats); logger.info(stats)

        # STEP 3: PROCESS NODE METADATA
        log_str = '*** Loading Node Metadata Data ***'; print(log_str); logger.info(log_str)
        meta = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: meta.metadata_processor(); meta.extract_metadata(self.graph); self.node_dict = meta.node_dict

        # STEP 4: CREATE GRAPH SUBSETS
        log_str = '*** Splitting Graph ***'; print(log_str); logger.info(log_str)
        self.graph, annotation_triples = splits_knowledge_graph(self.graph)
        full_kg_owl = self.full_kg.replace('noOWL', 'OWL') if self.decode_owl == 'yes' else self.full_kg
        annot, full = full_kg_owl[:-4] + '_AnnotationsOnly.nt', full_kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, self.write_location + annot, ' '); del annotation_triples
        stats = derives_graph_statistics(self.graph); print(stats); logger.info(stats)

        # STEP 5: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        log_str = '*** Building Knowledge Graph Edges ***'; print(log_str); logger.info(log_str)
        self.ont_classes = gets_ontology_classes(self.graph); self.obj_properties = gets_object_properties(self.graph)
        self.creates_new_edges(meta.creates_node_metadata)
        stats = derives_graph_statistics(self.graph); print(stats); logger.info(stats)
        # merge annotations with logic graph
        shutil.copy(self.write_location + annot, self.write_location + full)
        appends_to_existing_file(set(self.graph), self.write_location + full, ' ')

        del meta, self.edge_dict, self.graph, self.node_dict, self.relations_dict

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
        Load closed knowledge graph; (3) Process node metadata; (4) Create graph subsets; (5) Decode OWL-encoded
        classes; (6) Output knowledge graph files and create edge lists; and (7) Extract and write node metadata.

        Returns:
            None.

        Raises:
            OSError: If closed knowledge graph file does not exist.
            TypeError: If the closed knowledge graph file is empty.
        """

        log_str = '### Starting Knowledge Graph Build: POST-CLOSURE ###'; print('\n' + log_str)
        logger.info('*' * 10 + 'PKT STEP: CONSTRUCTING KNOWLEDGE GRAPH' + '*' * 10 + '\n' + log_str)

        # STEP 1: PROCESS RELATION AND INVERSE RELATION DATA
        log_str = '*** Loading Relations Data ***'; print(log_str); logger.info(log_str)
        self.reverse_relation_processor()

        # STEP 2: LOAD CLOSED KNOWLEDGE GRAPH
        closed_kg_location = glob.glob(self.write_location + '/*.owl')
        if len(closed_kg_location) == 0:
            log_str = 'The closed KG file does not exist!'; logger.error('OSError: ' + log_str); raise OSError(log_str)
        elif os.stat(closed_kg_location[0]).st_size == 0:
            log_str = '{} is empty'.format(closed_kg_location)
            logger.error('TypeError: ' + log_str); raise TypeError(log_str)
        else:
            log_str = '*** Loading Closed Knowledge Graph ***'; print(log_str); logger.info(log_str)
            os.rename(closed_kg_location[0], self.write_location + self.full_kg)  # rename closed kg file
            self.graph = Graph().parse(self.write_location + self.full_kg, format='xml')
        stats = derives_graph_statistics(self.graph); print(stats); logger.info(stats)

        # STEP 3: PROCESS NODE METADATA
        log_str = '*** Loading Node Metadata Data ***'; print(log_str); logger.info(log_str)
        meta = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: meta.metadata_processor(); meta.extract_metadata(self.graph); self.node_dict = meta.node_dict

        # STEP 4: CREATE GRAPH SUBSETS
        log_str = '*** Splitting Graph ***'; print(log_str); logger.info(log_str)
        self.graph, annotation_triples = splits_knowledge_graph(self.graph)
        full_kg_owl = self.full_kg.replace('noOWL', 'OWL') if self.decode_owl == 'yes' else self.full_kg
        annot, full = full_kg_owl[:-4] + '_AnnotationsOnly.nt', full_kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, self.write_location + annot, ' '); del annotation_triples
        stats = derives_graph_statistics(self.graph); print(stats); logger.info(stats)
        # merge annotations with logic graph
        shutil.copy(self.write_location + annot, self.write_location + full)
        appends_to_existing_file(set(self.graph), self.write_location + full, ' ')

        # STEP 5: DECODE OWL SEMANTICS
        if self.decode_owl:
            log_str = '*** Running OWL-NETS ***'; print('\n' + log_str); logger.info(log_str)
            owl_nets = OwlNets(self.graph, self.write_location, self.full_kg, self.construct_approach, self.owl_tools)
            results = owl_nets.run_owl_nets()
        else:
            logger.info('*** Converting Knowledge Graph to Networkx MultiDiGraph ***')
            convert_to_networkx(self.write_location, self.full_kg[:-4], self.graph); results = tuple([set(self.graph)])

        # STEP 6: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        log_str = '*** Building Knowledge Graph Edges ***'; print(log_str); logger.info(log_str)
        f_prefix = ('', '_' + self.construct_approach.upper() + '_purified') if len(results) == 2 else ('', '')
        for graph in results:
            if graph is not None:
                print('OWL-NETS Graph') if results.index(graph) == 0 else print('Purified OWL-NETS Graph')
                triple_list_file = self.full_kg[:-4] + f_prefix[results.index(graph)] + '_Triples_Integers.txt'
                triple_map = triple_list_file[:-5] + '_Identifier_Map.json'
                node_int_map = maps_ids_to_integers(graph, self.write_location, triple_list_file, triple_map)

                # STEP 8: EXTRACT AND WRITE NODE METADATA
                log_str = '*** Processing Metadata ***'; print('\n' + log_str); logger.info(log_str)
                meta.full_kg = self.full_kg[:-4] + f_prefix[results.index(graph)] + '.owl'
                if self.node_data: meta.output_metadata(node_int_map, graph)

        del meta, self.edge_dict, self.graph, self.node_dict, owl_nets, self.relations_dict, results

        return None


class FullBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph being built."""

        return 'Full Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a full knowledge graph. Please note that the process to build this version of the knowledge graph
        does not include running a reasoner. The full build includes the following steps: (1) Process relation/inverse
        relations; (2) Merge ontologies; (3) Process node metadata; (4) Create graph subsets; (5) Add master edge
        list to merged ontologies; (6) Decode OWL-encoded classes; (7) Output knowledge graphs and create edge lists
        and (8) Extract and write node metadata.

        Returns:
            None.
        """

        log_str = '### Starting Knowledge Graph Build: FULL ###'; print('\n' + log_str)
        logger.info('*' * 10 + 'PKT STEP: CONSTRUCTING KNOWLEDGE GRAPH' + '*' * 10 + '\n' + log_str)

        # STEP 1: PROCESS RELATION AND INVERSE RELATION DATA
        log_str = '*** Loading Relations Data ***'; print(log_str); logger.info(log_str)
        self.reverse_relation_processor()

        # STEP 2: MERGE ONTOLOGIES
        if self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            log_str = '*** Loading Merged Ontologies ***'; print(log_str); logger.info(log_str)
            self.graph = Graph().parse(self.merged_ont_kg, format='xml')
        else:
            log_str = '*** Merging Ontology Data ***'; print(log_str); logger.info(log_str)
            merges_ontologies(self.ontologies, self.merged_ont_kg.split('/')[-1], self.owl_tools)
            self.graph.parse(self.merged_ont_kg, format='xml')  # load the merged ontology
        stats = derives_graph_statistics(self.graph); print(stats); logger.info(stats)

        # STEP 3: PROCESS NODE METADATA
        log_str = '*** Loading Node Metadata Data ***'; print(log_str); logger.info(log_str)
        meta = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: meta.metadata_processor(); meta.extract_metadata(self.graph); self.node_dict = meta.node_dict

        # STEP 4: CREATE GRAPH SUBSETS
        log_str = '*** Splitting Graph ***'; print(log_str); logger.info(log_str)
        self.graph, annotation_triples = splits_knowledge_graph(self.graph)
        full_kg_owl = self.full_kg.replace('noOWL', 'OWL') if self.decode_owl == 'yes' else self.full_kg
        annot, full = full_kg_owl[:-4] + '_AnnotationsOnly.nt', full_kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, self.write_location + annot, ' '); del annotation_triples
        stats = derives_graph_statistics(self.graph); print(stats); logger.info(stats)

        # STEP 5: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        log_str = '*** Building Knowledge Graph Edges ***'; print('\n' + log_str); logger.info(log_str)
        self.ont_classes = gets_ontology_classes(self.graph); self.obj_properties = gets_object_properties(self.graph)
        self.creates_new_edges(meta.creates_node_metadata)
        stats = derives_graph_statistics(self.graph); print(stats); logger.info(stats)
        # merge annotations with logic graph
        shutil.copy(self.write_location + annot, self.write_location + full)
        appends_to_existing_file(set(self.graph), self.write_location + full, ' ')

        # STEP 6: DECODE OWL SEMANTICS
        if self.decode_owl:
            log_str = '*** Running OWL-NETS ***'; print('\n' + log_str); logger.info(log_str)
            owl_nets = OwlNets(self.graph, self.write_location, self.full_kg, self.construct_approach, self.owl_tools)
            results = owl_nets.run_owl_nets()
        else:
            logger.info('*** Converting Knowledge Graph to Networkx MultiDiGraph ***')
            convert_to_networkx(self.write_location, self.full_kg[:-4], self.graph); results = tuple([set(self.graph)])

        # STEP 7: WRITE OUT KNOWLEDGE GRAPH METADATA AND CREATE EDGE LISTS
        log_str = '*** Writing Knowledge Graph Edge Lists ***'; print('\n' + log_str); logger.info(log_str)
        f_prefix = ('', '_' + self.construct_approach.upper() + '_purified') if len(results) == 2 else ('', '')
        for graph in results:
            if graph is not None:
                print('OWL-NETS Graph') if results.index(graph) == 0 else print('Purified OWL-NETS Graph')
                triple_list_file = self.full_kg[:-4] + f_prefix[results.index(graph)] + '_Triples_Integers.txt'
                triple_map = triple_list_file[:-5] + '_Identifier_Map.json'
                node_int_map = maps_ids_to_integers(graph, self.write_location, triple_list_file, triple_map)

                # STEP 8: EXTRACT AND WRITE NODE METADATA
                log_str = '*** Processing Metadata ***'; print('\n' + log_str); logger.info(log_str)
                meta.full_kg = self.full_kg[:-4] + f_prefix[results.index(graph)] + '.owl'
                if self.node_data: meta.output_metadata(node_int_map, graph)

        del meta, self.edge_dict, self.graph, self.node_dict, owl_nets, self.relations_dict, results

        return None
