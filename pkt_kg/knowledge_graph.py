#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import copy
import glob
import json
import logging.config
import networkx  # type: ignore
import numpy as np  # type: ignore
import os
import os.path
import pandas  # type: ignore
import pickle
import ray  # type: ignore
import shutil
import subprocess

from abc import ABCMeta, abstractmethod
from collections import ChainMap, Counter  # type: ignore
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
log_dir, f_log, log_config = 'builds/logs', 'pkt_build_log.log', glob.glob('**/logging.ini', recursive=True)
try:
    if not os.path.exists(log_dir): os.mkdir(log_dir)
except FileNotFoundError:
    log_dir, log_config = '../builds/logs', glob.glob('../builds/logging.ini', recursive=True)
    if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + f_log})


class KGBuilder(object):
    """Class creates a semantic knowledge graph. The class currently facilitates two construction approaches and three
    build types. The current construction approaches are Instance-based and Subclass-based. The three build types are
    (1) Full (i.e. runs all build steps in the algorithm); (2) Partial (i.e. runs all of the build steps through
    adding new edges); and (3) Post-Closure: Runs the remaining build steps over a closed knowledge graph.

    Attributes:
        construction: A string indicating the construction approach (i.e. instance or subclass).
        node_data: A string ("yes" or "no") indicating whether or not to add node data to the knowledge graph.
        inverse_relations: A string ("yes" or "no") indicating whether or not to add inverse edge relations.
        decode_owl: A string containing "yes" or "no" indicating whether owl semantics should be removed.
        cpus: An integer indicating the number of workers to use.
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

    def __init__(self, construction: str, node_data: str, inverse_relations: str, decode_owl: str, cpus: int = 1,
                 write_location: str = os.path.abspath('./resources/knowledge_graphs')) -> None:

        self.cpus: int = cpus
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

        # CONSTRUCTION APPROACH
        const = construction.lower() if isinstance(construction, str) else str(construction).lower()
        if const not in ['subclass', 'instance']:
            log = 'construction not "instance" or "subclass"'; logger.error('ValueError: ' + log); raise ValueError(log)
        else: self.construct_approach: str = const

        # ONTOLOGIES DATA DIRECTORY
        onts = glob.glob(self.res_dir + '/ontologies/*.owl')
        if not os.path.exists(self.res_dir + '/ontologies'):
            log = "Can't find 'ontologies/' directory"; logger.error("OSError: " + log); raise OSError(log)
        elif len(onts) == 0: log = 'Ontologies dir is empty'; logger.error('TypeError: ' + log); raise TypeError(log)
        else: self.ontologies: List[str] = onts

        # GRAPH EDGE DATA
        edge_data = self.res_dir + '/Master_Edge_List_Dict.json'
        if not os.path.exists(edge_data):
            log = '{} file does not exist!'.format(edge_data); logger.error('OSError: ' + log); raise OSError(log)
        elif os.stat(edge_data).st_size == 0:
            log = '{} is empty'.format(edge_data); logger.error('TypeError: ' + log); raise TypeError(log)
        else:
            with(open(edge_data, 'r')) as _file: self.edge_dict: Dict = json.load(_file)

        # RELATIONS DATA
        inv, rel_dir = str(inverse_relations).lower(), glob.glob(self.res_dir + '/relations_data/*.txt')
        if inv not in ['yes', 'no']:
            log = 'inverse_relations not "no" or "yes"'; logger.error('ValueError: ' + log); raise ValueError(log)
        elif len(rel_dir) == 0:
            log = 'relations_data dir is empty'; logger.error('TypeError: ' + log); raise TypeError(log)
        elif inv == 'yes':
            self.inverse_relations: Optional[List] = rel_dir; self.inverse_relations_dict: Optional[Dict] = {}
            rel = '_inverseRelations'
        else: self.inverse_relations, self.inverse_relations_dict, rel = None, None, '_relationsOnly'

        # NODE METADATA
        node_data, node_dir = str(node_data).lower(), glob.glob(self.res_dir + '/node_data/*.pkl')
        if node_data not in ['yes', 'no']:
            log = 'node_data not "no" or "yes"'; logger.error('ValueError: ' + log); raise ValueError(log)
        elif node_data == 'yes' and len(node_dir) == 0:
            log = 'node_data dir is empty'; logger.error('TypeError: ' + log); raise TypeError(log)
        elif node_data == 'yes' and len(node_dir) != 0:
            self.node_data: Optional[List] = node_dir; self.node_dict: Optional[Dict] = dict()
        else: self.node_data, self.node_dict = None, None

        # OWL SEMANTICS
        decode_owl = str(decode_owl).lower()
        if decode_owl not in ['yes', 'no']:
            log = 'decode_semantics not "no" or "yes"'; logger.error('ValueError: ' + log); raise ValueError(log)
        elif decode_owl == 'yes': self.decode_owl: Optional[str] = decode_owl; owl_kg = '_noOWL'
        else: self.decode_owl, owl_kg = None, '_OWL'

        # KG FILE NAME
        self.full_kg: str = '/PheKnowLator_' + self.kg_version + '_' + self.build + '_' + const + rel + owl_kg + '.owl'

    def reverse_relation_processor(self) -> None:
        """Creates and converts a Pandas DataFrame to a specific dictionary depending on whether it contains inverse
        relation data or relation data identifiers and labels. Examples of each dictionary are provided below:
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

    class EdgeConstructor(object):
        """Inner class object used to facilitate ray parallelization.

        Attributes:
            construction: A string indicating the construction approach (i.e. instance or subclass).
            edge_data: A nested dictionary keyed by edge type that contains all information needed to construct an edge.
            kg_owl: A string containing a filename.
            rel_dict: A dictionary keyed by URI containing all relations for constructing an edge set.
            inverse_dict: A dictionary keyed by URI containing all relations and their inverse relation.
            node_data: A string ("yes" or "no") indicating whether or not to add node data to the knowledge graph.
            metadata: An instance of the metadata class with bound method needed for created edge metadata.
            ont_cls: A set of RDFLib URIRef terms representing all classes in the core merged ontologies.
            obj_props: A set of RDFLib URIRef terms representing all object properties in the core merged ontologies.
            write_loc: A string passed specifying the primary directory to write to.
        """

        def __init__(self, params) -> None:

            self.clean_graph: Graph = Graph()
            self.construction: str = params.get('construction')
            self.edge_dict: dict = params.get('edge_dict')
            self.error_dict: Dict = dict()
            self.graph: Graph = Graph()
            self.kg_owl = params.get('kg_owl')
            self.inverse_relations_dict: Optional[Dict] = params.get('inverse_dict')
            self.node_data: Optional[str] = 'yes' if params.get('node_data') is not None else None
            self.node_metadata_func: Callable = params.get('metadata')
            self.obj_properties: Set = params.get('obj_props')
            self.ont_classes: Set = params.get('ont_cls')
            self.relations_dict: Optional[Dict] = params.get('rel_dict')
            self.res_dir: str = os.path.abspath('/'.join(params.get('write_loc').split('/')[:-1]))
            self.write_location: str = params.get('write_loc')

        def graph_getter(self) -> Tuple[Graph, Graph]:
            """Methods returns two inner class RDFLib Graph objects the first contains pkt-namespaces and the second
            contains the bnodes (anonymous nodes) with the pkt_namespace removed."""

            return self.graph, self.clean_graph

        def error_dict_getter(self) -> Dict:
            """Methods returns inner class subclass error dict object."""

            return self.error_dict

        def verifies_object_property(self, object_property: URIRef) -> None:
            """Adds an object property to a knowledge graph.

            Args:
                object_property: A string containing an obo ontology object property.

            Returns:
                None.

            Raises:
                TypeError: If the object_property is not type rdflib.term.URIRef
            """

            if not isinstance(object_property, URIRef):
                log = 'object not rdflib.term.URIRef'; logger.error('TypeError: ' + log); raise TypeError(log)
            else:
                if object_property not in self.obj_properties:
                    self.graph.add((object_property, RDF.type, OWL.ObjectProperty))
                    self.obj_properties = gets_object_properties(self.graph)

            return None

        def checks_classes(self, edge_info) -> bool:
            """Determines whether or not an edge is safe to add to the knowledge graph by making sure that any ontology
            class nodes are also present in the current list of classes from the merged ontologies graph.

            Args:
                edge_info: A dict of information needed to add edge to graph, for example:
                    {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                     'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                     'edges': ['CHEBI_81395', 'DOID_12858']}

            Returns:
                True - if the class node is already in the graph or nodes are both non-class entities.
                False - if the edge contains at least 1 ontology class that is not present in the graph.
            """

            if edge_info['n1'] != 'class' and edge_info['n2'] != 'class': return True
            elif edge_info['n1'] == 'class' and edge_info['n2'] == 'class':
                n1, n2 = URIRef(obo + edge_info['edges'][0]), URIRef(obo + edge_info['edges'][1])
                return n1 in self.ont_classes and n2 in self.ont_classes
            else: return URIRef(finds_node_type(edge_info)['cls1']) in self.ont_classes

        def checks_relations(self, relation: str, edge_list: Union[List, Set]) -> Optional[str]:
            """Determines whether or not an inverse relation should be created and added to the graph and verifies
            that a
            relation and its inverse (if it exists) are both an existing owl:ObjectProperty in the graph.

            Args:
                relation: A string that contains the relation assigned to edge in resource_info.txt (e.g. 'RO_0000056').
                edge_list: A list or set of knowledge graph edges. For example: {["8837", "4283"], ["8837", "839"]}

            Returns:
                A string containing an ontology identifier (e.g. "RO_0000056) or None. Value depends on:
                    - inverse relation, if the stored relation has an inverse relation
                    - current relation, if the stored relation is an interaction and the edge count is symmetric
                    - None, assuming the prior listed conditions are not met
            """

            edge_list = set(tuple(x) for x in edge_list) if isinstance(edge_list, List) else edge_list
            if self.inverse_relations_dict is not None and relation in self.inverse_relations_dict.keys():
                self.verifies_object_property(URIRef(obo + self.inverse_relations_dict[relation]))
                return self.inverse_relations_dict[relation]
            elif self.relations_dict is not None:
                if relation in self.relations_dict.keys() and 'interact' in self.relations_dict[relation]:
                    return None if len([x for x in edge_list if x[-1::-1] not in edge_list]) == 0 else relation
                else: return None
            else: return None

        @staticmethod
        def gets_edge_statistics(edge_type: str, results: Set, entity_info: List) -> str:
            """Calculates the number of nodes and edges involved in constructing an edge type.

            Args:
                edge_type: A string point to a specific edge type (e.g. 'chemical-disease).
                results: A set of tuples representing the complete set of triples from the construction process.
                entity_info: 3 items: 1-2 are sets of node tuples and 3 is the total count of non-OWL edges.

            Returns:
                formatted_str: A string containing edge statistics.
            """

            n1, n2 = edge_type.split('-')[0], edge_type.split('-')[1]
            owl_nodes = set(i for j in [x[0::2] for x in results] for i in j)
            stats = [len(results), entity_info[2], len(owl_nodes), len(entity_info[0]), n1, len(entity_info[1]), n2]
            stats_str = '{} OWL Edges, {} Original Edges; {} OWL Nodes, Original Nodes: {} {}(s), {} {}(s)'
            formatted_str = stats_str.format(stats[0], stats[1], stats[2], stats[3], stats[4], stats[5], stats[6])

            return formatted_str

        def creates_new_edges(self, edge_type: str) -> Graph:
            """Takes a dictionary of information needed to construct and edge creates the associated triples.

            Args:
                edge_type: A list of strings representing the types of edges to build.

            Returns:
                graph: An RDFLib Graph object.
            """

            kg_bld = KGConstructionApproach(self.res_dir)
            f_name = self.write_location + '_'.join(self.kg_owl.split('_')[0:-1]) + '_OWL'
            anot = f_name + '_AnnotationsOnly.nt'; logic = f_name + '_LogicOnly.nt'
            edge_list = self.edge_dict[edge_type]['edge_list']; s, o = self.edge_dict[edge_type]['data_type'].split('-')
            rel, uri = self.edge_dict[edge_type]['edge_relation'], self.edge_dict[edge_type]['uri']
            invrel = self.checks_relations(rel, edge_list) if self.inverse_relations_dict is not None else None
            n1, n2, rels = set(), set(), 0; res: Set = set()  # ; pbar = tqdm(total=len(edge_list))
            while len(edge_list) > 0:
                edge = edge_list.pop(0)  # ; pbar.update(1)
                edge_info = {'n1': s, 'n2': o, 'rel': rel, 'inv_rel': invrel, 'uri': uri, 'edges': edge}
                meta = self.node_metadata_func(ent=[''.join(x) for x in list(zip(uri, edge))], e_type=[s, o])
                meta_logic = [True if (self.node_data is None and meta is None) or [s, o] == ['class', 'class']
                              or (self.node_data is not None and meta is not None) else False][0]
                if self.checks_classes(edge_info) and meta_logic:
                    if self.construction == 'subclass': edges = set(kg_bld.subclass_constructor(edge_info, edge_type))
                    else: edges = set(kg_bld.instance_constructor(edge_info, edge_type))
                    res |= edges; n1 |= {edge[0]}; n2 |= {edge[1]}; rels = rels + 1 if invrel is None else rels + 2
                    self.graph = adds_edges_to_graph(self.graph, edges, False); appends_to_existing_file(edges, logic)
                    if meta is not None: appends_to_existing_file(meta, anot)
                    cleaned_graph = updates_pkt_namespace_identifiers(edges, self.construction, False)
                    self.clean_graph = adds_edges_to_graph(self.clean_graph, cleaned_graph, False)
            stat = self.gets_edge_statistics(edge_type, res, [n1, n2, rels]); del [n1, n2, rels], res  # ; pbar.close()
            p = 'Created {} ({}-{}) Edges: {}'.format(edge_type.upper(), s, o, stat); print('\n' + p); logger.info(p)
            if len(kg_bld.subclass_error.keys()) > 0: self.error_dict = kg_bld.subclass_error

            return None


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
            merges_ontologies(self.ontologies, self.merged_ont_kg.split('/')[-1], self.owl_tools)
            self.graph.parse(self.merged_ont_kg, format='xml')
        stats = 'Merged Ontologies {}'.format(derives_graph_statistics(self.graph)); print(stats); logger.info(stats)

        # STEP 3: PROCESS NODE METADATA
        log_str = '*** Loading Node Metadata Data ***'; print(log_str); logger.info(log_str)
        meta = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: meta.metadata_processor(); meta.extract_metadata(self.graph)

        # STEP 4: CREATE GRAPH SUBSETS
        log_str = '*** Splitting Graph ***'; print(log_str); logger.info(log_str)
        f = self.write_location; self.graph, annotation_triples = splits_knowledge_graph(self.graph)
        s = 'Merged Ontologies - Logic Subset {}'.format(derives_graph_statistics(self.graph)); print(s); logger.info(s)
        kg_owl = '_'.join(self.full_kg.split('_')[0:-1]) + '_OWL.owl'
        annot, logic, full = kg_owl[:-4] + '_AnnotationsOnly.nt', kg_owl[:-4] + '_LogicOnly.nt', kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, f + annot); appends_to_existing_file(self.graph, f + logic)
        del annotation_triples

        # STEP 5: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        log_str = '*** Building Knowledge Graph Edges ***'; print(log_str); logger.info(log_str)
        self.ont_classes = gets_ontology_classes(self.graph); self.obj_properties = gets_object_properties(self.graph)
        # instantiate inner class to construct edge sets
        try: ray.init()
        except RuntimeError: pass
        args = {'construction': self.construct_approach, 'edge_dict': self.edge_dict, 'write_loc': self.write_location,
                'rel_dict': self.relations_dict, 'inverse_dict': self.inverse_relations_dict, 'kg_owl': kg_owl,
                'node_data': self.node_data, 'ont_cls': self.ont_classes, 'metadata': meta.creates_node_metadata,
                'obj_props': self.obj_properties}
        edges = sublist_creator({k: len(v['edge_list']) for k, v in self.edge_dict.items()}, self.cpus)
        actors = [ray.remote(self.EdgeConstructor).remote(args) for _ in range(self.cpus)]  # type: ignore
        for i in range(0, len(edges)): [actors[i].creates_new_edges.remote(j) for j in edges[i]]  # type: ignore
        # extract results, aggregate actor dictionaries into single dictionary, and write data to json file
        _ = ray.wait([x.graph_getter.remote() for x in actors], num_returns=len(actors))
        graph_res = ray.get([x.graph_getter.remote() for x in actors])
        graphs = [self.graph] + [x[0] for x in graph_res]  # ; clean_graphs = [x[1] for x in graph_res]
        error_dicts = dict(ChainMap(*ray.get([x.error_dict_getter.remote() for x in actors]))); del actors
        if len(error_dicts.keys()) > 0:  # output error logs
            log_file = glob.glob(self.res_dir + '/construction*')[0] + '/subclass_map_log.json'
            logger.info('See log: {}'.format(log_file)); outputs_dictionary_data(error_dicts, log_file)
        results = set(x for y in [set(x) for x in graphs] for x in y)
        stats = 'Full Logic {}'.format(derives_graph_statistics(results)); print(stats); logger.info(stats)

        # deduplicate logic and annotation files, merge them, and print final stats
        deduplicates_file(f + annot); deduplicates_file(f + logic); merges_files(f + annot, f + logic, f + full)
        graph = Graph().parse(f + full, format='nt')
        s = 'Full (Logic + Annotation) {}'.format(derives_graph_statistics(graph)); print('\n' + s); logger.info(s)

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
        closed_kg = glob.glob(self.write_location + '/*.owl')
        if len(closed_kg) == 0: logs = 'KG file does not exist!'; logger.error('OSError: ' + logs); raise OSError(logs)
        elif os.stat(closed_kg[0]).st_size == 0:
            logs = '{} is empty'.format(closed_kg); logger.error('TypeError: ' + logs); raise TypeError(logs)
        else:
            log_str = '*** Loading Closed Knowledge Graph ***'; print(log_str); logger.info(log_str)
            os.rename(closed_kg[0], self.write_location + self.full_kg)  # rename closed kg file
            self.graph = Graph().parse(self.write_location + self.full_kg, format='xml')
        stats = 'Input {}'.format(derives_graph_statistics(self.graph)); print(stats); logger.info(stats)

        # STEP 3: PROCESS NODE METADATA
        log_str = '*** Loading Node Metadata Data ***'; print(log_str); logger.info(log_str)
        meta = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: meta.metadata_processor(); meta.extract_metadata(self.graph)

        # STEP 4: CREATE GRAPH SUBSETS
        log_str = '*** Splitting Graph ***'; print(log_str); logger.info(log_str)
        _ = self.write_location; self.graph, annotation_triples = splits_knowledge_graph(self.graph)
        stats = 'Merged Logic Subset {}'.format(derives_graph_statistics(self.graph)); print(stats); logger.info(stats)
        kg_owl = '_'.join(self.full_kg.split('_')[0:-1]) + '_OWL.owl'; kg_owl_main = kg_owl[:-8] + '.owl'
        annot, logic, full = kg_owl[:-4] + '_AnnotationsOnly.nt', kg_owl[:-4] + '_LogicOnly.nt', kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, _ + annot); appends_to_existing_file(self.graph, _ + logic)
        del annotation_triples

        # STEP 5: DECODE OWL SEMANTICS
        results = [set(self.graph), None, None]
        stats = 'Full Logic {}'.format(derives_graph_statistics(results[0])); print(stats); logger.info(stats)
        logger.info('*** Converting Knowledge Graph to Networkx MultiDiGraph ***')
        s = convert_to_networkx(self.write_location, kg_owl[:-4], results[0], True)
        if s is not None: log_stats = 'Full Logic Subset (OWL) {}'.format(s); logger.info(log_stats); print(log_stats)
        if self.decode_owl:
            self.graph = updates_pkt_namespace_identifiers(self.graph, self.construct_approach)
            owlnets = OwlNets(self.graph, self.write_location, kg_owl_main, self.construct_approach, self.owl_tools)
            results = [results[0]] + list(owlnets.runs_owlnets(self.cpus))

        # STEP 7: WRITE OUT KNOWLEDGE GRAPH METADATA AND CREATE EDGE LISTS
        log_str = '*** Writing Knowledge Graph Edge Lists ***'; print('\n' + log_str); logger.info(log_str)
        f_prefix = ['_OWL', '_OWLNETS', '_OWLNETS_' + self.construct_approach.upper() + '_purified']
        for x in range(0, len(results)):
            graph = results[x]; p_str = 'OWL' if x == 0 else 'OWL-NETS' if x == 1 else 'Purified OWL-NETS'
            if graph is not None:
                log_str = '*** Processing {} Graph ***'.format(p_str); print(log_str); logger.info(log_str)
                triple_list_file = kg_owl[:-8] + f_prefix[x] + '_Triples_Integers.txt'
                triple_map = triple_list_file[:-5] + '_Identifier_Map.json'
                node_int_map = maps_ids_to_integers(graph, self.write_location, triple_list_file, triple_map)

                # STEP 8: EXTRACT AND WRITE NODE METADATA
                meta.full_kg = kg_owl[:-8] + f_prefix[x] + '.owl'
                if self.node_data: meta.output_metadata(node_int_map, graph)

        # deduplicate logic and annotation files and then merge them
        deduplicates_file(_ + annot); deduplicates_file(_ + logic); merges_files(_ + annot, _ + logic, _ + full)

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
            self.graph.parse(self.merged_ont_kg, format='xml')
        stats = 'Merged Ontologies {}'.format(derives_graph_statistics(self.graph)); print(stats); logger.info(stats)

        # STEP 3: PROCESS NODE METADATA
        log_str = '*** Loading Node Metadata Data ***'; print(log_str); logger.info(log_str)
        meta = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: meta.metadata_processor(); meta.extract_metadata(self.graph)

        # STEP 4: CREATE GRAPH SUBSETS
        log_str = '*** Splitting Graph ***'; print(log_str); logger.info(log_str)
        f = self.write_location; self.graph, annotation_triples = splits_knowledge_graph(self.graph)
        s = 'Merged Ontologies - Logic Subset {}'.format(derives_graph_statistics(self.graph)); print(s); logger.info(s)
        kg_owl = '_'.join(self.full_kg.split('_')[0:-1]) + '_OWL.owl'; kg_owl_main = kg_owl[:-8] + '.owl'
        annot, logic, full = kg_owl[:-4] + '_AnnotationsOnly.nt', kg_owl[:-4] + '_LogicOnly.nt', kg_owl[:-4] + '.nt'
        appends_to_existing_file(annotation_triples, f + annot); appends_to_existing_file(self.graph, f + logic)
        del annotation_triples

        # STEP 5: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        log_str = '*** Building Knowledge Graph Edges ***'; print('\n' + log_str); logger.info(log_str)
        self.ont_classes = gets_ontology_classes(self.graph); self.obj_properties = gets_object_properties(self.graph)
        try: ray.init()
        except RuntimeError: pass
        args = {'construction': self.construct_approach, 'edge_dict': self.edge_dict, 'node_data': self.node_data,
                'rel_dict': self.relations_dict, 'inverse_dict': self.inverse_relations_dict, 'kg_owl': kg_owl,
                'ont_cls': self.ont_classes, 'obj_props': self.obj_properties, 'metadata': meta.creates_node_metadata,
                'write_loc': self.write_location}
        edges = sublist_creator({k: len(v['edge_list']) for k, v in self.edge_dict.items()}, self.cpus)
        actors = [ray.remote(self.EdgeConstructor).remote(args) for _ in range(self.cpus)]  # type: ignore
        for i in range(0, len(edges)): [actors[i].creates_new_edges.remote(j) for j in edges[i]]  # type: ignore
        _ = ray.wait([x.graph_getter.remote() for x in actors], num_returns=len(actors))
        res = ray.get([x.graph_getter.remote() for x in actors]); g1 = [x[0] for x in res]; g2 = [x[1] for x in res]
        error_dicts = dict(ChainMap(*ray.get([x.error_dict_getter.remote() for x in actors]))); del actors
        if len(error_dicts.keys()) > 0:  # output error logs
            log_file = glob.glob(self.res_dir + '/construction*')[0] + '/subclass_map_log.json'
            logger.info('See log: {}'.format(log_file)); outputs_dictionary_data(error_dicts, log_file)

        # STEP 6: DECODE OWL SEMANTICS
        results = [set(x for y in [set(x) for x in [self.graph] + g1] for x in y), None, None]
        stats = 'Full Logic {}'.format(derives_graph_statistics(results[0])); print(stats); logger.info(stats)
        s1 = convert_to_networkx(self.write_location, kg_owl[:-4], results[0], True)
        if s1 is not None: log_stats = 'Full Logic Subset (OWL) {}'.format(s1); logger.info(log_stats); print(log_stats)
        # aggregates processed owl-nets output derived when constructing non-ontology edges
        if self.decode_owl is not None:
            graphs = [updates_pkt_namespace_identifiers(self.graph, self.construct_approach)] + g2
            owlnets = OwlNets(graphs, self.write_location, kg_owl_main, self.construct_approach, self.owl_tools)
            results = [results[0]] + list(owlnets.runs_owlnets(self.cpus))

        # STEP 7: WRITE OUT KNOWLEDGE GRAPH METADATA AND CREATE EDGE LISTS
        log_str = '*** Writing Knowledge Graph Edge Lists ***'; print('\n' + log_str); logger.info(log_str)
        f_prefix = ['_OWL', '_OWLNETS', '_OWLNETS_' + self.construct_approach.upper() + '_purified']
        for x in range(0, len(results)):
            graph = results[x]; p_str = 'OWL' if x == 0 else 'OWL-NETS' if x == 1 else 'Purified OWL-NETS'
            if graph is not None:
                log_str = '*** Processing {} Graph ***'.format(p_str); print('\n' + log_str); logger.info(log_str)
                triple_list_file = kg_owl[:-8] + f_prefix[x] + '_Triples_Integers.txt'
                triple_map = triple_list_file[:-5] + '_Identifier_Map.json'
                node_int_map = maps_ids_to_integers(graph, self.write_location, triple_list_file, triple_map)

                # STEP 8: EXTRACT AND WRITE NODE METADATA
                meta.full_kg = kg_owl[:-8] + f_prefix[x] + '.owl'
                if self.node_data: meta.output_metadata(node_int_map, graph)

        # deduplicate logic and annotation files, merge them, and print final stats
        deduplicates_file(f + annot); deduplicates_file(f + logic); merges_files(f + annot, f + logic, f + full)
        str1 = '\nLoading Full (Logic + Annotation) Graph'; print('\n' + str1); logger.info(str1)
        graph = Graph().parse(f + full, format='nt'); str2 = 'Deriving Stats'; print('\n' + str2); logger.info(str2)
        s = 'Full (Logic + Annotation) {}'.format(derives_graph_statistics(graph)); print('\n' + s); logger.info(s)

        return None
