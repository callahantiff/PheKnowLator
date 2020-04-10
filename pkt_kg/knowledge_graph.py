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
import hashlib

from abc import ABCMeta, abstractmethod
from rdflib import Graph, Namespace, URIRef, BNode  # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Any, Callable, Dict, IO, List, Optional, Set, Tuple

from pkt_kg.utils import *
from pkt_kg.metadata import Metadata
from pkt_kg.owlnets import OwlNets

# set global attributes
obo = Namespace('http://purl.obolibrary.org/obo/')


# TODO: extend functionality to improve KR for:
#  (1) instance-based builds that includes connections between 2 instance nodes
#  (2) the ability to combine instance and subclass-based methods


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
        owltools: A string pointing to the location of the owl tools library.
        relations_dict: A dict storing the relation identifiers and labels. For example,
            {'RO_0002616': 'related via evidence or inference to', 'RO_0002442': 'mutualistically interacts with}
        subclass_dict: A node data ontology class dict. Used for the subclass construction approach. Keys are
            non-ontology ids and values are lists of ontology classes mapped to each non-ontology id. For example,
            {'R-HSA-168277' :['http://purl.obolibrary.org/obo/PW_0001054','http://purl.obolibrary.org/obo/GO_0046730']}
        subclass_error: A dict that stores edge subclass nodes that were unable to be mapped to the
            subclass_dict. Keys are edge_type and values are lists of identifiers. For example:
                {'chemical-gene': [100490177, 34858593, 234]}
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
        Exception: An exception is raised if self.kg_metadata == "yes" and node_data == "no".
    """

    __metaclass__ = ABCMeta

    def __init__(self, kg_version: str, write_location: str, construction: str, edge_data: str,
                 node_data: Optional[str] = None, inverse_relations: Optional[str] = None, decode_owl: Optional[str]
                 = None, kg_metadata_flag: str = 'no') -> None:

        self.build: str = self.gets_build_type().lower().split()[0]
        self.decode_owl: Optional[str] = None
        self.edge_dict: Dict = dict()
        self.graph: Graph = Graph()
        self.inverse_relations: Optional[List] = None
        self.inverse_relations_dict: Optional[Dict] = None
        self.node_data: Optional[List] = None
        self.node_dict: Optional[Dict] = None
        self.kg_metadata: str = kg_metadata_flag.lower() if node_data else 'no'
        self.ont_classes: Set = set()
        self.obj_properties: Set = set()
        self.owltools = './pkt_kg/libs/owltools'
        self.relations_dict: Dict = dict()
        self.subclass_dict: Dict = dict()
        self.subclass_error: Dict = dict()

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
            self.merged_ont_kg = '/PheKnowLator_MergedOntologies.owl'

        # FINDING ONTOLOGIES DATA DIRECTORY
        if not os.path.exists(self.res_dir + '/ontologies'):
            raise OSError("Can't find 'ontologies/' directory, this directory is a required input")
        elif len(glob.glob(self.res_dir + '/ontologies/*.owl')) == 0:
            raise TypeError('The ontologies directory is empty')
        else:
            self.ontologies: List[str] = glob.glob(self.res_dir + '/ontologies/*.owl')

        # LOADING SUBCLASS DICTIONARY
        if not os.path.exists(glob.glob(self.res_dir + '/construction_*/*.pkl')[0]):
            raise OSError('The {} file does not exist!'.format(glob.glob(self.res_dir + '/construction_*/*.pkl')[0]))
        elif os.stat(glob.glob(self.res_dir + '/construction_*/*.pkl')[0]).st_size == 0:
            raise TypeError('The input file: {} is empty'.format(glob.glob(self.res_dir + '/construction_*/*.pkl')[0]))
        else:
            with open(glob.glob(self.res_dir + '/construction_*/*.pkl')[0], 'rb') as filepath:  # type: IO[Any]
                self.subclass_dict = pickle.load(filepath, encoding='bytes')

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
                    kg_rel = '/inverse_relations' + '/PheKnowLator_' + self.build + '_InverseRelations_'
            else:
                self.inverse_relations, self.inverse_relations_dict = None, None
                kg_rel = '/relations_only' + '/PheKnowLator_' + self.build + '_'

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
        """Takes a string that contains an object property, representing a relation between two nodes,
        and adds it to the knowledge graph.

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

    @staticmethod
    def finds_node_type(edge_info: Dict) -> Dict:
        """Takes a dictionary of edge information and parses the data type for each node in the edge. The function
        returns either None or a string containing a particular node from the edge.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'subclass', 'n2': 'class','relation': 'RO_0003302',
                'url': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                'edges': ['2', 'DOID_0110035']}

        Returns:
            A dictionary with 4 keys representing node type (i.e. "cls1", "cls2", "ent1", and "ent2") and values are
            strings containing a concatenation of the uri and the node. An example of a class-class edge is shown below:
                {'cls1': 'http://purl.obolibrary.org/obo/CHEBI_81395',
                'cls2': 'http://purl.obolibrary.org/obo/DOID_12858',
                'ent1': None,
                'ent2': None}
        """

        # initialize node types node type (cls=ontology class, ent1/ent2=instance or subclass node)
        nodes = {'cls1': None, 'cls2': None, 'ent1': None, 'ent2': None}

        if edge_info['n1'] == 'class' and edge_info['n2'] == 'class':
            nodes['cls1'] = edge_info['uri'][0] + edge_info['edges'][0]
            nodes['cls2'] = edge_info['uri'][1] + edge_info['edges'][1]
        elif edge_info['n1'] == 'class' and edge_info['n2'] != 'class':
            nodes['cls1'] = edge_info['uri'][0] + edge_info['edges'][0]
            nodes['ent1'] = edge_info['uri'][1] + edge_info['edges'][1]
        elif edge_info['n1'] != 'class' and edge_info['n2'] == 'class':
            nodes['ent1'] = edge_info['uri'][0] + edge_info['edges'][0]
            nodes['cls1'] = edge_info['uri'][1] + edge_info['edges'][1]
        else:
            nodes['ent1'] = edge_info['uri'][0] + edge_info['edges'][0]
            nodes['ent2'] = edge_info['uri'][1] + edge_info['edges'][1]

        return nodes

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
            class_found = URIRef(self.finds_node_type(edge_info)['cls1']) in self.ont_classes

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

    def adds_edges_to_graph(self, edge_list: Tuple, new_edges: List) -> List:
        """Takes a tuple of tuples representing new triples and adds them to a knowledge graph. At the same time,
        a list, meant to track all new edges added to the knowledge graph is updated.

        Args:
            edge_list: A tuple of tuples, where each tuple contains a triple.
            new_edges: A list of new tuples added to the knowledge graph, where each tuple contains a triple.

        Returns:
            new_edges: A list of new tuples added to the knowledge graph, where each tuple contains a triple.
        """

        for edge in edge_list:
            self.graph.add(edge)  # add edge to knowledge graph
            new_edges += [edge]   # update edge list of newly added edges

        return new_edges

    def maps_node_to_class(self, edge_type: str, entity: str, edge: List) -> Optional[List]:
        """Takes an entity and checks whether or not it exists in a dictionary of subclass content

        Args:
            edge_type: A string containing the edge_type (e.g. "gene-disease").
            entity: A string containing a node identifier (e.g. "DOID_162").
            edge: An edge list containing two nodes (e.g. ["DOID_162", "GO_1234567"]).

        Returns:
            None if the entity is not in the subclass_dict, otherwise a list of mappings between the instance or
            subclass entity node is returned.
        """

        if entity not in self.subclass_dict.keys():
            if self.subclass_error and edge_type in self.subclass_error.keys():
                self.subclass_error[edge_type] += [entity]
            else:
                self.subclass_error[edge_type] = [entity]

            # remove the edge from the edge_dict
            self.edge_dict[edge_type]['edge_list'].pop(self.edge_dict[edge_type]['edge_list'].index(edge))
            subclass_map = None
        else:
            subclass_map = self.subclass_dict[entity]

        return subclass_map

    def class_edge_constructor(self, node1: str, node2: str, relation: str, edge_type, inv_rel: str) -> List[Tuple]:
        """Constructs a single edge between to ontology classes as well as verifies if the user wants an inverse edge
        created and if so, then this edge is also added to the knowledge graph.

        Args:
            node1: A URIRef object containing a subject node of type owl:Class.
            node2: A URIRef object containing a object node of type owl:Class.
            relation: A  URIRef object containing an owl:ObjectProperty.
            edge_type: A string containing the edge_type (e.g. "gene-disease").
            inv_rel: A variable that either contains a string containing the identifier for an inverse relation
                (i.e. "RO_0002200") or None (i.e. indicator of no inverse relation).

        Returns:
            edge_counter: A list of new edges added to the knowledge graph.
        """

        edge_counter: List = []
        new_edge_inverse_relations: Tuple = tuple()

        # instance class-instance/instance-class and instance-instance edge types
        if self.construct_approach == 'instance' and self.edge_dict[edge_type]['data_type'] != 'class-class':
            node1_type = URIRef(node1) if 'http' in node1 else BNode(node1)
            node2_type = URIRef(node2) if 'http' in node2 else BNode(node2)
            new_edge_relations_only: Tuple = ((node1_type, URIRef(obo + relation), node2_type),)
            if inv_rel: new_edge_inverse_relations = ((node2_type, URIRef(obo + inv_rel), node1_type),)
        else:
            # all subclass (class-class/subclass-class/subclass-subclass) and instance class-class edge types
            rel_only_class_uuid = BNode(hashlib.sha1(node1.encode()).hexdigest())
            new_edge_relations_only = ((URIRef(node1), RDFS.subClassOf, rel_only_class_uuid),
                                       (rel_only_class_uuid, RDF.type, OWL.Restriction),
                                       (rel_only_class_uuid, OWL.someValuesFrom, URIRef(node2)),
                                       (rel_only_class_uuid, OWL.onProperty, URIRef(obo + relation)))
            if inv_rel:
                inv_rel_class_uuid = BNode(hashlib.sha1(node2.encode()).hexdigest())
                new_edge_inverse_relations = ((URIRef(node2), RDFS.subClassOf, inv_rel_class_uuid),
                                              (inv_rel_class_uuid, RDF.type, OWL.Restriction),
                                              (inv_rel_class_uuid, OWL.someValuesFrom, URIRef(node1)),
                                              (inv_rel_class_uuid, OWL.onProperty, URIRef(obo + inv_rel)))

        return self.adds_edges_to_graph(new_edge_relations_only + new_edge_inverse_relations, edge_counter)

    def subclass_constructor(self, edge_info: Dict, edge_type: str) -> List[Tuple]:
        """Adds edges for the subclass construction approach.

        Assumption: All ontology class nodes use the obo namespace.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                 'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                 'edges': ['CHEBI_81395', 'DOID_12858']}
            edge_type: A string containing the name of the edge_type (e.g. "gene-disease", "chemical-gene").

        Returns:
            edges: A list of new edges added to the knowledge graph.
        """

        results, rel, inv_rel = self.finds_node_type(edge_info), edge_info['rel'], edge_info['inv_rel']
        edges: List = []
        new_edges = []

        if results['cls1'] and results['cls2']:  # class-class edges
            edges += self.class_edge_constructor(results['cls1'], results['cls2'], rel, edge_type, inv_rel)
        elif results['cls1'] and results['ent1']:  # subclass-class/class-subclass edges
            mapped_node = self.maps_node_to_class(edge_type, results['ent1'].split('/')[-1], edge_info['edges'])

            if mapped_node:
                new_edges = [(URIRef(results['ent1']), RDF.type, OWL.Class)]
                new_edges += [(URIRef(results['ent1']), RDFS.subClassOf, URIRef(obo + x)) for x in mapped_node]

                if edge_info['n1'] == 'class':  # determine node order
                    edges += self.class_edge_constructor(results['cls1'], results['ent1'], rel, edge_type, inv_rel)
                else:
                    edges += self.class_edge_constructor(results['ent1'], results['cls1'], rel, edge_type, inv_rel)
        else:  # subclass-subclass edges
            mapped_node1 = self.maps_node_to_class(edge_type, results['ent1'].split('/')[-1], edge_info['edges'])
            mapped_node2 = self.maps_node_to_class(edge_type, results['ent2'].split('/')[-1], edge_info['edges'])

            if mapped_node1 and mapped_node2:
                new_edges += [(URIRef(x), RDF.type, OWL.Class) for x in [results['ent1'], results['ent2']]]
                new_edges += [(URIRef(results['ent1']), RDFS.subClassOf, URIRef(obo + x)) for x in mapped_node1]
                new_edges += [(URIRef(results['ent2']), RDFS.subClassOf, URIRef(obo + x)) for x in mapped_node2]
                edges += self.class_edge_constructor(results['ent1'], results['ent2'], rel, edge_type, inv_rel)

        return self.adds_edges_to_graph(tuple(new_edges), edges)

    def instance_constructor(self, edge_info: Dict, edge_type: str) -> List[Tuple]:
        """Adds edges for the instance construction approach.

        Assumption: All ontology class nodes use the obo namespace.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                 'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                 'edges': ['CHEBI_81395', 'DOID_12858']}
            edge_type: A string containing the name of the edge_type (e.g. "gene-disease", "chemical-gene").

        Returns:
            edges: A list of new edges added to the knowledge graph.
        """

        results = self.finds_node_type(edge_info)
        rel, inv_rel = edge_info['rel'], edge_info['inv_rel']
        edges: List = []
        new_edges = []

        if results['cls1'] and results['cls2']:  # class-class edges
            edges += self.class_edge_constructor(results['cls1'], results['cls2'], rel, edge_type, inv_rel)

        elif results['cls1'] and results['ent1']:  # class-instance/instance-class edges
            sha_uid = hashlib.sha1(results['cls1'].encode()).hexdigest()
            mapped_node = self.maps_node_to_class(edge_type, results['ent1'].split('/')[-1], edge_info['edges'])

            if mapped_node:
                new_edges = [(BNode(sha_uid), RDF.type, URIRef(results['cls1']))]
                new_edges += [(URIRef(results['ent1']), RDF.type, URIRef(obo + x)) for x in mapped_node]

                if edge_info['n1'] == 'class':  # ran to help determine order to pass nodes
                    edges += self.class_edge_constructor(sha_uid, results['ent1'], rel, edge_type, inv_rel)
                else:
                    edges += self.class_edge_constructor(results['ent1'], sha_uid, rel, edge_type, inv_rel)

        else:  # instance-instance edges
            mapped_node1 = self.maps_node_to_class(edge_type, results['ent1'].split('/')[-1], edge_info['edges'])
            mapped_node2 = self.maps_node_to_class(edge_type, results['ent2'].split('/')[-1], edge_info['edges'])

            if mapped_node1 and mapped_node2:
                new_edges += [(URIRef(results['ent1']), RDF.type, URIRef(obo + x)) for x in mapped_node1]
                new_edges += [(URIRef(results['ent2']), RDF.type, URIRef(obo + x)) for x in mapped_node2]
                edges += self.class_edge_constructor(results['ent1'], results['ent2'], rel, edge_type, inv_rel)

        return self.adds_edges_to_graph(tuple(new_edges), edges)

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

        for edge_type in self.edge_dict.keys():
            n1_type, n2_type = self.edge_dict[edge_type]['data_type'].split('-')
            rel, uri = self.edge_dict[edge_type]['edge_relation'], self.edge_dict[edge_type]['uri']
            edge_list = copy.deepcopy(self.edge_dict[edge_type]['edge_list'])
            self.verifies_object_property(URIRef(obo + rel))  # verify object property in knowledge graph
            invrel = self.checks_for_inverse_relations(rel, edge_list) if self.inverse_relations else None
            edge_results: List = []

            print('\nCreating {} ({}-{}) Edges ***'.format(edge_type.upper(), n1_type, n2_type))

            for edge in tqdm(edge_list):
                edge_info = {'n1': n1_type, 'n2': n2_type, 'rel': rel, 'inv_rel': invrel, 'uri': uri, 'edges': edge}

                if self.check_ontology_class_nodes(edge_info):  # verify edges - make sure ont class nodes are in KG
                    if self.construct_approach == 'subclass':
                        edge_results += self.subclass_constructor(edge_info, edge_type)
                    else:
                        edge_results += self.instance_constructor(edge_info, edge_type)
                else:
                    self.edge_dict[edge_type]['edge_list'].pop(self.edge_dict[edge_type]['edge_list'].index(edge))

            n1, n2 = edge_type.split('-')  # print edge-type statistics
            print('\nUnique Edges: {}'.format(len([list(x) for x in set([tuple(y) for y in edge_results])])))
            print('Unique {}: {}'.format(n1, len(set([x[0] for x in self.edge_dict[edge_type]['edge_list']]))))
            print('Unique {}: {}'.format(n2, len(set([x[1] for x in self.edge_dict[edge_type]['edge_list']]))))

            # kg.graph.serialize(destination='./resources/knowledge_graphs/TEST_ADD.owl', format='xml')
            # gets_ontology_statistics('./resources/knowledge_graphs/TEST_ADD.owl', kg.owltools)

        # output instance and subclass files
        const_app_dir = glob.glob(self.res_dir + '/construction*')[0]
        if len(self.subclass_error.keys()) > 0:
            log_file = const_app_dir + '/subclass_map_missing_node_log.json'
            print('Some edge lists nodes were missing from subclass_dict, see log: {}'.format(log_file))
            outputs_dictionary_data(self.subclass_error, log_file)

        # add ontology metadata and annotations, serialize graph, and apply OWL API formatting to output
        if self.kg_metadata == 'yes': node_metadata_func(self.graph, self.edge_dict)
        self.graph = ontology_annotator_func(self.full_kg.split('/')[-1], self.graph)
        self.graph.serialize(destination=self.write_location + self.full_kg, format='xml')
        ontology_file_formatter(self.write_location, self.full_kg, self.owltools)

        return None

    def construct_knowledge_graph(self) -> None:
        """Builds a knowledge graph. The knowledge graph build is completed differently depending on the build type
        that the user requested. The build types include: "full", "partial", or "post-closure". The knowledge graph
        is built through the following steps: (1) Set up environment; (2) Process relation/inverse relations; (3)
        Process node metadata; (4) Merge ontologies; (5) Add master edge list to merged ontologies; (6) Remove
        annotation assertions (if partial build); (7) Add annotation assertions (if post-closure build); (8)
        Extract and write node metadata; (9) Decode OWL-encoded classes; and (10) Output knowledge graph files and
        create edge lists.

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
        ontologies; (5) Add master edge list to merged ontologies; and (6) Remove annotation assertions.

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
            self.graph.parse(self.merged_ont_kg)
            gets_ontology_statistics(self.merged_ont_kg, self.owltools)
        else:
            if len(self.ontologies) == 0:
                raise TypeError('ERROR: the ontologies directory: {} is empty'.format(
                    self.write_location + '/' + glob.glob('*/ontologies')[0]))
            else:
                print('*** Merging Ontology Data ***')
                merges_ontologies(self.ontologies,
                                  self.write_location, '/' + self.merged_ont_kg.split('/')[-1],
                                  self.owltools)
                # load the merged ontology
                self.graph.parse(self.merged_ont_kg)
                gets_ontology_statistics(self.merged_ont_kg, self.owltools)

        self.ont_classes = gets_ontology_classes(self.graph)
        self.obj_properties = gets_object_properties(self.graph)

        # STEP 5: ADD MASTER EDGE DATA TO KNOWLEDGE GRAPH
        # create temporary directory to store partial builds and update path to write data to
        temp_dir = self.write_location + '/' + self.full_kg.split('/')[1] + '/partial_build'
        if temp_dir not in glob.glob(self.write_location + '/**/**'): os.mkdir(temp_dir)
        self.full_kg = '/'.join(self.full_kg.split('/')[:2] + ['partial_build'] + self.full_kg.split('/')[2:])
        metadata.full_kg = self.full_kg

        # build knowledge graph
        print('*** Building Knowledge Graph Edges ***')
        self.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)
        del self.graph, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict
        gets_ontology_statistics(self.write_location + self.full_kg, self.owltools)

        # STEP 6: REMOVE ANNOTATION ASSERTIONS
        print('*** Removing Annotation Assertions ***')
        metadata.removes_annotation_assertions(self.owltools)
        del metadata

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
        relation data; (3) Process node metadata; (4) Load closed knowledge graph and annotation assertions; (5) Add
        annotation assertions; (6) Extract and write node metadata; (7) Decode OWL-encoded classes; and (8) Output
        knowledge graph files and create edge lists.

        Returns:
            None.

        Raises:
            TypeError: If the knowledge graph and annotation assertion file types are not owl.
            OSError: If the closed_kg_location and annotation_assertions files do not exist.
            TypeError: If closed_kg_location and annotation_assertions files are empty.
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

        closed_kg_location = input('Filepath to the closed knowledge graph: ')

        if '.owl' not in closed_kg_location:
            raise TypeError('The provided file is not type .owl')
        elif not os.path.exists(closed_kg_location):
            raise OSError('The {} file does not exist!'.format(closed_kg_location))
        elif os.stat(closed_kg_location).st_size == 0:
            raise TypeError('input file: {} is empty'.format(closed_kg_location))
        else:
            print('*** Loading Closed Knowledge Graph ***')
            self.graph = Graph()
            self.graph.parse(closed_kg_location)
            gets_ontology_statistics(closed_kg_location, self.owltools)

        # STEP 5: ADD ANNOTATION ASSERTIONS
        annotation_assertions = input('Filepath to the knowledge graph with annotation assertions: ')

        if '.owl' not in annotation_assertions:
            raise TypeError('The provided file is not type .owl')
        elif not os.path.exists(annotation_assertions):
            raise OSError('The {} file does not exist!'.format(annotation_assertions))
        elif os.stat(annotation_assertions).st_size == 0:
            raise TypeError('input file: {} is empty'.format(annotation_assertions))
        else:
            print('*** Loading Annotation Assertions Edge List ***')
            self.graph = metadata.adds_annotation_assertions(self.graph, annotation_assertions)

            # add ontology metadata and annotations, serialize graph, and apply OWL API formatting to output
            if self.kg_metadata == 'yes': metadata.adds_node_metadata(self.graph, self.edge_dict)
            self.graph = metadata.adds_ontology_annotations(self.full_kg.split('/')[-1], self.graph)
            self.graph.serialize(destination=self.write_location + self.full_kg, format='xml')
            ontology_file_formatter(self.write_location, self.full_kg, self.owltools)
            gets_ontology_statistics(self.write_location + self.full_kg, self.owltools)

        # STEP 6: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        if self.node_data is not None: metadata.output_knowledge_graph_metadata(self.graph)
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 7: DECODE OWL SEMANTICS
        # if self.decode_owl:
        #     print('*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
        #    wl_nets = OwlNets(self.graph, self.write_location, self.full_kg, self.construct_approach)
        #     self.graph = owl_nets.run_owl_nets()

        # STEP 8: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
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
            self.graph.parse(self.merged_ont_kg)
            gets_ontology_statistics(self.merged_ont_kg, self.owltools)
        else:
            if len(self.ontologies) == 0:
                raise TypeError('The ontologies directory is empty')
            else:
                print('*** Merging Ontology Data ***')
                merges_ontologies(self.ontologies,
                                  self.write_location, '/' + self.merged_ont_kg.split('/')[-1],
                                  self.owltools)
                # load the merged ontology
                self.graph.parse(self.merged_ont_kg)
                gets_ontology_statistics(self.merged_ont_kg, self.owltools)

        self.ont_classes = gets_ontology_classes(self.graph)
        self.obj_properties = gets_object_properties(self.graph)

        # STEP 5: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        print('\n*** Building Knowledge Graph Edges ***')
        self.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)
        gets_ontology_statistics(self.write_location + self.full_kg, self.owltools)

        # STEP 6: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        if self.node_data is not None: metadata.output_knowledge_graph_metadata(self.graph)
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 7: DECODE OWL SEMANTICS
        # if self.decode_owl:
        #     print('\n*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
        #   owl_nets = OwlNets(self.graph, self.write_location, self.full_kg, self.construct_approach)
        #     self.graph = owl_nets.run_owl_nets()

        # STEP 8: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        print('\n*** Writing Knowledge Graph Edge Lists ***')
        maps_node_ids_to_integers(self.graph, self.write_location,
                                  self.full_kg[:-6] + 'Triples_Integers.txt',
                                  self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

        converts_rdflib_to_networkx(self.write_location, self.full_kg, self.graph)

        return None
