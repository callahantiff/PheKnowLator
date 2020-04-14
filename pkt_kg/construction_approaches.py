#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import hashlib
import networkx   # type: ignore
import os
import os.path
import pickle

from collections import Counter
from rdflib import Graph, Namespace, BNode, Literal, URIRef   # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, IO, List, Optional, Set, Tuple, Union

from pkt_kg.utils import adds_edges_to_graph, finds_node_type

# set global attributes
obo = Namespace('http://purl.obolibrary.org/obo/')


class KGConstructionApproach(object):
    """Class removes OWL semantics from an ontology or knowledge graph using the OWL-NETS method.

    OWL Semantics are nodes and edges in the graph that are needed in order to create a rich semantic representation
    and to do things like run reasoners. Many of these nodes and edges are not clinically or biologically meaningful.
    This class is designed to decode all owl-encoded classes and return a knowledge graph that is semantically rich and
    clinically and biologically meaningful.

    Attributes:
        edge_dict: A nested master edge list dict. The outer key is an edge-type (e.g.go-gene) and inner key is a dict
            storing data from the resource_info.txt input document. For example:
            {'chemical-complex': {'source_labels': ';;', 'data_type': 'class-instance', 'edge_relation': 'RO_0002436',
             'uri': ['http://purl.obolibrary.org/obo/', 'https://reactome.org/content/detail/'], 'delimiter': 't',
             'column_idx': '0;1', 'identifier_maps': 'None', 'evidence_criteria': 'None', 'filter_criteria': 'None',
             'edge_list': [['CHEBI_24505', 'R-HSA-1006173'], ...] }, }
        subclass_dict: A node data ontology class dict. Used for the subclass construction approach. Keys are
            non-ontology ids and values are lists of ontology classes mapped to each non-ontology id. For example,
            {'R-HSA-168277' :['http://purl.obolibrary.org/obo/PW_0001054','http://purl.obolibrary.org/obo/GO_0046730']}
        subclass_error: A dict that stores edge subclass nodes that were unable to be mapped to the
            subclass_dict. Keys are edge_type and values are lists of identifiers. For example:
                {'chemical-gene': [100490177, 34858593, 234]}
        write_location: A string pointing to the 'resources' directory.

    Raises:
        TypeError: If graph is not an rdflib.graph object.
        TypeError: If edge_info and edge_dict are not dictionary objects.
        ValueError: If graph, edge_info, edge_dict, or subclass_dict files are empty.
        OSError: If there is no subclass_dict file in the resources/construction_approach directory.
    """

    def __init__(self, edge_dict: Dict, write_location: str) -> None:

        self.subclass_dict: Dict = dict()
        self.subclass_error: Dict = dict()

        # EDGE_DICT
        if not isinstance(edge_dict, Dict):
            raise TypeError('edge_dict must be a dictionary.')
        elif len(edge_dict) == 0:
            raise TypeError('edge_dict is empty.')
        else:
            self.edge_dict = edge_dict

        # WRITE LOCATION
        if write_location is None:
            raise ValueError('write_location must contain a valid filepath, not None')
        else:
            self.write_location = write_location

        # LOADING SUBCLASS DICTIONARY
        if len(glob.glob(self.write_location + '/construction_*/*.pkl')) == 0:
            raise OSError('The {} file does not exist!'.format('subclass_construction_map.pkl'))
        elif os.stat(glob.glob(self.write_location + '/construction_*/*.pkl')[0]).st_size == 0:
            raise TypeError(
                'The input file: {} is empty'.format(glob.glob(self.write_location + '/construction_*/*.pkl')[0]))
        else:
            with open(glob.glob(self.write_location + '/construction_*/*.pkl')[0], 'rb') as filepath:  # type: IO[Any]
                self.subclass_dict = pickle.load(filepath, encoding='bytes')

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

    @staticmethod
    def edge_constructor(graph: Graph, node1: Union[BNode, URIRef], node2: Union[BNode, URIRef], relation: URIRef,
                         inverse_relation: Optional[URIRef]) -> Tuple[Graph, List]:
        """Constructs a single edge between to ontology classes as well as verifies if the user wants an inverse edge
        created and if so, then this edge is also added to the knowledge graph.

        Args:
            graph: An RDFLib Graph object.
            node1: A URIRef object containing a subject node of type owl:Class.
            node2: A URIRef object containing a object node of type owl:Class.
            relation: A URIRef object containing an owl:ObjectProperty.
            inverse_relation: A variable that either contains a string containing the identifier for an inverse relation
                (i.e. "RO_0002200") or None (i.e. indicator of no inverse relation).

        Returns:
            graph: An RDFLib Graph object.
            edge_counter: A list of new edges added to the knowledge graph.
        """

        edge_counter: List = []
        new_edge_inverse_rel: Tuple = tuple()
        rel_only_class_uuid, inv_rel_class_uuid = BNode(), BNode()

        # all subclass (class-class/subclass-class/subclass-subclass) and instance class-class edge types
        new_edge_rel_only = ((node1, RDFS.subClassOf, rel_only_class_uuid),
                             (rel_only_class_uuid, RDF.type, OWL.Restriction),
                             (rel_only_class_uuid, OWL.someValuesFrom, node2),
                             (rel_only_class_uuid, OWL.onProperty, relation))
        if inverse_relation:
            new_edge_inverse_rel = ((node2, RDFS.subClassOf, inv_rel_class_uuid),
                                    (inv_rel_class_uuid, RDF.type, OWL.Restriction),
                                    (inv_rel_class_uuid, OWL.someValuesFrom, node1),
                                    (inv_rel_class_uuid, OWL.onProperty, inverse_relation))

        return adds_edges_to_graph(graph, tuple(new_edge_rel_only + new_edge_inverse_rel), edge_counter)

    def subclass_constructor(self, graph: Graph, edge_info: Dict, edge_type: str) -> Tuple[Dict, Graph, List]:
        """Adds edges for the subclass construction approach.

        Assumption: All ontology class nodes use the obo namespace.

        Args:
            graph: An RDFLib Graph object.
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                 'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                 'edges': ['CHEBI_81395', 'DOID_12858']}
            edge_type: A string containing the name of the edge_type (e.g. "gene-disease", "chemical-gene").

        Returns:
            edge_dict:
            graph: An RDFLib Graph object.
            edges: A list of new edges added to the knowledge graph.
        """

        res = finds_node_type(edge_info)
        rel = URIRef(obo + edge_info['rel'])
        inv_rel = URIRef(obo + edge_info['inv_rel']) if edge_info['inv_rel'] else None
        uri1, uri2 = edge_info['uri']
        new_edges, kg = [], Graph()
        edges: List = []

        if res['cls1'] and res['cls2']:  # class-class edges
            updated_edges = self.edge_constructor(graph, URIRef(res['cls1']), URIRef(res['cls2']), rel, inv_rel)
            return (self.edge_dict,) + updated_edges
        else:
            if res['cls1'] and res['ent1']:  # subclass-class/class-subclass edges
                x = res['ent1'].replace(uri2, '') if edge_info['n1'] == 'class' else res['ent1'].replace(uri1, '')
                mapped_node = self.maps_node_to_class(edge_type, x, edge_info['edges'])

                if mapped_node:
                    new_edges = [(URIRef(res['ent1']), RDF.type, OWL.Class)]
                    new_edges += [(URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + x)) for x in mapped_node]

                    if edge_info['n1'] == 'class':  # determine node order
                        kg, edges = self.edge_constructor(graph, URIRef(res['cls1']), URIRef(res['ent1']), rel, inv_rel)
                    else:
                        kg, edges = self.edge_constructor(graph, URIRef(res['ent1']), URIRef(res['cls1']), rel, inv_rel)
            else:  # subclass-subclass edges
                mapped_node1 = self.maps_node_to_class(edge_type, res['ent1'].replace(uri1, ''), edge_info['edges'])
                mapped_node2 = self.maps_node_to_class(edge_type, res['ent2'].replace(uri2, ''), edge_info['edges'])

                if mapped_node1 and mapped_node2:
                    new_edges += [(URIRef(x), RDF.type, OWL.Class) for x in [res['ent1'], res['ent2']]]
                    new_edges += [(URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + x)) for x in mapped_node1]
                    new_edges += [(URIRef(res['ent2']), RDFS.subClassOf, URIRef(obo + x)) for x in mapped_node2]
                    kg, edges = self.edge_constructor(graph, URIRef(res['ent1']), URIRef(res['ent2']), rel, inv_rel)

            return (self.edge_dict,) + adds_edges_to_graph(kg, tuple(new_edges), edges)

    def instance_constructor(self, graph: Graph, edge_info: Dict, edge_type: str) -> Tuple[Dict, Graph, List]:
        """Adds edges for the instance construction approach.

        Assumption: All ontology class nodes use the obo namespace.

        Args:
            graph: An RDFLib Graph object.
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                 'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                 'edges': ['CHEBI_81395', 'DOID_12858']}
            edge_type: A string containing the name of the edge_type (e.g. "gene-disease", "chemical-gene").

        Returns:
            edges: A list of new edges added to the knowledge graph.
        """

        res = finds_node_type(edge_info)
        rel = URIRef(obo + edge_info['rel'])
        inv_rel = URIRef(obo + edge_info['inv_rel']) if edge_info['inv_rel'] else None
        uri1, uri2 = edge_info['uri']
        new_edges = []

        if res['cls1'] and res['cls2']:  # class-class edges
            updated_edges = self.edge_constructor(graph, URIRef(res['cls1']), URIRef(res['cls2']), rel, inv_rel)
            return (self.edge_dict,) + updated_edges
        else:
            if res['cls1'] and res['ent1']:  # class-instance/instance-class edges
                sha_uid = BNode('N' + hashlib.md5(res['cls1'].encode()).hexdigest())  # meeting NCName requirements
                x = res['ent1'].replace(uri2, '') if edge_info['n1'] == 'class' else res['ent1'].replace(uri1, '')
                mapped_node = self.maps_node_to_class(edge_type, x, edge_info['edges'])

                if mapped_node:
                    new_edges = [(sha_uid, RDF.type, URIRef(res['cls1'])),
                                 (URIRef(res['ent1']), RDF.type, OWL.NamedIndividual)]
                    new_edges += [(URIRef(res['ent1']), RDF.type, URIRef(obo + x)) for x in mapped_node]

                    if edge_info['n1'] == 'class':  # determine order to pass nodes
                        new_edges += [(sha_uid, rel, URIRef(res['ent1']))]
                        if inv_rel: new_edges += [(URIRef(res['ent1']), inv_rel, sha_uid)]
                    else:
                        new_edges += [(URIRef(res['ent1']), rel, sha_uid)]
                        if inv_rel: new_edges += [(sha_uid, inv_rel, URIRef(res['ent1']))]
            else:  # instance-instance edges
                mapped_node1 = self.maps_node_to_class(edge_type, res['ent1'].replace(uri1, ''), edge_info['edges'])
                mapped_node2 = self.maps_node_to_class(edge_type, res['ent2'].replace(uri2, ''), edge_info['edges'])

                if mapped_node1 and mapped_node2:
                    new_edges = [(URIRef(x), RDF.type, OWL.NamedIndividual) for x in [res['ent1'], res['ent2']]]
                    new_edges += [(URIRef(res['ent1']), rel, URIRef(res['ent2']))]
                    if inv_rel: new_edges += [(URIRef(res['ent2']), inv_rel, URIRef(res['ent1']))]

                    # add instance-subclass dict typing
                    new_edges += [(URIRef(res['ent1']), RDF.type, URIRef(obo + x)) for x in mapped_node1]
                    new_edges += [(URIRef(res['ent2']), RDF.type, URIRef(obo + x)) for x in mapped_node2]

            return (self.edge_dict,) + adds_edges_to_graph(graph, tuple(new_edges), [])
