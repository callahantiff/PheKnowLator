#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import hashlib
import logging.config
import os
import os.path
import pickle

from rdflib import Graph, Namespace, BNode, Literal, URIRef  # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, IO, List, Optional, Tuple, Union

from pkt_kg.utils import *

# set global attributes
obo = Namespace('http://purl.obolibrary.org/obo/')
pkt = Namespace('https://github.com/callahantiff/PheKnowLator/pkt/')
pkt_bnode = Namespace('https://github.com/callahantiff/PheKnowLator/pkt/bnode/')
# logging
log_dir, log, log_config = 'builds/logs', 'pkt_build_log.log', glob.glob('**/logging.ini', recursive=True)
try:
    if not os.path.exists(log_dir): os.mkdir(log_dir)
except FileNotFoundError:
    log_dir, log_config = '../builds/logs', glob.glob('../builds/logging.ini', recursive=True)
    if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


class KGConstructionApproach(object):
    """Class stores different methods that can be used to construct knowledge graph edges.

    Two Construction Approaches:
        (1) Instance-based: Adds edge data that is not from an ontology by connecting each non-ontology data node to
            an instances of existing ontology class node;
        (2) Subclass-based: Adds edge data that is not from an ontology by connecting each non-ontology data node to
            an existing ontology class node.

    Attributes:
        write_location: A string pointing to the 'resources' directory.

    Raises:
        TypeError: If graph is not an rdflib.graph object.
        TypeError: If edge_info and edge_dict are not dictionary objects.
        ValueError: If graph, edge_info, edge_dict, or subclass_dict files are empty.
        OSError: If there is no subclass_dict file in the resources/construction_approach directory.
    """

    def __init__(self, write_location: str) -> None:
        self.subclass_dict: Dict = dict()
        self.subclass_error: Dict = dict()

        # WRITE LOCATION
        if write_location is None:
            log_str = 'write_location must contain a valid filepath.'; logger.error('ValueError: ' + log_str)
            raise ValueError(log_str)
        else: self.write_location = write_location

        # LOADING SUBCLASS DICTIONARY
        file_name = self.write_location + '/construction_*/*.pkl'
        if len(glob.glob(file_name)) == 0:
            log_str = 'subclass_construction_map.pkl does not exist!'; logger.error('OSError: ' + log_str)
            raise OSError(log_str)
        elif os.stat(glob.glob(file_name)[0]).st_size == 0:
            log_str = 'The input file: {} is empty'.format(glob.glob(file_name)[0])
            logger.error('TypeError: ' + log_str); raise TypeError(log_str)
        else:
            with open(glob.glob(file_name)[0], 'rb') as filepath:  # type: IO[Any]
                self.subclass_dict = pickle.load(filepath, encoding='bytes')

    def maps_node_to_class(self, edge_type: str, entity: str) -> Optional[List]:
        """Takes an entity and checks whether or not it exists in a dictionary of subclass content, such that keys
        are non-class entity identifiers (e.g. Reactome identifiers) and values are sets of ontology class identifiers
        mapped to that non-class entity. For example:
            {'R-HSA-5601843': {'PW_0000001'}, 'R-HSA-77584': {'PW_0000001', 'GO_0008334'}}

        The motivation for verifying whether a non-ontology node identifier is not in the subclass_map is to try and
        catch errors in the edge sources used to generate the edges. For example, there were genes taken from CTD
        that were tagged in the downloaded data as being human, that were not actually human and the way that we
        caught that error was by checking against the identifiers included in the subclass_map dict.

        Args:
            edge_type: A string containing the edge_type (e.g. "gene-pathway").
            entity: A string containing a node identifier (e.g. "R-HSA-5601843").

        Returns:
            None if the non-class entity is not in the subclass_dict, otherwise a list of mappings between the
            non-class entity node is returned.
        """

        if entity not in self.subclass_dict.keys():
            if self.subclass_error and edge_type in self.subclass_error.keys():
                if entity not in self.subclass_error[edge_type]: self.subclass_error[edge_type] += [entity]
            else: self.subclass_error[edge_type] = [entity]
            subclass_map = None
        else: subclass_map = self.subclass_dict[entity]

        return subclass_map

    @staticmethod
    def subclass_core_constructor(node1: URIRef, node2: URIRef, relation: URIRef, inv_relation: URIRef) -> Tuple:
        """Core subclass-based edge construction method. Constructs a single edge between to ontology classes as well as
        verifies if the user wants an inverse edge created and if so, then this edge is also added to the knowledge
        graph. Note that a Bnode is used for subclass construction versus the UUID hash + pkt namespace that is used
        for instance-based construction.

        Note. We explicitly type each node and each relation/inverse relation. This may seem redundant, but it is
        needed in order to ensure consistency between the data after applying the OWL API to reformat the data.

        Args:
            node1: A URIRef or BNode object containing a subject node.
            node2: A URIRef or BNode object containing a object node.
            relation: A URIRef object containing an owl:ObjectProperty.
            inv_relation: A string containing an inverse relation identifier (i.e. RO_0002200) or None (i.e.
                indicating no inverse relation).

        Returns:
            A list of tuples representing new edges to add to the knowledge graph.
        """

        rel_core = n3(node1) + n3(relation) + n3(node2)
        u1 = URIRef(pkt + 'N' + hashlib.md5(rel_core.encode()).hexdigest())
        u2 = URIRef(pkt_bnode + 'N' + hashlib.md5((rel_core + n3(OWL.Restriction)).encode()).hexdigest())

        new_edge_inverse_rel: Tuple = tuple()
        new_edge_rel_only: Tuple = ((node1, RDF.type, OWL.Class),
                                    (u1, RDFS.subClassOf, node1),
                                    (u1, RDF.type, OWL.Class),
                                    (u1, RDFS.subClassOf, u2),
                                    (u2, RDF.type, OWL.Restriction),
                                    (u2, OWL.someValuesFrom, node2),
                                    (node2, RDF.type, OWL.Class),
                                    (u2, OWL.onProperty, relation),
                                    (relation, RDF.type, OWL.ObjectProperty))
        if inv_relation:
            inv_rel_core = n3(node2) + n3(inv_relation) + n3(node1)
            u3 = URIRef(pkt + 'N' + hashlib.md5(inv_rel_core.encode()).hexdigest())
            u4 = URIRef(pkt_bnode + 'N' + hashlib.md5((inv_rel_core + n3(OWL.Restriction)).encode()).hexdigest())

            new_edge_inverse_rel = ((node2, RDF.type, OWL.Class),
                                    (u3, RDFS.subClassOf, node2),
                                    (u3, RDF.type, OWL.Class),
                                    (u3, RDFS.subClassOf, u4),
                                    (u4, RDF.type, OWL.Restriction),
                                    (u4, OWL.someValuesFrom, node1),
                                    (node1, RDF.type, OWL.Class),
                                    (u4, OWL.onProperty, inv_relation),
                                    (inv_relation, RDF.type, OWL.ObjectProperty))

        return new_edge_rel_only + new_edge_inverse_rel

    def subclass_constructor(self, edge_info: Dict, edge_type: str) -> List:
        """Adds edges for the subclass construction approach.

        Assumption: All ontology class nodes use the obo namespace.

        Note. We explicitly type each node as a owl:Class and each relation/inverse relation as a owl:ObjectProperty.
        This may seem redundant, but it is needed in order to ensure consistency between the data after applying the
        OWL API to reformat the data.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                 'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                 'edges': ['CHEBI_81395', 'DOID_12858']}
            edge_type: A string containing the name of the edge_type (e.g. "gene-disease", "chemical-gene").

        Returns:
            edges: A set of tuples containing new edges to add to the knowledge graph.
        """

        res = finds_node_type(edge_info); uri1, uri2 = edge_info['uri']; edges: List = []
        rel = URIRef(obo + edge_info['rel'])
        irel = URIRef(obo + edge_info['inv_rel']) if edge_info['inv_rel'] is not None else None
        if res['cls1'] and res['cls2']:  # class-class edges
            edges = list(self.subclass_core_constructor(URIRef(res['cls1']), URIRef(res['cls2']), rel, irel))
        elif res['cls1'] and res['ent1']:  # entity-class/class-entity edges
            x = res['ent1'].replace(uri2, '') if edge_info['n1'] == 'class' else res['ent1'].replace(uri1, '')
            mapped_node = self.maps_node_to_class(edge_type, x)
            if mapped_node:  # get entity mappings to current classes from subclass_construction_map
                edges = [x for y in [((URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + i)),) +
                                     ((URIRef(obo + i), RDF.type, OWL.Class),) for i in mapped_node] for x in y]
                ent_order = ['cls1', 'ent1'] if edge_info['n1'] == 'class' else ['ent1', 'cls1']  # determine node order
                edges += self.subclass_core_constructor(URIRef(res[ent_order[0]]), URIRef(res[ent_order[1]]), rel, irel)
        else:  # entity-entity edges
            mapped_node1 = self.maps_node_to_class(edge_type, res['ent1'].replace(uri1, ''))
            mapped_node2 = self.maps_node_to_class(edge_type, res['ent2'].replace(uri2, ''))
            if mapped_node1 and mapped_node2:  # get entity mappings to current classes from subclass_construction_map
                edges += [x for y in [((URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + i)),) +
                                      ((URIRef(obo + i), RDF.type, OWL.Class),) for i in mapped_node1] for x in y]
                edges += [x for y in [((URIRef(res['ent2']), RDFS.subClassOf, URIRef(obo + i)),) +
                                      ((URIRef(obo + i), RDF.type, OWL.Class),) for i in mapped_node2] for x in y]
                edges += self.subclass_core_constructor(URIRef(res['ent1']), URIRef(res['ent2']), rel, irel)

        return edges

    @staticmethod
    def instance_core_constructor(node1: URIRef, node2: URIRef, relation: URIRef, inv_relation: URIRef) -> Tuple:
        """Core instance-based edge construction method. Constructs a single edge between two ontology classes as
        well as verifies if the user wants an inverse edge created and if so, then this edge is also added to the
        knowledge graph.

        Note. We explicitly type each node and each relation/inverse relation. This may seem redundant, but it is
        needed in order to ensure consistency between the data after applying the OWL API to reformat the data.

        Args:
            node1: A URIRef or BNode object containing a subject node.
            node2: A URIRef or BNode object containing a object node.
            relation: A URIRef object containing an owl:ObjectProperty.
            inv_relation: A string containing the identifier for an inverse relation (i.e. RO_0002200) or None
                (i.e. indicator of no inverse relation).

        Returns:
            A list of tuples representing new edges to add to the knowledge graph.
        """

        # select hash relation - if rel and inv rel take first in alphabetical order else use rel
        rels = sorted([relation, inv_relation])[0] if inv_relation is not None else [relation][0]
        rel_core = n3(node1) + n3(rels) + n3(node2)
        u1 = URIRef(pkt + 'N' + hashlib.md5((rel_core + 'subject').encode()).hexdigest())
        u2 = URIRef(pkt + 'N' + hashlib.md5((rel_core + 'object').encode()).hexdigest())
        new_edge_inverse_rel: Tuple = tuple()
        new_edge_rel_only: Tuple = ((u1, RDF.type, node1), (u1, RDF.type, OWL.NamedIndividual),
                                    (u2, RDF.type, node2), (u2, RDF.type, OWL.NamedIndividual),
                                    (u1, relation, u2), (relation, RDF.type, OWL.ObjectProperty))
        if inv_relation: new_edge_inverse_rel = ((u2, inv_relation, u1), (inv_relation, RDF.type, OWL.ObjectProperty))

        return new_edge_rel_only + new_edge_inverse_rel

    def instance_constructor(self, edge_info: Dict, edge_type: str) -> List:
        """Adds edges for the instance construction approach.

        Assumption: All ontology class nodes use the obo namespace.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                 'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                 'edges': ['CHEBI_81395', 'DOID_12858']}
            edge_type: A string containing the name of the edge_type (e.g. "gene-disease", "chemical-gene").

        Returns:
            edges: A set of tuples containing new edges to add to the knowledge graph.
        """

        res = finds_node_type(edge_info); uri1, uri2 = edge_info['uri']; edges: List = []
        rel = URIRef(obo + edge_info['rel'])
        irel = URIRef(obo + edge_info['inv_rel']) if edge_info['inv_rel'] is not None else None
        if res['cls1'] and res['cls2']:  # class-class edges
            edges = list(self.instance_core_constructor(URIRef(res['cls1']), URIRef(res['cls2']), rel, irel))
        elif res['cls1'] and res['ent1']:  # class-entity/entity-class edges
            x = res['ent1'].replace(uri2, '') if edge_info['n1'] == 'class' else res['ent1'].replace(uri1, '')
            mapped_node = self.maps_node_to_class(edge_type, x)
            if mapped_node:  # get entity mappings to current classes from subclass_construction_map
                edges = [x for y in [((URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + i)),) +
                                     ((URIRef(obo + i), RDF.type, OWL.Class),) +
                                     ((URIRef(res['ent1']), RDF.type, OWL.Class),) for i in mapped_node] for x in y]
                ent_order = ['cls1', 'ent1'] if edge_info['n1'] == 'class' else ['ent1', 'cls1']  # determine node order
                edges += self.instance_core_constructor(URIRef(res[ent_order[0]]), URIRef(res[ent_order[1]]), rel, irel)
        else:  # entity-entity edges
            mapped_node1 = self.maps_node_to_class(edge_type, res['ent1'].replace(uri1, ''))
            mapped_node2 = self.maps_node_to_class(edge_type, res['ent2'].replace(uri2, ''))
            if mapped_node1 and mapped_node2:  # get entity mappings to current classes from subclass_construction_map
                edges += [x for y in [((URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + i)),) +
                                      ((URIRef(obo + i), RDF.type, OWL.Class),) +
                                      ((URIRef(res['ent1']), RDF.type, OWL.Class),) for i in mapped_node1] for x in y]
                edges += [x for y in [((URIRef(res['ent2']), RDFS.subClassOf, URIRef(obo + i)),) +
                                      ((URIRef(obo + i), RDF.type, OWL.Class),) +
                                      ((URIRef(res['ent2']), RDF.type, OWL.Class),) for i in mapped_node2] for x in y]
                edges += self.instance_core_constructor(URIRef(res['ent1']), URIRef(res['ent2']), rel, irel)

        return edges
