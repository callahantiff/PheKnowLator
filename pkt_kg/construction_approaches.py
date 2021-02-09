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

from pkt_kg.utils import finds_node_type

# set global attributes
obo = Namespace('http://purl.obolibrary.org/obo/')
pkt = Namespace('https://github.com/callahantiff/PheKnowLator/pkt/')
# logging
log_dir, log, log_config = 'builds/logs', 'pkt_build_log.log', glob.glob('**/logging.ini', recursive=True)
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
        edge_dict: A nested master edge list dict. The outer key is an edge-type (e.g.go-gene) and inner key is a dict
            storing data from the resource_info.txt input document. For example:
            {'chemical-complex': {'source_labels': ';;', 'data_type': 'class-instance', 'edge_relation': 'RO_0002436',
             'uri': ['http://purl.obolibrary.org/obo/', 'https://reactome.org/content/detail/'], 'delimiter': 't',
             'column_idx': '0;1', 'identifier_maps': 'None', 'evidence_criteria': 'None', 'filter_criteria': 'None',
             'edge_list': [['CHEBI_24505', 'R-HSA-1006173'], ...] }}
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
            logger.error('TypeError: edge_dict must be a dictionary.')
            raise TypeError('edge_dict must be a dictionary.')
        elif len(edge_dict) == 0:
            logger.error('TypeError: edge_dict is empty.')
            raise TypeError('edge_dict is empty.')
        else: self.edge_dict = edge_dict

        # WRITE LOCATION
        if write_location is None:
            logger.error('ValueError: write_location must contain a valid filepath, not None.')
            raise ValueError('write_location must contain a valid filepath, not None')
        else: self.write_location = write_location

        # LOADING SUBCLASS DICTIONARY
        file_name = self.write_location + '/construction_*/*.pkl'
        if len(glob.glob(file_name)) == 0:
            logger.error('OSError: {} does not exist!'.format('subclass_construction_map.pkl'))
            raise OSError('{} does not exist!'.format('subclass_construction_map.pkl'))
        elif os.stat(glob.glob(file_name)[0]).st_size == 0:
            logger.error('TypeError: The input file: {} is empty'.format(glob.glob(file_name)[0]))
            raise TypeError('The input file: {} is empty'.format(glob.glob(file_name)[0]))
        else:
            with open(glob.glob(file_name)[0], 'rb') as filepath:  # type: IO[Any]
                self.subclass_dict = pickle.load(filepath, encoding='bytes')

    def maps_node_to_class(self, edge_type: str, entity: str, edge: List) -> Optional[List]:
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
            edge: An edge list containing two nodes (e.g. ["R-HSA-5601843", "GO_1234567"]).

        Returns:
            None if the non-class entity is not in the subclass_dict, otherwise a list of mappings between the
            non-class entity node is returned.
        """

        e_type = edge_type
        if entity not in self.subclass_dict.keys():
            if self.subclass_error and e_type in self.subclass_error.keys(): self.subclass_error[e_type] += [entity]
            else: self.subclass_error[e_type] = [entity]
            # remove the edge from the edge_dict
            self.edge_dict[e_type]['edge_list'].pop(self.edge_dict[e_type]['edge_list'].index(edge))
            subclass_map = None
        else: subclass_map = self.subclass_dict[entity]

        return subclass_map

    @staticmethod
    def subclass_edge_constructor(node1: Union[BNode, URIRef], node2: Union[BNode, URIRef], relation: URIRef,
                                  inverse_relation: Optional[URIRef]) -> Tuple:
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
            inverse_relation: A string containing an inverse relation identifier (i.e. "RO_0002200") or None (i.e.
                indicating no inverse relation).

        Returns:
            A list of tuples representing new edges to add to the knowledge graph.
        """

        new_edge_inverse_rel: Tuple = tuple()
        # set uuids
        rel_core = str(node1) + str(relation) + str(node2)
        inv_rel_core = str(node2) + str(inverse_relation) + str(node1)
        u1 = URIRef(pkt + 'N' + hashlib.md5(str(rel_core).encode()).hexdigest())
        u2 = 'N' + hashlib.md5(str(rel_core + str(OWL.Restriction)).encode()).hexdigest()
        u3 = URIRef(pkt + 'N' + hashlib.md5(str(inv_rel_core).encode()).hexdigest())
        u4 = 'N' + hashlib.md5(str(inv_rel_core + str(OWL.Restriction)).encode()).hexdigest()
        # creates a restriction in order to connect two nodes using the input relation
        new_edge_rel_only = ((node1, RDF.type, OWL.Class), (u1, RDFS.subClassOf, node1),
                             (u1, RDF.type, OWL.Class), (u1, RDFS.subClassOf, BNode(u2)),
                             (BNode(u2), RDF.type, OWL.Restriction), (BNode(u2), OWL.someValuesFrom, node2),
                             (node2, RDF.type, OWL.Class), (BNode(u2), OWL.onProperty, relation),
                             (relation, RDF.type, OWL.ObjectProperty))
        if inverse_relation:
            new_edge_inverse_rel = ((node2, RDF.type, OWL.Class), (u3, RDFS.subClassOf, node2),
                                    (u3, RDF.type, OWL.Class), (u3, RDFS.subClassOf, BNode(u4)),
                                    (BNode(u4), RDF.type, OWL.Restriction), (BNode(u4), OWL.someValuesFrom, node1),
                                    (node1, RDF.type, OWL.Class), (BNode(u4), OWL.onProperty, inverse_relation),
                                    (inverse_relation, RDF.type, OWL.ObjectProperty))

        return new_edge_rel_only + new_edge_inverse_rel

    def subclass_constructor(self, edge_info: Dict, edge_type: str) -> Tuple[Dict, List]:
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
            edge_dict: A nested master edge list dict. The outer key is an edge-type (e.g.go-gene) and inner key is a
                dict storing data from the resource_info.txt input document. See class doc note for more information.
            edges: A set of tuples containing new edges to add to the knowledge graph.
        """

        res = finds_node_type(edge_info)
        rel, irel = URIRef(obo + edge_info['rel']), URIRef(obo + edge_info['inv_rel']) if edge_info['inv_rel'] else None
        uri1, uri2 = edge_info['uri']
        edges: List = []

        if res['cls1'] and res['cls2']:  # class-class edges
            edges = list(self.subclass_edge_constructor(URIRef(res['cls1']), URIRef(res['cls2']), rel, irel))
        elif res['cls1'] and res['ent1']:  # entity-class/class-entity edges
            x = res['ent1'].replace(uri2, '') if edge_info['n1'] == 'class' else res['ent1'].replace(uri1, '')
            mapped_node = self.maps_node_to_class(edge_type, x, edge_info['edges'])
            if mapped_node:  # get non-class node mappings to ontology classes
                edges = [x for y in [((URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + x)),) +
                                     ((URIRef(obo + x), RDF.type, OWL.Class),) for x in mapped_node] for x in y]
                if edge_info['n1'] == 'class':  # determine node order
                    edges += self.subclass_edge_constructor(URIRef(res['cls1']), URIRef(res['ent1']), rel, irel)
                else:
                    edges += self.subclass_edge_constructor(URIRef(res['ent1']), URIRef(res['cls1']), rel, irel)
        else:  # entity-entity edges
            mapped_node1 = self.maps_node_to_class(edge_type, res['ent1'].replace(uri1, ''), edge_info['edges'])
            mapped_node2 = self.maps_node_to_class(edge_type, res['ent2'].replace(uri2, ''), edge_info['edges'])
            if mapped_node1 and mapped_node2:  # get non-class node mappings to ontology classes
                edges += [x for y in [((URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + x)),) +
                                      ((URIRef(obo + x), RDF.type, OWL.Class),) for x in mapped_node1] for x in y]
                edges += [x for y in [((URIRef(res['ent2']), RDFS.subClassOf, URIRef(obo + x)),) +
                                      ((URIRef(obo + x), RDF.type, OWL.Class),) for x in mapped_node2] for x in y]
                edges += self.subclass_edge_constructor(URIRef(res['ent1']), URIRef(res['ent2']), rel, irel)

        return self.edge_dict, list(set(edges))

    @staticmethod
    def instance_edge_constructor(node1: Union[BNode, URIRef], node2: Union[BNode, URIRef], relation: URIRef,
                                  inverse_relation: Optional[URIRef]) -> Tuple:
        """Core instance-based edge construction method. Constructs a single edge between two ontology classes as
        well as verifies if the user wants an inverse edge created and if so, then this edge is also added to the
        knowledge graph.

        Note. We explicitly type each node and each relation/inverse relation. This may seem redundant,
        but it is needed in order to ensure consistency between the data after applying the OWL API to reformat the
        data.

        Args:
            node1: A URIRef or BNode object containing a subject node.
            node2: A URIRef or BNode object containing a object node.
            relation: A URIRef object containing an owl:ObjectProperty.
            inverse_relation: A string containing the identifier for an inverse relation (i.e. "RO_0002200") or None
                (i.e. indicator of no inverse relation).

        Returns:
            A list of tuples representing new edges to add to the knowledge graph.
        """

        new_edge_inverse_rel: Tuple = tuple()
        # create uuid
        rels = sorted([relation, inverse_relation])[0] if inverse_relation is not None else [relation][0]
        u1 = URIRef(pkt + 'N' + hashlib.md5(str(str(node1) + str(rels) + str(node2) + 'subject').encode()).hexdigest())
        u2 = URIRef(pkt + 'N' + hashlib.md5(str(str(node1) + str(rels) + str(node2) + 'object').encode()).hexdigest())
        # creates a restriction in order to connect two nodes using the input relation
        new_edge_rel_only = ((u1, RDF.type, node1), (u1, RDF.type, OWL.NamedIndividual),
                             (u2, RDF.type, node2), (u2, RDF.type, OWL.NamedIndividual),
                             (u1, relation, u2), (relation, RDF.type, OWL.ObjectProperty))
        if inverse_relation:
            new_edge_inverse_rel = ((u2, inverse_relation, u1), (inverse_relation, RDF.type, OWL.ObjectProperty))

        return new_edge_rel_only + new_edge_inverse_rel

    def instance_constructor(self, edge_info: Dict, edge_type: str) -> Tuple[Dict, List]:
        """Adds edges for the instance construction approach.

        Assumption: All ontology class nodes use the obo namespace.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'class', 'n2': 'class','rel': 'RO_0002606', 'inv_rel': 'RO_0002615',
                 'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                 'edges': ['CHEBI_81395', 'DOID_12858']}
            edge_type: A string containing the name of the edge_type (e.g. "gene-disease", "chemical-gene").

        Returns:
            edge_dict: A nested master edge list dict. The outer key is an edge-type (e.g.go-gene) and inner key is a
                dict storing data from the resource_info.txt input document. See class doc note for more information.
            edges: A set of tuples containing new edges to add to the knowledge graph.
        """

        res = finds_node_type(edge_info)
        rel, irel = URIRef(obo + edge_info['rel']), URIRef(obo + edge_info['inv_rel']) if edge_info['inv_rel'] else None
        uri1, uri2 = edge_info['uri']
        edges: List = []

        if res['cls1'] and res['cls2']:  # class-class edges
            edges = list(self.instance_edge_constructor(URIRef(res['cls1']), URIRef(res['cls2']), rel, irel))
        elif res['cls1'] and res['ent1']:  # class-entity/entity-class edges
            x = res['ent1'].replace(uri2, '') if edge_info['n1'] == 'class' else res['ent1'].replace(uri1, '')
            mapped_node = self.maps_node_to_class(edge_type, x, edge_info['edges'])
            if mapped_node:  # get non-class node mappings to ontology classes
                edges = [x for y in [((URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + x)),) +
                                     ((URIRef(obo + x), RDF.type, OWL.Class),) +
                                     ((URIRef(res['ent1']), RDF.type, OWL.Class),) for x in mapped_node] for x in y]
                if edge_info['n1'] == 'class':  # determine node order
                    edges += self.instance_edge_constructor(URIRef(res['cls1']), URIRef(res['ent1']), rel, irel)
                else:
                    edges += self.instance_edge_constructor(URIRef(res['ent1']), URIRef(res['cls1']), rel, irel)
        else:  # entity-entity edges
            mapped_node1 = self.maps_node_to_class(edge_type, res['ent1'].replace(uri1, ''), edge_info['edges'])
            mapped_node2 = self.maps_node_to_class(edge_type, res['ent2'].replace(uri2, ''), edge_info['edges'])
            if mapped_node1 and mapped_node2:  # get non-class node mappings to ontology classes
                edges += [x for y in [((URIRef(res['ent1']), RDFS.subClassOf, URIRef(obo + x)),) +
                                      ((URIRef(obo + x), RDF.type, OWL.Class),) +
                                      ((URIRef(res['ent1']), RDF.type, OWL.Class),) for x in mapped_node1] for x in y]
                edges += [x for y in [((URIRef(res['ent2']), RDFS.subClassOf, URIRef(obo + x)),) +
                                      ((URIRef(obo + x), RDF.type, OWL.Class),) +
                                      ((URIRef(res['ent2']), RDF.type, OWL.Class),) for x in mapped_node2] for x in y]
                edges += self.instance_edge_constructor(URIRef(res['ent1']), URIRef(res['ent2']), rel, irel)

        return self.edge_dict, list(set(edges))
