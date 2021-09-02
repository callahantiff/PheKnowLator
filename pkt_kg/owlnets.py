#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import logging.config
import networkx  # type: ignore
import os
import os.path
import pickle
import ray  # type: ignore
# import re

from collections import ChainMap  # type: ignore
from random import sample, shuffle
from rdflib import BNode, Graph, Literal, Namespace, URIRef  # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from statistics import mode, StatisticsError
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from pkt_kg.utils import *

# add global variables
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


# TODO:
#  (1) need to verify losslessness with respect to pkt-specific uuids; verify dict keyed with serialized nodes
#  (2) Method is currently built to handle class axioms; small modifications needed to handle propertyChainAxioms


class OwlNets(object):
    """Class removes OWL semantics from an ontology or knowledge graph using the OWL-NETS method. OWL-encoded or
    semantic edges are needed in a graph in order to enable a rich semantic representation. Many of the nodes in
    semantic edges are not clinically or biologically meaningful. This class is designed to decode all owl-encoded
    classes and return a knowledge graph that is semantically rich and clinically and biologically meaningful.

    KG CONSTRUCTION PURIFICATION: The method includes extra functionality to purify knowledge graphs according to an
    input construction approach type (i.e. 'instance- and subclass-based). The default approach is to leave the input
    graph alone and provide no purification steps. Alternatively, one can select "instance" or "subclass"
    purification. For more information see the purifies_graph_build() method.

    ASSUMPTIONS: In order to prevent the filtered graph from becoming unnecessarily disconnected, all OWL-NETS entities
    are checked to ensure that at least one of their ancestor concepts in the cleaned graph is a subclass of
    BFO_0000001 ('Entity'). While this is not the best solution long-term is the cleanest way to ensure the graph
    remains connected and to introduce the least amount of extra edges (i.e. avoids having to make every  node
    rdfs:subClassOf BFO_0000001).

    Additional Information: https://github.com/callahantiff/PheKnowLator/wiki/OWL-NETS-2.0
    Notebook Ex: https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/OWLNETS_Example_Application.ipynb

    Attributes:
        graph: An RDFLib object or a list of RDFLib Graph objects.
        write_location: A file path used for writing knowledge graph data (e.g. "resources/".
        filename: A string containing the filename for the full knowledge graph (e.g. "/hpo_owlnets").
        kg_construct_approach: A string containing the type of construction approach used to build the knowledge graph.
        owl_tools: A string pointing to the location of the owl tools library.
        top_level: A list of ontology namespaces that should not appear in any subject or object in the clean graph (
        default list: ['ISO', 'SUMO', 'BFO']).
        support: A list of ontology namespaces that should not appear in any subject, object, or relation in the clean
        graph (default list: ['IAO', 'SWO', 'OBI', 'UBPROP']).
        relations: A list of ontology namespaces that should not appear in any subject or object in the clean graph (
        default list ['RO']).

    Raises:
        TypeError: If graph is not an rdflib.graph object.
        ValueError: If graph is an empty rdflib.graph object.
        TypeError: If the file containing owl object properties is not a txt file.
        TypeError: If the file containing owl object properties is empty.
    """

    def __init__(self, graph: Union[Graph, List, str], write_location: str, filename: str,
                 kg_construct_approach: Optional[str] = None, owl_tools: str = './pkt_kg/libs/owltools',
                 top_level: Optional[List] = None, support: Optional[List] = None,
                 relations: Optional[List] = None) -> None:

        self.owl_tools = owl_tools
        self.kg_construct_approach = kg_construct_approach
        self.write_location = write_location
        self.res_dir = os.path.relpath('/'.join(self.write_location.split('/')[:-1]))
        self.filename = filename
        self.top_level: List = ['ISO', 'SUMO', 'BFO'] if top_level is None else top_level  # can only be in predicates
        self.support: List = ['IAO', 'SWO', 'OBI', 'UBPROP'] if support is None else support  # never in triples
        self.relations: List = ['RO'] if relations is None else relations  # can only appear as relations

        # VERIFY INPUT GRAPH
        if not isinstance(graph, Graph) and not isinstance(graph, List) and not isinstance(graph, str):
            logs = 'Graph must be RDFLib Graph or set.'; logger.error('TypeError: ' + logs); raise TypeError(logs)
        elif (isinstance(graph, Graph) or isinstance(graph, List)) and len(graph) == 0:
            log_str = 'Graph Object is empty.'; logger.error('ValueError: ' + log_str); raise ValueError(log_str)
        elif isinstance(graph, str) and not os.path.exists(graph):
            logs = "Can't find graph file"; logger.error("OSError: " + logs); raise OSError(logs)
        else:
            graph = graph if isinstance(graph, Graph) or isinstance(graph, List) else Graph().parse(graph)
            self.graph_list: List = [graph] if not isinstance(graph, List) else graph
        self.graph: Graph = self.graph_list[0]

        # OWL-NETS CLEANING DICTIONARY
        self.owl_nets_dict: Dict = {'decoded_entities': {}, 'cardinality': {}, 'misc': {}, 'complementOf': {},
                                    'negation': {}, 'disjointWith': set(), 'filtered_triples': set()}

    def gets_owlnets_dict(self) -> Dict:
        """Returns the owl_nets_dict dictionary."""

        return self.owl_nets_dict

    def gets_owlnets_graph(self) -> Graph:
        """Returns the graph RDFLib Graph object."""

        return self.graph

    def removes_disjoint_with_axioms(self) -> None:
        """Removes owl:disjointWith axioms from an RDFLib Graph object.

        Returns:
            None.
        """

        log_str = 'Removing owl:disjointWith Axioms'; logger.info(log_str); print(log_str)

        triples = set(
            list(self.graph.triples((None, OWL.disjointWith, None))) +
            list(self.graph.triples((None, None, OWL.disjointWith))))
        self.graph = remove_edges_from_graph(self.graph, triples)

        self.owl_nets_dict['disjointWith'] |= set(triples)

        return None

    def removes_edges_with_owl_semantics(self, verbose: bool = True) -> Graph:
        """Creates a filtered knowledge graph, such that only nodes that are owl:Class/owl:Individual connected via a
        owl:ObjectProperty and not an owl:AnnotationProperty. For example:
            REMOVE - edges needed to support owl semantics (not biologically meaningful):
                subject: obo:CLO_0037294; predicate: owl:AnnotationProperty; object: rdf:about=obo.CLO_0037294

            KEEP - biologically meaningful edges:
                subject: obo:CHEBI_16130; predicate: obo:RO_0002606; object: obo:HP_0000832

        Args:
            verbose: A bool indicating whether or not to print/log method use.

        Returns:
            filtered_graph: An RDFLib graph that contains only clinically and biologically meaningful triples.
        """

        if verbose: log_str = 'Filtering Triples'; logger.info(log_str); print(log_str)

        keep, filtered = set(), set(); exclude = self.top_level + self.relations + self.support
        pbar = tqdm(total=len(self.graph)) if verbose else None
        for x in self.graph:
            if verbose: pbar.update()
            if isinstance(x[0], URIRef) and isinstance(x[1], URIRef) and isinstance(x[2], URIRef):
                # handle top-level, relation, and support ontologies (top/rel can only be rel; remove support onts)
                subj = not any(i for i in exclude if str(x[0]).split('/')[-1].startswith(i + '_'))
                obj = not any(i for i in exclude if str(x[2]).split('/')[-1].startswith(i + '_'))
                rel = not any(i for i in self.support if str(x[1]).split('/')[-1].startswith(i + '_'))
                if subj and obj and rel:
                    s = [i for i in list(self.graph.triples((x[0], RDF.type, None)))
                         if (OWL.Class in i[2] or OWL.NamedIndividual in i[2]) and '#' not in str(x[0])]
                    o = [i for i in list(self.graph.triples((x[2], RDF.type, None)))
                         if (OWL.Class in i[2] or OWL.NamedIndividual in i[2]) and '#' not in str(x[2])]
                    p = [i for i in list(self.graph.triples((x[1], RDF.type, None)))
                         if i[2] != OWL.AnnotationProperty and i[2] != OWL.DatatypeProperty]
                    if len(s) > 0 and len(o) > 0 and len(p) > 0:
                        if OWL.ObjectProperty in [x[2] for x in p]: keep.add(x)
                        else: filtered |= {x}
                    elif len(s) > 0 and len(o) > 0 and len(p) == 0:
                        if RDFS.subClassOf in x[1]: keep.add(x)
                        elif RDF.type in x[1]: keep.add(x)
                        else: filtered |= {x}
                    elif x[1] == RDFS.subClassOf and (str(OWL) not in str(x[2]) and 'ObsoleteClass' not in str(x[2])):
                        keep.add(x)
                    else: filtered |= {x}
                else: filtered |= {x}
            else: filtered |= {x}
        if verbose: pbar.close()
        filtered_graph = adds_edges_to_graph(Graph(), list(keep), False)

        self.owl_nets_dict['filtered_triples'] |= filtered

        return filtered_graph

    def cleans_decoded_graph(self, verbose: bool = True) -> Graph:
        """Creates a filtered knowledge graph, such that only nodes that are owl:Class/owl:Individual connected via a
        owl:ObjectProperty and not an owl:AnnotationProperty. This method is a reduced version of the
        removes_edges_with_owl_semantics method, which is meant to be applied to a graph after it's been decoded.

        Args:
            verbose: A bool indicating whether or not to print/log progress.

        Returns:
             filtered_graph: An RDFLib graph that contains only clinically and biologically meaningful triples.
        """

        if verbose: log_str = 'Filtering Triples'; logger.info(log_str); print(log_str)

        keep_predicates, filtered_triples = set(), set(); exclude = self.top_level + self.relations + self.support
        for x in self.graph:
            if isinstance(x[0], URIRef) and isinstance(x[1], URIRef) and isinstance(x[2], URIRef):
                # handle top-level, relation, and support ontologies (top/rel can only be rel; remove support onts)
                subj = not any(i for i in exclude if str(x[0]).split('/')[-1].startswith(i + '_'))
                obj = not any(i for i in exclude if str(x[2]).split('/')[-1].startswith(i + '_'))
                rel = not any(i for i in self.support if str(x[1]).split('/')[-1].startswith(i + '_'))
                if subj and obj and rel:
                    if str(OWL) not in str(x[0]) and str(OWL) not in str(x[2]):
                        if ('XMLSchema' not in str(x[0])) and ('XMLSchema' not in str(x[2])):
                            keep_predicates.add(x)
                    else: filtered_triples |= {x}
                else: filtered_triples |= {x}
            else: filtered_triples |= {x}

        filtered_graph = adds_edges_to_graph(Graph(), list(keep_predicates), False)  # create a new graph from filtered
        self.owl_nets_dict['filtered_triples'] |= filtered_triples

        return filtered_graph

    def recurses_axioms(self, visited: List[BNode], axioms: List[Any]) -> List[BNode]:
        """Function recursively searches a list of graph nodes and tracks the nodes it has visited. Once all nodes in
        the input axioms list have been visited, a final unique list of relevant nodes is returned. This list is
        assumed to include all necessary BNodes needed to re-create an OWL:equivalentClass.

        Args:
            visited: A list which may or may not contain knowledge graph nodes.
            axioms: A list of axioms, e.g. [(BNode('N3e23fe5f05ff4a7d992c548607c86277'),
                                             URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                                             URIRef('http://www.w3.org/2002/07/owl#Class'))]

        Returns:
            seen_nodes: A list of knowledge graph BNodes.
        """

        search_axioms: List = []; tracked: List = []
        for axiom in axioms:
            for element in axiom:
                if isinstance(element, BNode) and element not in visited:
                    tracked.append(element); search_axioms += list(self.graph.triples((element, None, None)))
        if len(tracked) > 0: visited += list(set(tracked)); return self.recurses_axioms(visited, search_axioms)
        else: return visited

    def finds_uri(self, n1: Union[BNode, URIRef], n2: Optional[URIRef], node_list: Optional[list] = None) -> URIRef:
        """Method searches for the RDFLib URIRef object that represents a BNode that is either an OWL.annotatedSource or
        OWL.annotatedTarget within an OWL.Axiom.

        Args:
            n1: An RDFLib BNode object.
            n2: An RDFLib URIRef object or None.
            node_list: A list of RDFLib BNode and/or URIRef objects.

        Returns:
            node: A RDFLib URIRef object.
        """

        n = list(self.graph.objects(n1)) if node_list is None else node_list
        n = [x for x in n if x != n2 and (isinstance(x, BNode) or OWL.Class in set(self.graph.objects(x, RDF.type)))]
        n1 = n.pop(0)
        if n1 != n2 and OWL.Class in list(self.graph.objects(n1, RDF.type)): return n1
        else: n += [x for x in set(self.graph.objects(n1)) if x not in n]; return self.finds_uri(n1, n2, n)

    def reconciles_axioms(self, src: Union[BNode, URIRef], tgt: Union[BNode, URIRef]) -> Tuple:
        """Method takes two RDFLib objects (both are either a URIRef or a BNode) and performs two steps: (1) if
        target or the source is an RDFLib BNode, the URIRef object representing that node is returned; and (2)

        Args:
            src: A RDFLib URIRef or BNode object representing the source of an axiom annotation.
            tgt: A RDFLib URIRef or BNode object representing the target of an axiom annotation.

        Returns:
            src: An RDFLib URIRef object representing the OWL.annotatedSource of the axiom.
            matches: A list of triples comprising the axiom.
        """

        if isinstance(src, BNode) and isinstance(tgt, BNode):
            org_tgt, tgt = tgt, self.finds_uri(tgt, None)
            org_src, src = src, src if isinstance(src, URIRef) else self.finds_uri(src, tgt)
            bnodes = [org_src, org_tgt]
        else:
            org_src, src = src, src if isinstance(src, URIRef) else self.finds_uri(src, tgt)
            org_tgt, tgt = tgt, tgt if isinstance(tgt, URIRef) else self.finds_uri(tgt, src)
            bnodes = [org_src] if isinstance(org_src, BNode) and not isinstance(org_tgt, BNode) else [org_tgt]
        master, matches = set(), set()
        while len(bnodes) > 0:
            x = bnodes.pop(0); master |= {x}; matches |= set(self.graph.triples((x, None, None)))
            node_list = set([x for y in [i[0::2] for i in matches] for x in y])
            bnodes += [x for x in node_list if isinstance(x, BNode) and x not in master]

        return src, matches

    def reconciles_classes(self, node: URIRef) -> Set:
        """Method searches for all triples which are out edges from all BNodes that can be reached from the input node.

        Args:
            node: An RDFLib URIRef object.

        Returns:
            matches: A set of tuples, where each tuple contains a triple that is comprised of three RDFLib objects of
                type URIRef, BNode, and/or Literal.
        """

        matches: Set = set()
        out_edges = set(x for y in self.graph.triples((node, None, None)) for x in y if isinstance(x, BNode))
        node_list = list(out_edges)
        while len(node_list) != 0:
            entity = node_list.pop(0)
            for element in self.recurses_axioms([], list(self.graph.triples((entity, None, None)))):
                matches |= set(self.graph.triples((element, None, None)))
            hits = set(x for y in matches for x in y if isinstance(x, BNode) if x not in out_edges)
            node_list = list(set(node_list) | hits); out_edges |= set(node_list)

        return matches

    def creates_edge_dictionary(self, node: URIRef) -> Optional[Tuple[URIRef, Dict, Set]]:
        """Creates a nested edge dictionary from an input class  or axiom node by obtaining all outgoing edges and
        recursively looping over each anonymous out-edge node. While creating the dictionary, if cardinality is used
        then a formatted string that contains the class node and the anonymous node naming the element that includes
        cardinality is constructed and added to a set.

        Axioms: Retrieve a URIRef for all core components of an axiom (i.e. owl:AnnotatedSource, owl:AnnotatedTarget,
        and owl:AnnotatedProperty). Then, a list of core triples used to define the axiom is returned.

        Args:
            node: An RDFLib Term object.

        Returns:
            node: A URIRef object containing the node being decoded.
            edge_dict: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys
                are owl:ObjectProperty values from each out edge triple that come out of that anonymous node. For ex:
                    {BNode('N3243b60f69ba468687aa3cbe4e66991f'): {
                        someValuesFrom': rdflib.term.URIRef('http://purl.obolibrary.org/obo/PATO_0000587'),
                        type': rdflib.term.URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                        onProperty': rdflib.term.URIRef('http://purl.obolibrary.org/obo/RO_0000086')}}
            cardinality: A set of strings, where each string is formatted such that the substring before ':' is the
            class node and the substring after is the anonymous node naming the element where cardinality was used.
        """

        edge_dict: Dict = dict(); cardinality: Set = set()
        if OWL.Axiom in set(self.graph.objects(node, RDF.type)):
            src = list(self.graph.objects(node, OWL.annotatedSource))[0]
            tgt = list(self.graph.objects(node, OWL.annotatedTarget))[0]
            if isinstance(src, Literal) or isinstance(tgt, Literal): matches = None
            elif isinstance(src, URIRef) and isinstance(tgt, URIRef): return src, {node: {'subClassOf': tgt}}, set()
            else: node, matches = self.reconciles_axioms(src, tgt)
        else: matches = self.reconciles_classes(node)
        if matches is not None:
            for s, p, o in sorted(list(matches)):
                if 'cardinality' in str(p).lower(): cardinality |= {'{}: {}'.format(node, s)}
                else:
                    if s in edge_dict: edge_dict[s][p.split('#')[-1]] = {}; edge_dict[s][p.split('#')[-1]] = o
                    else: edge_dict[s] = {}; edge_dict[s][p.split('#')[-1]] = o

            return node, edge_dict, cardinality

        else: return None

    def captures_cardinality_axioms(self, node_info: Set, node: URIRef) -> None:
        """Method takes a tuple of information about a node and searches the information for nodes that contain
        semantic support information, but which also contain cardinality, which we don't yet fully process.

        Note. Class and axioms containing cardinality are currently only partially processed. Triples will be
        created, but if the triples point to a literal numeric value, those are not currently utilized.

        Args:
            node_info: A set of strings, where each string is formatted such that the substring that occurs before
                the ':' is the class node and the substring after the ':' is the anonymous node naming the element
                where cardinality was used.
            node: An RDFLib URIRef object containing node information.

        Returns:
            None.
        """

        if len(node_info) != 0:  # process triples ignoring the specific cardinality integer value
            self.owl_nets_dict['cardinality'][n3(node)] = set(
                self.graph.triples((BNode(list(node_info)[0].split(': ')[-1]), None, None)))

        return None

    def detects_negation_axioms(self, node_info: Dict, node: URIRef) -> bool:
        """Removes axioms from an RDFLib Graph object that convey or contain negation. The method currently checks
        for negation by searching for any occurrence of the following key words: "not", "lacks".

        Args:
            node_info: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys are
                owl:ObjectProperty values from each out edge triple that comes out of that anonymous node.
            node: An RDFLib URIRef object containing node information.

        Returns:
            True if the class is detected to contain a negation axiom.
            False if the class is not detected to contain a negation axiom.
        """

        neg_terms = ['lacks_', 'not_']  # can be extended to add additional properties as needed
        neg_res = {k: v for k, v in node_info.items() if 'onProperty' in v.keys()
                   and any(i for i in neg_terms if i in str(v['onProperty']).lower())}
        if len(neg_res) > 0: self.owl_nets_dict['negation'][n3(node)] = neg_res; return True
        else: return False

    def detects_complement_of_constructed_classes(self, node_info: Dict, node: URIRef) -> bool:
        """Removes classes from an RDFLib Graph object that were constructed using the owl:ComplementOf constructor.
        Currently, this type of constructor is removed because it conveys a negative relationship, which we are not
        currently able to represent using OWL-NETS.

        Args:
            node_info: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys are
                owl:ObjectProperty values from each out edge triple that comes out of that anonymous node.
            node: An RDFLib URIRef object containing node information.

        Returns:
            True if the class is detected to contain a owl:ComplementOf constructor.
            False if the class is not detected to contain a owl:ComplementOf constructor.
        """

        comp_res = {k: v for k, v in node_info.items() if 'complementOf' in v.keys()}
        if len(comp_res) > 0: self.owl_nets_dict['complementOf'][n3(node)] = comp_res; return True
        else: return False

    @staticmethod
    def returns_object_property(sub: URIRef, obj: URIRef, prop: URIRef = None) -> URIRef:
        """Checks the subject and object node types in order to determine the correct type of owl:ObjectProperty.

        The following ObjectProperties are returned for each of the following subject-object types:
            - subject + object are not PATO terms + prop is None --> rdfs:subClassOf
            - sub + obj are PATO terms + prop is None --> rdfs:subClassOf
            - sub is not a PATO term, but obj is a PATO term --> owl:RO_000086
            - sub is a PATO term + obj is a PATO term + prop is not None --> prop

        Args:
            sub: An rdflib.term object.
            obj: An rdflib.term object.
            prop: An rdflib.term object, which is provided as the value of owl:onProperty.

        Returns:
            An rdflib.term object that represents an owl:ObjectProperty.
        """

        if ('PATO' in sub and 'PATO' in obj) and not prop: return RDFS.subClassOf
        elif ('PATO' not in sub and 'PATO' not in obj) and not prop: return RDFS.subClassOf
        elif 'PATO' not in sub and 'PATO' in obj: return URIRef(obo + 'RO_0000086')
        else: return prop

    @staticmethod
    def parses_anonymous_axioms(edges: Dict, class_dict: Dict) -> Dict:
        """Parses axiom dictionaries that only include anonymous axioms (i.e. 'first' and 'rest') and returns an
        updated axiom dictionary that contains an owl:Restriction or an owl constructor (i.e. owl:unionOf or
        owl:intersectionOf).

        Args:
            edges: A dictionary where keys are owl:Objects (i.e. 'first', 'rest', 'onProperty', or 'someValuesFrom').
            class_dict: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys
                are owl:ObjectProperty values from each out edge triple that comes out of that anonymous node.

        Returns:
             updated_edges: dict subset. Keys are owl:Objects (e.g. 'first', 'rest', 'onProperty', or 'someValuesFrom').
        """

        if isinstance(edges['first'], URIRef) and isinstance(edges['rest'], BNode): return class_dict[edges['rest']]
        elif isinstance(edges['first'], URIRef) and isinstance(edges['rest'], URIRef): return class_dict[edges['first']]
        elif isinstance(edges['first'], BNode) and isinstance(edges['rest'], URIRef): return class_dict[edges['first']]
        else: return {**class_dict[edges['first']], **class_dict[edges['rest']]}

    @staticmethod
    def parses_subclasses(node: URIRef, edges: Dict, class_dict: Dict) -> Tuple[Set, Optional[Dict]]:
        """Parses a subset of a dictionary containing RDFLib objects participating in a RDFS:SubclassOf relationship
        and outputs a triple (referenced by node) representing this relationship. An example is provided below:
            INPUT: <owl:annotatedProperty rdf:resource="http://www.w3.org/2000/01/rdf-schema#subClassOf"/>
                   <owl:annotatedTarget rdf:resource="http://purl.obolibrary.org/obo/UBERON_0002238"/>
            OUTPUT: node, RDFS:subClassOf, obo.UBERON_0002238

        Args:
            node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
            edges: A dictionary, keys are owl:Objects (i.e. 'first', 'rest', 'onProperty', or 'someValuesFrom').
            class_dict: A nested dictionary. Outer keys are BNodes, inner keys are owl:ObjectProperty values.

        Returns:
            cleaned_classes: A list of tuples, where each tuple represents a class that had OWL semantics removed.
            updated_edges: A dictionary subset, where keys are owl:Objects (e.g. 'first', 'rest', 'onProperty').
        """

        if not isinstance(edges['subClassOf'], BNode):
            cleaned_classes: Set = {(node, RDFS.subClassOf, edges['subClassOf'])}
            updated_edges = {k: v for k, v in edges.items() if k != 'subClassOf'}
        else:
            cleaned_classes = set()
            updated_edges = {**class_dict[edges['subClassOf']], **{k: v for k, v in edges.items() if k != 'subClassOf'}}

        return cleaned_classes, updated_edges

    def parses_constructors(self, node: URIRef, edges: Dict, class_dict: Dict, relation: URIRef = None) \
            -> Tuple[Set, Optional[Dict]]:
        """Traverses a dictionary of rdflib objects used in the owl:unionOf or owl:intersectionOf constructors, from
        which the original set of edges used to the construct the class_node are edited, such that all owl-encoded
        information is removed. For example:
            INPUT: <!-- http://purl.obolibrary.org/obo/CL_0000995 -->
                        <owl:Class rdf:about="http://purl.obolibrary.org/obo/CL_0000995">
                            <owl:equivalentClass>
                                <owl:Class>
                                    <owl:unionOf rdf:parseType="Collection">
                                        <rdf:Description rdf:about="http://purl.obolibrary.org/obo/CL_0001021"/>
                                        <rdf:Description rdf:about="http://purl.obolibrary.org/obo/CL_0001026"/>
                                    </owl:unionOf>
                                </owl:Class>
                            </owl:equivalentClass>
                            <rdfs:subClassOf rdf:resource="http://purl.obolibrary.org/obo/CL_0001060"/>
                        </owl:Class>
            OUTPUT: [(CL_0000995, rdfs:subClassOf, CL_0001021), (CL_0000995, rdfs:subClassOf, CL_0001026)]

        Args:
            node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
            edges: A dictionary, keys are owl:Objects (i.e. 'first', 'rest', 'onProperty', or 'someValuesFrom').
            class_dict: A nested dictionary. Outer keys are BNodes, inner keys are owl:ObjectProperty values.
            relation: An RDFLib URIRef object containing an owl:onProperty (defaults=None).

        Returns:
            cleaned: A list of tuples, where each tuple represents a class that had OWL semantics removed.
            batch: A dictionary subset, where keys are owl:Objects (e.g. 'first', 'rest', 'onProperty').
        """

        cleaned: Set = set()
        if 'unionOf' in edges.keys() or 'intersectionOf' in edges.keys():
            batch = class_dict[edges['unionOf' if 'unionOf' in edges.keys() else 'intersectionOf']]
        else: batch = edges

        while batch:
            if ('first' in batch.keys() and 'rest' in batch.keys()) and 'type' not in batch.keys():
                if isinstance(batch['first'], URIRef) and isinstance(batch['rest'], BNode):
                    obj_property = self.returns_object_property(node, batch['first'], relation)
                    if node != batch['first']:
                        cleaned |= {(node, obj_property, batch['first'])}
                        batch = class_dict[batch['rest']] if 'rest' in batch.keys() else None
                    else: batch = class_dict[batch['rest']]
                elif isinstance(batch['first'], URIRef) and isinstance(batch['rest'], URIRef):
                    obj_property = self.returns_object_property(node, batch['first'], relation)
                    cleaned |= {(node, obj_property, batch['first'])}; batch = None
                else: batch = self.parses_anonymous_axioms(batch, class_dict)
            else: break

        return cleaned, batch

    def parses_restrictions(self, node: URIRef, edges: Dict, class_dict: Dict) -> Optional[Tuple]:
        """Parses a subset of a dictionary containing rdflib objects participating in a restriction and reconstructs the
        class (referenced by node) in order to remove owl-encoded information. An example is shown below:
            INPUT:    <!-- http://purl.obolibrary.org/obo/GO_0000785 -->
                        <owl:Class rdf:about="http://purl.obolibrary.org/obo/GO_0000785">
                            <rdfs:subClassOf rdf:resource="http://purl.obolibrary.org/obo/GO_0110165"/>
                            <rdfs:subClassOf>
                                <owl:Restriction>
                                    <owl:onProperty rdf:resource="http://purl.obolibrary.org/obo/BFO_0000050"/>
                                    <owl:someValuesFrom rdf:resource="http://purl.obolibrary.org/obo/GO_0005694"/>
                                </owl:Restriction>
                            </rdfs:subClassOf>
                        </owl:Class>
            OUTPUT: [(GO_0000785, BFO_0000050, GO_0005694)]

        Assumptions: If the restriction contains a cardinality constraint, it is not processed at this time.
        Source: https://www.cs.vu.nl/~guus/public/owl-restrictions/

        Args:
            node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
            edges: A dictionary, keys are owl:Objects (i.e. 'first', 'rest', 'onProperty', or 'someValuesFrom').
            class_dict: A nested dictionary. Outer keys are BNodes, inner keys are owl:ObjectProperty values.

        Returns:
            cleaned: A list of tuples, where each tuple represents a class that had OWL semantics removed.
            batch/axioms: A dictionary subset, where keys are owl:Objects (e.g. 'first', 'rest', 'onProperty').
        """

        prop_types = ['allValuesFrom', 'someValuesFrom', 'hasSelf', 'hasValue', 'onClass']  # can be extended
        restriction_components = ['type', 'first', 'rest', 'onProperty']
        object_type = [x for x in edges.keys() if x not in restriction_components and x in prop_types]
        if len(object_type) == 0: return None
        else: object_type = object_type[0]; batch = edges; cleaned: Set = set()

        if isinstance(batch[object_type], URIRef) or isinstance(batch[object_type], Literal):
            object_node = node if object_type == 'hasSelf' else batch[object_type]
            if len(batch) == 3:
                cleaned |= {(node, batch['onProperty'], object_node)}
                return cleaned, None
            else:
                cleaned |= {(node, batch['onProperty'], object_node)}
                return cleaned, self.parses_anonymous_axioms(batch, class_dict)
        else:
            axioms = class_dict[batch[object_type]]
            if 'unionOf' in axioms.keys() or 'intersectionOf' in axioms.keys():
                results = self.parses_constructors(node, axioms, class_dict, batch['onProperty']); cleaned |= results[0]
                return cleaned, results[1]
            else: return cleaned, axioms

    def cleans_owl_encoded_entities(self, node_list: List, verbose: bool = True) -> None:
        """Loops over a all owl:Class and owl: Axiom objects and decodes the OWL semantics returning the corresponding
        triples for each type without OWL semantics.

        Args:
            node_list: A list of owl:Class and owl:Axiom entities to decode.
            verbose: A bool indicating whether or not to print/log progress.

        Returns:
             None.
        """

        if verbose: s = 'Decoding {} OWL Classes and Axioms'.format(len(node_list)); logger.info(s); print(s)

        decoded_graph: Graph = Graph(); cleaned_entities: Set = set()  # ; pbar = tqdm(total=len(node_list))
        while node_list:
            # pbar.update(1)
            node = node_list.pop(0); node_info = self.creates_edge_dictionary(node)
            if node_info is not None and len(node_info[1]) != 0:
                self.captures_cardinality_axioms(node_info[2], node)
                neg = True if self.detects_negation_axioms(node_info[1], node) is True else False
                comp = True if self.detects_complement_of_constructed_classes(node_info[1], node) is True else False
                if not neg and not comp:
                    node, org = (node_info[0], node) if isinstance(node, BNode) else (node, node)
                    cleaned_entities |= {org}; cleaned_classes: Set = set()
                    bnodes = set(x for x in self.graph.objects(org) if isinstance(x, BNode))
                    for element in (bnodes if len(bnodes) > 0 else node_info[1].keys()):
                        edges = node_info[1][element]
                        while edges:
                            if 'subClassOf' in edges.keys():
                                results: Optional[Tuple] = self.parses_subclasses(node, edges, node_info[1])
                                if results is not None: cleaned_classes |= results[0]; edges = results[1]
                                else: edges = None
                            elif 'intersectionOf' in edges.keys() or 'unionOf' in edges.keys():
                                results = self.parses_constructors(node, edges, node_info[1])
                                if results is not None: cleaned_classes |= results[0]; edges = results[1]
                                else: edges = None
                            elif 'type' in edges.keys() and 'Restriction' in edges['type']:
                                results = self.parses_restrictions(node, edges, node_info[1])
                                if results is not None: cleaned_classes |= results[0]; edges = results[1]
                                else: edges = None
                            else:  # catch all other axioms -- only catching owl:onProperty
                                misc = [x for x in edges.keys() if x not in ['type', 'first', 'rest', 'onProperty']]
                                edges = None; self.owl_nets_dict['misc'][n3(node)] = {tuple(misc)}
                    decoded_graph = adds_edges_to_graph(decoded_graph, list(cleaned_classes), False)
                    self.owl_nets_dict['decoded_entities'][n3(node)] = cleaned_classes
        self.graph = decoded_graph; self.graph = self.cleans_decoded_graph(verbose)  # ; pbar.close()

        return None

    def makes_graph_connected(self, graph: Graph, common_ancestor: Union[URIRef, str] = obo.BFO_0000001) -> Graph:
        """In order to prevent the filtered graph from becoming unnecessarily disconnected, all OWL-NETS nodes are
        checked to ensure that at least one of their ancestor concepts is a subclass of common_ancestor. While this is
        not the best solution long-term is the cleanest way to ensure the graph remains connected and to introduce the
        least amount of extra edges (i.e. avoids having to make every node rdfs:subClassOf BFO_0000001).

        Args:
            graph: An RDFLib Graph object.
            common_ancestor: A URIRef or str containing a URI that represents the node that should be used as the
                common ancestor when making the graph a single connected component (default=obo.BFO_0000001).

        Returns:
            graph: An RDFLib Graph object that has been updated to be connected.
        """

        logs = 'Ensuring OWL-NETS Graph Contains a Single Connected Component'; logger.info(logs); print(logs)

        if not str(common_ancestor).startswith('http'): raise ValueError('Error: common_ancestor must be a valid URL')
        else:
            log_str = 'Obtaining node list'; print(log_str); logger.info(log_str)
            anc_node, roots = common_ancestor if isinstance(common_ancestor, URIRef) else URIRef(common_ancestor), set()
            nodes = set([x for x in tqdm(list(graph.subjects()) + list(graph.objects())) if isinstance(x, URIRef)])

            print('Identifying root nodes')
            for x in tqdm(nodes):
                ancs = gets_entity_ancestors(graph, [x], RDFS.subClassOf)
                if len(ancs) == 0:
                    nbhd = set(graph.objects(x))
                    ancs = [x for y in [gets_entity_ancestors(graph, [i], RDFS.subClassOf) for i in nbhd] for x in y]
                    if len(ancs) == 0: ancs = [x]
                    else:
                        try: ancs = [mode(ancs)]
                        except StatisticsError: ancs = sample(ancs, 1) if not any(x for x in ancs if x in roots) else []
                roots |= {ancs[0]} if len(ancs) > 0 else {x}

            log_str = 'Updating graph connectivity'; print(log_str); logger.info(log_str)
            rel = RDF.type if self.kg_construct_approach == 'instance' else RDFS.subClassOf
            needed_triples = set((URIRef(x), rel, anc_node) for x in roots if x != anc_node)
            graph = adds_edges_to_graph(graph, needed_triples, False)

            logs = '{} triples added to make connected'.format(len(needed_triples)); logger.info(logs); print(logs)

            return graph

    def purifies_graph_build(self, graph: Graph) -> Graph:
        """Makes graph consistent with the kg_construction approach (i.e. subclass or instance). If kg_construction is
        subclass, all triples where the subject and object are connected by RDF.type are updated to RDFS.subClassOf
        and the subjects of these triples are made RDFS.subClassOf all ancestors of the objects. If kg_construction is
        instance, all triples where the subject and object are connected by RDFS.subClassOf are updated to RDF.type and
        the subjects of these triples are made RDF.type all ancestors of the objects. Examples are provided below.

        Returns:
             graph: An RDFLib object that has been purified to the kg_construction approach.
        """

        log_str = 'Purifying Graph Based on Construction Approach'; logger.info(log_str); print(log_str)

        org_rel = RDF.type if self.kg_construct_approach == 'subclass' else RDFS.subClassOf
        pure_rel = RDFS.subClassOf if org_rel == RDF.type else RDF.type

        log_str = 'Determining what triples need purification'; print(log_str); logger.info(log_str)
        triples = list(graph.triples((None, org_rel, None)))

        log_str = 'Processing {} {} triples'.format(len(triples), org_rel); print(log_str); logger.info(log_str)
        for edge in tqdm(triples):
            graph.add((edge[0], pure_rel, edge[2])); graph.remove(edge)
            o_ancs = gets_entity_ancestors(graph, [edge[2]], RDFS.subClassOf, [edge[2]])
            ancs_filter = tuple([x for x in o_ancs if x.startswith('http') and URIRef(x) != edge[2]])
            for node in ancs_filter: graph.add((edge[0], pure_rel, URIRef(node)))

        return graph

    def write_out_results(self, graph: Union[Set, Graph], kg_const: Optional[str] = None) -> None:
        """Serializes graph and prints out basic statistics.

        Args:
            graph: An RDF Graph lib object or a set of RDFLib triples.
            kg_const: A string specifying the type of knowledge graph construction to implement.

        NOTE. It is important to check the number of unique nodes and relations in OWL-NETS and to compare the counts
        with and without the URIs (i.e. http://purl.obolibrary.org/obo/HP_0000000 vs HP_0000000). Doing this provides a
        nice sanity check and can help identify duplicate nodes (i.e. nodes with the same identifier, but different
        URIs -- where the URIs should be the same).

        Return:
             None.
        """

        personalize = '' if kg_const is None else kg_const.title() + '-Purified '
        log_str = 'Serializing {}OWL-NETS Graph'.format(personalize); logger.info(log_str); print(log_str)

        f_name_lab = '_OWLNETS_' + kg_const.upper() + '_purified' if kg_const else '_OWLNETS'
        f_name = [self.filename[:-4] + f_name_lab if '.owl' in self.filename
                  else '.'.join(self.filename.split('.')[:-1]) + f_name_lab if '.' in self.filename
                  else self.filename + f_name_lab][0]
        f_name = '/' + f_name + '.nt' if not f_name.startswith('/') else f_name + '.nt'
        # write graph to n-triples file
        if isinstance(graph, Graph): graph.serialize(destination=self.write_location + f_name, format='nt')
        else: appends_to_existing_file(graph, self.write_location + f_name)
        # write out owl_nets dictionary
        with open(self.write_location + f_name.strip('.nt') + '_decoding_dict.pkl', 'wb') as out:
            pickle.dump(self.owl_nets_dict, out)
        s = convert_to_networkx(self.write_location, f_name.strip('.nt'), graph, True)
        if s is not None: log_stats = '{}OWL-NETS {}'.format(personalize, s); logger.info(log_stats); print(log_stats)

        return None

    def runs_owlnets(self, cpus: int = 1) -> Tuple:
        """Method facilitates the parallel processing of OWL-NETS over a list of n RDFLib Graph objects.

        Args:
            cpus: An integer representing the number of workers (default=1).

        Return:
            graph 1: A set of rdflib.Graph object triples.
            graph 2: A set of rdflib.Graph object triples purified according to the kg_construct_approach.
        """

        log_str = '*** Running OWL-NETS ***'; print('\n' + log_str); logger.info(log_str)

        full_graph = Graph(); res2 = []
        loc, f, cons, ot = self.write_location, self.filename, self.kg_construct_approach, self.owl_tools
        for g in tqdm(self.graph_list):
            self.graph = g; self.removes_disjoint_with_axioms()
            full_graph = adds_edges_to_graph(full_graph, self.removes_edges_with_owl_semantics(), False)
            owl_classes = list(gets_ontology_classes(self.graph)); owl_axioms = []
            for x in set(self.graph.subjects(RDF.type, OWL.Axiom)):
                src = set(self.graph.objects(list(self.graph.objects(x, OWL.annotatedSource))[0], RDF.type))
                tgt = set(self.graph.objects(list(self.graph.objects(x, OWL.annotatedTarget))[0], RDF.type))
                if OWL.Class in src and OWL.Class in tgt: owl_axioms += [x]
                elif (OWL.Class in src and len(tgt) == 0) or (OWL.Class in tgt and len(src) == 0): owl_axioms += [x]
                else: pass
            ents_to_decode = list(set(owl_classes) | set(owl_axioms)); shuffle(ents_to_decode)
            if len(ents_to_decode) > 0:
                entities = [ents_to_decode[i::cpus] for i in range(cpus)]
                try: ray.init()
                except RuntimeError: pass
                acts = [ray.remote(OwlNets).remote(self.graph, loc, f, cons, ot) for _ in range(cpus)]  # type: ignore
                for i in range(0, cpus): acts[i % cpus].cleans_owl_encoded_entities.remote(entities[i])  # type: ignore
                _ = ray.wait([x.gets_owlnets_graph.remote() for x in acts], num_returns=len(acts))
                graph_res = ray.get([x.gets_owlnets_graph.remote() for x in acts])  # type: ignore
                full_graph = adds_edges_to_graph(full_graph, set(x for y in set(graph_res) for x in y), False)
                res2 += ray.get([x.gets_owlnets_dict.remote() for x in acts]); del acts  # type: ignore
        conn_graph = self.makes_graph_connected(full_graph); graph1 = set(conn_graph).copy(); graph2 = None
        g1 = derives_graph_statistics(graph1); g2 = 'None'; self.write_out_results(graph1)
        if self.kg_construct_approach is not None:
            graph2 = set(self.purifies_graph_build(conn_graph)); g2 = derives_graph_statistics(graph2)
            self.write_out_results(graph2, self.kg_construct_approach)
        stats = '\n\nOWL-NETS {};\nPurified OWL-NETS {}'.format(g1, g2); print(stats); logger.info(stats)

        # process owl decoding results
        for k in self.owl_nets_dict.keys():
            if not isinstance(self.owl_nets_dict[k], Set):
                self.owl_nets_dict[k].update(dict(ChainMap(*[d[k] for d in res2])))
            else: self.owl_nets_dict[k] = self.owl_nets_dict[k] | set(ChainMap(*[d[k] for d in res2]))
        str1 = 'Decoded {} owl-encoded classes and axioms. Note the following:\nPartially processed {} cardinality ' \
               'elements\nRemoved {} owl:disjointWith axioms\nIgnored: {} misc classes; {} classes constructed with ' \
               'owl:complementOf; {} classes containing negation (e.g. pr#lacks_part, cl#has_not_completed)\n' \
               'Filtering removed {} semantic support triples'
        stats_str = str1.format(
            len(self.owl_nets_dict['decoded_entities'].keys()), len(self.owl_nets_dict['cardinality'].keys()),
            len(self.owl_nets_dict['disjointWith']), len(self.owl_nets_dict['misc'].keys()),
            len(self.owl_nets_dict['complementOf'].keys()), len(self.owl_nets_dict['negation'].keys()),
            len(self.owl_nets_dict['filtered_triples']))
        print('=' * 155 + '\n' + stats_str + '\n' + '=' * 155); logger.info(stats_str)

        return graph1, graph2
