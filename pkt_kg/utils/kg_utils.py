#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Knowledge Graph Utility Functions.

Interacts with OWL Tools API
* gets_ontology_classes
* gets_ontology_statistics
* gets_object_properties
* gets_ontology_class_dbxrefs
* gets_ontology_class_synonyms
* gets_ontology_definitions
* merges_ontologies
* ontology_file_formatter
* adds_annotation_assertions

Interacts with Knowledge Graphs
* adds_edges_to_graph
* remove_edges_from_graph
* updates_graph_namespace
* gets_entity_ancestors
* connected_components
* removes_self_loops
* derives_graph_statistics
* adds_namespace_to_bnodes
* removes_namespace_from_bnodes
* updates_pkt_namespace_identifiers
* splits_knowledge_graph

Writes Triple Lists
* maps_ids_to_integers
* n3
* appends_to_existing_file

File Type Conversion
* convert_to_networkx
"""

# import needed libraries
import glob
import hashlib
import json
import networkx as nx  # type: ignore
import os
import os.path
import random

from collections import Counter  # type: ignore
from more_itertools import unique_everseen  # type: ignore
from rdflib import BNode, Graph, Literal, Namespace, URIRef  # type: ignore
from rdflib.namespace import OWL, RDF, RDFS  # type: ignore
from rdflib.plugins.serializers.nt import _quoteLiteral  # type: ignore
import subprocess

from tqdm import tqdm  # type: ignore
from typing import Dict, List, Optional, Set, Tuple, Union

# set-up environment variables
obo = Namespace('http://purl.obolibrary.org/obo/')
oboinowl = Namespace('http://www.geneontology.org/formats/oboInOwl#')
pkt = Namespace('https://github.com/callahantiff/PheKnowLator/pkt/')
pkt_bnode = Namespace('https://github.com/callahantiff/PheKnowLator/pkt/bnode/')
schema = Namespace('http://www.w3.org/2001/XMLSchema#')


def gets_ontology_classes(graph: Graph) -> Set:
    """Queries a knowledge graph and returns a list of all owl:Class objects (excluding BNodes) in the graph.

    Args:
        graph: An rdflib Graph object.

    Returns:
        class_list: A list of all of the classes in the graph.

    Raises:
        ValueError: If the query returns zero nodes with type owl:ObjectProperty.
    """

    class_list = {x for x in graph.subjects(RDF.type, OWL.Class) if isinstance(x, URIRef)}

    return class_list


def gets_ontology_definitions(graph: Graph) -> Dict:
    """Queries a knowledge graph and returns a list of all object definitions (obo:IAO_0000115) in the graph.

    Args:
        graph: An rdflib Graph object.

    Returns:
        obj_defs: A dictionary where keys are object URiRefs and values are Literal object definitions. For example:
                    {rdflib.term.URIRef('http://purl.obolibrary.org/obo/OBI_0001648'):
                     rdflib.term.Literal('A B cell epitope qualitative binding to antibody assay that uses an antibody
                     cross-blocking assay.', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#string')),
                     ...}
    """

    obj_defs = {x[0]: x[2] for x in graph.triples((None, obo.IAO_0000115, None))}

    return obj_defs


def gets_deprecated_ontology_classes(graph: Graph) -> Set:
    """Queries a knowledge graph and returns a list of all deprecated owl:Class objects in the graph.

    Args:
        graph: An rdflib Graph object.

    Returns:
        class_list: A list of all of the deprecated OWL classes in the graph.
    """

    class_list = {x for x in graph.subjects(OWL.deprecated, Literal('true', datatype=URIRef(schema + 'boolean')))}

    return class_list


def gets_object_properties(graph: Graph) -> Set:
    """Queries a knowledge graph and returns a list of all owl:ObjectProperty objects in the graph.

    Args:
        graph: An rdflib Graph object.

    Returns:
        object_property_list: A list of all of the object properties in the graph.

    Raises:
        ValueError: If the query returns zero nodes with type owl:ObjectProperty.
    """

    object_property_list = {x for x in graph.subjects(RDF.type, OWL.ObjectProperty) if isinstance(x, URIRef)}

    return object_property_list


def gets_ontology_class_synonyms(graph: Graph) -> Tuple:
    """Queries a knowledge graph and returns a tuple of dictionaries. The first dictionary contains all owl:Class
    objects and their synonyms in the graph. The second dictionary contains the synonyms and their OWL synonym types.

    Args:
        graph: An rdflib Graph object.

    Returns:
        A tuple of dictionaries:
            synonyms: A dictionary where keys are string synonyms and values are ontology URIs. For example:
                    {'modified l selenocysteine': 'http://purl.obolibrary.org/obo/SO_0001402',
                    'frameshift truncation': 'http://purl.obolibrary.org/obo/SO_0001910', ...}
            synonym_type: A dictionary where keys are synonyms and values are OWL synonym types. for example:
                    {'susceptibility to herpesvirus': 'hasExactSynonym', 'full upper lip': 'hasExactSynonym'}
    """

    class_list = [x for x in graph if 'synonym' in str(x[1]).lower() and isinstance(x[0], URIRef)]
    synonyms = {str(x[2]).lower(): str(x[0]) for x in class_list}
    synonym_type = {str(x[2]).lower(): str(x[1]).split('#')[-1] for x in class_list}

    return synonyms, synonym_type


def gets_ontology_class_dbxrefs(graph: Graph):
    """Queries a knowledge graph and returns a dictionary containing all owl:Class objects and their database
    cross references (dbxref). Function also includes exact matches. A tuple of dictionaries: (1) contains dbxref and
    exact matches (URIs and labels); and (2) contains dbxref/exactmatch uris and a string indicating the type (i.e.
    dbxref or exact match).

    Assumption: That none of the hasdbxref ids overlap with any of the exactmatch ids.

    Args:
        graph: An rdflib Graph object.

    Returns:
        dbxref: A dictionary where keys are dbxref strings and values are ontology URIs.
        dbxref_type: A dict where keys are dbxref/exact uris; values are str indicating if the uri is dbxref or exact.
    """

    dbx = [x for x in graph if 'hasdbxref' in str(x[1]).lower() if isinstance(x[0], URIRef)]
    dbx_uris, dbx_type = {str(x[2]).lower(): str(x[0]) for x in dbx}, {str(x[2]).lower(): 'DbXref' for x in dbx}
    ex = [x for x in graph if 'exactmatch' in str(x[1]).lower() if isinstance([0], URIRef)]
    ex_uris, ex_type = {str(x[2]).lower(): str(x[0]) for x in ex}, {str(x[2]).lower(): 'ExactMatch' for x in ex}

    return {**dbx_uris, **ex_uris}, {**dbx_type, **ex_type}


def gets_ontology_statistics(file_location: str, owltools_location: str = './pkt_kg/libs/owltools') -> str:
    """Uses the OWL Tools API to generate summary statistics (i.e. counts of axioms, classes, object properties, and
    individuals).

    Args:
        file_location: A string that contains the file path and name of an ontology.
        owltools_location: A string pointing to the location of the owl tools library.

    Returns:
        stats: A formatted string containing descriptive statistics.

    Raises:
        TypeError: If the file_location is not type str.
        OSError: If file_location points to a non-existent file.
        ValueError: If file_location points to an empty file.
    """

    if not isinstance(file_location, str): raise TypeError('file_location must be a string')
    elif not os.path.exists(file_location): raise OSError('{} does not exist!'.format(file_location))
    elif os.stat(file_location).st_size == 0: raise ValueError('{} is empty'.format(file_location))
    else: output = subprocess.check_output([os.path.abspath(owltools_location), file_location, '--info'])
    res = output.decode('utf-8').split('\n')[-5:]
    cls, axs, op, ind = res[0].split(':')[-1], res[3].split(':')[-1], res[2].split(':')[-1], res[1].split(':')[-1]
    sent = 'The knowledge graph contains {0} classes, {1} axioms, {2} object properties, and {3} individuals'
    stats = sent.format(cls, axs, op, ind)

    return stats


def merges_ontologies(onts: List[str], loc: str, merged: str,
                      owltools: str = os.path.abspath('./pkt_kg/libs/owltools')) -> Graph:
    """Using the OWLTools API, each ontology listed in in the ontologies attribute is recursively merged with into a
    master merged ontology file and saved locally to the provided file path via the merged_ontology attribute. The
    function assumes that the file is written to the directory specified by the write_location attribute.

    Args:
        onts: A list of ontology file paths.
        loc: A string pointing to a local directory for writing data.
        merged: A string pointing to the location of the merged ontology file.
        owltools: A string pointing to the location of the owl tools library.

    Returns:
        None.
    """

    if not onts: return None
    else:
        if loc + merged in glob.glob(loc + '/*.owl'): o1, o2 = onts.pop(), loc + merged
        else: o1, o2 = onts.pop(), onts.pop()
        try:
            print('Merging Ontologies: {ont1}, {ont2}'.format(ont1=o1.split('/')[-1], ont2=o2.split('/')[-1]))
            subprocess.check_call([owltools, str(o1), str(o2), '--merge-support-ontologies', '-o', loc + merged])
        except subprocess.CalledProcessError as error: print(error.output)

        return merges_ontologies(onts, loc, merged, owltools)


def ontology_file_formatter(loc: str, full_kg: str, owltools: str = os.path.abspath('./pkt_kg/libs/owltools')) -> None:
    """Reformat an .owl file to be consistent with the formatting used by the OWL API. To do this, an ontology
    referenced by graph_location is read in and output to the same location via the OWLTools API.

    Args:
        loc: A string pointing to a local directory for writing data.
        full_kg: A string containing the subdirectory and name of the the knowledge graph file.
        owltools: A string pointing to the location of the owl tools library.

    Returns:
        None.

    Raises:
        TypeError: If something other than an .owl file is passed to function.
        IOError: If the graph_location file is empty.
        TypeError: If the input file contains no data.
    """

    print('Applying OWL API Formatting to Knowledge Graph OWL File')

    graph_write_location = loc + full_kg
    if not os.path.exists(graph_write_location): raise IOError('{} does not exist!'.format(graph_write_location))
    elif os.stat(graph_write_location).st_size == 0: raise TypeError('{} is empty'.format(graph_write_location))
    else:
        try: subprocess.check_call([owltools, graph_write_location, '-o', graph_write_location])
        except subprocess.CalledProcessError as error: print(error.output)

    return None


def adds_edges_to_graph(graph: Graph, edge_list: Union[List, Set], progress_bar: bool = True) -> Graph:
    """Takes a set or list of tuples representing new triples and adds them to a knowledge graph.

    Args:
        graph: An RDFLib Graph object.
        edge_list: A list or set of tuples, where each tuple contains a triple.
        progress_bar: A boolean indicating whether or not the progress bar should be used.

    Returns:
        graph: An updated RDFLib graph.
    """

    edge_list_updated = set(edge_list) if isinstance(edge_list, List) else edge_list
    edge_set = tqdm(edge_list_updated) if progress_bar else edge_list_updated
    for edge in edge_set: graph.add(edge)

    return graph


def remove_edges_from_graph(graph: Graph, edge_list: Union[List, Set]) -> Graph:
    """Takes a tuple of tuples and removes them from a knowledge graph.

    Args:
        graph: An RDFLib Graph object.
        edge_list: A list or set of tuples, where each tuple contains a triple.

    Returns:
        graph: An updated RDFLib graph.
    """

    edge_list_updated = set(edge_list) if isinstance(edge_list, List) else edge_list
    for edge in edge_list_updated:
        graph.remove(edge)

    return graph


def updates_graph_namespace(entity_namespace: str, graph: Graph, node: str) -> Graph:
    """Adds a triple to a graph specifying a node's namespace. This is only used for non-ontology entities.

    Args:
        entity_namespace: A string containing an entity namespace (i.e. "pathway", "gene").
        graph: An RDFLib Graph object.
        node: A string containing the URI for a node in the graph.

    Returns:
        graph: An RDFLib Graph object.
    """

    graph.add((URIRef(node), URIRef(oboinowl + 'hasOBONamespace'), Literal(entity_namespace)))

    return graph


def finds_node_type(edge_info: Dict) -> Dict:
    """Takes a dictionary of edge information and parses the data type for each node in the edge. Returns either None or
    a string containing a particular node from the edge.

    Args:
        edge_info: A dict of information needed to add edge to graph, for example:
            {'n1': 'subclass', 'n2': 'class','relation': 'RO_0003302',
            'url': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
            'edges': ['2', 'DOID_0110035']}

    Returns:
        A dictionary with 4 keys representing node type (i.e. "cls1", "cls2", "ent1", and "ent2") and values are
        strings containing a concatenation of the uri and the node. An example of a class-class edge is shown below:
            {'cls1': 'http://purl.obolibrary.org/obo/CHEBI_81395', 'cls2': 'http://purl.obolibrary.org/obo/DOID_12858',
            'ent1': None, 'ent2': None}
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


def gets_entity_ancestors(graph: Graph, uris: List[Union[URIRef, str]], rel: Union[URIRef, str] = RDFS.subClassOf,
                          cls_lst: Optional[List] = None) -> List:
    """A method that recursively searches an ontology hierarchy to pull all ancestor concepts for an input entity.

    Args:
        graph: An RDFLib graph object assumed to contain ontology data.
        uris: A list of at least one ontology RDFLib URIRef object or string.
        rel: A string or RDFLib URI object containing a predicate.
        cls_lst: A list of URIs representing the ancestor classes found for the input class_uris.

    Returns:
        An ordered (desc; root to leaf) list of ontology objects containing the input uris ancestor hierarchy. Example:
            input: [URIRef('http://purl.obolibrary.org/NCBITaxon_11157')]
            output: ['http://purl.obolibrary.org/NCBITaxon_10239', 'http://purl.obolibrary.org/NCBITaxon_2559587',
                'http://purl.obolibrary.org/NCBITaxon_2497569', 'http://purl.obolibrary.org/NCBITaxon_11157']
    """

    prop = rel if isinstance(rel, URIRef) else URIRef(rel); cls_lst = [] if cls_lst is None else cls_lst
    cls_lst = list(unique_everseen([x if isinstance(x, URIRef) else URIRef(obo + x) for x in cls_lst]))
    uris = list(unique_everseen([x if isinstance(x, URIRef) else URIRef(obo + x) for x in uris]))
    ancs = list(unique_everseen([j for k in [graph.objects(x, prop) for x in uris] for j in k]))
    if len(ancs) == 0 or len(set(ancs).difference(set(cls_lst))) == 0:
        return list(unique_everseen([str(x) for x in cls_lst]))
    else:
        uris = [x for x in ancs if x not in cls_lst]; cls_lst.insert(0, [x for x in uris][0])
        return gets_entity_ancestors(graph, uris, prop, cls_lst)


def connected_components(graph: Union[Graph, Set]) -> List:
    """Creates a dictionary where the keys are integers representing a component number and the values are sets
    containing the nodes for a given component. This method works by first converting the RDFLib graph into a
    NetworkX multi-directed graph, which is converted to a undirected graph prior to calculating the connected
    components.

    Args:
        graph: An RDFLib Graph object.

    Returns:
        components: A list of the nodes in each component detected in the graph.
    """

    nx_mdg = nx.MultiDiGraph()
    for s, p, o in tqdm(graph): nx_mdg.add_edge(s, o, **{'key': p})
    print('Calculating Connected Components')
    components = list(nx.connected_components(nx_mdg.to_undirected()))

    return components


def removes_self_loops(graph: Graph) -> List:
    """Method iterates over a graph and identifies all triples that contain self-loops. The method returns a list of
    all self-loops.

    Args:
        graph: An RDFLib Graph object.

    Returns:
        self_loops: A list of triples containing self-loops that need to be removed.
    """

    self_loops: Set = set()
    for edge in tqdm(graph):
        if edge[0] == edge[2]: self_loops.add(edge)

    return list(self_loops)


def derives_graph_statistics(graph: Union[Graph, Set, nx.MultiDiGraph]) -> str:
    """Derives statistics from an input knowledge graph and prints them to the console. Note that we are not
    converting each node to a string before deriving our counts. This is purposeful as the number of unique nodes is
    altered when you it converted to a string. For example, in the HPO when honoring the RDF type of each node
    there are 406,717 unique nodes versus 406,331 unique nodes when ignoring the RDF type of each node.

    Args:
        graph: An RDFLib graph object or a networkx.MultiDiGraph.

    Returns:
        stats: A formatted string containing descriptive statistics.
    """

    if isinstance(graph, Graph):
        triples = len(graph); nodes = len(set(list(graph.subjects()) + list(graph.objects())))
        rels = set(list(graph.predicates())); cls = set([x for x in graph.subjects(RDF.type, OWL.Class)])
        inds = set([x for x in graph.subjects(RDF.type, OWL.NamedIndividual)])
        obj_prop = set([x for x in graph.subjects(RDF.type, OWL.ObjectProperty)])
        ant_prop = set([x for x in graph.subjects(RDF.type, OWL.AnnotationProperty)])
        x = ' {} triples, {} nodes, {} predicates, {} classes, {} individuals, {} object props, {} annotation props'
        stat = 'Graph Stats:' + x.format(triples, nodes, len(rels), len(cls), len(inds), len(obj_prop), len(ant_prop))
    elif isinstance(graph, Set):
        triples = len(graph); nodes = len(set(i for j in [[s, o] for s, p, o in graph] for i in j))
        rels = set([p for s, p, o in graph])
        cls = set(s for s, p, o in graph if p == RDF.type and o == OWL.Class)
        inds = set(s for s, p, o in graph if p == RDF.type and o == OWL.NamedIndividual)
        obj_prop = set(s for s, p, o in graph if p == RDF.type and o == OWL.ObjectProperty)
        ant_prop = set(s for s, p, o in graph if p == RDF.type and o == OWL.AnnotationProperty)
        x = ' {} triples, {} nodes, {} predicates, {} classes, {} individuals, {} object props, {} annotation props'
        stat = 'Graph Stats:' + x.format(triples, nodes, len(rels), len(cls), len(inds), len(obj_prop), len(ant_prop))
    else:
        nx_graph_und = graph.to_undirected()
        nodes = nx.number_of_nodes(graph); edges = nx.number_of_edges(graph); self_loops = nx.number_of_selfloops(graph)
        conn = Counter([str(x[2]) for x in graph.edges(keys=True)])  # type: ignore
        ce = sorted(conn.items(), key=lambda x: x[1], reverse=1)[:6]  # type: ignore
        dens = nx.density(graph); avg_deg = float(edges) / nodes
        n_deg = sorted([(str(x[0]), x[1]) for x in graph.degree], key=lambda x: x[1], reverse=1)[:6]  # type: ignore
        c = sorted(list(nx.connected_components(nx_graph_und)), key=len, reverse=True)
        cc = {x: str(len(c[x])) + ' nodes: ' + ' | '.join(c[x]) if len(c[x]) < 50 else len(c[x]) for x in range(len(c))}
        x = '{} nodes, {} edges, {} self-loops, 5 most most common edges: {}, average degree {}, 5 highest degree '\
            'nodes: {}, density: {}, {} component(s): {}'
        stat = 'Graph Stats: ' + x.format(nodes, edges, self_loops, ', '.join([x[0] + ':' + str(x[1]) for x in ce]),
                                          avg_deg, ', '.join([x[0] + ':' + str(x[1]) for x in n_deg]), dens, len(c), cc)

    return stat


def adds_namespace_to_bnodes(graph: Graph, ns: Union[str, Namespace] = pkt_bnode) -> Graph:
    """Method adds a namespace to all anonymous (RDFLib Term type BNode).

    Args:
        graph: An RDFLib Graph object.
        ns: A string or RDFLib Namespace object (default='https://github.com/callahantiff/PheKnowLator/pkt/bnode/')

    Returns:
         updated_graph: An RDFLib Graph object with updated BNodes.
    """

    print('Adding Namespace to BNodes')
    # print('Processing Original Nodes')
    all_triples = set(graph)
    sub_only_bnodes_org = {x for x in graph if isinstance(x[0], BNode) and not isinstance(x[2], BNode)}
    obj_only_bnodes_org = {x for x in graph if isinstance(x[2], BNode) and not isinstance(x[0], BNode)}
    sub_and_obj_bnodes_org = {x for x in graph if isinstance(x[0], BNode) and isinstance(x[2], BNode)}
    graph_no_bnodes = all_triples - (sub_only_bnodes_org | obj_only_bnodes_org | sub_and_obj_bnodes_org)
    del all_triples, graph
    # print('Converting BNodes to Namespaced-URIs')
    ns_uri = ns if isinstance(ns, Namespace) else Namespace(ns)
    sub_fixed = {(URIRef(ns_uri + str(x[0])), x[1], x[2]) for x in sub_only_bnodes_org}
    obj_fixed = {(x[0], x[1], URIRef(ns_uri + str(x[2]))) for x in obj_only_bnodes_org}
    both_fixed = {(URIRef(ns_uri + str(x[0])), x[1], URIRef(ns_uri + str(x[2]))) for x in sub_and_obj_bnodes_org}
    del sub_only_bnodes_org, obj_only_bnodes_org, sub_and_obj_bnodes_org
    # print('Finalizing Updated Graph')
    updated_graph = Graph()
    for s, p, o in (graph_no_bnodes | sub_fixed | obj_fixed | both_fixed): updated_graph.add((s, p, o))

    return updated_graph


def removes_namespace_from_bnodes(graph: Graph, ns: Union[str, Namespace] = pkt_bnode, verbose: bool = True) -> Graph:
    """Methods removes namespace from nodes originally assumed to be RDFLib BNodes. This method acts to reverse the
    pkt_kg.utils.adds_namespace_to_bnodes method.

    Args:
        graph: An RDFLib Graph object.
        ns: A string or RDFLib Namespace object (default='https://github.com/callahantiff/PheKnowLator/pkt/bnode/')
        verbose: A bool flag used to indicate whether or not to print method function (default=False).

    Returns:
        updated_graph: An RDFLib Graph object with bnode namespaces removed.
    """

    if verbose: print('Removing Namespace from BNodes'); print('Processing Original Nodes')
    ns_uri = str(ns) if isinstance(ns, Namespace) else ns
    all_triples = set(graph)
    sub_only_bnodes_ns = {(s, p, o) for s, p, o in graph if str(s).startswith(ns_uri) and not str(o).startswith(ns_uri)}
    obj_only_bnodes_ns = {(s, p, o) for s, p, o in graph if str(o).startswith(ns_uri) and not str(s).startswith(ns_uri)}
    sub_and_obj_bnodes_ns = {(s, p, o) for s, p, o in graph if str(s).startswith(ns_uri) and str(o).startswith(ns_uri)}
    graph_no_bnodes = all_triples - (sub_only_bnodes_ns | obj_only_bnodes_ns | sub_and_obj_bnodes_ns)
    del all_triples, graph
    if verbose: print('Removing Namespace from BNodes')
    sub_fixed = {(BNode(str(s).split('/')[-1]), p, o) for s, p, o in sub_only_bnodes_ns}
    obj_fixed = {(s, p, BNode(str(o).split('/')[-1])) for s, p, o in obj_only_bnodes_ns}
    both_fixed = {(BNode(str(s).split('/')[-1]), p, BNode(str(o).split('/')[-1])) for s, p, o in sub_and_obj_bnodes_ns}
    del sub_only_bnodes_ns, obj_only_bnodes_ns, sub_and_obj_bnodes_ns
    if verbose: print('Finalizing Updated Graph')
    updated_graph = Graph()
    for s, p, o in (graph_no_bnodes | sub_fixed | obj_fixed | both_fixed): updated_graph.add((s, p, o))

    return updated_graph


def updates_pkt_namespace_identifiers(graph: Union[Graph, Set], const: str, verbose: bool = True) -> Union[Graph, Set]:
    """Iterates over all entities in a pkt knowledge graph that were constructed using the instance- and
    subclass-based construction approaches and converts pkt-namespaced BNodes back to the original ontology
    class identifier. A new edge for each triple, containing an instance of a class is updated with the original
    ontology identifier, is added to the graph.

    Assumptions: (1) all instances/classes of a BNode identifier contain the pkt namespace and (2) all relations used
    when adding new edges to a graph are part of the OBO namespace.

    Args:
        graph: An RDFLib Graph object containing pkt-namespacing.
        const: A string containing the type of construction approach used to build the knowledge graph.
        verbose: A bool flag used to indicate whether or not to print method function (default=False).

    Returns:
         graph: An RDFLib Graph object or set of RDFLib triples updated to remove bnode namespacing.
    """

    if verbose: print('Post-processing pkt-kg-Namespaced Anonymous Nodes')
    if isinstance(graph, Set): graph = adds_edges_to_graph(Graph(), graph, False)

    # STEP 1: check for pkt-namespaced bnodes (original bnodes) and remove them if present
    ns_bnodes = {x for x in graph if str(x[0]).startswith(pkt_bnode) or str(x[2]).startswith(pkt_bnode)}
    if len(ns_bnodes) > 0: graph = removes_namespace_from_bnodes(graph=graph, verbose=verbose)

    # STEP 2: check for pkt-namespaced bnodes (pkt-added bnodes) and remove them if present
    pred = RDF.type if const == 'instance' else RDFS.subClassOf
    pkt_ns_dict = {x[0]: x[2] for x in list(graph.triples((None, pred, None))) if isinstance(x[2], URIRef)
                   and ((str(x[0]).startswith(str(pkt) + 'N') and 'bnode' not in str(x[0]))
                        and x[2] not in [OWL.NamedIndividual, OWL.Class])}
    if len(pkt_ns_dict) > 0:
        remove_edges: Set = set()  # update triples containing BNodes with original ontology class
        for node in pkt_ns_dict.keys():
            triples = list(graph.triples((node, None, None))) + list(graph.triples((None, None, node)))
            for edge in triples:
                sub = pkt_ns_dict[edge[0]] if edge[0] in pkt_ns_dict.keys() else edge[0]
                obj = pkt_ns_dict[edge[2]] if edge[2] in pkt_ns_dict.keys() else edge[2]
                if sub != obj: graph.add((sub, edge[1], obj))  # ensures we are not adding self-loops
            # verify that updating node doesn't introduce punning (i.e. node is not NamedIndividual and Class)
            node_types = list(graph.triples((pkt_ns_dict[node], RDF.type, None)))
            if len(node_types) > 1: triples += [tuple(x) for x in node_types if x[2] == OWL.NamedIndividual]
            remove_edges |= set(triples)
        graph = remove_edges_from_graph(graph, list(remove_edges))

    return graph


def splits_knowledge_graph(graph: Graph, graph_output: bool = False) -> Tuple[Graph, Union[Graph, Set]]:
    """Method takes an input RDFLib Graph object and splits it into two new graphs where the first graph contains
    only those triples needed to maintain a base logical subset and the second contains only annotation assertions.
    Please note that the code below processes both entities (i.e. owl:Class and owl:ObjectProperties

    Source: https://www.w3.org/TR/owl2-syntax/#Annotation_Assertion

    Args:
        graph: An RDFLib Graph object.
        graph_output: (Bool) if True, the annotation and logic graph are returned as RDFLib Graph objects, if False,
            the logic_graph is returned as an RDFLib Graph and the annotation subset is returned as a
            set of triples (default=False).

    Returns:
        logic_graph: An RDFLib Graph object containing only logical axioms.
        annotation_graph: An RDFLib Graph object or a set of RDFLib triples containing non-logical annotation
            assertions.
    """

    graph = adds_namespace_to_bnodes(graph)

    print('Creating Logic and Annotation Subsets of Graph')
    # get information needed to find annotation assertions
    annot_props = set([x for x in graph.subjects(RDF.type, OWL.AnnotationProperty) if x != RDF.type])
    core_annot_props = {OWL.annotatedSource, OWL.annotatedProperty, OWL.annotatedTarget}
    all_annot_props = annot_props | core_annot_props
    axioms = set(graph.subjects(RDF.type, OWL.Axiom))
    entities = set([x[0] for x in graph if isinstance(x[0], URIRef) and (x[1] in annot_props and x[0] not in axioms)])
    annot_triples = set()
    for ent in tqdm(axioms | entities):
        triples = set(graph.triples((ent, None, None))) | set(graph.triples((None, None, ent)))
        target = [x for x in triples if (x[0] == ent and x[1] == OWL.annotatedTarget and isinstance(x[2], URIRef))]
        source = [x for x in triples if (x[0] == ent and x[1] == OWL.annotatedSource and isinstance(x[2], URIRef))]
        if len(target) > 0 and len(source) > 0: annot_triples |= {x for x in triples if x[1] in annot_props}
        elif len(target) == 0 and len(source) == 0:
            match = [x for x in triples if x[2] == ent and (x[1] == OWL.annotatedTarget or x[1] == OWL.annotatedSource)]
            keep = [x for x in match if x not in annot_triples]
            annot_triples |= {x for x in triples if (x[1] in all_annot_props or x[2] == OWL.Axiom) and x not in keep}
        else: annot_triples |= {x for x in triples if x[1] in all_annot_props or x[2] == OWL.Axiom}
    # create graph subsets
    all_triples = set(graph); logic_triples = all_triples - annot_triples
    print('Annotation Assertions (n={} Triples)'.format(len(annot_triples)))
    if len(logic_triples) + len(annot_triples) == len(all_triples):
        print('Creating Logic Graph (n={} Triples)'.format(len(logic_triples)))
        logic_graph = adds_edges_to_graph(Graph(), logic_triples)
        if graph_output:
            print('Creating Annotation Graph (n={} Triples)'.format(len(annot_triples)))
            annotation_graph = adds_edges_to_graph(Graph(), annot_triples)
        else: annotation_graph = annot_triples
        return logic_graph, annotation_graph
    else: raise ValueError('Error: Graph Subsetting was Unsuccessful!')


def maps_ids_to_integers(graph: Union[Graph, Set], write_location: str, output_ints: str, output_ints_map: str) -> Dict:
    """Loops over the knowledge graph in order to create three different types of files:
        - Integers: tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
          subject, predicate, object). The subject, predicate, and object identifiers have been mapped to integers.
        - Identifiers: tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
          subject, predicate, object). Both the subject and object identifiers have not been mapped to integers.
        - Identifier-Integer Map: JSON file containing a dict where keys are node identifiers and values are integers.

    Args:
        graph: A set of RDFLib Graph object triples or an RDFLib Graph.
        write_location: A string pointing to a local directory for writing data.
        output_ints: the name and file path to write out results.
        output_ints_map: the name and file path to write out results.

    Returns:
        entity_map: A dictionary where keys are integers and values are identifiers.

    Raises:
        ValueError: If the length of the graph is not the same as the number of extracted triples.
    """

    print('Mapping Node and Relation Identifiers to Integers')

    entity_map, output_triples, entity_counter = {}, 0, 0; graph_len = len(graph)  # type: ignore
    ints = open(write_location + output_ints, 'w', encoding='utf-8')
    ids = open(write_location + output_ints.replace('Integers', 'Identifiers'), 'w', encoding='utf-8')
    ints.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
    ids.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
    for s, p, o in tqdm(graph):
        subj, pred, obj = n3(s), n3(p), n3(o)
        if subj not in entity_map: entity_counter += 1; entity_map[subj] = entity_counter
        if pred not in entity_map: entity_counter += 1; entity_map[pred] = entity_counter
        if obj not in entity_map: entity_counter += 1; entity_map[obj] = entity_counter
        ints.write('%d' % entity_map[subj] + '\t' + '%d' % entity_map[pred] + '\t' + '%d' % entity_map[obj] + '\n')
        try: ids.write(subj + '\t' + pred + '\t' + obj + '\n')
        except UnicodeEncodeError:
            s, p, o = s.encode('utf-8').decode(), p.encode('utf-8').decode(), o.encode('utf-8').decode()
            ids.write(s + '\t' + p + '\t' + o + '\n')
        output_triples += 1
    ints.close(), ids.close()
    # CHECK - verify we get the number of edges that we would expect to get
    if graph_len != output_triples: raise ValueError('ERROR: The number of triples is incorrect!')
    else:
        with open(write_location + '/' + output_ints_map, 'w') as file_name:
            json.dump(entity_map, file_name)

    return entity_map


def n3(node: Union[URIRef, BNode, Literal]) -> str:
    """Method takes an RDFLib node of type BNode, URIRef, or Literal and serializes it to meet the RDF 1.1 NTriples
    format.

    Src: https://github.com/RDFLib/rdflib/blob/c11f7b503b50b7c3cdeec0f36261fa09b0615380/rdflib/plugins/serializers/nt.py

    Args:
        node: An RDFLib

    Returns:
        serialized_node: A string containing the serialized
    """

    if isinstance(node, Literal): serialized_node = "%s" % _quoteLiteral(node)
    else: serialized_node = "%s" % node.n3()

    return serialized_node


def convert_to_networkx(write_loc: str, filename: str, graph: Union[Graph, Set], stats: bool = False) -> Optional[str]:
    """Converts an RDFLib.Graph object into a Networkx MultiDiGraph and pickles a copy locally. Each node is provided a
    key that is the URI identifier and each edge is given a key which is an md5 hash of the triple and a weight of
    0.0. An example of the output is shown below. The md5 hash is meant to store a unique key that represents that
    predicate with respect to the triples it occurs with.

    Source: https://networkx.org/documentation/stable/reference/classes/multidigraph.html

    Example:
        Input: (obo.SO_0000288', RDFS.subClassOf', obo.SO_0000287')
        Output:
            - node data: [(obo.SO_0000288, {'key': 'http://purl.obolibrary.org/obo/SO_0000288'}),
                          (RDFS.subClassOf', {'key': 'http://www.w3.org/2000/01/rdf-schema#subClassOf'}),
                          (obo.SO_0000287, {'key': 'http://purl.obolibrary.org/obo/SO_0000287'})]
            - edge data: [(obo.SO_0000288, obo.SO_0000287', {'predicate_key': '9cbd4826291e7b38eb', 'weight': 0.0})]

    Args:
        write_loc: A string pointing to a local directory for writing data.
        filename: A string containing the subdirectory and name of the the knowledge graph file.
        graph: An RDFLib Graph object or set of RDFLib Graph triples.
        stats: A bool indicating whether or not to derive network statistics after writing networkx file to disk.

    Returns:
        network_stats: A string containing network statistics information.
    """

    print('Converting Knowledge Graph to MultiDiGraph')

    nx_mdg = nx.MultiDiGraph()
    for s, p, o in tqdm(graph):
        pred_key = hashlib.md5('{}{}{}'.format(n3(s), n3(p), n3(o)).encode()).hexdigest()
        nx_mdg.add_node(s, key=n3(s)); nx_mdg.add_node(o, key=n3(o))
        nx_mdg.add_edge(s, o, **{'key': p, 'predicate_key': pred_key, 'weight': 0.0})
    print('Pickling MultiDiGraph')
    nx.write_gpickle(nx_mdg, write_loc + filename + '_NetworkxMultiDiGraph.gpickle')
    if stats: print('Generating Network Statistics'); return derives_graph_statistics(nx_mdg)
    else: return None


def appends_to_existing_file(edges: Union[List, Set, Graph], filepath: str, sep: str = ' ') -> None:
    """Method adds data to the end of an existing file. Assumes that it is adding data to the end of a n-triples file.

    Args:
        edges: A list or set of tuple, where each tuple is a triple. Or an RDFLib Graph object.
        filepath: A string specifying a path to an existing file.
        sep: A string containing a separator e.g. '\t', ',' (default=' ').

    Returns:
        None.
    """

    if not os.path.exists(filepath): os.system('touch {}'.format(filepath))
    with open(filepath, 'a', newline='') as out:
        for edge in edges:
            out.write(n3(edge[0]) + sep + n3(edge[1]) + sep + n3(edge[2]) + ' .\n')
    out.close()

    return None
