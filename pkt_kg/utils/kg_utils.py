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
* merges_ontologies
* ontology_file_formatter
* removes_annotation_assertions
* adds_annotation_assertions

Interacts with Knowledge Graphs
* adds_edges_to_graph
* remove_edges_from_graph
* updates_graph_namespace
* gets_class_ancestors
* connected_components
* removes_self_loops
* derives_graph_statistics
* splits_knowledge_graph

Writes Triple Lists
* maps_node_ids_to_integers
* nt_serializes_node
* appends_to_existing_file

File Type Conversion
* converts_rdflib_to_networkx
"""

# import needed libraries
import glob
import hashlib
import json
import networkx  # type: ignore
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
    if len(class_list) > 0: return class_list
    else: raise ValueError('ERROR: No classes returned from query.')


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
    if len(object_property_list) > 0: return object_property_list
    else: raise ValueError('ERROR: No object properties returned from query.')


def gets_ontology_class_synonyms(graph: Graph) -> Tuple:
    """Queries a knowledge graph and returns a tuple of dictionaries. The first dictionary contains all owl:Class
    objects and their synonyms in the graph. The second dictionary contains the synonyms and their OWL synonym types.

    Args:
        graph: An rdflib Graph object.

    Returns:
        A tuple of dictionaries:
            synonyms: A dictionary where keys are string synonyms and values are ontology URIs. An example is shown
                below:
                    {'modified l selenocysteine': 'http://purl.obolibrary.org/obo/SO_0001402',
                    'modified l-selenocysteine': 'http://purl.obolibrary.org/obo/SO_0001402',
                    'frameshift truncation': 'http://purl.obolibrary.org/obo/SO_0001910', ...}
            synonym_type: A dictionary where keys are string synonyms and values are OWL synonym types. An example is
                shown below:
                    {'susceptibility to herpesvirus': 'hasExactSynonym', 'full upper lip': 'hasExactSynonym'}
    """

    class_list = [x for x in graph if 'synonym' in str(x[1]).lower() and isinstance(x[0], URIRef)]
    synonyms = {str(x[2]).lower(): str(x[0]) for x in class_list}
    synonym_type = {str(x[2]).lower(): str(x[1]).split('#')[-1] for x in class_list}

    return synonyms, synonym_type


def gets_ontology_class_dbxrefs(graph: Graph):
    """Queries a knowledge graph and returns a dictionary that contains all owl:Class objects and their database
    cross references (dbxrefs) in the graph. This function will also include concepts that have been identified as
    exact matches. The query returns a tuple of dictionaries where the first dictionary contains the dbxrefs and
    exact matches (URIs and labels) and the second dictionary contains the dbxref/exactmatch uris and a string
    indicating the type (i.e. dbxref or exact match).

    Assumption: That none of the hasdbxref ids overlap with any of the exactmatch ids.

    Args:
        graph: An rdflib Graph object.

    Returns:
        dbxref: A dictionary where keys are dbxref strings and values are ontology URIs. An example is shown below:
            {'loinc:LA6690-7': 'http://purl.obolibrary.org/obo/SO_1000002',
             'RNAMOD:055': 'http://purl.obolibrary.org/obo/SO_0001347',
             'RNAMOD:076': 'http://purl.obolibrary.org/obo/SO_0001368',
             'loinc:LA6700-2': 'http://purl.obolibrary.org/obo/SO_0001590', ...}
        dbxref_type: A dictionary where keys are dbxref/exact match uris and values are string indicating if the uri
            is for a dbxref or an exact match. An example is shown below:
                {
    """

    # dbxrefs
    dbxref_res = [x for x in graph if 'hasdbxref' in str(x[1]).lower() if isinstance(x[0], URIRef)]
    dbxref_uris = {str(x[2]).lower(): str(x[0]) for x in dbxref_res}
    dbxref_type = {str(x[2]).lower(): 'DbXref' for x in dbxref_res}

    # exact match
    exact_res = [x for x in graph if 'exactmatch' in str(x[1]).lower() if isinstance([0], URIRef)]
    exact_uris = {str(x[2]).lower(): str(x[0]) for x in exact_res}
    exact_type = {str(x[2]).lower(): 'ExactMatch' for x in exact_res}

    # combine dictionaries
    uris = {**dbxref_uris, **exact_uris}
    types = {**dbxref_type, **exact_type}

    return uris, types


def gets_ontology_statistics(file_location: str, owltools_location: str = './pkt_kg/libs/owltools') -> None:
    """Uses the OWL Tools API to generate summary statistics (i.e. counts of axioms, classes, object properties, and
    individuals).

    Args:
        file_location: A string that contains the file path and name of an ontology.
        owltools_location: A string pointing to the location of the owl tools library.

    Returns:
        None.

    Raises:
        TypeError: If the file_location is not type str.
        OSError: If file_location points to a non-existent file.
        ValueError: If file_location points to an empty file.
    """

    if not isinstance(file_location, str): raise TypeError('file_location must be a string')
    elif not os.path.exists(file_location): raise OSError('{} does not exist!'.format(file_location))
    elif os.stat(file_location).st_size == 0: raise ValueError('{} is empty'.format(file_location))
    else: output = subprocess.check_output([os.path.abspath(owltools_location), file_location, '--info'])
    # print stats
    res = output.decode('utf-8').split('\n')[-5:]
    cls, axs, op, ind = res[0].split(':')[-1], res[3].split(':')[-1], res[2].split(':')[-1], res[1].split(':')[-1]
    sent = '\nThe knowledge graph contains {0} classes, {1} axioms, {2} object properties, and {3} individuals\n'
    print(sent.format(cls, axs, op, ind))

    return None


def merges_ontologies(ontology_files: List[str], write_location: str, merged_ont_kg: str,
                      owltools_location: str = os.path.abspath('./pkt_kg/libs/owltools')) -> Graph:
    """Using the OWLTools API, each ontology listed in in the ontologies attribute is recursively merged with into a
    master merged ontology file and saved locally to the provided file path via the merged_ontology attribute. The
    function assumes that the file is written to the directory specified by the write_location attribute.

    Args:
        ontology_files: A list of ontology file paths.
        write_location: A string pointing to a local directory for writing data.
        merged_ont_kg: A string pointing to the location of the merged ontology file.
        owltools_location: A string pointing to the location of the owl tools library.

    Returns:
        None.
    """

    if not ontology_files:
        return None
    else:
        if write_location + merged_ont_kg in glob.glob(write_location + '/*.owl'):
            ont1, ont2 = ontology_files.pop(), write_location + merged_ont_kg
        else:
            ont1, ont2 = ontology_files.pop(), ontology_files.pop()
        try:
            print('Merging Ontologies: {ont1}, {ont2}'.format(ont1=ont1.split('/')[-1], ont2=ont2.split('/')[-1]))
            subprocess.check_call([owltools_location, str(ont1), str(ont2), '--merge-support-ontologies',
                                   '-o', write_location + merged_ont_kg])
        except subprocess.CalledProcessError as error:
            print(error.output)

        return merges_ontologies(ontology_files, write_location, merged_ont_kg, owltools_location)


def ontology_file_formatter(write_location: str, full_kg: str,
                            owltools_location: str = os.path.abspath('./pkt_kg/libs/owltools')) -> None:
    """Reformat an .owl file to be consistent with the formatting used by the OWL API. To do this, an ontology
    referenced by graph_location is read in and output to the same location via the OWLTools API.

    Args:
        write_location: A string pointing to a local directory for writing data.
        full_kg: A string containing the subdirectory and name of the the knowledge graph file.
        owltools_location: A string pointing to the location of the owl tools library.

    Returns:
        None.

    Raises:
        TypeError: If something other than an .owl file is passed to function.
        IOError: If the graph_location file is empty.
        TypeError: If the input file contains no data.
    """

    print('Applying OWL API Formatting to Knowledge Graph OWL File')

    graph_write_location = write_location + full_kg
    if '.owl' not in graph_write_location: raise TypeError('The provided file is not type .owl')
    elif not os.path.exists(graph_write_location): raise IOError('{} does not exist!'.format(graph_write_location))
    elif os.stat(graph_write_location).st_size == 0: raise TypeError('{} is empty'.format(graph_write_location))
    else:
        try:
            subprocess.check_call([owltools_location, graph_write_location, '-o', graph_write_location])
        except subprocess.CalledProcessError as error:
            print(error.output)

    return None


def removes_annotation_assertions(filename: str,
                                  owltools_location: str = os.path.abspath('./pkt_kg/libs/owltools')) -> None:
    """Utilizes OWLTools to remove annotation assertions. The '--remove-annotation-assertions' method in OWLTools
    removes annotation assertions to make a pure logic subset', which reduces the overall size of the knowledge
    graph, while still being compatible with a reasoner.

    Args:
        filename: A string pointing to a filename and local directory for writing data to.
        owltools_location: A string pointing to the location of the owl tools library.

    Returns:
        None.
    """

    try:
        subprocess.check_call([owltools_location, filename, '--remove-annotation-assertions',
                               '-o', filename[:-4] + '_NoAnnotationAssertions.owl'])
    except subprocess.CalledProcessError as error:
        print(error.output)

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
    for edge in edge_set:
        graph.add(edge)

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


def gets_class_ancestors(graph: Graph, class_uris: List[Union[URIRef, str]], class_list: Optional[List] = None) -> List:
    """A method that recursively searches an ontology hierarchy to pull all ancestor concepts for an input class.

    Args:
        graph: An RDFLib graph object assumed to contain ontology data.
        class_uris: A list of at least one ontology class RDFLib URIRef object.
        class_list: A list of URIs representing the ancestor classes found for the input class_uris.

    Returns:
        An ordered (ascending; root to leaf) list of ontology classes containing the input ancestor hierarchy for the
            class_uris. For example:
                input: [URIRef('http://purl.obolibrary.org/obo/NCBITaxon_11157')]
                output: [
                    'http://purl.obolibrary.org/obo/NCBITaxon_10239',
                    'http://purl.obolibrary.org/obo/NCBITaxon_2559587',
                    'http://purl.obolibrary.org/obo/NCBITaxon_2497569',
                    'http://purl.obolibrary.org/obo/NCBITaxon_11157'
                    ]
    """

    # instantiate list object if none passed to function
    class_list = [] if class_list is None else class_list
    class_list = list(unique_everseen([x if isinstance(x, URIRef) else URIRef(obo + x) for x in class_list]))
    # check class uris are formatted correctly and get their ancestors
    class_uris = list(unique_everseen([x if isinstance(x, URIRef) else URIRef(obo + x) for x in class_uris]))
    ancestors = list(unique_everseen([j for k in [graph.objects(x, RDFS.subClassOf) for x in class_uris] for j in k]))
    if len(ancestors) == 0 or len(set(ancestors).difference(set(class_list))) == 0:
        return list(unique_everseen([str(x) for x in class_list]))
    else:
        class_uris = [x for x in ancestors if x not in class_list]
        class_list.insert(0, [x for x in class_uris][0])
        return gets_class_ancestors(graph, class_uris, class_list)


def connected_components(graph: Graph) -> List:
    """Creates a dictionary where the keys are integers representing a component number and the values are sets
    containing the nodes for a given component. This method works by first converting the RDFLib graph into a
    NetworkX multi-directed graph, which is converted to a undirected graph prior to calculating the connected
    components.

    Args:
        graph: An RDFLib Graph object.

    Returns:
        component_dict: A dictionary where the keys are integers representing a component number and the values are sets
            containing the nodes for a given component.
    """

    nx_mdg = networkx.MultiDiGraph()
    for s, p, o in tqdm(graph):
        nx_mdg.add_edge(s, o, **{'key': p})

    # get connected components
    print('Calculating Connected Components')
    components = list(networkx.connected_components(nx_mdg.to_undirected()))
    component_dict = sorted(components, key=len, reverse=True)

    return component_dict


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
        if edge[0] == edge[2]:
            self_loops.add(edge)

    return list(self_loops)


def derives_graph_statistics(graph: Union[Graph, networkx.MultiDiGraph]) -> str:
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
        triples = len(graph)
        nodes = len(set(list(graph.subjects()) + list(graph.objects())))
        rels = set(list(graph.predicates()))
        cls = set([x for x in graph.subjects(RDF.type, OWL.Class)])
        inds = set([x for x in graph.subjects(RDF.type, OWL.NamedIndividual)])
        obj_prop = set([x for x in graph.subjects(RDF.type, OWL.ObjectProperty)])
        ant_prop = set([x for x in graph.subjects(RDF.type, OWL.AnnotationProperty)])
        x = ' {} triples, {} nodes, {} predicates, {} classes, {} individuals, {} object properties, {} annotation ' \
            'properties'
        stats = 'Graph Stats:' + x.format(triples, nodes, len(rels), len(cls), len(inds), len(obj_prop), len(ant_prop))
    else:
        nx_graph_und = graph.to_undirected()
        nodes = networkx.number_of_nodes(graph)
        edges = networkx.number_of_edges(graph)
        self_loops = networkx.number_of_selfloops(graph)
        ce = sorted(Counter([str(x[2]) for x in graph.edges(keys=True)]).items(),  # type: ignore
                    key=lambda x: x[1],  # type: ignore
                    reverse=1)[:6]  # type: ignore
        avg_degree = float(edges) / nodes
        n_deg = sorted([(str(x[0]), x[1]) for x in graph.degree], key=lambda x: x[1], reverse=1)[:6]  # type: ignore
        density = networkx.density(graph)
        components = sorted(list(networkx.connected_components(nx_graph_und)), key=len, reverse=True)
        # cc_sizes = {x: len(components[x]) for x in range(len(components))}
        cc_content = {x: str(len(components[x])) + ' nodes: ' + ' | '.join(components[x]) if len(components[x]) < 500
                      else len(components[x]) for x in range(len(components))}
        x = '{} nodes, {} edges, {} self-loops, 5 most most common edges: {}, average degree {}, 5 highest degree '\
            'nodes: {}, density: {}, {} component(s): {}'
        stats = 'Graph Stats: ' + x.format(nodes, edges, self_loops,
                                           ', '.join([x[0] + ':' + str(x[1]) for x in ce]),
                                           avg_degree,
                                           ', '.join([x[0] + ':' + str(x[1]) for x in n_deg]),
                                           density, len(components), cc_content)

    return stats


def splits_knowledge_graph(graph: Graph) -> Tuple[Graph, Graph]:
    """Method takes an input RDFLib Graph object and splits it into two new graphs where the first graph contains
    only those triples needed to maintain a base logical subset and the second contains only annotation assertions.

    Args:
        graph: An RDFLib Graph object.

    Returns:
        split_graphs: A tuple of RDFLib Graph objects, where the first graph contains the logical subset from the
            original graph and the second graph contains all of the
    """

    print('Creating Logic and Annotation Graph Subsets')

    entities = set([x[0] for x in graph.triples((None, RDF.type, None)) if isinstance(x[0], URIRef)])
    annotation_props = set(graph.subjects(RDF.type, OWL.AnnotationProperty))
    annotation_axioms = set()
    for ent in tqdm(entities):
        axioms = list(graph.triples((None, OWL.annotatedSource, ent)))
        if len(axioms) > 0:
            for axiom in axioms:
                annotation_axioms |= set(graph.triples((axiom[0], None, None)))
                annotation_axioms |= set([x for x in graph.triples((axiom[2], None, None)) if x[1] in annotation_props])
        else:
            annotation_axioms |= set([x for x in graph.triples((ent, None, None)) if x[1] in annotation_props])

    # create graph subsets
    all_triples = set(list(graph.triples((None, None, None))))
    logic_triples = all_triples.difference(annotation_axioms)
    annotation_graph = adds_edges_to_graph(Graph(), annotation_axioms)
    logic_graph = adds_edges_to_graph(Graph(), logic_triples)

    if len(logic_triples) + len(annotation_axioms) != len(all_triples):
        raise ValueError('Error: Subsetting was Unsuccessful!')
    else:
        print('logic_graph: {} triples; annotation_graph: {} triples'.format(len(logic_graph), len(annotation_graph)))
        # add rdf:type to annotations -- needed for extracting node and relation metadata
        annotation_graph = adds_edges_to_graph(annotation_graph, list(graph.triples((None, RDF.type, None))), False)

        return logic_graph, annotation_graph


def maps_node_ids_to_integers(graph: Graph, write_location: str, output_ints: str, output_ints_map: str) -> Dict:
    """Loops over the knowledge graph in order to create three different types of files:
        - Integers: a tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
          subject, predicate, object). The subject, predicate, and object identifiers have been mapped to integers.
        - Identifiers: a tab-delimited `.txt` file containing three columns, one for each part of a triple (i.e.
          subject, predicate, object). Both the subject and object identifiers have not been mapped to integers.
        - Identifier-Integer Map: a `.json` file containing a dictionary where the keys are node identifiers and
          the values are integers.

    Args:
        graph: An rdflib graph object.
        write_location: A string pointing to a local directory for writing data.
        output_ints: the name and file path to write out results.
        output_ints_map: the name and file path to write out results.

    Returns:
        entity_map: A dictionary where keys are integers and values are identifiers.

    Raises:
        ValueError: If the length of the graph is not the same as the number of extracted triples.
    """

    entity_map, output_triples, entity_counter = {}, 0, 0  # type: ignore
    graph_len = len(graph)
    # build graph from input file and set counter
    out_ints = open(write_location + output_ints, 'w', encoding='utf-8')
    out_ids = open(write_location + output_ints.replace('Integers', 'Identifiers'), 'w', encoding='utf-8')
    out_ints.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
    out_ids.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
    for edge in tqdm(graph):
        subj, pred, obj = edge[0].n3(), edge[1].n3(), edge[2].n3()
        if subj not in entity_map:
            entity_counter += 1
            entity_map[subj] = entity_counter
        if pred not in entity_map:
            entity_counter += 1
            entity_map[pred] = entity_counter
        if obj not in entity_map:
            entity_counter += 1
            entity_map[obj] = entity_counter
        # convert edge labels to ints
        out_ints.write('%d' % entity_map[subj] + '\t' + '%d' % entity_map[pred] + '\t' + '%d' % entity_map[obj] + '\n')
        try:
            out_ids.write(subj + '\t' + pred + '\t' + obj + '\n')
        except UnicodeEncodeError:
            out_ids.write(edge[0].encode('utf-8').decode() + '\t' +
                          edge[1].encode('utf-8').decode() + '\t' +
                          edge[2].encode('utf-8').decode() + '\n')
        # update counter and delete edge
        output_triples += 1
    out_ints.close(), out_ids.close()

    # CHECK - verify we get the number of edges that we would expect to get
    if graph_len != output_triples:
        raise ValueError('ERROR: The number of triples is incorrect!')
    else:
        with open(write_location + '/' + output_ints_map, 'w') as file_name:
            json.dump(entity_map, file_name)

    return entity_map


def nt_serializes_node(node: Union[URIRef, BNode, Literal]) -> str:
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


def appends_to_existing_file(edges: List, filepath: str, sep: str) -> None:
    """Method adds data to the end of an existing file. Assumes that it is adding data to the end of a n-triples file.

    Args:
        edges: A tuple of 3 RDFLib terms.
        filepath: A string specifying a path to an existing file.
        sep: A string containing a separator (e.g. '\t', ',').

    Returns:
        None.
    """

    with open(filepath, 'a', newline='') as out:
        for edge in edges:
            out.write(edge[0].n3() + sep + edge[1].n3() + sep + edge[2].n3() + ' .\n')
    out.close()

    return None


def converts_rdflib_to_networkx(write_location: str, full_kg: str, graph: Optional[Graph] = None) -> None:
    """Converts an RDFLib.Graph object into a Networkx MultiDiGraph and pickles a copy locally. Each node is provided a
    key that is the URI identifier and each edge is given a key which is an md5 hash of the triple and a weight of
    0.0. An example of the output is shown below. The md5 hash is meant to store a unique key that represents that
    predicate with respect to the triples it occurs with.

    Source: https://networkx.org/documentation/stable/reference/classes/multidigraph.html

        Example:
            Input: (URIRef('http://purl.obolibrary.org/obo/SO_0000288'),
                    URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'),
                    URIRef('http://purl.obolibrary.org/obo/SO_0000287'))
            Output:
                - node data: [
                        (URIRef('http://purl.obolibrary.org/obo/SO_0000288'),
                        {'key': 'http://purl.obolibrary.org/obo/SO_0000288'}),
                        (URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'),
                        {'key': 'http://www.w3.org/2000/01/rdf-schema#subClassOf'}),
                        (URIRef('http://purl.obolibrary.org/obo/SO_0000287'),
                        {'key': 'http://purl.obolibrary.org/obo/SO_0000287'})
                            ]
                - edge data: [
                        (URIRef('http://purl.obolibrary.org/obo/SO_0000288'),
                         URIRef('http://purl.obolibrary.org/obo/SO_0000287'),
                         {'predicate_key': '9cbd482627d217b38eb407d7eba48020', 'weight': 0.0})
                            ]

    Args:
        write_location: A string pointing to a local directory for writing data.
        full_kg: A string containing the subdirectory and name of the the knowledge graph file.
        graph: An rdflib graph object.

    Returns:
        None.

    Raises:
        IOError: If the file referenced by filename does not exist.
    """

    print('Converting Knowledge Graph to MultiDiGraph')

    # read in knowledge graph if class graph attribute is not present
    if not isinstance(graph, Graph):
        file_type = 'xml' if 'OWLNETS' not in full_kg else full_kg.split('.')[-1]
        ext = '.owl' if file_type == 'xml' else '.nt'
        graph = Graph().parse(write_location + full_kg + ext, format=file_type)

    # convert graph to networkx object
    nx_mdg = networkx.MultiDiGraph()
    for s, p, o in tqdm(graph):
        pred_key = hashlib.md5('{}{}{}'.format(str(s), str(p), str(o)).encode()).hexdigest()
        nx_mdg.add_node(s, key=str(s))
        nx_mdg.add_node(o, key=str(o))
        nx_mdg.add_edge(s, o, **{'key': p, 'predicate_key': pred_key, 'weight': 0.0})

    # pickle networkx graph
    print('Pickling MultiDiGraph -- For Large Networks Process Takes Several Minutes.')
    networkx.write_gpickle(nx_mdg, write_location + full_kg + '_NetworkxMultiDiGraph.gpickle')
    del nx_mdg   # clean up environment

    return None
