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

Interacts with Knowledge Graphs
* adds_edges_to_graph
* remove_edges_from_graph
* updates_graph_namespace
* gets_class_ancestors

Writes Triple Lists
* maps_node_ids_to_integers

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
from rdflib import Graph, Literal, Namespace, URIRef  # type: ignore
from rdflib.namespace import OWL, RDF, RDFS  # type: ignore
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

        return merges_ontologies(ontology_files, write_location, merged_ont_kg)


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


def adds_edges_to_graph(graph: Graph, edge_list: List) -> Graph:
    """Takes a tuple of tuples representing new triples and adds them to a knowledge graph.

    Args:
        graph: An RDFLib Graph object.
        edge_list: A list of tuples, where each tuple contains a triple.

    Returns:
        graph: An updated RDFLib graph.
    """

    for edge in set(edge_list):
        graph.add(edge)  # add edge to knowledge graph

    return graph


def remove_edges_from_graph(graph: Graph, edge_list: List) -> Graph:
    """Takes a tuple of tuples and removes them from a knowledge graph.

    Args:
        graph: An RDFLib Graph object.
        edge_list: A list of tuples, where each tuple contains a triple.

    Returns:
        graph: An updated RDFLib graph.
    """

    for edge in set(edge_list):
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
        node_map: A dictionary where keys are integers and values are identifiers.

    Raises:
        ValueError: If the length of the graph is not the same as the number of extracted triples.
    """

    node_map, output_triples, node_counter = {}, 0, 0  # type: ignore
    graph_len = len(graph)
    # build graph from input file and set counter
    out_ints = open(write_location + output_ints, 'w', encoding='utf-8')
    out_ids = open(write_location + output_ints.replace('Integers', 'Identifiers'), 'w', encoding='utf-8')
    out_ints.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
    out_ids.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
    for edge in tqdm(graph):
        if str(edge[0]) not in node_map:
            node_counter += 1
            node_map[str(edge[0])] = node_counter
        if str(edge[1]) not in node_map:
            node_counter += 1
            node_map[str(edge[1])] = node_counter
        if str(edge[2]) not in node_map:
            node_counter += 1
            node_map[str(edge[2])] = node_counter
        # convert edge labels to ints
        subj, pred, obj = str(edge[0]), str(edge[1]), str(edge[2])
        out_ints.write('%d' % node_map[subj] + '\t' + '%d' % node_map[pred] + '\t' + '%d' % node_map[obj] + '\n')
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
            json.dump(node_map, file_name)

    return node_map


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


def gets_class_ancestors(graph: Graph, class_uris: Set[Union[URIRef, str]], class_list: Optional[Set] = None) -> Set:
    """A method that recursively searches an ontology hierarchy to pull all ancestor concepts for an input class.

    Args:
        graph: An RDFLib graph object assumed to contain ontology data.
        class_uris: A list of at least one ontology class RDFLib URIRef object.
        class_list: A list of URIs representing the ancestor classes found for the input class_uris.

    Returns:
        A list of ontology class ordered by the ontology hierarchy.
    """

    # instantiate list object if none passed to function
    class_list = set() if class_list is None else class_list
    class_list = set([x if isinstance(x, URIRef) else URIRef(obo + x) for x in class_list])

    # check class uris are formatted correctly and get their ancestors
    class_uris = set([x if isinstance(x, URIRef) else URIRef(obo + x) for x in class_uris])
    ancestors = set([j for k in [list(graph.objects(x, RDFS.subClassOf)) for x in set(class_uris)] for j in k])

    if len(ancestors) == 0 or len(ancestors.difference(class_list)) == 0:
        return set([str(x) for x in class_list])
    else:
        class_uris = ancestors.difference(class_list)
        class_list |= ancestors.difference(class_list)
        return gets_class_ancestors(graph, class_uris, class_list)
