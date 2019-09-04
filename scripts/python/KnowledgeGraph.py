#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import json
import subprocess
import uuid

from rdflib import Namespace
from rdflib import Graph
from rdflib.namespace import RDF
from rdflib import URIRef
from tqdm import tqdm


def merges_ontologies(ontology_list):
    """Takes a list of lists, where the each nested list contains a pair of ontologies and a file path. Using the
    OWLTools API, each pair of ontologies is merged and saved locally to provied file path.

    Args:
        ontology_list (list): A nested list

    Returns:
        None.
    """

    for ont in tqdm(ontology_list):
        print('\nMerging Ontologies: {ont1}, {ont2}\n'.format(ont1=ont[0].split('/')[-1],
                                                              ont2=ont[1].split('/')[-1]))

        try:
            subprocess.check_call(['./resources/lib/owltools',
                                   str(ont[0]),
                                   str(ont[1]),
                                   '--merge-support-ontologies',
                                   '-o',
                                   str(ont[2]) + '_merged.owl'])

        except subprocess.CalledProcessError as error:
            print(error.output)


def creates_knowledge_graph_edges(edge_dict, data_type, graph, output_loc, kg_class_iri_map=None):
    """Takes a nested dictionary of edge lists creates and adds new edges.

    If the data_type is 'class' two types of edges are created: (1) instance of class and (2) class instance to
    instance. The instance of the class is also added to a dictionary that maps the class instance to its class.

    If the data_type is not 'class' then a single edge is created.

    Args:
        edge_dict (dict): a nested dictionary of edge lists by data source
        data_type (str): A string naming the data type (i.e. class or other)
        graph (class): An rdflib graph
        output_loc (str): Name and file path to write knowledge graph to
        kg_class_iri_map (dict): An empty dictionary that is used to store the mapping between a class and its instance

    Returns:
        An rdflib graph is returned and written to the location specified in input arguments
    """

    # define namespaces
    class_inst_ns = Namespace('https://github.com/callahantiff/PheKnowLator/obo/ext/')
    obo = Namespace('http://purl.obolibrary.org/obo/')

    # get number of starting edges
    start_edges = len(graph)
    start_nodes = len(set([str(node) for edge in list(graph) for node in edge[0::2]]))

    # print message
    if data_type == 'class':
        print('\n' + '=' * len('Creating Instances of Classes and Class-Instance Edges\n'))
        print('Creating Instances of Classes and Class-Instance Edges')
        print('=' * len('Creating Instances of Classes and Class-Instance Edges\n') + '\n')
    else:
        print('\n' + '=' * len('Creating Instance-Instance and Class-Class Edges'))
        print('Creating Instance-Instance and Class-Class Edges')
        print('=' * len('Creating Instance-Instance and Class-Class Edges\n') + '\n')

    # loop over classes to create instances
    for source in tqdm(edge_dict):
        for edge in edge_dict[source]['edge_list']:
            if data_type == 'class':
                class_loc = edge_dict[source]['data_type'].split('-').index('class')
                inst_loc = edge_dict[source]['data_type'].split('-').index('instance')

                # add uuid for class-instance to dictionary - but check if one has been created first
                if str(edge_dict[source]['uri'][class_loc] + edge[class_loc]) in kg_class_iri_map.keys():
                    ont_class_iri = kg_class_iri_map[str(edge_dict[source]['uri'][class_loc] + edge[class_loc])]
                else:
                    ont_class_iri = class_inst_ns + str(uuid.uuid4())
                    kg_class_iri_map[str(edge_dict[source]['uri'][class_loc] + edge[class_loc])] = ont_class_iri

                # add instance of class
                graph.add((URIRef(ont_class_iri),
                           RDF.type,
                           URIRef(str(edge_dict[source]['uri'][class_loc] + edge[class_loc]))))

                # add relation between instance of class and instance
                graph.add((URIRef(ont_class_iri),
                           URIRef(str(obo + edge_dict[source]['edge_relation'])),
                           URIRef(str(edge_dict[source]['uri'][inst_loc] + edge[inst_loc]))))

            else:
                # add instance-instance and class-class edges
                graph.add((URIRef(str(edge_dict[source]['uri'][0] + edge[0])),
                           URIRef(str(obo + edge_dict[source]['edge_relation'])),
                           URIRef(str(edge_dict[source]['uri'][1] + edge[1]))))

    # get node and edge count
    end_edges = len(graph)
    end_nodes = len(set([str(node) for edge in list(graph) for node in edge[0::2]]))
    print('\nKG started with {s1}, {s2} nodes/edges and ended with {s3}, {s4} nodes/edges\n'.format(s1=start_nodes,
                                                                                                    s2=start_edges,
                                                                                                    s3=end_nodes,
                                                                                                    s4=end_edges))
    # serialize graph
    graph.serialize(destination=output_loc, format='xml')

    # write iri dictionary to file
    if kg_class_iri_map is not None:
        with open('.' + output_loc.split('.')[1] + '_ClassInstanceMap.json', 'w') as filepath:
            json.dump(kg_class_iri_map, filepath)

    return graph


def removes_disointness_axioms(graph, output):
    """Queries an RDFLib graph object to identify and remove all disjoint axioms.

    Args:
        graph (graph): An RDFlib graph object with disjoint axioms
        output (str): A string naming a file path to write out results

    Returns:
        None.
    """

    print('\n\n' + '=' * len('Removing Disjoint Axioms'))
    print('Removing Disjointness Axioms')
    print('=' * len('Removing Disjoint Axioms') + '\n')

    # query graph to find disjoint axioms
    owl = Namespace("http://www.w3.org/2002/07/owl#")

    results = graph.query(
        """SELECT DISTINCT ?source ?c
           WHERE {
              ?c rdf:type owl:Class .
              ?c owl:disjointWith ?source .}
           """, initNs={"rdf": 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                        "owl": 'http://www.w3.org/2002/07/owl#',
                        "oboInOwl": 'http://www.geneontology.org/formats/oboInOwl#'})

    print('Identified and Removed {disjoint} Disjointness Axioms\n'.format(disjoint=len(list(results))))

    # remove disjoint axioms
    graph.remove((None, URIRef(str(owl) + 'disjointWith'), None))

    # get node and edge count
    edge_count = len(graph)
    node_count = len(set([str(node) for edge in list(graph) for node in edge[0::2]]))
    print('\nKG ended with {node} nodes and {edge} edges\n'.format(node=node_count, edge=edge_count))

    # serialize graph
    graph.serialize(destination=output, format='xml')

    return None


def closes_knowledge_graph(graph, reasoner, output):
    """Use OWLTools via the command line and to run reasoners to deductively close an input graph. The resulting
    graph is then serialized.

    Args:
        graph (str): A file path/file storing an RDF graph
        reasoner (str): A string indicating the type of reasoner that should be used
        output (str): A string containing the name and file path to write out results

    Returns:
        None.
    """

    print('\n\n' + '=' * len('Closing Knowledge Graph using the {reasoner} Reasoner'.format(reasoner=reasoner.upper())))
    print('Closing Knowledge Graph using the {reasoner} Reasoner'.format(reasoner=reasoner.upper()))
    print('=' * len('Closing Knowledge Graph using the {reasoner} Reasoner'.format(reasoner=reasoner.upper())))

    # set command line argument
    try:
        subprocess.check_call(['resources/lib/owltools',
                               str(graph),
                               '--reasoner ' + str(reasoner),
                               '--run-reasoner',
                               '--assert-implied',
                               '-o',
                               './' + str(output).split('.')[1] + '_Closed_' + str(reasoner).upper() + '.owl'])

    except subprocess.CalledProcessError as error:
        print(error.output)

    return None


def removes_metadata_nodes(graph, output, iri_mapper):
    """Queries the RDFLib graph object to identify only those subjects, objects, and predicates that are
    RDfResources. From this filtered output, the results are further reduced to remove any triples that are: class
    instance-rdf:Type-owl:NamedIndividual or instance-rdf:Type-owl:Class.

    Args:
        graph (graph): An RDFlib graph object with disjoint axioms.
        output (str): A string containing the name and file path to write out results.
        iri_mapper (str): A string naming the location of the class instance iri-class identifier map.

    Returns:
        An rdflib graph with metadata nodes removed is returned and written to the location specified in input
        arguments.
    """

    print('\n\n' + '=' * len('Removing Metadata Nodes'))
    print('Removing Metadata Nodes')
    print('=' * len('Removing Metadata Nodes') + '\n')

    # read in and reverse dictionary to map iris back to labels
    iri_map = {val: key for (key, val) in json.load(open(iri_mapper)).items()}

    # from those triples with URI, remove triples that are about instances of classes
    update_graph = Graph()

    # loop over results and add eligible edges to new graph
    for edge in tqdm(list(graph)):
        if not any(str(x) for x in edge if not str(x).startswith('http')):

            if any(x for x in edge[0::2] if str(x) in iri_map.keys()):
                if str(edge[2]) in iri_map.keys() and 'ns#type' not in str(edge[1]):
                    update_graph.add((edge[0], edge[1], URIRef(iri_map[str(edge[2])])))

                else:
                    update_graph.add((URIRef(iri_map[str(edge[0])]), edge[1], edge[2]))

            elif not any(str(x) for x in edge[0::2] if '#' in str(x)):
                if not any(str(x) for x in edge if ('ns#type' in str(x)) or ('PheKnowLator' in str(x))):
                    update_graph.add(edge)

            else:
                pass

    # get node and edge count
    edge_count = len(update_graph)
    node_count = len(set([str(node) for edge in list(update_graph) for node in edge[0::2]]))
    print('\nKG has {node} nodes and {edge} edges\n'.format(node=node_count, edge=edge_count))

    # serialize edges
    update_graph.serialize(destination=output, format='xml')

    return update_graph


def maps_str_to_int(graph, output_trip_ints, output_map):
    """Converts nodes with string labels to integers and saves this mapping to a dictionary. Two text files are saved
    locally, the original labeled edges and edges with the labels replaced by integers.

    Args:
        graph (graph): An rdflib graph is returned and written to the location specified in input arguments.
        output_trip_ints (str): A string naming the name and file path to write out results.
        output_map (str): A string naming the name and file path to write out results.

    Returns:
        None.
    """

    print('\n\n' + '=' * len('Mapping Integer Node Labels to String Label'))
    print('Mapping Integer Node Labels to String Label')
    print('=' * len('Mapping Integer Node Labels to String Label') + '\n')

    # create dictionary for mapping and list to write edges to
    node_map = {}
    update_graph_ints = []

    # build graph from input file and set int counter
    node_counter = 0

    # open file to write to
    out1 = open(output_trip_ints, 'w')
    out2 = open('_'.join(output_trip_ints.split('_')[:-1]) + '_Labels.txt', 'w')

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
        update_graph_ints.append([node_map[str(edge[0])], node_map[str(edge[1])], node_map[str(edge[2])]])

        # write edge to text file
        out1.write('%d' % node_map[str(edge[0])] + '\t' +
                   '%d' % node_map[str(edge[1])] + '\t' +
                   '%d' % node_map[str(edge[2])] + '\n')

        out2.write(str(edge[0]) + '\t' + str(edge[1]) + '\t' + str(edge[2]) + '\n')

    # close file
    out1.close()
    out2.close()

    # CHECK - verify we get the number of edges that we would expect to get
    if len(graph) != len(update_graph_ints):
        raise Exception('ERROR: The number of triples is incorrect!')
    else:
        # write string-tto-int dictionary to file
        with open(output_map, 'w') as fp:
            json.dump(node_map, fp)

    return None
