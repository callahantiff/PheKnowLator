##########################################################################################
# KnowledgeGraph.py
# Purpose: script to create a RDF knowledge graph using classes and instances
# version 1.0.0
# date: 11.12.2017
# Python 3.6.2
##########################################################################################


# import needed libraries
import json
from rdflib import Namespace
from rdflib import Graph
from rdflib.namespace import RDF
from rdflib import URIRef
import subprocess
import uuid



def OntologyMerger(ont1, ont2, output):
    """Function takes two strings as arguments which contain the names and location to ontology files. The function
    then merges the ontologies using a call to the OWLTools command line (
    https://github.com/owlcollab/owltools/wiki/Extract-Properties-Command). The resulting graph is then written to the
    location as specified by the input argument. Currently the function only merges two ontologies - this can be
    modified

    :param
        ont1 (str): name and file path to location of ontology

        ont2 (str): name and file path to location of ontology

        output (str): name and file path to write results to

    :return:
        the merged ontologies are output to the location specified in input arguments

    """
    print('Merging the following ontologies: ' + str(ont1) + ', ' + str(ont2))

    # set command line argument
    try:
        subprocess.check_call(['./resources/lib/owltools',
                               str(ont1),
                               str(ont2),
                               '--merge-support-ontologies',
                               '-o',
                               str(output) + '_merged.owl'])

    except subprocess.CalledProcessError as error:
        print(error.output)



def classEdges(edge_dict, graph_output, input_graph, iri_map_output):
    """Function takes a nested dictionary of edge lists by data source and uses it to create add instance-instance
    edges to a rdflib knowledge graph. The function also takes three strings indicating where the resulting graph should
    be saved, name and file path to write out class instance iri-class identifier map as well as what graph to read in
    and add edges to. Once all edges have been added the input graph, the graph is serialized and saved to the
    location specified by the input arguments

    :param
        edge_dict: a nested dictionary of edge lists by data source

        output (str): name and file path to write out results

        input_graph (str): name and file path to graph input

        iri_map_output (str): name and file path to write out class instance iri-class identifier map

    :return:
        graph (graph): an rdflib graph is output to the location specified in input arguments

    """
    print('Adding instance of class and class instance-instance relations to graph \n')

    # initialize graphs
    graph = Graph()
    graph.parse(input_graph)

    # namespace
    kg_edges = Namespace("http://ccp.ucdenver.edu/obo/ext/")
    OBO = Namespace("http://purl.obolibrary.org/obo/")

    # loop over classes to create instances
    KG_class_iri_map = {}

    for source, edges in edge_dict.items():
        print('Adding edges for: ' + str(source) + '\n')

        for key, value in edges.items():
            ont_class = key[0]
            relation = key[1]

            for i in list(value):
                # add uuid class instance to dictionary
                ont_class_iri = str(uuid.uuid4())
                KG_class_iri_map[ont_class_iri] = ont_class

                # add relation between class + instance of class
                class_individual = URIRef(kg_edges + ont_class_iri)
                entity = URIRef(i)

                # print(class_individual, RDF.type, URIRef(ont_class))
                # print(entity, URIRef(str(OBO) + str(relation)), class_individual)
                graph.add((class_individual, RDF.type, URIRef(ont_class)))

                # add relation between instance of class and instance
                graph.add((entity, URIRef(str(OBO) + str(relation)), class_individual))

    # serialize graph
    graph.serialize(destination=graph_output, format='xml')

    # write iri dictionary to file
    with open(iri_map_output, "w") as fp:
        json.dump(KG_class_iri_map, fp)

    return graph




def instanceEdges(graph, edge_dict, output):
    """Function takes a nested dictionary of edge lists by data source and uses it to create add instance-instance
    edges to a rdflib knowledge graph. The function also takes a string indicating where the resulting graph should
    be saved. Once all edges have been added to the input graph, the graph is serialized and saved to the location
    specified by the input arguments

    :param
        graph (graph): a rdflib graph

        edge_dict: a nested dictionary of edge lists by data source

        output (str): name and file path to write out results

    :return:
        graph (graph): an rdflib graph is output to the location specified in input arguments

    """
    print('Adding instance-instance relations to graph \n')

    # namespace
    OBO = Namespace("http://purl.obolibrary.org/obo/")

    for source, edges in edge_dict.items():
        print(str(source) + '\n')

        for key, value in edges.items():
            ont_class = URIRef(key[0])
            relation = key[1]

            for i in list(value):
                entity = URIRef(i)

                # print(ont_class, URIRef(str(OBO) + str(relation)), entity)
                # add relation between instance and instance
                graph.add((ont_class, URIRef(str(OBO) + str(relation)), entity))

    # serialize graph
    graph.serialize(destination= output, format='xml')

    return graph



def removeDisointness(graph, output):
    """Function takes an rdflib graph object and queries it to identify disjoint axioms. The function also takes a
    string indicating where the resulting graph should be saved. The function then removes all triples that are
    disjoint. The graph is then serialized and saved to the location specified by the input arguments

    :param
        graph (graph): a rdflib graph object with disjoint axioms

        output (str): name and file path to write out results

    :return:
        graph (graph): a rdflib graph object without disjoint axioms

    """
    print('Removing disjoint axioms \n')

    # query graph to find disjoint axioms
    OWL = Namespace("http://www.w3.org/2002/07/owl#")
    results = graph.query(
                """SELECT DISTINCT ?source ?c
                   WHERE {
                      ?c rdf:type owl:Class .
                      ?c owl:disjointWith ?source .}
                   """, initNs={"rdf": 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                                "owl": 'http://www.w3.org/2002/07/owl#',
                                "oboInOwl": 'http://www.geneontology.org/formats/oboInOwl#'})

    print('There are ' + str(len(list(results))) + ' disjointness axioms')

    # remove disjoint axioms
    graph.remove((None, URIRef(str(OWL) + 'disjointWith'), None))

    # serialize graph
    graph.serialize(destination=output, format='xml')



def CloseGraph(graph, reasoner, output):
    """Function takes a string that represents a file path/file storing an RDF graph, a string indicating the name of a
    reasoner, and a string indicating where the resulting graph should be saved. With this input the function
    accesses OWLTools via a command line argument (
    https://github.com/owlcollab/owltools/wiki/Extract-Properties-Command) and runs the elk reasoner to check the
    consistency of the graph as well as to assert implied links. The resulting graph is then serialized and saved to
    the location specified by the input arguments

    :param
        graph (str): a file path/file storing an RDF graph

        reasoner (str): a string indicating the type of reasoner that should be used

        output (str): name and file path to write out results

    :return:
        results of running reasoner are serialized and saved to the location specified by the input arguments

    """
    print('Deductively closing the graph using the ' + str(reasoner) + ' reasoner \n')

    # set command line argument
    try:
        subprocess.check_call(['resources/lib/owltools',
                               str(graph),
                               '--reasoner ' + str(reasoner),
                               '--run-reasoner',
                               '--assert-implied',
                               '-o',
                               str(output) + '_elk.owl'])

    except subprocess.CalledProcessError as error:
        print(error.output)


def KGEdgeList(input_graph, output, iri_mapper):
    """Function takes as inputs a file/path to RDF graph, a a file/path to class instance iri-class identifier map,
    and a file/path specifying a location to write serialized results to. With these inputs the function queries the
    graph to identify only those subjects, objects, and predicates that are RDfResources. From this filtered the
    output, the results are further reduced to remove any triples that are: class
    instance-rdf:Type-owl:NamedIndividual or instance-rdf:Type-owl:Class

    :param
        input_graph (str): a file path/file storing an RDF graph

        output (str): name and file path to write out results

        iri_mapper: name and file path to read in class instance iri-class identifier map

    :return:
        updated graph is serialized and saved to the location specified by the input arguments

    """
    print('Creating edge list of triples in graph  \n')

    # read in dictionary to map iris
    with open() as json_data:
        iri_map = json.load(json_data)

    # build graph from ELK reasoner results
    graph = Graph()
    graph.parse(input_graph)

    # loop over the triples and only keep those that are RDF resources
    results_uri = graph.query(
        """SELECT DISTINCT ?s ?p ?o
           WHERE {
              ?s ?p ?o .
              filter IsIRI(?s)  .
              filter IsIRI(?p)  .
              filter IsIRI(?o)  .
              
              # identify class-class instance relations - rdflib does not have minus
              # minus { ?s rdf:type owl:NamedIndividual .
              # ?s rdf:type ?c .
              # ?c rdf:type owl:Class .}
              }
           """, initNs={"rdf": 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                        "owl": 'http://www.w3.org/2002/07/owl#',
                        "rdfs": 'http://www.w3.org/2000/01/rdf-schema#'})

    # from those triples with URI, remove triples that are about instances of classes
    update_graph = Graph()

    # loop over results and add eligible edges to new graph
    for s in results_uri:
        if 'ccp.ucdenver' in s[0] and 'ns#type' in s[1]:
            pass
        else:
            if s[2].split('/')[-1] in iri_map.keys():
                update_graph.add((URIRef(s[0]), URIRef(s[1]), URIRef(iri_map[s[2].split('/')[-1]])))
            else:
                update_graph.add((URIRef(s[0]), URIRef(s[1]), URIRef(s[2])))

    # CHECK - verify we get the number of edges that we would expect to get
    # ASSUMPTION: there are two triples per class-instance we want to remove
    if len(update_graph) != len(results_uri) - (len(iri_map)*2):
        raise Exception('ERROR: The number of triples is incorrect!')
    else:
        # serialize graph to n-triples
        update_graph.serialize(destination=output, format='nt')



def NodeMapper(input_graph, output_trip_ints, output_map):
    """Function takes as inputs a file/path to RDF graph, a file/path to write node-integer identifier map,
    and a file/path specifying a location to write integer edges to. The function converts node labels to integers
    and stores a mapping back to the node labels for future use. The function writes out the node-integer mapping
    file as well as the triples (as integers) to a location specified by input arguments

    :param
        input_graph (str): a file path/file storing an RDF graph

        output_trip_ints (str): name and file path to write out results

        output_map (str): name and file path to write out results

    :return:
        updated edges and edge map are saved to the location specified by the input arguments

    """
    # create dictionary for mapping and list to write edges to
    triple_mapper = {}
    update_graph_ints = []

    # build graph from input file
    graph = Graph()
    graph.parse(input_graph)

    # open file to write to
    outfile = open(output_trip_ints, "w")

    # counter to generate ints
    counter = 0

    for edge in graph:
        print(edge)
        if edge[0] not in triple_mapper:
            counter += 1
            triple_mapper[edge[0]] = counter
        if edge[1] not in triple_mapper:
            counter += 1
            triple_mapper[edge[1]] = counter
        if edge[2] not in triple_mapper:
            counter += 1
            triple_mapper[edge[2]] = counter

        # convert edge labels to ints
        update_graph_ints.append([triple_mapper[edge[0]], triple_mapper[edge[1]], triple_mapper[edge[2]]])

        # write edge to text file
        outfile.write('%d' % triple_mapper[edge[0]] + '\t' + '%d' % triple_mapper[edge[1]] + '\t' +
                         '%d' % triple_mapper[edge[2]] + '\n')
    # close file
    outfile.close()

    # CHECK - verify we get the number of edges that we would expect to get
    if len(graph) != len(update_graph_ints):
        raise Exception('ERROR: The number of triples is incorrect!')
    else:
        # write iri dictionary to file
        with open(output_map, "w") as fp:
            json.dump(triple_mapper, fp)

