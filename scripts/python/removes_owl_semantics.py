#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import rdflib

from networkx import networkx
from rdflib.extras.external_graph_libs import *
from tqdm import tqdm


class OWLNETS(object):
    """Class removes OWL semantics from an ontology or knowledge graph using the OWL-NETS method.

    OWL Semantics are nodes and edges in the graph that are needed in order to create a rich semantic representation
    and to do things like run reasoners. Many of these nodes and edges  and The first step of this class
    is to convert an input rdflib graph into a networkx multidigraph object. Once the
    knowledge graph has been converted, it is queried to obtain all owl:Class type nodes. Using the list of classes,
    the multidigraph is queried in order to identify

    Attributes:
        knowledge_graph: An RDFLib object.
        uuid_map: A dictionary dictionary that stores the mapping between each class and its instance.
        nx_multidigraph: A networkx graph object.
        class_list: A list of owl classes from the input knowledge graph.
    """

    def __init__(self, knowledge_graph: rdflib.Graph, uuid_class_map: dict):
        self.knowledge_graph = knowledge_graph
        self.uuid_map = uuid_class_map
        self.class_list: list = None

        # convert knowledge graph to multidigraph
        print('Converting knowledge graph to MultiDiGraph. Please be patient, this process can take ~50 minutes')
        self.nx_multidigraph = rdflib_to_networkx_multidigraph(knowledge_graph)

        # knowledge_graph = Graph()
        # knowledge_graph.parse('resources/ontologies/hp_with_imports.owl')

        # # convert knowledge graph to multidigraph
        # self.nx_graph: Graph = nx.MultiDiGraph()
        #
        # # add edges with attributes
        # for edge in tqdm(knowledge_graph):
        #     nx_graph.add_edge(str(edge[0]), str(edge[2]))
        #     nx_graph[str(edge[0])][str(edge[2])][0]['predicate'] = [str(edge[1])]

        # # convert RDF graph to multidigraph (the ontology is large so this takes ~45 minutes)
        # import time
        # start = time.time()
        # print('Converting knowledge graph to MultiDiGraph. Note, this process takes ~50 minutes')
        # self.nx_multidigraph = rdflib_to_networkx_multidigraph(knowledge_graph)
        # end = time.time()
        # print('Total time to run code: {} seconds'.format(end - start))

    def finds_classes(self):
        """Queries a knowledge graph and returns a list of all owl:Class objects in the graph.

        Returns:
            class_list: A list of all of the class in the graph.

        Raises:
            An error is raised if the query returns no nodes of type owl:Class.
        """

        # find all classes in graph
        kg_classes = self.knowledge_graph.query(
            """SELECT DISTINCT ?c
                   WHERE {?c rdf:type owl:Class . }
                   """, initNs={'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                                'owl': 'http://www.w3.org/2002/07/owl#'}
                                          )

        # convert results to list of classes
        class_list = [str(res[0]) for res in tqdm(kg_classes)]

        if len(class_list) > 0:
            self.class_list = class_list
        else:
            raise Exception('ERROR: No classes returned from query.')

    @staticmethod
    def recurses_ontology_axioms(seen_nodes: list, axioms: list):
        """Function recursively searches a list of knowledge graph nodes, tracking those nodes it has visited. Once
        it has visited all nodes in the input axioms list, a final unique list of relevant nodes is returned. This
        list is assumed to include all necessary nodes needed to re-create an OWL equivalentClass.

        Args:
            seen_nodes: A list which may or may not contain knowledge graph nodes.
            axioms:

        Returns:
            seen_nodes: A list of knowledge graph nodes.
        """

        search_axioms, tracked_nodes = [], []

        for axiom in axioms:
            for element in axiom:
                if not element.startswith('http'):
                    if element not in seen_nodes:
                        tracked_nodes.append(element)
                        search_axioms += list(nx_multidigraph.out_edges(element, keys=True))

        if len(tracked_nodes) > 0:
            seen_nodes += list(set(tracked_nodes))
            return recurses_ontology_axioms(seen_nodes, search_axioms)
        else:
            return seen_nodes

    @staticmethod
    def parses_constructors(class_node: rdflib.term, constructor_type: list, cardinality: list):
        """Traverses a list of rdflib objects and depending on the constructor type (i.e. owl:intersectionOf or
        owl:unionOf), the original set of edges used to the construct the class_node are edited, such that all
        owl-encoded information is removed.

        Examples of the transformations performed for these constructor types and owl:restrictions are shown below:
            - owl:intersectionOf: HP_0000340, turns into:
                - HP_0000340, RO_0000086, PATO_0001481
                - HP_0000340, RO_0000052, UBERON_0008200
                - HP_0000340, RO_0002573, PATO_0000460

            - owl:unionOf: HP_0000597, turns into:
                - HP_0000597, rdfs:subClassOf, HP_0000602
                - HP_0000597, rdfs:subClassOf, HP_0007715

        Args:
            class_node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
            constructor_type: A list of rdflib objects.
            cardinality: A list of owl-encoded classes which are constructed using cardinality.

        Returns:
            A list of lists, where:
                - cleaned_classes: A list of tuples, where each tuple represents a class which has had the OWL
                  semantics removed.
                - cardinality: A list containing class names which were part of an intersection constructor and
                  included cardinality.
        Raises:
            An error is raised if trying to index an axiom using a keyword which cannot be found within the axiom.
        """

        # TODO: check for cardinality!!

        constructor_parts = list(nx_multidigraph.out_edges(constructor_type[0][1], keys=True))

        # get intersection description
        if 'intersectionOf' in constructor_type[0][2]:
            property_edge = rdflib.URIRef('http://purl.obolibrary.org/obo/RO_0000086')
        else:
            property_edge = rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf')

        cleaned_classes = [(class_node, property_edge,
                            rdflib.URIRef([str(x[1]) for x in constructor_parts if 'first' in str(x[2])][0]))]

        # get intersection parts from constructor
        anonymous_node = [x for x in constructor_parts if 'first' not in str(x[2])][0][1]
        anonymous_node_axioms = recurses_ontology_axioms([], list(nx_multidigraph.out_edges(anonymous_node, keys=True)))
        edges = [list(nx_multidigraph.out_edges(x, keys=True)) for x in anonymous_node_axioms]

        # loop over edge pieces and reconstruct cleaned class edges
        for axioms in edges:
            part_one, part_two = '', ''

            try:
                if 'intersectionOf' in constructor_type[0][2]:
                    part_one = [x[1] for x in axioms if 'onproperty' in x[2].lower()][0]
                    part_two = [x[1] for x in axioms if 'somevaluesfrom' in x[2].lower()][0]
                else:
                    part_one = property_edge
                    part_two = [x[1] for x in axioms if 'first' in x[2].lower()][0]
            except IndexError:
                pass

            # only add complete edges
            if len(part_one) > 0 and len(part_two) > 0: cleaned_classes.append((class_node, part_one, part_two))

        return cleaned_classes

    @staticmethod
    def parses_restrictions(class_node: rdflib.term, edge: rdflib.term, cardinality: list):
        """Traverses a list of rdflib objects and depending on the constructor type (i.e. owl:intersectionOf or
        owl:unionOf), the original set of edges used to the construct the class_node are edited, such that all
        owl-encoded information is removed.

        Examples of the transformations performed for these constructor types and owl:restrictions are shown below:
            - owl:restriction CHEBI_16782





        Args:
            class_node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
            edge: An rdflib object that references an anonymous node.
            cardinality: A list of owl-encoded classes which are constructed using cardinality.

        Returns:
            A list of lists, where:
                - cleaned_classes: A list of tuples, where each tuple represents a class which has had the OWL
                  semantics removed.
                - cardinality: A list containing class names which were part of an intersection constructor and
                  included cardinality.
        Raises:
            An error is raised if trying to index an axiom using a keyword which cannot be found within the axiom.
        """
        part_one, part_two = '', ''

        # get edges involved in restriction
        restriction_parts = list(nx_multidigraph.out_edges(edge[1], keys=True))

        # get pieces of restriction need to decode class
        try:
            part_one = [x[1] for x in restriction_parts if 'someValuesFrom' in x[2]][0]
            part_two = [x[1] for x in restriction_parts if 'onProperty' in x[2]][0]
        except IndexError:
            pass

        # only add complete edges
        if len(on_property) > 0 and len(some_values_from) > 0: cleaned_classes.append((class_node, part_one, part_two))

        return cleaned_class

    def cleans_owl_encoded_classes(self):
        """Loops over a list of owl classes in a knowledge graph searching for edges that include owl:equivalentClass
        nodes (i.e. to find classes assembled using owl constructors) and rdfs:subClassof nodes (i.e. to find
        owl:restrictions). Once these edges are found, the method loops over the in and out edges of anonymous nodes
        in the edges in order to eliminate the owl-encoded nodes.

        Returns:
            cleaned_classes: A list of tuples, where each tuple represents a class which has had the OWL
                  semantics removed.
        """

        # TODO - CHECK FOR CARDINALITY!!

        # set up needed vars
        cleaned_classes, complement_constructors, cardinality = [], [], []

        # loop over each class and identify classes with restrictions
        for cls in tqdm(class_list):
            node = rdflib.URIRef(cls) if 'http' in cls else rdflib.BNode(cls)
            in_edges = list(nx_multidigraph.out_edges(node, keys=True))

            if all(x for x in in_edges if x[2] == rdflib.URIRef('http://www.w3.org/2002/07/owl#annotatedSource')):
                for edge in list(nx_multidigraph.out_edges(node, keys=True)):

                    # find equivalent classes
                    if edge[2] == rdflib.URIRef('http://www.w3.org/2002/07/owl#equivalentClass'):
                        for nodes in list(nx_multidigraph.out_edges(edge[1], keys=True)):
                            if rdflib.URIRef('http://www.w3.org/2002/07/owl#someValuesFrom') in nodes[2]:
                                constructor = list(nx_multidigraph.out_edges(nodes[1], keys=True))
                                constructor_type = [x for x in constructor if 'type' not in x[2]]

                                # check constructor type - ignore owl:complementOf
                                if 'complementOf' in constructor_type[0][2]:
                                    complement_constructors.append([str(x) for x in constructor_type[0]])
                                    self.class_list.remove(node)
                                elif 'intersectionOf' in constructor_type[0][2] or 'unionOf' in constructor_type[0][2]:
                                    cleaned_classes = parses_constructors(node, constructor_type, [])
                                else:
                                    pass
                    # find restrictions
                    else:
                        if rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf') in edge:
                            cleaned_classes = parses_restrictions(node, edge[1], cardinality)

        # print stats on class types that were removed
        print('{} owl classes constructed using cardinality were removed\n'.format(len(cardinality)))
        print('{} owl classes constructed using owl:complementOf were removed\n'.format(len(complement_constructors)))

        return cleaned_classes

    def removes_edges_with_owl_semantics(self):
        """Filters the knowledge graph with the goal of removing all edges that contain entities that are needed to
        support owl semantics, but are not biologically meaningful. For example:

            REMOVE - edge needed to support owl semantics that are not biologically meaningful:
                subject: http://purl.obolibrary.org/obo/CLO_0037294
                predicate: owl:AnnotationProperty
                object: rdf:about="http://purl.obolibrary.org/obo/CLO_0037294"

            KEEP - biologically meaningful edges:
                subject: http://purl.obolibrary.org/obo/CHEBI_16130
                predicate: http://purl.obolibrary.org/obo/RO_0002606
                object: http://purl.obolibrary.org/obo/HP_0000832

        Additionally, all class-instances which appear as hash are reverted back to the original url. For examples:
            Instance hash: https://github.com/callahantiff/PheKnowLator/obo/ext/925298d1-7b95-49de-a21b-27f03183f57a
            Reverted to: http://purl.obolibrary.org/obo/CHEBI_24505

        Returns:
            An RDFlib graph that has been cleaned.
        """

        # read in and reverse dictionary to map IRIs back to labels
        uuid_map = {val: key for (key, val) in self.kg_uuid_map.items()}

        # from those triples with URIs, remove triples that are about instances of classes
        update_graph = Graph()

        for edge in tqdm(self.graph):
            if not any(str(x) for x in edge if not str(x).startswith('http')):
                if any(x for x in edge[0::2] if str(x) in uuid_map.keys()):
                    if str(edge[2]) in uuid_map.keys() and 'ns#type' not in str(edge[1]):
                        update_graph.add((edge[0], edge[1], URIRef(uuid_map[str(edge[2])])))
                    else:
                        update_graph.add((URIRef(uuid_map[str(edge[0])]), edge[1], edge[2]))

                # catch and remove all owl
                elif not any(str(x) for x in edge[0::2] if '#' in str(x)):
                    if not any(str(x) for x in edge if ('ns#type' in str(x)) or ('PheKnowLator' in str(x))):
                        update_graph.add(edge)
                else:
                    pass

        # print kg statistics
        edges = len(set(list(update_graph)))
        nodes = len(set([str(node) for edge in list(update_graph) for node in edge[0::2]]))
        print('\nThe filtered KG contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        # add ontology annotations
        update_graph_annot = self.adds_ontology_annotations(self.full_kg.split('/')[-1], update_graph)

        # serialize edges
        update_graph_annot.serialize(destination=self.write_location + self.full_kg, format='xml')

        # apply OWL API formatting to file
        self.ontology_file_formatter(self.write_location + self.full_kg)

        return update_graph
