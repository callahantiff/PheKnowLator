#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import rdflib

from networkx import networkx
from rdflib.extras.external_graph_libs import *
from tqdm import tqdm
from typing import Dict, List, Tuple


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
            ValueError: If the query returns no nodes of type owl:Class.
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
            raise ValueError('ERROR: No classes returned from query.')

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
                if isinstance(element, rdflib.BNode):
                    if element not in seen_nodes:
                        tracked_nodes.append(element)
                        search_axioms += list(nx_multidigraph.out_edges(element, keys=True))

        if len(tracked_nodes) > 0:
            seen_nodes += list(set(tracked_nodes))
            return recurses_ontology_axioms(seen_nodes, search_axioms)
        else:
            return seen_nodes

    @staticmethod
    def creates_edge_dictionary(class_node: rdflib.term):
        """Creates a nested edge dictionary from an input class node by obtaining all of the edges that come
        out from the class and then recursively looping over each anonymous out edge node. The outer
        dictionary keys are anonymous nodes and the inner keys are owl:ObjectProperty values from each out
        edge triple that comes out of that anonymous node. For example:
            {rdflib.term.BNode('N3243b60f69ba468687aa3cbe4e66991f'): {
                someValuesFrom': rdflib.term.URIRef('http://purl.obolibrary.org/obo/PATO_0000587'),
                type': rdflib.term.URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                onProperty': rdflib.term.URIRef('http://purl.obolibrary.org/obo/RO_0000086')
                                                                      }
            }

        Args:
            class_node: An rdflib.term object.

        Returns:
            class_edge_dict: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys
                are owl:ObjectProperty values from each out edge triple that comes out of that anonymous node. An
                example of the dictionaries contents are shown above.
        """

        matches, class_edge_dict = [], {}

        # get all edges that come out of class node
        out_edges = [x for axioms in list(nx_multidigraph.out_edges(class_node, keys=True)) for x in axioms]

        # recursively loop over anonymous nodes in out_edges to create a list of relevant edges to rebuild class
        for element in out_edges:
            if isinstance(element, rdflib.BNode):
                for node in recurses_ontology_axioms([], list(nx_multidigraph.out_edges(element, keys=True))):
                    matches += list(nx_multidigraph.out_edges(node, keys=True))

        # create dictionary of edge lists
        for match in matches:
            if match[0] in class_edge_dict:
                class_edge_dict[match[0]][match[2].split('#')[-1]] = {}
                class_edge_dict[match[0]][match[2].split('#')[-1]] = match[1]
            else:
                class_edge_dict[match[0]] = {}
                class_edge_dict[match[0]][match[2].split('#')[-1]] = match[1]

        return class_edge_dict

    @staticmethod
    def returns_object_property(subj_node: rdflib.term, obj_node: rdflib.term, relation: rdflib.term = None):
        """Checks the subject and object node types in order to return the correct type of owl:ObjectProperty. The
        following ObjectProperties are returned for each of the following subject-object types:
            - subj_node and obj_node are not PATO terms --> rdfs:subClassOf
            - subj_node and obj_node are PATO terms --> rdfs:subClassOf
            - subj_node is not a PATO term, but obj_node is a PATO term --> owl:RO_000086

        Args:
            subj_node: An rdflib.term object.
            obj_node: An rdflib.term object.
            relation: An rdflib.term object, which is provided as the value of owl:onProperty.

        Returns:
            An rdflib.term object that represents an owl:ObjectProperty.
        """

        if ('PATO' in subj_node and 'PATO' in obj_node) and not relation:
            return rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf')
        elif ('PATO' not in subj_node and 'PATO' not in obj_node) and not relation:
            return rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf')
        elif 'PATO' not in subj_node and 'PATO' in obj_node:
            return rdflib.URIRef('http://purl.obolibrary.org/obo/RO_0000086')
        else:
            return relation

    @staticmethod
    def parses_anonymous_axioms(edges: Dict, class_dict: Dict):
        """Parses axiom dictionaries that only include anonymous axioms (i.e. 'first' and 'rest') and returns an
        updated axiom dictionary that contains owl:Restrictions or an owl constructor (i.e. owl:unionOf or
        owl:intersectionOf).

        Args:
            edges: A subset of dictionary where keys are owl:Objects (i.e. 'first', 'rest', 'onProperty',
                or 'someValuesFrom').
            class_dict: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys
                are owl:ObjectProperty values from each out edge triple that comes out of that anonymous node.

        Returns:
             updated_edges: A subset of dictionary where keys are owl:Objects (i.e. 'first', 'rest', 'onProperty',
                or 'someValuesFrom').
        """

        if isinstance(edges['first'], rdflib.URIRef) and isinstance(edges['rest'], rdflib.BNode):
            return class_dict[edges['rest']]
        elif isinstance(edges['first'], rdflib.URIRef) and isinstance(edges['rest'], rdflib.URIRef):
            return class_dict[edges['first']]
        elif isinstance(edges['first'], rdflib.BNode) and isinstance(edges['rest'], rdflib.URIRef):
            return class_dict[edges['first']]
        else:
            return {**class_dict[edges['first']], **class_dict[edges['rest']]}

    @staticmethod
    def parses_constructors(class_node: rdflib.URIRef, edges: Dict, class_dict: Dict, relation: rdflib.URIRef = None):
        """Traverses a dictionary of rdflib objects used in the owl:unionOf or owl:intersectionOf constructors, from
        which the original set of edges used to the construct the class_node are edited, such that all owl-encoded
        information is removed. Examples of the transformations performed for these constructor types are shown below:
            - owl:unionOf: CL_0000995, turns into:
                - CL_0000995, rdfs:subClassOf, CL_0001021
                - CL_0000995, rdfs:subClassOf, CL_0001026

            - owl:intersectionOf: PR_000050170, turns into:
                - PR_000050170, rdfs:subClassOf, GO_0071738
                - PR_000050170, RO_0002160, NCBITaxon_9606

        Args:
            class_node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
            edges: A subset of dictionary where keys are owl:Objects (i.e. 'first', 'rest', 'onProperty',
                or 'someValuesFrom').
            class_dict: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys
                are owl:ObjectProperty values from each out edge triple that comes out of that anonymous node.
            relation: An rdflib.URIRef object that defaults to None, but if provided by a user contains an
                owl:onProperty relation.

        Returns:
            cleaned_classes: A list of tuples, where each tuple represents a class which has had the OWL semantics
                removed.
             edge_batch: A subset of dictionary where keys are owl:Objects (i.e. 'first', 'rest', 'onProperty',
                or 'someValuesFrom').
        """

        cleaned_classes = set()
        edge_batch = class_dict[edges['unionOf' if 'unionOf' in edges.keys() else 'intersectionOf']]

        while edge_batch:
            if ('first' in edge_batch.keys() and 'rest' in edge_batch.keys()) and 'type' not in edge_batch.keys():
                if isinstance(edge_batch['first'], rdflib.URIRef) and isinstance(edge_batch['rest'], rdflib.BNode):
                    obj_property = returns_object_property(node, edge_batch['first'], relation)
                    cleaned_classes |= {(class_node, obj_property, edge_batch['first'])}
                    edge_batch = class_dict[edge_batch['rest']]
                elif isinstance(edge_batch['first'], rdflib.URIRef) and isinstance(edge_batch['rest'], rdflib.URIRef):
                    obj_property = returns_object_property(node, edge_batch['first'], relation)
                    cleaned_classes |= {(class_node, obj_property, edge_batch['first'])}
                    edge_batch = None
                else:
                    edge_batch = parses_anonymous_axioms(edge_batch, class_dict)
            else:
                break

        return cleaned_classes, edge_batch

    @staticmethod
    def parses_restrictions(class_node: rdflib.URIRef, edges: Dict, class_dict: Dict) -> Tuple:
        """Parses a subset of a dictionary containing rdflib objects participating in a constructor (i.e.
        owl:intersectionOf or owl:unionOf) and reconstructs the class (referenced by class_node) in order to remove
        owl-encoded information. An example is shown below:
            - owl:restriction PR_000050170, turns into: (PR_000050170, RO_0002180, PR_000050183)

        Args:
            class_node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
            edges: A subset of dictionary where keys are owl:Objects (e.g. 'first', 'rest', 'onProperty',
                'onClass', or 'someValuesFrom').
            class_dict: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys
                are owl:ObjectProperty values from each out edge triple that comes out of that anonymous node.

        Returns:
             cleaned_classes: A list of tuples, where each tuple represents a class which has had the OWL semantics
                removed.
             edge_batch: A subset of dictionary where keys are owl:Objects (e.g. 'first', 'rest', 'onProperty',
                'onClass', or 'someValuesFrom').
        """

        cleaned_classes = set()
        edge_batch = edges
        object_type = 'onClass' if 'onClass' in edge_batch.keys() else 'someValuesFrom'

        if isinstance(edge_batch[object_type], rdflib.URIRef):
            if len(edge_batch) == 3:
                cleaned_classes |= {(class_node, edge_batch['onProperty'], edge_batch[object_type])}
                return cleaned_classes, None
            else:
                cleaned_classes |= {(class_node, edge_batch['onProperty'], edge_batch[object_type])}
                return cleaned_classes, parses_anonymous_axioms(edge_batch, class_dict)
        else:
            axioms = class_dict[edge_batch[object_type]]
            results = parses_constructors(class_node, axioms, class_dict, edge_batch['onProperty'])
            cleaned_classes |= results[0]
            edge_batch = results[1]

            return cleaned_classes, edge_batch

    def cleans_owl_encoded_classes(self):
        """Loops over a list of owl classes in a knowledge graph searching for edges that include owl:equivalentClass
        nodes (i.e. to find classes assembled using owl constructors) and rdfs:subClassof nodes (i.e. to find
        owl:restrictions). Once these edges are found, the method loops over the in and out edges of anonymous nodes
        in the edges in order to eliminate the owl-encoded nodes.

        Returns:
            cleaned_classes: A set of tuples, where each tuple represents a class which has had the OWL semantics
                removed.
        """

        cleaned_classes, complement_constructors, cardinality = set(), set(), set()

        for cls in tqdm(self.class_list):
            node = rdflib.URIRef(cls) if 'http' in cls else rdflib.BNode(cls)
            class_edge_dict = creates_edge_dictionary(node)

            # do not process owl:complementOf constructors
            if any(v for v in class_edge_dict.values() if 'complementOf' in v.keys()):
                complement_constructors |= {node}
                class_list.remove(node)
            else:
                out_edges = list(nx_multidigraph.out_edges(node, keys=True))
                semantic_chunk_nodes = [x[1] for x in out_edges if isinstance(x[1], rdflib.BNode)]

                # decode owl-encoded edges
                for element in semantic_chunk_nodes:
                    edges = class_edge_dict[element]

                    # check for cardinality -- keeping a count of classes constructed using cardinality
                    if any(v for v in edges.keys() if 'cardinality' in v.lower()):
                        cardinality |= {'{}: {}'.format(node, element)}
                        del edges[[key for key in edges.keys() if 'cardinality' in key.lower()][0]]

                    while edges:
                        if 'unionOf' in edges.keys():
                            results = parses_constructors(node, edges, class_edge_dict)
                            cleaned_classes |= results[0]
                            edges = results[1]
                        elif 'intersectionOf' in edges.keys():
                            results = parses_constructors(node, edges, class_edge_dict)
                            cleaned_classes |= results[0]
                            edges = results[1]
                        elif 'Restriction' in edges['type']:
                            results = parses_restrictions(node, edges, class_edge_dict)
                            cleaned_classes |= results[0]
                            edges = results[1]
                        else:
                            print('STOP!!')
                            edges = None

        print('\nFINISHED: Completed Decoding Owl-Encoded Classes')
        print('{} owl class elements containing cardinality were ignored\n'.format(len(cardinality)))
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
