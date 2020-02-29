#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import networkx   # type: ignore
import pickle
import rdflib  # type: ignore

from collections import Counter
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Optional, Set, Tuple

# TODO: mypy throws errors for lines 319 and 441 for optional[dict] usage, this is an existing bug in mypy
# https://github.com/python/mypy/issues/4359


class OWLNETS(object):
    """Class removes OWL semantics from an ontology or knowledge graph using the OWL-NETS method.

    OWL Semantics are nodes and edges in the graph that are needed in order to create a rich semantic representation
    and to do things like run reasoners. Many of these nodes and edges are not clinically or biologically meaningful.
    This class is designed to decode all owl-encoded classes and return a knowledge graph that is semantically rich and
    clinically and biologically meaningful.

    Attributes:
        knowledge_graph: An RDFLib object.
        uuid_map: A dictionary that stores the mapping between each class and its instance.
        write_location: A file path used for writing knowledge graph data.
        full_kg: A string containing the filename for the full knowledge graph.
        nx_multidigraph: A networkx graph object that contains the same edges as knowledge_graph.
        class_list: A list of owl classes from the input knowledge graph.

    Raises:
        TypeError: If knowledge_graph is not an rdflib.graph object.
        ValueError: If knowledge_graph is an empty rdflib.graph object.
    """

    def __init__(self, knowledge_graph: rdflib.Graph, uuid_class_map: Dict, write_location: str, full_kg: str) -> None:

        # verify input graphs
        if not isinstance(knowledge_graph, rdflib.Graph):
            raise TypeError('knowledge_graph must be an RDFLib Graph Object.')
        elif len(knowledge_graph) == 0:
            raise ValueError('knowledge_graph is empty.')
        else:
            self.knowledge_graph = knowledge_graph

        self.uuid_map = uuid_class_map if uuid_class_map else None  # type: ignore
        self.write_location = write_location
        self.full_kg = full_kg
        self.class_list: Optional[List[rdflib.URIRef]] = None

        # convert RDF graph to multidigraph
        print('\nConverting knowledge graph to MultiDiGraph. Note, this process can take up to 20 minutes.')
        self.nx_multidigraph: networkx.MultiDiGraph = networkx.MultiDiGraph()

        for s, p, o in tqdm(self.knowledge_graph):
            self.nx_multidigraph.add_edge(s, o, **{'key': p})

    def finds_classes(self) -> None:
        """Queries a knowledge graph and returns a list of all owl:Class objects in the graph.

        Returns:
            class_list: A list of all of the class in the graph.

        Raises:
            ValueError: If the query returns zero nodes with type owl:Class.
        """

        print('Querying Knowledge Graph to Obtain all OWL:Class Nodes')

        # find all classes in graph
        kg_classes = self.knowledge_graph.query(
            """SELECT DISTINCT ?c
                   WHERE {?c rdf:type owl:Class . }
                   """, initNs={'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                                'owl': 'http://www.w3.org/2002/07/owl#'}
        )

        # convert results to list of classes
        class_list = [res[0] for res in tqdm(kg_classes) if isinstance(res[0], rdflib.URIRef)]

        if len(class_list) > 0:
            self.class_list = class_list
        else:
            raise ValueError('ERROR: No classes returned from query.')

        return None

    def recurses_axioms(self, seen_nodes: List[rdflib.BNode], axioms: List[Any]) -> List[rdflib.BNode]:
        """Function recursively searches a list of knowledge graph nodes and tracks the nodes it has visited. Once
        it has visited all nodes in the input axioms list, a final unique list of relevant nodes is returned. This
        list is assumed to include all necessary nodes needed to re-create an OWL equivalentClass.

        Args:
            seen_nodes: A list which may or may not contain knowledge graph nodes.
            axioms: A list of axioms, e.g. [(rdflib.term.BNode('N3e23fe5f05ff4a7d992c548607c86277'),
                                            rdflib.term.URIRef('http://www.w3.org/2002/07/owl#Class'),
                                            rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'))]

        Returns:
            seen_nodes: A list of knowledge graph nodes.
        """

        search_axioms, tracked_nodes = [], []  # type: ignore

        for axiom in axioms:
            for element in axiom:
                if isinstance(element, rdflib.BNode):
                    if element not in seen_nodes:
                        tracked_nodes.append(element)
                        search_axioms += list(self.nx_multidigraph.out_edges(element, keys=True))

        if len(tracked_nodes) > 0:
            seen_nodes += list(set(tracked_nodes))
            return self.recurses_axioms(seen_nodes, search_axioms)
        else:
            return seen_nodes

    def creates_edge_dictionary(self, node: rdflib.URIRef) -> Tuple[Dict, Set]:
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

        While creating the dictionary, if cardinality is used then a formatted string that contains the class node
        and the anonymous node naming the element that includes cardinality is constructed and added to a set.

        Args:
            node: An rdflib.term object.

        Returns:
            class_edge_dict: A nested dictionary. The outer dictionary keys are anonymous nodes and the inner keys
                are owl:ObjectProperty values from each out edge triple that comes out of that anonymous node. An
                example of the dictionaries contents are shown above.
            cardinality: A set of strings, where each string is formatted such that the substring that occurs before
                the ':' is the class node and the substring after the ':' is the anonymous node naming the element where
                cardinality was used.
        """

        matches, class_edge_dict, cardinality = [], {}, set()  # type: ignore

        # get all edges that come out of class node
        out_edges = [x for axioms in list(self.nx_multidigraph.out_edges(node, keys=True)) for x in axioms]

        # recursively loop over anonymous nodes in out_edges to create a list of relevant edges to rebuild class
        for axiom in out_edges:
            if isinstance(axiom, rdflib.BNode):
                for element in self.recurses_axioms([], list(self.nx_multidigraph.out_edges(axiom, keys=True))):
                    matches += list(self.nx_multidigraph.out_edges(element, keys=True))

        # create dictionary of edge lists
        for match in matches:
            if 'cardinality' in str(match[2]).lower():
                cardinality |= {'{}: {}'.format(node, match[0])}
                pass
            else:
                if match[0] in class_edge_dict:
                    class_edge_dict[match[0]][match[2].split('#')[-1]] = {}
                    class_edge_dict[match[0]][match[2].split('#')[-1]] = match[1]
                else:
                    class_edge_dict[match[0]] = {}
                    class_edge_dict[match[0]][match[2].split('#')[-1]] = match[1]

        return class_edge_dict, cardinality

    @staticmethod
    def returns_object_property(sub: rdflib.URIRef, obj: rdflib.URIRef, prop: rdflib.URIRef = None) -> rdflib.URIRef:
        """Checks the subject and object node types in order to return the correct type of owl:ObjectProperty. The
        following ObjectProperties are returned for each of the following subject-object types:
            - sub + obj are not PATO terms + prop is None --> rdfs:subClassOf
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

        if ('PATO' in sub and 'PATO' in obj) and not prop:
            return rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf')
        elif ('PATO' not in sub and 'PATO' not in obj) and not prop:
            return rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf')
        elif 'PATO' not in sub and 'PATO' in obj:
            return rdflib.URIRef('http://purl.obolibrary.org/obo/RO_0000086')
        else:
            return prop

    @staticmethod
    def parses_anonymous_axioms(edges: Dict, class_dict: Dict) -> Dict:
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

    def parses_constructors(self, node: rdflib.URIRef, edges: Dict, class_dict: Dict, relation: rdflib.URIRef = None)\
            -> Tuple[Set, Optional[Dict]]:
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
            node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
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

        cleaned_classes = set()  # type: ignore
        edge_batch = class_dict[edges['unionOf' if 'unionOf' in edges.keys() else 'intersectionOf']]

        while edge_batch:
            if ('first' in edge_batch.keys() and 'rest' in edge_batch.keys()) and 'type' not in edge_batch.keys():
                if isinstance(edge_batch['first'], rdflib.URIRef) and isinstance(edge_batch['rest'], rdflib.BNode):
                    obj_property = self.returns_object_property(node, edge_batch['first'], relation)
                    cleaned_classes |= {(node, obj_property, edge_batch['first'])}
                    edge_batch = class_dict[edge_batch['rest']]
                elif isinstance(edge_batch['first'], rdflib.URIRef) and isinstance(edge_batch['rest'], rdflib.URIRef):
                    obj_property = self.returns_object_property(node, edge_batch['first'], relation)
                    cleaned_classes |= {(node, obj_property, edge_batch['first'])}
                    edge_batch = None
                else:
                    edge_batch = self.parses_anonymous_axioms(edge_batch, class_dict)
            else:
                break

        return cleaned_classes, edge_batch

    def parses_restrictions(self, node: rdflib.URIRef, edges: Dict, class_dict: Dict) -> Tuple[Set, Optional[Dict]]:
        """Parses a subset of a dictionary containing rdflib objects participating in a constructor (i.e.
        owl:intersectionOf or owl:unionOf) and reconstructs the class (referenced by node) in order to remove
        owl-encoded information. An example is shown below:
            - owl:restriction PR_000050170, turns into: (PR_000050170, RO_0002180, PR_000050183)

        Args:
            node: An rdflib term of type URIRef or BNode that references an OWL-encoded class.
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

        cleaned_classes = set()  # type: ignore
        edge_batch = edges
        object_type = [x for x in edge_batch.keys() if x not in ['type', 'first', 'rest', 'onProperty']][0]

        if isinstance(edge_batch[object_type], rdflib.URIRef) or isinstance(edge_batch[object_type], rdflib.Literal):
            object_node = node if object_type == 'hasSelf' else edge_batch[object_type]

            if len(edge_batch) == 3:
                cleaned_classes |= {(node, edge_batch['onProperty'], object_node)}
                return cleaned_classes, None
            else:
                cleaned_classes |= {(node, edge_batch['onProperty'], object_node)}
                return cleaned_classes, self.parses_anonymous_axioms(edge_batch, class_dict)
        else:
            axioms = class_dict[edge_batch[object_type]]

            if 'unionOf' in axioms.keys() or 'intersectionOf' in axioms.keys():
                results = self.parses_constructors(node, axioms, class_dict, edge_batch['onProperty'])
                cleaned_classes |= results[0]
                edge_batch = results[1]
                return cleaned_classes, edge_batch
            else:
                return cleaned_classes, axioms

    def cleans_owl_encoded_classes(self) -> Dict:
        """Loops over a list of owl classes in a knowledge graph searching for edges that include owl:equivalentClass
        nodes (i.e. to find classes assembled using owl constructors) and rdfs:subClassof nodes (i.e. to find
        owl:restrictions). Once these edges are found, the method loops over the in and out edges of anonymous nodes
        in the edges in order to eliminate the owl-encoded nodes.

        Returns:
            cleaned_class_dict: A dictionary where keys are owl-encoded nodes and values are a set of tuples, where
                where each tuple represents a triple which has had the OWL semantics removed. This dictionary is also
                saved locally.
        """

        cleaned_class_dict, complement_constructors, cardinality, misc = dict(), set(), set(), []  # type: ignore

        for node in tqdm(self.class_list):
            node_information = self.creates_edge_dictionary(node)
            class_edge_dict = node_information[0]
            cardinality |= node_information[1]

            if len(class_edge_dict) == 0:
                pass
            elif any(v for v in class_edge_dict.values() if 'complementOf' in v.keys()):
                complement_constructors |= {node}
                pass
            else:
                cleaned_class_dict[node], cleaned_classes = dict(), set()  # type: ignore
                out_edges = list(self.nx_multidigraph.out_edges(node, keys=True))
                semantic_chunk_nodes = [x[1] for x in out_edges if isinstance(x[1], rdflib.BNode)]

                # decode owl-encoded edges
                for element in semantic_chunk_nodes:
                    edges = class_edge_dict[element]

                    while edges:
                        if 'unionOf' in edges.keys():
                            results = self.parses_constructors(node, edges, class_edge_dict)
                            cleaned_classes |= results[0]
                            edges = results[1]
                        elif 'intersectionOf' in edges.keys():
                            results = self.parses_constructors(node, edges, class_edge_dict)
                            cleaned_classes |= results[0]
                            edges = results[1]
                        elif 'Restriction' in edges['type']:
                            results = self.parses_restrictions(node, edges, class_edge_dict)
                            cleaned_classes |= results[0]
                            edges = results[1]
                        else:
                            # catch all other axioms -- only catching owl:onProperty with owl:oneOf
                            misc += [x for x in edges.keys() if x not in ['type', 'first', 'rest', 'onProperty']]
                            edges = None

                # add results to dictionary
                cleaned_class_dict[node]['owl-encoded'] = class_edge_dict
                cleaned_class_dict[node]['owl-decoded'] = cleaned_classes

        # save dictionary of cleaned classes
        pickle.dump(cleaned_class_dict, open(self.write_location + self.full_kg[:-7] + '_OWLNETS_results.pickle', 'wb'))
        # cleaned_class_dict = pickle.load(open(write_location + full_kg[:-7] + '_OWLNETS_results.json', 'rb'))

        # delete networkx graph object to free up memory
        del self.nx_multidigraph

        print('\nFINISHED: Decoded {} owl-encoded classes. Note the following:'.format(len(cleaned_class_dict.keys())))
        print('{} owl class elements containing cardinality were ignored'.format(len(cardinality)))
        print('The following {} misc owl class elements were ignored: {}'.format(len(misc), list(Counter(misc))))
        print('{} owl classes constructed using owl:complementOf were removed\n'.format(len(complement_constructors)))

        return cleaned_class_dict

    @staticmethod
    def adds_cleaned_classes_to_graph(graph: rdflib.Graph, cleaned_class_dict: Dict) -> rdflib.Graph:
        """Adds triples from a dictionary containing a set of decoded triples to a rdflib.graph.

        Args:
            graph: An rdflib.Graph object.
            cleaned_class_dict:

        Returns:
            An rdflib.Graph object that has been updated to include all triples from the cleaned_classes dictionary.
        """

        # add cleaned classes to graph
        for node in tqdm(cleaned_class_dict.keys()):
            for triple in cleaned_class_dict[node]['owl-decoded']:
                graph.add((triple[0], triple[1], triple[2]))

        return graph

    def removes_edges_with_owl_semantics(self) -> rdflib.Graph:
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

        Additionally, all class-instances are reverted back to the original url. For examples:
            Instance hash: https://github.com/callahantiff/PheKnowLator/obo/ext/925298d1-7b95-49de-a21b-27f03183f57a
            Reverted to: http://purl.obolibrary.org/obo/CHEBI_24505

        Returns:
            An RDFlib.Graph that has been cleaned.
        """

        # run OWL-NETS -- running this first in order to free up memory by deleting the networkx object
        print('\nRunning OWL-NETS')
        self.finds_classes()
        cleaned_class_dict = self.cleans_owl_encoded_classes()

        # read in and reverse dictionary to map IRIs back to labels
        reverse_uuid_map = {val: key for (key, val) in self.uuid_map.items() if self.uuid_map}

        # from those triples with URIs, remove triples that are about instances of classes
        update_graph = rdflib.Graph()

        for edge in tqdm(self.knowledge_graph):
            if all(str(x) for x in edge if str(x).startswith('http')):

                # look for created class-instances so they can be reversed
                if any(x for x in edge[0::2] if str(x) in reverse_uuid_map.keys()):
                    if str(edge[2]) in reverse_uuid_map.keys() and 'ns#type' not in str(edge[1]):
                        update_graph.add((edge[0], edge[1], rdflib.URIRef(reverse_uuid_map[str(edge[2])])))
                    else:
                        update_graph.add((rdflib.URIRef(reverse_uuid_map[str(edge[0])]), edge[1], edge[2]))

                # keep edges with nodes that do not include '#' and predicates without 'ns#type'
                elif not any(str(x) for x in edge[0::2] if '#' in str(x)) and 'ns#type' not in str(edge[1]):
                    update_graph.add(edge)
                else:
                    pass

        # add cleaned classes to graph
        update_graph = self.adds_cleaned_classes_to_graph(update_graph, cleaned_class_dict)

        # print kg statistics
        edges = len(set(list(update_graph)))
        nodes = len(set([str(node) for edge in list(update_graph) for node in edge[0::2]]))
        print('\nFINISHED: Completed the Removal of OWL Semantics')
        print('The Decoded Knowledge graph contains: {node} nodes and {edge} edges\n'.format(node=nodes, edge=edges))

        return update_graph
