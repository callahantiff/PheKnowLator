import glob
import logging
import os
import ray
import shutil
import unittest
import warnings

from rdflib import Graph, BNode, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from typing import Dict, List, Set, Tuple

from pkt_kg.owlnets import OwlNets
from pkt_kg.utils import adds_edges_to_graph

# set namespace
obo = Namespace('http://purl.obolibrary.org/obo/')


class TestOwlNets(unittest.TestCase):
    """Class to test the OwlNets class from the owlnets script."""

    def setUp(self):
        warnings.simplefilter('ignore', ResourceWarning)

        # initialize file location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data')
        self.dir_loc = os.path.abspath(dir_loc)

        # set-up environment - make temp directory
        dir_loc_resources = os.path.join(current_directory, 'data/resources')
        self.dir_loc_resources = os.path.abspath(dir_loc_resources)
        os.mkdir(self.dir_loc_resources)
        os.mkdir(self.dir_loc_resources + '/knowledge_graphs')
        os.mkdir(self.dir_loc_resources + '/owl_decoding')

        # handle logging
        self.logs = os.path.abspath(current_directory + '/builds/logs')
        logging.disable(logging.CRITICAL)
        if len(glob.glob(self.logs + '/*.log')) > 0: os.remove(glob.glob(self.logs + '/*.log')[0])

        # copy data
        # ontology data
        shutil.copyfile(self.dir_loc + '/ontologies/so_with_imports.owl',
                        self.dir_loc_resources + '/knowledge_graphs/so_with_imports.owl')
        # set-up input arguments
        self.write_location = self.dir_loc_resources + '/knowledge_graphs'
        self.kg_filename = '/so_with_imports.owl'
        # read in knowledge graph
        self.graph = Graph().parse(self.dir_loc_resources + '/knowledge_graphs/so_with_imports.owl', format='xml')
        # initialize class
        self.owl_nets = OwlNets(kg_construct_approach='subclass', graph=self.graph,
                                write_location=self.write_location, filename=self.kg_filename)
        self.owl_nets2 = OwlNets(kg_construct_approach='instance', graph=self.graph,
                                 write_location=self.write_location, filename=self.kg_filename)

        # update class attributes
        dir_loc_owltools = os.path.join(current_directory, 'utils/owltools')
        self.owl_nets.owl_tools = os.path.abspath(dir_loc_owltools)
        self.owl_nets2.owl_tools = os.path.abspath(dir_loc_owltools)

        return None

    def test_initialization_state(self):
        """Tests the class initialization state."""

        # write_location
        self.assertIsInstance(self.write_location, str)
        self.assertEqual(self.dir_loc_resources + '/knowledge_graphs', self.write_location)
        self.assertIsInstance(self.write_location, str)
        self.assertEqual(self.dir_loc_resources + '/knowledge_graphs', self.write_location)

        return None

    def test_initialization_owltools_default(self):
        """Tests the class initialization state for the owl_tools parameter when no default argument is passed."""

        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename)

        self.assertEqual(owl_nets.owl_tools, './pkt_kg/libs/owltools')

        return None

    def test_initialization_owltools(self):
        """Tests the class initialization state for the owl_tools parameter when an argument is passed."""

        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename,
                           owl_tools='test_location')

        self.assertEqual(owl_nets.owl_tools, 'test_location')

        return None

    def test_initialization_support(self):
        """Tests the class initialization state for the support parameter."""

        # when no list is passed
        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename)
        self.assertEqual(owl_nets.support, ['IAO', 'SWO', 'OBI', 'UBPROP'])
        # when an argument is passed
        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename,
                           support=['IAO'])
        self.assertEqual(owl_nets.support, ['IAO'])

        return None

    def test_initialization_top_level(self):
        """Tests the class initialization state for the top_level parameter."""

        # when no list is passed
        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename)
        self.assertEqual(owl_nets.top_level, ['ISO', 'SUMO', 'BFO'])
        # when an argument is passed
        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename,
                           top_level=['BFO'])
        self.assertEqual(owl_nets.top_level, ['BFO'])

        return None

    def test_initialization_relations(self):
        """Tests the class initialization state for the relations parameter."""

        # when no list is passed
        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename)
        self.assertEqual(owl_nets.relations, ['RO'])
        # when an argument is passed
        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename,
                           relations=['RO'])
        self.assertEqual(owl_nets.relations, ['RO'])

        return None

    def test_initialization_state_graph(self):
        """Tests the class initialization state for graphs."""

        # verify input graph object - when wrong data type
        self.assertRaises(TypeError, OwlNets,
                          kg_construct_approach='subclass', graph=1,
                          write_location=self.write_location, filename=self.kg_filename)

        # verify input graph object - when graph file is empty
        self.assertRaises(ValueError, OwlNets,
                          kg_construct_approach='subclass', graph=list(),
                          write_location=self.write_location, filename=self.kg_filename)
        self.assertRaises(ValueError, OwlNets, kg_construct_approach='subclass', graph=[],
                          write_location=self.write_location, filename=self.kg_filename)

        # verify input graph object points to a file that does not exist
        self.assertRaises(OSError, OwlNets, kg_construct_approach='subclass',
                          graph=self.dir_loc_resources + '/knowledge_graphs/so_with_import_FAKE.owl',
                          write_location=self.write_location, filename=self.kg_filename)

        return None

    def test_graph_input_types(self):
        """Tests different graph input types."""

        # when graph is provided
        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.graph,
                           write_location=self.write_location,
                           filename=self.kg_filename,
                           owl_tools='test_location')
        self.assertIsInstance(owl_nets.graph, Graph)

        # when path to graph is provided
        owl_nets = OwlNets(kg_construct_approach='subclass',
                           graph=self.dir_loc_resources + '/knowledge_graphs/so_with_imports.owl',
                           write_location=self.write_location,
                           filename=self.kg_filename,
                           owl_tools='test_location')
        self.assertIsInstance(owl_nets.graph, Graph)

        return None

    def test_initialization_state_construction_approach(self):
        """Tests the class initialization state for construction approach type."""

        self.assertIsInstance(self.owl_nets.kg_construct_approach, str)
        self.assertTrue(self.owl_nets.kg_construct_approach == 'subclass')
        self.assertFalse(self.owl_nets.kg_construct_approach == 'instance')

        return None

    def test_initialization_owl_nets_dict(self):
        """Tests the class initialization state for owl_nets_dict."""

        self.assertIsInstance(self.owl_nets.owl_nets_dict, Dict)
        self.assertIn('decoded_entities', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('cardinality', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('misc', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('negation', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('complementOf', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('disjointWith', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('filtered_triples', self.owl_nets.owl_nets_dict.keys())

        return None

    def test_removes_disjoint_with_axioms(self):
        """Tests the removes_disjoint_with_axioms method."""

        # create test data
        triples = [(BNode('N9f94b'), URIRef('http://www.geneontology.org/formats/oboInOwl#source'),
                    Literal('lexical', datatype=URIRef('http://www.w3.org/2001/XMLSchema#string'))),
                   (BNode('N9f94b'), RDF.type, OWL.Axiom),
                   (BNode('N9f94b'), OWL.AnnotatedTarget, obo.UBERON_0022716),
                   (BNode('N9f94b'), OWL.AnnotatedSource, obo.UBERON_0022352),
                   (BNode('N9f94b'), OWL.AnnotatedProperty, OWL.disjointWith)]
        self.owl_nets.graph = adds_edges_to_graph(Graph(), triples, False)

        # test method
        self.owl_nets.removes_disjoint_with_axioms()
        self.assertTrue(len(self.owl_nets.graph) == 4)

        return None

    def test_removes_edges_with_owl_semantics(self):
        """Tests the removes_edges_with_owl_semantics method."""

        filtered_graph = self.owl_nets.removes_edges_with_owl_semantics()

        self.assertIsInstance(filtered_graph, Graph)
        self.assertEqual(len(filtered_graph), 2328)

        return None

    def test_cleans_decoded_graph(self):
        """Tests the cleans_decoded_graph method when owl has been decoded."""

        self.owl_nets.owl_nets_dict['decoded_classes'] = [1, 2, 3, 4, 5]

        # run method
        filtered_graph = self.owl_nets.cleans_decoded_graph()
        self.assertIsInstance(filtered_graph, Graph)
        self.assertEqual(len(filtered_graph), 2745)

        return None

    def test_recurses_axioms(self):
        """Tests the recurses_axioms method."""

        # run method when passing axioms that include BNodes
        seen_nodes = []
        axioms = [(BNode('N194ae548a89740849c3536d9753d39d8'), OWL.someValuesFrom, obo.SO_0000784)]
        visited_nodes = self.owl_nets.recurses_axioms(seen_nodes, axioms)
        self.assertIsInstance(visited_nodes, List)
        self.assertEqual(len(visited_nodes), 1)
        self.assertIn(BNode('N194ae548a89740849c3536d9753d39d8'), visited_nodes)

        # run method when passing axioms that do not include BNodes
        seen_nodes = []
        axioms = [(obo.SO_0002047, RDF.type, OWL.Class)]
        visited_nodes = self.owl_nets.recurses_axioms(seen_nodes, axioms)
        self.assertIsInstance(visited_nodes, List)
        self.assertEqual(len(visited_nodes), 0)

        return None

    def test_finds_uri(self):
        """Tests the finds_bnode_uri method."""

        # set-up testing data
        triples = [(BNode('N31fefc6d'), RDF.type, OWL.Axiom),
                   (BNode('N31fefc6d'), OWL.annotatedProperty, RDFS.subClassOf),
                   (BNode('N31fefc6d'), OWL.annotatedSource, obo.UBERON_0002373),
                   (BNode('N31fefc6d'), OWL.annotatedTarget, BNode('N26cd7b2c')),
                   (BNode('N26cd7b2c'), RDF.type, OWL.Restriction),
                   (BNode('N26cd7b2c'), OWL.onProperty, obo.RO_0002202),
                   (BNode('N26cd7b2c'), OWL.someValuesFrom, obo.UBERON_0010023),
                   (obo.UBERON_0010023, RDF.type, OWL.Class)]
        self.owl_nets.graph = adds_edges_to_graph(Graph(), triples)

        # test method
        node = self.owl_nets.finds_uri(BNode('N26cd7b2c'), obo.UBERON_0002373)
        self.assertEqual(node, obo.UBERON_0010023)

        return None

    def test_reconciles_axioms(self):
        """Tests the reconciles_axioms method."""

        # set-up testing data
        triples = [(BNode('N31fefc6d'), RDF.type, OWL.Axiom),
                   (BNode('N31fefc6d'), OWL.annotatedProperty, RDFS.subClassOf),
                   (BNode('N31fefc6d'), OWL.annotatedSource, obo.UBERON_0002373),
                   (BNode('N31fefc6d'), OWL.annotatedTarget, BNode('N26cd7b2c')),
                   (BNode('N26cd7b2c'), RDF.type, OWL.Restriction),
                   (BNode('N26cd7b2c'), OWL.onProperty, obo.RO_0002202),
                   (BNode('N26cd7b2c'), OWL.someValuesFrom, obo.UBERON_0010023),
                   (obo.UBERON_0010023, RDF.type, OWL.Class)]
        result = {(BNode('N26cd7b2c'), RDF.type, OWL.Restriction),
                  (BNode('N26cd7b2c'), OWL.onProperty, obo.RO_0002202),
                  (BNode('N26cd7b2c'), OWL.someValuesFrom, obo.UBERON_0010023)}
        self.owl_nets.graph = adds_edges_to_graph(Graph(), triples)

        # test method
        node, matches = self.owl_nets.reconciles_axioms(obo.UBERON_0002373, BNode('N26cd7b2c'))
        self.assertIsInstance(node, URIRef)
        self.assertIsInstance(matches, Set)
        self.assertEqual(sorted(list(matches)), sorted(list(result)))

        return None

    def test_reconciles_classes(self):
        """Tests the reconciles_classes method."""

        # set-up testing data
        triples = [(obo.UBERON_0002374, RDFS.subClassOf, BNode('N41c7c5fd')),
                   (BNode('N41c7c5fd'), RDF.type, OWL.Restriction),
                   (BNode('N41c7c5fd'), OWL.onProperty, obo.BFO_0000050),
                   (BNode('N41c7c5fd'), OWL.someValuesFrom, obo.UBERON_0010544)]
        result = {(BNode('N41c7c5fd'), OWL.someValuesFrom, obo.UBERON_0010544),
                  (BNode('N41c7c5fd'), RDF.type, OWL.Restriction),
                  (BNode('N41c7c5fd'), OWL.onProperty, obo.BFO_0000050)}
        self.owl_nets.graph = adds_edges_to_graph(Graph(), triples)

        # test method
        matches = self.owl_nets.reconciles_classes(obo.UBERON_0002374)
        self.assertIsInstance(matches, Set)
        self.assertEqual(sorted(list(matches)), sorted(list(result)))

        return None

    def test_creates_edge_dictionary(self):
        """Tests the creates_edge_dictionary method."""

        node, edge_dict, cardinality = self.owl_nets.creates_edge_dictionary(obo.SO_0000822)
        self.assertIsInstance(node, URIRef)
        self.assertIsInstance(edge_dict, Dict)
        self.assertEqual(len(edge_dict), 5)
        self.assertIsInstance(edge_dict[list(edge_dict.keys())[0]], Dict)
        self.assertIsInstance(cardinality, Set)
        self.assertEqual(len(cardinality), 0)

        return None

    def test_detects_complement_of_constructed_classes_true(self):
        """Tests the detects_complement_of_constructed_classes method when complementOf is present."""

        # set-up test data
        node_info = {BNode('N6ebac4ecc22240cdafe506f43d240733'): {'complementOf': OWL.Restriction}}

        result = self.owl_nets.detects_complement_of_constructed_classes(node_info, obo.UBERON_0000061)
        self.assertTrue(result)

        return None

    def test_detects_complement_of_constructed_classes_false(self):
        """Tests the detects_complement_of_constructed_classes method when complementOf is not present."""

        # set-up test data
        node_info = {BNode('N6ebac4ecc22240cdafe506f43d240733'): {
            'type': OWL.Restriction, 'onClass': obo.UBERON_0000061, 'onProperty': obo.RO_0002180}}

        result = self.owl_nets.detects_complement_of_constructed_classes(node_info, obo.UBERON_0000061)
        self.assertFalse(result)

        return None

    def test_detects_negation_axioms_true(self):
        """Tests the detects_negation_axioms method for negation axioms when one is present"""

        # set-up test data
        node_info = {BNode('N6ebac4ecc22240cdafe506f43d240733'): {
            'type': OWL.Restriction, 'onClass': obo.UBERON_0000061,
            'onProperty': URIRef('http://purl.obolibrary.org/obo/cl#lacks_part')}}

        result = self.owl_nets.detects_negation_axioms(node_info, obo.UBERON_0000061)
        self.assertTrue(result)

        return None

    def test_detects_negation_axioms_false(self):
        """Tests the detects_negation_axioms method for negation axioms when none present"""

        # set-up test data
        node = obo.UBERON_0000061
        node_info = {BNode('N6ebac4ecc22240cdafe506f43d240733'): {
            'type': OWL.Restriction, 'onClass': obo.UBERON_0000061, 'onProperty': obo.RO_0001111}}

        result = self.owl_nets.detects_negation_axioms(node_info, node)
        self.assertFalse(result)

        return None

    def test_captures_cardinality_axioms(self):
        """Tests the captures_cardinality_axioms method for a cardinality object."""

        # set-up input
        triples = [
            (BNode('N6ebac'), URIRef('http://www.w3.org/2002/07/owl#minQualifiedCardinality'),
             Literal('2', datatype=URIRef('http://www.w3.org/2001/XMLSchema#nonNegativeInteger'))),
            (BNode('N6ebac'), OWL.onClass, obo.UBERON_0000061), (BNode('N6ebac'), RDF.type, OWL.Restriction),
            (BNode('N6ebac'), OWL.onProperty, obo.RO_0002180)
        ]
        self.owl_nets.graph = adds_edges_to_graph(Graph(), triples)

        # test method
        self.owl_nets.captures_cardinality_axioms({str(obo.UBERON_0034923) + ': N6ebac'}, obo.UBERON_0034923)
        card_triples = self.owl_nets.owl_nets_dict['cardinality']
        self.assertIsInstance(card_triples, dict)
        self.assertIsInstance(card_triples['<http://purl.obolibrary.org/obo/UBERON_0034923>'], set)
        self.assertEqual(len(card_triples['<http://purl.obolibrary.org/obo/UBERON_0034923>']), 4)

        return None

    def test_returns_object_property(self):
        """Tests the returns_object_property method."""

        # when sub and obj are PATO terms and property is none
        res1 = self.owl_nets.returns_object_property(obo.PATO_0001199, obo.PATO_0000402, None)
        self.assertIsInstance(res1, URIRef)
        self.assertEqual(res1, RDFS.subClassOf)

        # when sub and obj are NOT PATO terms and property is none
        res2 = self.owl_nets.returns_object_property(obo.SO_0000784, obo.GO_2000380, None)
        self.assertIsInstance(res2, URIRef)
        self.assertEqual(res2, RDFS.subClassOf)

        # when the obj is a PATO term and property is none
        res3 = self.owl_nets.returns_object_property(obo.SO_0000784, obo.PATO_0001199, None)
        self.assertIsInstance(res3, URIRef)
        self.assertEqual(res3, obo.RO_0000086)

        # when the obj is a PATO term and property is NOT none
        res4 = self.owl_nets.returns_object_property(obo.SO_0000784, obo.PATO_0001199, obo.RO_0002202)
        self.assertIsInstance(res4, URIRef)
        self.assertEqual(res4, obo.RO_0000086)

        # when sub is a PATO term and property is NOT none
        res5 = self.owl_nets.returns_object_property(obo.PATO_0001199, obo.SO_0000784, obo.RO_0002202)
        self.assertIsInstance(res5, URIRef)
        self.assertEqual(res5, obo.RO_0002202)

        # when sub is a PATO term and property is none
        res6 = self.owl_nets.returns_object_property(obo.PATO_0001199, obo.SO_0000784, None)
        self.assertEqual(res6, None)

        return None

    def test_parses_subclasses(self):
        """Tests the parses_subclasses method."""

        # set-up input data
        node = obo.UBERON_0010757
        edges = {'type': OWL.Class, 'subClassOf': obo.UBERON_0002238, 'intersectionOf': BNode('N6add87')}
        class_dict = {
            BNode('N2af571'): {'first': BNode('N8a9450'), 'rest': RDF.nil},
            BNode('N5fef06'): {'type': OWL.Class, 'subClassOf': obo.UBERON_0002238, 'intersectionOf': BNode('N6add87')},
            BNode('N6add87'): {'first': obo.UBERON_0010757, 'rest': BNode('N2af571')},
            BNode('N8a9450'): {'type': OWL.Restriction, 'onProperty': obo.BFO_0000050, 'someValuesFrom': obo.NCBI_9606}}

        # test method
        results = self.owl_nets.parses_subclasses(node, edges, class_dict)
        self.assertIsInstance(results[0], set)
        self.assertIsInstance(results[1], dict)
        self.assertEqual(results[0], {(obo.UBERON_0010757, RDFS.subClassOf, obo.UBERON_0002238)})
        self.assertEqual(results[1], {'type': OWL.Class, 'intersectionOf': BNode('N6add87')})

        return None

    def test_parses_anonymous_axioms(self):
        """Tests the parses_anonymous_axioms method."""

        # set-up input variables
        class_dict = {
            BNode('N41aa20'): {'first': obo.SO_0000340, 'rest': BNode('N6e7b')},
            BNode('Nbb739'): {'intersectionOf': BNode('N41aa20'), 'type': OWL.Class},
            BNode('N6e7b'): {'first': BNode('N5119'), 'rest': RDF.nil},
            BNode('N5119'): {
                'onProperty': URIRef('http://purl.obolibrary.org/obo/so#has_origin'),
                'someValuesFrom': obo.SO_0000746, 'type': OWL.Restriction},
            BNode('Na36bfb34a35047838a8df32b37a8ff50'): {
                'someValuesFrom': obo.SO_0000746,
                'type': OWL.Restriction, 'onProperty': URIRef('http://purl.obolibrary.org/obo/so#has_origin')}
        }
        edges = {'first': obo.SO_0000340, 'rest': BNode('N6e7b')}

        # test when first is a URIRef and rest is a BNode
        res1 = self.owl_nets.parses_anonymous_axioms(edges, class_dict)
        self.assertIsInstance(res1, Dict)
        self.assertTrue(len(res1), 2)
        self.assertIn('first', res1.keys())
        self.assertIn('rest', res1.keys())

        # test when first is a BNode and rest is a URIRef
        edges = {'first': BNode('N5119'), 'rest': RDF.nil}
        res2 = self.owl_nets.parses_anonymous_axioms(edges, class_dict)

        self.assertIsInstance(res2, Dict)
        self.assertTrue(len(res2), 3)
        self.assertIn('onProperty', res2.keys())
        self.assertIn('type', res2.keys())
        self.assertIn('someValuesFrom', res2.keys())

        return None

    def test_parses_constructors_intersection(self):
        """Tests the parses_constructors method for the intersectionOf class constructor"""

        # set-up inputs
        node = obo.SO_0000034
        node_info = self.owl_nets.creates_edge_dictionary(node)
        bnodes = set(x for x in self.owl_nets.graph.objects(node, None) if isinstance(x, BNode))
        edges = {k: v for k, v in node_info[1].items() if 'intersectionOf' in v.keys() and k in bnodes}
        edges = node_info[1][list(x for x in bnodes if x in edges.keys())[0]]

        # test method
        res = self.owl_nets.parses_constructors(node, edges, node_info[1])
        self.assertIsInstance(res, Tuple)
        self.assertEqual(res[0], {(obo.SO_0000034, RDFS.subClassOf, obo.SO_0001247)})
        self.assertEqual(len(res[1]), 3)

        return None

    def test_parses_constructors_intersection2(self):
        """Tests the parses_constructors method for the UnionOf class constructor"""

        # set-up inputs
        node = obo.SO_0000078
        node_info = self.owl_nets.creates_edge_dictionary(node)
        bnodes = set(x for x in self.owl_nets.graph.objects(node, None) if isinstance(x, BNode))
        edges = {k: v for k, v in node_info[1].items() if 'intersectionOf' in v.keys() and k in bnodes}
        edges = node_info[1][list(x for x in bnodes if x in edges.keys())[0]]

        # test method
        res = self.owl_nets.parses_constructors(node, edges, node_info[1])
        self.assertIsInstance(res, Tuple)
        self.assertEqual(res[0], {(obo.SO_0000078, RDFS.subClassOf, obo.SO_0000673)})
        self.assertEqual(len(res[1]), 3)

        return None

    def test_parses_restrictions(self):
        """Tests the parses_restrictions method."""

        # set-up inputs
        node = obo.SO_0000078
        node_info = self.owl_nets.creates_edge_dictionary(node)
        bnodes = set(x for x in self.owl_nets.graph.objects(node, None) if isinstance(x, BNode))
        edges = {k: v for k, v in node_info[1].items()
                 if ('type' in v.keys() and v['type'] == OWL.Restriction) and k in bnodes}
        edges = node_info[1][list(x for x in bnodes if x in edges.keys())[0]]

        # test method
        res = self.owl_nets.parses_restrictions(node, edges, node_info[1])
        self.assertIsInstance(res, Tuple)
        self.assertEqual(res[0], {(
            obo.SO_0000078, URIRef('http://purl.obolibrary.org/obo/so#has_quality'), obo.SO_0000880)})
        self.assertEqual(res[1], None)

        return None

    def test_cleans_owl_encoded_entities(self):
        """Tests the cleans_owl_encoded_entities method"""

        # test method
        self.owl_nets.cleans_owl_encoded_entities([obo.SO_0000822])
        self.assertIsInstance(self.owl_nets.graph, Graph)
        self.assertEqual(len(self.owl_nets.graph), 2)
        self.assertEqual(
            sorted([str(x) for y in list(self.owl_nets.graph.triples((None, None, None))) for x in y]),
            ['http://purl.obolibrary.org/obo/SO_0000340', 'http://purl.obolibrary.org/obo/SO_0000746',
             'http://purl.obolibrary.org/obo/SO_0000822', 'http://purl.obolibrary.org/obo/SO_0000822',
             'http://purl.obolibrary.org/obo/so#has_origin', 'http://www.w3.org/2000/01/rdf-schema#subClassOf'])

        return None

    def test_makes_graph_connected_default(self):
        """Tests the makes_graph_connected method using the default argument for common_ancestor."""

        starting_size = len(self.owl_nets.graph)
        connected_graph = self.owl_nets.makes_graph_connected(self.owl_nets.graph)
        self.assertTrue(len(connected_graph) > starting_size)

        return None

    def test_makes_graph_connected_other(self):
        """Tests the makes_graph_connected method using something other than the default arg for common_ancestor."""

        starting_size = len(self.owl_nets.graph)

        # test when bad node is passed
        self.assertRaises(ValueError, self.owl_nets.makes_graph_connected, self.owl_nets.graph, 'SO_0000110')

        # test when good node is passed
        node = 'http://purl.obolibrary.org/obo/SO_0000110'
        connected_graph = self.owl_nets.makes_graph_connected(self.owl_nets.graph, node)
        self.assertTrue(len(connected_graph) > starting_size)

        return None

    def test_purifies_graph_build_none(self):
        """Tests the purifies_graph_build method when kg_construction is None."""

        # initialize method
        owl_nets = OwlNets(graph=self.graph, write_location=self.write_location, filename=self.kg_filename)

        # test method
        self.graph = owl_nets.purifies_graph_build(self.graph)
        self.assertTrue(len(self.graph), 3054)

        return None

    def test_purifies_graph_build_instance(self):
        """Tests the purifies_graph_build method when kg_construction is instance."""

        # initialize method
        owl_nets = OwlNets(kg_construct_approach='instance', graph=self.graph,
                           write_location=self.write_location, filename=self.kg_filename)

        # test method
        self.graph = owl_nets.purifies_graph_build(self.graph)
        self.assertTrue(len(self.graph), 3054)

        return None

    def test_purifies_graph_build_subclass(self):
        """Tests the purifies_graph_build method when kg_construction is subclass."""

        # initialize method
        owl_nets = OwlNets(kg_construct_approach='subclass', graph=self.graph,
                           write_location=self.write_location, filename=self.kg_filename)

        # test method
        self.graph = owl_nets.purifies_graph_build(self.graph)
        self.assertTrue(len(self.graph), 3054)

        return None

    def test_write_out_results_regular(self):
        """Tests the write_out_results method."""

        self.owl_nets.kg_construct_approach = None
        graph1, graph2 = self.owl_nets.runs_owlnets(); ray.shutdown()

        # test graph output
        self.assertIsInstance(graph1, Set)
        self.assertEqual(graph2, None)

        # make sure files are written locally
        nx_mdg_file = 'so_with_imports_OWLNETS_NetworkxMultiDiGraph.gpickle'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/so_with_imports_OWLNETS.nt'))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + nx_mdg_file))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs'
                                                                '/so_with_imports_OWLNETS_decoding_dict.pkl'))

        return None

    def test_write_out_results_subclass_purified(self):
        """Tests the owl_nets method."""

        self.owl_nets.kg_construct_approach = 'subclass'
        graph1, graph2 = self.owl_nets.runs_owlnets(); ray.shutdown()

        # test graph output
        self.assertIsInstance(graph1, Set)
        self.assertIsInstance(graph2, Set)
        self.assertTrue(len(graph2) >= len(graph1))

        # make sure files are written locally for each graph
        # purified
        nx_mdg_file = 'so_with_imports_OWLNETS_SUBCLASS_purified_NetworkxMultiDiGraph.gpickle'
        nt_file = 'so_with_imports_OWLNETS_SUBCLASS_purified.nt'
        dict_file = '/so_with_imports_OWLNETS_SUBCLASS_purified_decoding_dict.pkl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + nt_file))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + nx_mdg_file))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs' + dict_file))
        # regular
        nx_mdg_file = 'so_with_imports_OWLNETS_NetworkxMultiDiGraph.gpickle'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/so_with_imports_OWLNETS.nt'))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + nx_mdg_file))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs'
                                                                '/so_with_imports_OWLNETS_decoding_dict.pkl'))

        return None

    def test_write_out_results_instance_purified(self):
        """Tests the owl_nets method."""

        graph1, graph2 = self.owl_nets2.runs_owlnets(); ray.shutdown()

        # test graph output
        self.assertIsInstance(graph1, Set)
        self.assertIsInstance(graph2, Set)
        self.assertTrue(len(graph2) > len(graph1))

        # make sure files are written locally for each graph
        # purified
        nx_mdg_file = 'so_with_imports_OWLNETS_INSTANCE_purified_NetworkxMultiDiGraph.gpickle'
        nt_file = 'so_with_imports_OWLNETS_INSTANCE_purified.nt'
        dict_file = '/so_with_imports_OWLNETS_INSTANCE_purified_decoding_dict.pkl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + nt_file))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + nx_mdg_file))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs' + dict_file))
        # regular
        nx_mdg_file = 'so_with_imports_OWLNETS_NetworkxMultiDiGraph.gpickle'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/so_with_imports_OWLNETS.nt'))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + nx_mdg_file))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs'
                                                                '/so_with_imports_OWLNETS_decoding_dict.pkl'))

        return None

    def tests_gets_owlnets_dict(self):
        """Tests gets_owlnets_dict method."""

        results = self.owl_nets.gets_owlnets_dict()

        # verify results
        self.assertIsInstance(results, dict)

        return None

    def tests_gets_owlnets_graph(self):
        """Tests gets_owlnets_graphs method."""

        graphs = self.owl_nets.gets_owlnets_graph()

        # verify results
        self.assertIsInstance(graphs, Graph)

        return None

    def tearDown(self):
        warnings.simplefilter('default', ResourceWarning)

        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
