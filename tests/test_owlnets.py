import glob
import logging
import networkx
import os
import shutil
import unittest

from rdflib import Graph, BNode, Literal, URIRef
from typing import Dict, List, Set, Tuple

from pkt_kg.owlnets import OwlNets
from pkt_kg.utils import adds_edges_to_graph


class TestOwlNets(unittest.TestCase):
    """Class to test the OwlNets class from the owlnets script."""

    def setUp(self):
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

        # write_location
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

    def test_initialization_state_graph(self):
        """Tests the class initialization state for graphs."""

        # verify input graph object - when wrong data type
        self.assertRaises(TypeError, OwlNets,
                          kg_construct_approach='subclass', graph=list(),
                          write_location=self.write_location, filename=self.kg_filename)

        # verify input graph object - when graph file is empty
        self.assertRaises(ValueError, OwlNets, kg_construct_approach='subclass', graph=Graph(),
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
        self.assertIn('owl_nets', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('decoded_classes', self.owl_nets.owl_nets_dict['owl_nets'])
        self.assertIn('complementOf', self.owl_nets.owl_nets_dict['owl_nets'].keys())
        self.assertIn('cardinality', self.owl_nets.owl_nets_dict['owl_nets'].keys())
        self.assertIn('negation', self.owl_nets.owl_nets_dict['owl_nets'].keys())
        self.assertIn('misc', self.owl_nets.owl_nets_dict['owl_nets'].keys())
        self.assertIn('disjointWith', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('filtered_triples', self.owl_nets.owl_nets_dict.keys())
        self.assertIn('{}_approach_purified'.format(self.owl_nets.kg_construct_approach),
                      self.owl_nets.owl_nets_dict.keys())

        return None

    def test_initialization_node_list(self):
        """Tests the class initialization state for node_list."""

        self.assertIsInstance(self.owl_nets.node_list, List)
        self.assertEqual(len(self.owl_nets.node_list), 2573)

        return None

    def test_updates_pkt_namespace_identifiers_instance(self):
        """Tests the updates_pkt_namespace_identifiers method for an instance-based construction approach."""

        # update graph
        edges = (URIRef('https://github.com/callahantiff/PheKnowLator/pkt/Nc07cdd6d483027110022e6e4364a83f1'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://purl.obolibrary.org/obo/CHEBI_2504')), \
                (URIRef('https://github.com/callahantiff/PheKnowLator/pkt/Nc07cdd6d483027110022e6e4364a83f1'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#NamedIndividual')), \
                (URIRef('http://purl.obolibrary.org/obo/CHEBI_2504'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#Class')), \
                (URIRef('https://www.ncbi.nlm.nih.gov/gene/55847'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#NamedIndividual')), \
                (URIRef('https://www.ncbi.nlm.nih.gov/gene/55847'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://purl.obolibrary.org/obo/SO_0001217')), \
                (URIRef('http://purl.obolibrary.org/obo/SO_0001217'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#Class')), \
                (URIRef('https://github.com/callahantiff/PheKnowLator/pkt/Nc07cdd6d483027110022e6e4364a83f1'),
                 URIRef('http://purl.obolibrary.org/obo/RO_0002434'),
                 URIRef('https://www.ncbi.nlm.nih.gov/gene/55847')), \
                (URIRef('http://purl.obolibrary.org/obo/RO_0002434'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#ObjectProperty'))

        self.owl_nets2.graph = adds_edges_to_graph(Graph(), edges)

        # run method to roll back to re-map instances of classes
        self.owl_nets2.updates_pkt_namespace_identifiers()
        self.assertEqual(len(self.owl_nets2.graph), 6)
        self.assertIn((URIRef('http://purl.obolibrary.org/obo/CHEBI_2504'),
                       URIRef('http://purl.obolibrary.org/obo/RO_0002434'),
                       URIRef('https://www.ncbi.nlm.nih.gov/gene/55847')),
                      self.owl_nets2.graph)

        return None

    def test_updates_pkt_namespace_identifiers_subclass(self):
        """Tests the updates_pkt_namespace_identifiers method for a subclass-based construction approach."""

        # update graph
        edges = (URIRef('https://github.com/callahantiff/PheKnowLator/pkt/Nf1f6ce0f4e4eddb81d48e89115facef2'),
                 URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'),
                 URIRef('http://purl.obolibrary.org/obo/DOID_3075')), \
                (URIRef('https://github.com/callahantiff/PheKnowLator/pkt/Nf1f6ce0f4e4eddb81d48e89115facef2'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#Class')), \
                (URIRef('http://purl.obolibrary.org/obo/DOID_3075'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#Class')),\
                (URIRef('https://github.com/callahantiff/PheKnowLator/pkt/Nf1f6ce0f4e4eddb81d48e89115facef2'),
                 URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'),
                 BNode('N4ba9c4585bada420f5f94b3a2c6146e1')), \
                (BNode('N4ba9c4585bada420f5f94b3a2c6146e1'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#Restriction')), \
                (BNode('N4ba9c4585bada420f5f94b3a2c6146e1'),
                 URIRef('http://www.w3.org/2002/07/owl#onProperty'),
                 URIRef('http://purl.obolibrary.org/obo/RO_0003302')),\
                (BNode('N4ba9c4585bada420f5f94b3a2c6146e1'),
                 URIRef('http://www.w3.org/2002/07/owl#someValuesFrom'),
                 URIRef('http://purl.obolibrary.org/obo/DOID_1080')), \
                (URIRef('http://purl.obolibrary.org/obo/DOID_1080'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#Class')), \
                (URIRef('http://purl.obolibrary.org/obo/RO_0003302'),
                 URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                 URIRef('http://www.w3.org/2002/07/owl#ObjectProperty'))

        self.owl_nets.graph = adds_edges_to_graph(Graph(), edges)

        # run method to roll back to re-map instances of classes
        self.owl_nets.updates_pkt_namespace_identifiers()
        self.assertEqual(len(self.owl_nets.graph), 7)
        self.assertTrue(((URIRef('https://github.com/callahantiff/PheKnowLator/pkt/Nf1f6ce0f4e4eddb81d48e89115facef2'),
                          URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'),
                          BNode('http://purl.obolibrary.org/obo/DOID_3075'))) not in self.owl_nets.graph)

        return None

    def test_converts_rdflib_to_networkx_multidigraph(self):
        """Tests the converts_rdflib_to_networkx_multidigraph method."""

        self.owl_nets.converts_rdflib_to_networkx_multidigraph()
        self.assertIsInstance(self.owl_nets.nx_mdg, networkx.MultiDiGraph)
        self.assertTrue(len(self.owl_nets.nx_mdg) == 20277)

        return None

    def test_removes_disjoint_with_axioms(self):
        """Tests the removes_disjoint_with_axioms method."""

        # create test data
        graph = Graph()
        triples = [(BNode('N9f94b1ff016149d0859c059b74e5360f'),
                    URIRef('http://www.geneontology.org/formats/oboInOwl#source'),
                    Literal('lexical', datatype=URIRef('http://www.w3.org/2001/XMLSchema#string'))),
                   (BNode('N9f94b1ff016149d0859c059b74e5360f'),
                    URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                    URIRef('http://www.w3.org/2002/07/owl#Axiom')),
                   (BNode('N9f94b1ff016149d0859c059b74e5360f'),
                    URIRef('http://www.w3.org/2002/07/owl#annotatedTarget'),
                    URIRef('http://purl.obolibrary.org/obo/UBERON_0022716')),
                   (BNode('N9f94b1ff016149d0859c059b74e5360f'),
                    URIRef('http://www.w3.org/2002/07/owl#annotatedSource'),
                    URIRef('http://purl.obolibrary.org/obo/UBERON_0022352')),
                   (BNode('N9f94b1ff016149d0859c059b74e5360f'),
                    URIRef('http://www.w3.org/2002/07/owl#annotatedProperty'),
                    URIRef('http://www.w3.org/2002/07/owl#disjointWith'))]
        for x in triples: graph.add(x)
        self.owl_nets.graph = graph

        # test method
        self.owl_nets.removes_disjoint_with_axioms()
        self.assertTrue(len(self.owl_nets.graph) == 0)

        return None

    def test_removes_edges_with_owl_semantics(self):
        """Tests the removes_edges_with_owl_semantics method."""

        # run method
        filtered_graph = self.owl_nets.removes_edges_with_owl_semantics()

        # check output type
        self.assertIsInstance(filtered_graph, Graph)

        # check output length
        self.assertEqual(len(filtered_graph), 2328)

        return None

    def test_removes_edges_with_owl_semantics_decoded(self):
        """Tests the removes_edges_with_owl_semantics method when owl has been decoded."""

        self.owl_nets.owl_nets_dict['owl_nets']['decoded_classes'] = [1, 2, 3, 4, 5]

        # run method
        filtered_graph = self.owl_nets.removes_edges_with_owl_semantics()

        # check output type
        self.assertIsInstance(filtered_graph, Graph)

        # check output length
        self.assertEqual(len(filtered_graph), 2745)

        return None

    def test_recurses_axioms(self):
        """Tests the recurses_axioms method."""

        # run method when passing axioms that include BNodes
        # function inputs
        seen_nodes = []
        axioms = [(BNode('N194ae548a89740849c3536d9753d39d8'),
                   URIRef('http://www.w3.org/2002/07/owl#someValuesFrom'),
                   URIRef('http://purl.obolibrary.org/obo/SO_0000784'))]

        visited_nodes = self.owl_nets.recurses_axioms(seen_nodes, axioms)

        self.assertIsInstance(visited_nodes, List)
        self.assertEqual(len(visited_nodes), 1)
        self.assertIn(BNode('N194ae548a89740849c3536d9753d39d8'), visited_nodes)

        # run method when passing axioms that do not include BNodes
        # function inputs
        seen_nodes = []
        axioms = [(URIRef('http://purl.obolibrary.org/obo/SO_0002047'),
                   URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                   URIRef('http://www.w3.org/2002/07/owl#Class'))]

        visited_nodes = self.owl_nets.recurses_axioms(seen_nodes, axioms)

        self.assertIsInstance(visited_nodes, List)
        self.assertEqual(len(visited_nodes), 0)

        return None

    def test_creates_edge_dictionary(self):
        """Tests the creates_edge_dictionary method."""

        # set-up inputs
        self.owl_nets.converts_rdflib_to_networkx_multidigraph()
        node = URIRef('http://purl.obolibrary.org/obo/SO_0000822')
        edge_dict = self.owl_nets.creates_edge_dictionary(node)

        # test method
        self.assertIsInstance(edge_dict[0], Dict)
        self.assertEqual(len(edge_dict[0]), 5)
        self.assertIsInstance(edge_dict[0][list(edge_dict[0].keys())[0]], Dict)
        self.assertIsInstance(edge_dict[1], Set)
        self.assertEqual(len(edge_dict[1]), 0)

        return None

    def test_detects_constructed_class_to_ignore_complement(self):
        """Tests the detects_constructed_class_to_ignore method for a complementOf object."""

        # set-up input
        node = URIRef('http://purl.obolibrary.org/obo/SO_0000340')
        results = ({BNode('N47395fcce3fc4bae86eb0a785f6a50fe'): {
            'type': URIRef('http://www.w3.org/2002/07/owl#Class'),
            'complementOf': URIRef('http://purl.obolibrary.org/obo/BFO_0000016')},
                       BNode('N7d4b70b12467414383dcda0b2de14fac'): {
                           'type': URIRef(
                               'http://www.w3.org/2002/07/owl#Restriction'),
                           'onProperty': URIRef('http://purl.obolibrary.org/obo/BFO_0000186'),
                           'allValuesFrom': BNode('N47395fcce3fc4bae86eb0a785f6a50fe')},
                       BNode('N9912bdc5f0b64056b8f733834c3f8788'): {
                           'complementOf': URIRef(
                               'http://purl.obolibrary.org/obo/BFO_0000016'),
                           'type': URIRef('http://www.w3.org/2002/07/owl#Class')},
                       BNode('N20dddb25602944319568f385e40fd434'): {
                           'type': URIRef(
                               'http://www.w3.org/2002/07/owl#Restriction'),
                           'onProperty': URIRef('http://purl.obolibrary.org/obo/BFO_0000176'),
                           'allValuesFrom': BNode('N9912bdc5f0b64056b8f733834c3f8788')}}, set())

        # test method
        decision = self.owl_nets.detects_constructed_class_to_ignore(results, node)
        self.assertIsInstance(decision, bool)
        self.assertEqual(decision, True)

        return None

    def test_detects_constructed_class_to_ignore_cardinality(self):
        """Tests the detects_constructed_class_to_ignore method for a cardinality object."""

        # set-up input
        node = URIRef('http://purl.obolibrary.org/obo/SO_0000340')
        results = ({BNode('Nbfdbf873070f4ff4b8e421afdafcc0d1'): {
            'onProperty': URIRef('http://purl.obolibrary.org/obo/RO_0002180'),
            'onClass': URIRef('http://purl.obolibrary.org/obo/PR_Q9BW19'),
            'type': URIRef('http://www.w3.org/2002/07/owl#Restriction')},
                       BNode('N92d1234ef3d1431e9696302f8b640855'): {
                           'type': URIRef('http://www.w3.org/2002/07/owl#Class'),
                           'intersectionOf': BNode('N75923190ee8d4c569ad0860e55143000')},
                       BNode('N75923190ee8d4c569ad0860e55143000'): {
                           'rest': BNode('Ndb37cfaab1534414acc1bd5782cd226d'),
                           'first': URIRef('http://purl.obolibrary.org/obo/PR_000027264')},
                       BNode('Ndb37cfaab1534414acc1bd5782cd226d'): {
                           'first': BNode('N7f08599ce3a945f2a0a7188e30313957'),
                           'rest': URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#nil')},
                       BNode('N7f08599ce3a945f2a0a7188e30313957'): {
                           'onProperty': URIRef(
                               'http://purl.obolibrary.org/obo/RO_0002160'),
                           'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                           'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/NCBITaxon_9606')},
                       BNode('Nacc48f51eda64c25ab95f675c9de9b22'): {
                           'onProperty': URIRef(
                               'http://purl.obolibrary.org/obo/RO_0002160'),
                           'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                           'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/NCBITaxon_9606')}},
                   {'http://purl.obolibrary.org/obo/PR_000027428: Nbfdbf873070f4ff4b8e421afdafcc0d1'})

        # test method
        decision = self.owl_nets.detects_constructed_class_to_ignore(results, node)
        self.assertIsInstance(decision, bool)
        self.assertEqual(decision, False)

        return None

    def test_detects_constructed_class_to_ignore_lacks_part(self):
        """Tests the detects_constructed_class_to_ignore method for a lacks_part object."""

        # set-up input
        node = URIRef('http://purl.obolibrary.org/obo/SO_000047373')
        results = ({BNode('Nfb450d1260944ec0a6be7f302e785ee9'): {
            'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
            'onProperty': URIRef('http://purl.obolibrary.org/obo/RO_0002160'),
            'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/NCBITaxon_9606')},
                       BNode('N22b5a01aab6f4257b3bdc5d291c01e31'): {
                           'someValuesFrom': URIRef(
                               'http://purl.obolibrary.org/obo/MOD_00115'),
                           'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                           'onProperty': URIRef('http://purl.obolibrary.org/obo/BFO_0000051')},
                       BNode('N9407d67a6bb642c09c50cf98fba99eec'): {
                           'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                           'onProperty': URIRef('http://purl.obolibrary.org/obo/pr#lacks_part'),
                           'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/PR_000021937')}}, set())

        # test method
        decision = self.owl_nets.detects_constructed_class_to_ignore(results, node)
        self.assertIsInstance(decision, bool)
        self.assertEqual(decision, True)

        return None

    def test_detects_constructed_class_to_ignore_regular(self):
        """Tests the detects_constructed_class_to_ignore method for a regular class."""

        # set-up input
        node = URIRef('http://purl.obolibrary.org/obo/MONDO_0014439')
        results = ({BNode('N384aa47973974606a24846db62455903'): {
            'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/NCBITaxon_9606'),
            'onProperty': URIRef('http://purl.obolibrary.org/obo/RO_0002160'),
            'type': URIRef('http://www.w3.org/2002/07/owl#Restriction')}}, set())

        # test method
        decision = self.owl_nets.detects_constructed_class_to_ignore(results, node)
        self.assertIsInstance(decision, bool)
        self.assertEqual(decision, False)

        return None

    def test_returns_object_property(self):
        """Tests the returns_object_property method."""

        # when sub and obj are PATO terms and property is none
        res1 = self.owl_nets.returns_object_property(URIRef('http://purl.obolibrary.org/obo/PATO_0001199'),
                                                     URIRef('http://purl.obolibrary.org/obo/PATO_0000402'),
                                                     None)

        self.assertIsInstance(res1, URIRef)
        self.assertEqual(res1, URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'))

        # when sub and obj are NOT PATO terms and property is none
        res2 = self.owl_nets.returns_object_property(URIRef('http://purl.obolibrary.org/obo/SO_0000784'),
                                                     URIRef('http://purl.obolibrary.org/obo/GO_2000380'),
                                                     None)

        self.assertIsInstance(res2, URIRef)
        self.assertEqual(res2, URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'))

        # when the obj is a PATO term and property is none
        res3 = self.owl_nets.returns_object_property(URIRef('http://purl.obolibrary.org/obo/SO_0000784'),
                                                     URIRef('http://purl.obolibrary.org/obo/PATO_0001199'),
                                                     None)

        self.assertIsInstance(res3, URIRef)
        self.assertEqual(res3, URIRef('http://purl.obolibrary.org/obo/RO_0000086'))

        # when the obj is a PATO term and property is NOT none
        res4 = self.owl_nets.returns_object_property(URIRef('http://purl.obolibrary.org/obo/SO_0000784'),
                                                     URIRef('http://purl.obolibrary.org/obo/PATO_0001199'),
                                                     URIRef('http://purl.obolibrary.org/obo/RO_0002202'))

        self.assertIsInstance(res4, URIRef)
        self.assertEqual(res4, URIRef('http://purl.obolibrary.org/obo/RO_0000086'))

        # when sub is a PATO term and property is NOT none
        res5 = self.owl_nets.returns_object_property(URIRef('http://purl.obolibrary.org/obo/PATO_0001199'),
                                                     URIRef('http://purl.obolibrary.org/obo/SO_0000784'),
                                                     URIRef('http://purl.obolibrary.org/obo/RO_0002202'))

        self.assertIsInstance(res5, URIRef)
        self.assertEqual(res5, URIRef('http://purl.obolibrary.org/obo/RO_0002202'))

        # when sub is a PATO term and property is none
        res6 = self.owl_nets.returns_object_property(URIRef('http://purl.obolibrary.org/obo/PATO_0001199'),
                                                     URIRef('http://purl.obolibrary.org/obo/SO_0000784'),
                                                     None)

        self.assertEqual(res6, None)

        return None

    def test_parses_anonymous_axioms(self):
        """Tests the parses_anonymous_axioms method."""

        # set-up input variables
        class_dict = {
            BNode('N41aa20de8e3d4f8cac6047850b200829'): {
                'first': URIRef('http://purl.obolibrary.org/obo/SO_0000340'),
                'rest': BNode('N6e7b4832dafc413f9b8376f0019df405')},
            BNode('Nbb739d65cc61479ca8970cf68c276f8a'): {
                'intersectionOf': BNode('N41aa20de8e3d4f8cac6047850b200829'),
                'type': URIRef('http://www.w3.org/2002/07/owl#Class')},
            BNode('N6e7b4832dafc413f9b8376f0019df405'): {
                'first': BNode('N51191013960246b2abf331675b3a3331'),
                'rest': URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#nil')},
            BNode('N51191013960246b2abf331675b3a3331'): {
                'onProperty': URIRef('http://purl.obolibrary.org/obo/so#has_origin'),
                'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/SO_0000746'),
                'type': URIRef('http://www.w3.org/2002/07/owl#Restriction')},
            BNode('Na36bfb34a35047838a8df32b37a8ff50'): {
                'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/SO_0000746'),
                'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                'onProperty': URIRef('http://purl.obolibrary.org/obo/so#has_origin')}
        }

        # test when first is a URIRef and rest is a BNode
        edges = {'first': URIRef('http://purl.obolibrary.org/obo/SO_0000340'),
                 'rest': BNode('N6e7b4832dafc413f9b8376f0019df405')}
        res1 = self.owl_nets.parses_anonymous_axioms(edges, class_dict)

        self.assertIsInstance(res1, Dict)
        self.assertTrue(len(res1), 2)
        self.assertIn('first', res1.keys())
        self.assertIn('rest', res1.keys())

        # test when first is a BNode and rest is a URIRef
        edges = {'first': BNode('N51191013960246b2abf331675b3a3331'),
                 'rest': URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#nil')}
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
        node = URIRef('http://purl.obolibrary.org/obo/PATO_0000380')

        edges = {'intersectionOf': BNode('Ne7ce944017f64e62bc445ec2c336a481'),
                 'type': URIRef('http://www.w3.org/2002/07/owl#Class')}

        class_dict = {
            BNode('Ne7ce944017f64e62bc445ec2c336a481'): {
                'first': URIRef('http://purl.obolibrary.org/obo/PATO_0000044'),
                'rest': BNode('Neb29f9314a9344cb886ae5a3da065ccf')},
            BNode('Neb29f9314a9344cb886ae5a3da065ccf'): {
                'first': BNode('N8e6e0832f80e497cb694cd1894699102'),
                'rest': URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#nil')},
            BNode('N8e6e0832f80e497cb694cd1894699102'): {
                'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                'onProperty': URIRef('http://purl.obolibrary.org/obo/pato#increased_in_magnitude_relative_to'),
                'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/PATO_0000461')}
        }

        # test method
        res = self.owl_nets.parses_constructors(node, edges, class_dict)
        self.assertIsInstance(res, Tuple)
        self.assertEqual(res[0], {(URIRef('http://purl.obolibrary.org/obo/PATO_0000380'),
                                   URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'),
                                   URIRef('http://purl.obolibrary.org/obo/PATO_0000044'))})
        self.assertEqual(res[1], {'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                                  'onProperty': URIRef(
                                      'http://purl.obolibrary.org/obo/pato#increased_in_magnitude_relative_to'),
                                  'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/PATO_0000461')})

        return None

    def test_parses_constructors_union(self):
        """Tests the parses_constructors method for the UnionOf class constructor"""

        # set-up inputs
        node = URIRef('http://purl.obolibrary.org/obo/CL_0000995')

        edges = {'type': URIRef('http://www.w3.org/2002/07/owl#Class'),
                 'unionOf': BNode('Nbd4f84c8a171450cbef8c1c925245484')}

        class_dict = {BNode('Nbd4f84c8a171450cbef8c1c925245484'): {
            'first': URIRef('http://purl.obolibrary.org/obo/CL_0001021'),
            'rest': BNode('N039be74c7577473d93f664a5074c57b2')},
            BNode('N039be74c7577473d93f664a5074c57b2'): {
                'first': URIRef('http://purl.obolibrary.org/obo/CL_0001026'),
                'rest': URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#nil')}}

        # test method
        res = self.owl_nets.parses_constructors(node, edges, class_dict)
        self.assertIsInstance(res, Tuple)
        self.assertEqual(res[0], {(URIRef('http://purl.obolibrary.org/obo/CL_0000995'),
                                   URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'),
                                   URIRef('http://purl.obolibrary.org/obo/CL_0001026')),
                                  (URIRef('http://purl.obolibrary.org/obo/CL_0000995'),
                                   URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'),
                                   URIRef('http://purl.obolibrary.org/obo/CL_0001021'))})
        self.assertEqual(res[1], None)

        return None

    def test_parses_restrictions(self):
        """Tests the parses_restrictions method."""

        # set-up inputs
        node = URIRef('http://purl.obolibrary.org/obo/PATO_0000380')

        edges = {'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                 'onProperty': URIRef(
                     'http://purl.obolibrary.org/obo/pato#increased_in_magnitude_relative_to'),
                 'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/PATO_0000461')}

        class_dict = {
            BNode('Ne7ce944017f64e62bc445ec2c336a481'): {
                'first': URIRef('http://purl.obolibrary.org/obo/PATO_0000044'),
                'rest': BNode('Neb29f9314a9344cb886ae5a3da065ccf')},
            BNode('Neb29f9314a9344cb886ae5a3da065ccf'): {
                'first': BNode('N8e6e0832f80e497cb694cd1894699102'),
                'rest': URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#nil')},
            BNode('N8e6e0832f80e497cb694cd1894699102'): {
                'type': URIRef('http://www.w3.org/2002/07/owl#Restriction'),
                'onProperty': URIRef('http://purl.obolibrary.org/obo/pato#increased_in_magnitude_relative_to'),
                'someValuesFrom': URIRef('http://purl.obolibrary.org/obo/PATO_0000461')}
        }

        # test method
        res = self.owl_nets.parses_restrictions(node, edges, class_dict)
        self.assertIsInstance(res, Tuple)
        self.assertEqual(res[0], {(URIRef('http://purl.obolibrary.org/obo/PATO_0000380'),
                                   URIRef('http://purl.obolibrary.org/obo/pato#increased_in_magnitude_relative_to'),
                                   URIRef('http://purl.obolibrary.org/obo/PATO_0000461'))})
        self.assertEqual(res[1], None)

        return None

    def test_cleans_owl_encoded_classes(self):
        """Tests the cleans_owl_encoded_classes method"""

        # set-up inputs
        self.owl_nets.node_list = [URIRef('http://purl.obolibrary.org/obo/SO_0000822')]
        self.owl_nets.converts_rdflib_to_networkx_multidigraph()

        # test method
        decoded_graph = self.owl_nets.cleans_owl_encoded_classes()
        self.assertIsInstance(decoded_graph, Graph)
        self.assertEqual(len(decoded_graph), 2)

        self.assertEqual(sorted([str(x) for y in list(decoded_graph.triples((None, None, None))) for x in y]),
                         ['http://purl.obolibrary.org/obo/SO_0000340',
                          'http://purl.obolibrary.org/obo/SO_0000746',
                          'http://purl.obolibrary.org/obo/SO_0000822',
                          'http://purl.obolibrary.org/obo/SO_0000822',
                          'http://purl.obolibrary.org/obo/so#has_origin',
                          'http://www.w3.org/2000/01/rdf-schema#subClassOf'])

        return None

    def test_purifies_graph_build_none(self):
        """Tests the purifies_graph_build method when kg_construction is None."""

        # initialize method
        owl_nets = OwlNets(graph=self.graph, write_location=self.write_location, filename=self.kg_filename)

        # test method
        owl_nets.purifies_graph_build()
        dict_keys = owl_nets.owl_nets_dict['{}_approach_purified'.format(owl_nets.kg_construct_approach)]
        self.assertTrue(len(dict_keys), 0)

        return None

    def test_purifies_graph_build_instance(self):
        """Tests the purifies_graph_build method when kg_construction is instance."""

        # initialize method
        owl_nets = OwlNets(kg_construct_approach='instance', graph=self.graph,
                           write_location=self.write_location, filename=self.kg_filename)

        # test method
        owl_nets.purifies_graph_build()
        dict_keys = owl_nets.owl_nets_dict['{}_approach_purified'.format(owl_nets.kg_construct_approach)]
        self.assertTrue(len(dict_keys), 3054)

        return None

    def test_purifies_graph_build_subclass(self):
        """Tests the purifies_graph_build method when kg_construction is subclass."""

        # initialize method
        owl_nets = OwlNets(kg_construct_approach='subclass', graph=self.graph,
                           write_location=self.write_location, filename=self.kg_filename)

        # test method
        owl_nets.purifies_graph_build()
        dict_keys = owl_nets.owl_nets_dict['{}_approach_purified'.format(owl_nets.kg_construct_approach)]
        self.assertTrue(len(dict_keys), 6616)

        return None

    def test_write_out_results_regular(self):
        """Tests the write_out_results method."""

        self.owl_nets.kg_construct_approach = None
        owl_nets_graph = self.owl_nets.run_owl_nets()
        self.assertIsInstance(owl_nets_graph, Tuple)
        self.assertIsInstance(owl_nets_graph[0], Graph)
        self.assertEqual(owl_nets_graph[1], None)
        self.assertEqual(len(owl_nets_graph[0]), 2940)

        # make sure files are written locally
        nx_mdg_file = 'so_with_imports_OWLNETS_NetworkxMultiDiGraph.gpickle'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/so_with_imports_OWLNETS.nt'))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + nx_mdg_file))
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs'
                                                                '/so_with_imports_OWLNETS_decoding_dict.pkl'))

        return None

    def test_write_out_results_subclass_purified(self):
        """Tests the run_owl_nets method."""

        self.owl_nets.kg_construct_approach = "subclass"
        owl_nets_graph = self.owl_nets.run_owl_nets()
        self.assertIsInstance(owl_nets_graph, Tuple)
        self.assertIsInstance(owl_nets_graph[0], Graph)
        self.assertEqual(len(owl_nets_graph[0]), 2940)
        self.assertIsInstance(owl_nets_graph[1], Graph)
        self.assertEqual(len(owl_nets_graph[0]), len(owl_nets_graph[1]))

        # make sure files are written locally for each graph
        # purified
        nx_mdg_file = 'so_with_imports_SUBCLASS_purified_OWLNETS_NetworkxMultiDiGraph.gpickle'
        nt_file = 'so_with_imports_SUBCLASS_purified_OWLNETS.nt'
        dict_file = '/so_with_imports_SUBCLASS_purified_OWLNETS_decoding_dict.pkl'
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
        """Tests the run_owl_nets method."""

        owl_nets_graph = self.owl_nets2.run_owl_nets()
        self.assertIsInstance(owl_nets_graph, Tuple)
        self.assertIsInstance(owl_nets_graph[0], Graph)
        self.assertEqual(len(owl_nets_graph[0]), 2940)
        self.assertIsInstance(owl_nets_graph[1], Graph)
        self.assertTrue(len(owl_nets_graph[1]) > len(owl_nets_graph[0]))

        # make sure files are written locally for each graph
        # purified
        nx_mdg_file = 'so_with_imports_INSTANCE_purified_OWLNETS_NetworkxMultiDiGraph.gpickle'
        nt_file = 'so_with_imports_INSTANCE_purified_OWLNETS.nt'
        dict_file = '/so_with_imports_INSTANCE_purified_OWLNETS_decoding_dict.pkl'
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

    def tearDown(self):
        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
