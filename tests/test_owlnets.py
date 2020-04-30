import glob
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

        # copy data
        # ontology data
        shutil.copyfile(self.dir_loc + '/ontologies/so_with_imports.owl',
                        self.dir_loc_resources + '/knowledge_graphs/so_with_imports.owl')

        # owl properties
        shutil.copyfile(self.dir_loc + '/OWL_NETS_Property_Types.txt',
                        self.dir_loc_resources + '/owl_decoding/OWL_NETS_Property_Types.txt')

        # set-up input arguments
        self.write_location = self.dir_loc_resources + '/knowledge_graphs'
        self.kg_filename = 'so_with_imports.owl'
        self.object_properties = self.dir_loc_resources + '/owl_decoding/OWL_NETS_Property_Types.txt'

        # read in knowledge graph
        self.graph = Graph()
        self.graph.parse(self.dir_loc_resources + '/knowledge_graphs/so_with_imports.owl', format='xml')

        # initialize class
        self.owl_nets = OwlNets(kg_construct_approach='subclass',
                                graph=self.graph,
                                write_location=self.write_location,
                                full_kg=self.kg_filename)

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

    def test_initialization_state_graph(self):
        """Tests the class initialization state for graphs."""

        # verify input graph object - when wrong data type
        self.assertRaises(TypeError, OwlNets, 'subclass', list(), self.write_location, self.kg_filename)

        # verify input graph object - when graph file is empty
        self.assertRaises(ValueError, OwlNets, 'subclass', Graph(), self.write_location, self.kg_filename)

        return None

    def test_initialization_state_construction_approach(self):
        """Tests the class initialization state for construction approach type."""

        self.assertIsInstance(self.owl_nets.kg_construct_approach, str)
        self.assertTrue(self.owl_nets.kg_construct_approach == 'subclass')
        self.assertFalse(self.owl_nets.kg_construct_approach == 'instance')

        return None

    def test_initialization_state_object_properties(self):
        """Tests the class initialization state for object properties."""

        # verify owl properties file - when not a text file
        os.remove(self.dir_loc_resources + '/owl_decoding/OWL_NETS_Property_Types.txt')
        shutil.copyfile(self.dir_loc + '/OWL_NETS_Property_Types.txt',
                        self.dir_loc_resources + '/owl_decoding/OWL_NETS_Property_Types.csv')

        self.assertRaises(TypeError, OwlNets, self.graph, self.write_location, self.kg_filename)

        # verify owl properties file - when the file is empty
        os.remove(self.dir_loc_resources + '/owl_decoding/OWL_NETS_Property_Types.csv')
        shutil.copyfile(self.dir_loc + '/ontology_source_list_empty.txt',
                        self.dir_loc_resources + '/owl_decoding/OWL_NETS_Property_Types.txt')

        self.assertRaises(TypeError, OwlNets, self.graph, self.write_location, self.kg_filename)

        # check length of file after a successful run
        self.assertIsInstance(self.owl_nets.keep_properties, List)
        self.assertTrue(len(self.owl_nets.keep_properties) == 52)

        return None

    def test_initialization_state_object_properties_keep_list(self):
        """Tests the class initialization state for object properties, specifically checking that the list contains
        the two properties needed to run OWL-NETS."""

        self.assertIn('http://purl.obolibrary.org/obo/RO_0000086', self.owl_nets.keep_properties)
        self.assertIn('http://www.w3.org/2000/01/rdf-schema#subClassOf', self.owl_nets.keep_properties)

        return None

    def test_multidigraph_conversion(self):
        """Tests the transformation of a RDFLib graph object into a Networkx MultiDiGraph."""

        # make sure that the object exists and is the right type
        self.assertFalse(not self.owl_nets.nx_mdg)
        self.assertIsInstance(self.owl_nets.nx_mdg, networkx.MultiDiGraph)

        # check the length of the object
        self.assertTrue(len(self.owl_nets.nx_mdg) == 20277)

        return None

    def test_updates_class_instance_identifiers_instance(self):
        """Tests the updates_class_instance_identifiers method  for a subclass construction approach."""

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

        self.owl_nets.graph = adds_edges_to_graph(Graph(), edges)

        # run method to roll back to re-map instances of classes
        self.owl_nets.updates_class_instance_identifiers()
        self.assertEqual(len(self.owl_nets.graph), 9)
        self.assertIn((URIRef('http://purl.obolibrary.org/obo/CHEBI_2504'),
                       URIRef('http://purl.obolibrary.org/obo/RO_0002434'),
                       URIRef('https://www.ncbi.nlm.nih.gov/gene/55847')),
                      self.owl_nets.graph)

        return None

    def test_updates_class_instance_identifiers_subclass(self):
        """Tests the updates_class_instance_identifiers method for a subclass construction approach."""

        # make sure it does not run for subclass construction approach
        self.owl_nets.graph = Graph()
        self.owl_nets.kg_construct_approach = 'subclass'
        self.owl_nets.updates_class_instance_identifiers()
        self.assertEqual(len(self.owl_nets.graph), 0)

        return None

    def test_removes_edges_with_owl_semantics(self):
        """Tests the removes_edges_with_owl_semantics method."""

        # run method
        filtered_graph, owl_graph = self.owl_nets.removes_edges_with_owl_semantics()

        # check output type
        self.assertIsInstance(filtered_graph, Graph)
        self.assertIsInstance(owl_graph, Graph)

        # check output length
        self.assertEqual(len(filtered_graph), 2328)
        self.assertEqual(len(owl_graph), 39909)
        self.assertEqual(len(self.owl_nets.graph), len(filtered_graph) + len(owl_graph))

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
        node = URIRef('http://purl.obolibrary.org/obo/SO_0000822')
        edge_dict, cardinality = self.owl_nets.creates_edge_dictionary(node)

        # test method
        self.assertIsInstance(edge_dict, Dict)
        self.assertEqual(len(edge_dict), 5)
        self.assertIsInstance(edge_dict[list(edge_dict.keys())[0]], Dict)
        self.assertIsInstance(cardinality, Set)
        self.assertEqual(len(cardinality), 0)

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
        self.owl_nets.class_list = [URIRef('http://purl.obolibrary.org/obo/SO_0000822')]

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

    def test_run_owl_nets(self):
        """Tests the run_owl_nets method."""

        owl_nets_graph = self.owl_nets.run_owl_nets()

        self.assertIsInstance(owl_nets_graph, Graph)
        self.assertEqual(len(owl_nets_graph), 2940)

        # check that owl-nets bi-product graph was written out
        file_path = self.dir_loc_resources + '/knowledge_graphs/so_with_imports_OWLNets_BiProduct.nt'
        self.assertTrue(os.path.exists(file_path))

        return None

    def tearDown(self):
        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
