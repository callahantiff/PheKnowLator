import unittest

from pkt_kg.owlnets import *


class TestOwlNets(unittest.TestCase):
    """Class to test the OwlNets class from the owlnets script."""

    def setUp(self):

        # OwlNets()

        # (self, knowledge_graph: rdflib.Graph, uuid_class_map: Dict, write_location: str, full_kg: str,
        # keep_property_types: Optional[List[str]] = None)

        # class_list2 = [rdflib.URIRef('http://purl.obolibrary.org/obo/GO_0000785'),
        #                rdflib.URIRef('http://purl.obolibrary.org/obo/CL_0000995'),
        #                rdflib.URIRef('http://purl.obolibrary.org/obo/PATO_0000380'),
        #                rdflib.URIRef('http://purl.obolibrary.org/obo/HP_0000340'),
        #                rdflib.URIRef('http://purl.obolibrary.org/obo/GO_0000228'),
        #                rdflib.URIRef('http://purl.obolibrary.org/obo/PR_000050170'),
        #                rdflib.URIRef('http://purl.obolibrary.org/obo/CL_0000037'),
        #                rdflib.URIRef('http://purl.obolibrary.org/obo/ENVO_01001057')]

    def test_finds_classes(self):
        """Tests the finds_classes method."""

        return None

    def test_removes_edges_with_owl_semantics(self):
        """Tests the removes_edges_with_owl_semantics method."""

        return None

    def test_recurses_axioms(self):
        """Tests the recurses_axioms method."""


        # (self, seen_nodes: List[rdflib.BNode], axioms: List[Any])

        return None

    def test_creates_edge_dictionary(self):
        """Tests the creates_edge_dictionary method."""

        # (self, node: rdflib.URIRef)

        return None

    def test_returns_object_property(self):
        """Tests the returns_object_property method."""

        # (sub: rdflib.URIRef, obj: rdflib.URIRef, prop: rdflib.URIRef = None)

        return None

    def test_parses_anonymous_axioms(self):
        """Tests the parses_anonymous_axioms method."""

        # (edges: Dict, class_dict: Dict)

        return None

    def test_parses_constructors(self):
        """Tests the parses_constructors method"""

        # (self, node: rdflib.URIRef, edges: Dict, class_dict: Dict, relation: rdflib.URIRef = None)

        return None

    def test_parses_restrictions(self):
        """Tests the parses_restrictions method."""

        # (self, node: rdflib.URIRef, edges: Dict, class_dict: Dict)

        return None

    def test_cleans_owl_encoded_classes(self):
        """Tests the cleans_owl_encoded_classes method"""

        return None

    def test_run_owl_nets(self):
        """Tests the run_owl_nets method."""

        return None