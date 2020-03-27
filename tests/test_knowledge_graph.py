import unittest

from pkt_kg.knowledge_graph import *


class TestKGBuilder(unittest.TestCase):
    """Class to test the KGBuilder class from the knowledge graph script."""

    def setUp(self):
        # (self, kg_version: str, write_location: str, edge_data: Optional[str] = None,
        # node_data: Optional[str] = None, inverse_relations: Optional[str] = None,
        # decode_owl_semantics: Optional[str] = None)

        KGBuilder()

        return None

    def test_sets_up_environment(self):
        """Tests the sets_up_environment method."""

        return None

    def test_reverse_relation_processor(self):
        """Tests the reverse_relation_processor method."""

        return None

    def test_checks_for_inverse_relations(self):
        """Tests the checks_for_inverse_relations method."""

        # (self, relation: str, edge_list: List[List[str]])

        return None

    def test_creates_instance_instance_data_edges(self):
        """Tests the creates_instance_instance_data_edges method."""

        # (self, edge_type: str, creates_node_metadata_func: Callable)

        return None

    def test_creates_class_class_data_edges(self):
        """Tests the creates_class_class_data_edges method."""

        # (self, edge_type: str)

        return None

    def test_creates_instance_class_data_edges(self):
        """Tests the creates_instance_class_data_edges method."""

        # (self, edge_type: str, creates_node_metadata_func: Callable)

        return None

    def test_creates_knowledge_graph_edges(self):
        """Tests the creates_knowledge_graph_edges method."""

        # (self, creates_node_metadata_func: Callable, ontology_annotator_func: Callable)

        return None

    def test_ontology_file_formatter(self):
        """Tests the ontology_file_formatter method."""

        return None

    def test_construct_knowledge_graph(self):
        """Tests the construct_knowledge_graph(self) method."""

        return None
