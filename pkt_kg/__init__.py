#!/usr/bin/env python
# -*- coding: utf-8 -*-


""" A Library for Automated Construction of Large Scale Heterogeneous Biomedical Knowledge Graphs

PheKnowLator (Phenotype Knowledge Translator), a fully automated Python 3 library explicitly designed for optimized
construction of semantically-rich, large-scale biomedical KGs from complex heterogeneous data. The PheKnowLator
framework provides detailed Jupyter Notebooks and scripts which greatly simplify KG construction, assisting even
non-technical users through all steps of the build process. To accommodate a wide range of users and use cases,
PheKnowLator has three build types (partial, full, and post-reasoner), can include inverse edges to link nodes,
outputs KGs with and without OWL semantics (e.g. OWL-NETS), and generates KGs in several formats (e.g. triple edge
lists, OWL API-formatted RDFXML, graph-pickled Networkx MultiDiGraph).

There are two ways to run PheKnowLator:
  1. Jupyter Notebook (main.ipynb)
  2. Command line via argparse (Main.py)
"""

__all__ = [
    'KGConstructionApproach',

    'OntData',
    'LinkedData',

    'CreatesEdgeList',

    'PartialBuild',
    'PostClosureBuild',
    'FullBuild',

    'Metadata',
    'OwlNets'
]

from pkt_kg.construction_approaches import KGConstructionApproach
from pkt_kg.downloads import LinkedData, OntData
from pkt_kg.edge_list import CreatesEdgeList
from pkt_kg.knowledge_graph import PartialBuild, PostClosureBuild, FullBuild
from pkt_kg.metadata import Metadata
from pkt_kg.owlnets import OwlNets
