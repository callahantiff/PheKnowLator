#!/usr/bin/env python
# -*- coding: utf-8 -*-

#######################################################################################################################
# purpose #
# This script serves as development space for exploring different ways to characterize the types of paths that exist
# between two input entities. This script works

#######################################################################################################################

# import needed libraries
import json
import matplotlib.pyplot as plt
import networkx as nx
import random

# from networkx.drawing.nx_pydot import graphviz_layout
from pyvis.network import Network  # type: ignore
from rdflib import URIRef  # type: ignore
from rdflib.namespace import OWL, RDF, RDFS  # type: ignore
from typing import Callable, Dict, List, Optional, Tuple, Union


def format_path_ancestors(anc_dict: Dict, node_metadata: Dict) -> List:
    """Processes a dictionary of node ancestors into a list.

    Args:
        anc_dict: A dictionary where keys are ints formatted as strings and values are sets of URL strings for each
            concept that was found at that level. The level is the distance in the hierarchy from the searched node.
        node_metadata: A nested dictionary containing node attributes.

    Returns:
        ancestors: A nested list where each inner list contains ontology identifier strings.
    """

    ancestors = [['{} ({})'.format(node_metadata[str(x)]['label'], x) for x in anc_dict[str(k)]]
                 for k in sorted([int(x) for x in anc_dict.keys()])]

    return ancestors


def formats_node_information(node: URIRef, neighborhood: List, metadata_dict: Dict, verbose: bool = False) -> \
        None:
    """Processes neighborhood results.

    Args:
        node: A string containing a node URL.
        neighborhood: A nested list of strings, where each string contains a node identifier.
        metadata_dict: A nested dictionary containing node attributes.
        verbose: A bool indicating whether or not node and edge metadata should be printed.

    Returns:
        None
    """

    for e, o in neighborhood:
        spe = '\n' if neighborhood.index([e, o]) == 0 else '\n\n'
        s, s_lab = str(node[0]).split('/')[-1], metadata_dict[str(node[0])]['label']
        e_lab = metadata_dict[str(e)]['label']
        # TODO -- remove this line once the metadata are fixed
        e_lab = 'causally related to' if e_lab == 'is substance that treats' else e_lab
        o, o_lab, o_def = str(o).split('/')[-1], metadata_dict[str(o)]['label'], metadata_dict[str(o)]['description']
        if verbose:
            if o_def != 'None':
                print(spe + '>>> {} ({}) - {} - {} ({})\n{} Definition: {}'.format(s_lab, s, e_lab, o_lab, o, o, o_def))
            else:
                print(spe + '>>> {} ({}) - {} - {} ({})'.format(s_lab, s, e_lab, o_lab, o))
        else:
            print('>>> {} ({}) - {} - {} ({})'.format(s_lab, s, e_lab, o_lab, o))

    return None


def metadata_formatter(s: str, o: str, metadata_dict: Dict, out_type: bool = False) -> Optional[str]:
    """Function looks up edge-level metadata and prints it.

    Args:
        s: A string containing the identifier for the subject node of a predicate or triple.
        o: A string containing the identifier for the object node of a predicate or triple.
        metadata_dict: A nested dictionary containing node and edge-level metadata.
        out_type: A bool indicating how to return metadata (default=False).

    Returns:
        None.
    """

    s = s + '-reactome_' if 'R-HSA' in s else s
    o = o + '-reactome_' if 'R-HSA' in o else o

    if s + '-' + o in metadata_dict['edges'].keys():
        s = json.dumps(metadata_dict['edges'][s + '-' + o], indent=4)
        if not out_type: print('\nEdge Evidence'); print(s)
        else: return s
    elif o + '-' + s in metadata_dict['edges'].keys():
        s = json.dumps(metadata_dict['edges'][o + '-' + s], indent=4)
        if not out_type: print('\nEdge Evidence'); print(s)
        else: return s

    else: return None


def formats_path_information(kg: nx.multidigraph.MultiDiGraph, paths: List, path_type: str, metadata_func: Callable,
                             metadata_dict: Dict, node_metadata: Dict, verbose: bool = False, rand: bool = False,
                             sample_size: int = 10) -> None:
    """Processes shortest and simple path results.

    Args:
        kg: A networkx MultiDiGraph object.
        paths: A nested list of strings, where each string contains an an entity identifier.
        path_type: A string, either 'simple' or 'shortest' that indicates the types of paths to process.
        metadata_func: A function that processes edge metadata.
        metadata_dict: A nested dictionary containing node and edge-level metadata.
        node_metadata: A nested dictionary containing node attributes.
        verbose: A bool indicating whether or not node and edge metadata should be printed.
        rand: A bool indicating whether or not to draw random samples from the path.
        sample_size: An integer used when rand is True to specify the size of the random sample to draw.

    Returns:
        None
    """

    if path_type == 'shortest':
        if rand:
            sample_size = sample_size if sample_size < len(paths) else len(paths)
            paths = random.sample(paths, sample_size)
        for path in paths:
            print('*' * 100)
            for i in range(0, len(path) - 1):
                s = path[i]; o = path[i + 1]
                edges = kg.get_edge_data(*(s, o)).keys()
                for e in edges:
                    s_cut, s_label = str(s).split('/')[-1], node_metadata[str(s)]['label']
                    e_label = node_metadata[str(e)]['label']
                    # TODO -- remove this line once the metadata are fixed
                    e_label = 'causally related to' if e_label == 'is substance that treats' else e_label
                    o_cut, o_label = str(o).split('/')[-1], node_metadata[str(o)]['label']
                    if verbose:
                        print('>>> {} ({}) - {} - {} ({})'.format(s_label, s_cut, e_label, o_label, o_cut))
                        metadata_func(s_cut, o_cut, metadata_dict)
                    else: print('>>> {} ({}) - {} - {} ({})'.format(s_label, s_cut, e_label, o_label, o_cut))
            print('*' * 100); print('\n')
    else:
        if rand:
            sample_size = sample_size if sample_size < len(paths) else len(paths)
            paths = random.sample(paths, sample_size)
        for path in paths:
            print('*' * 100)
            for i in range(0, len(path) - 1):
                s = path[i]; o = path[i + 1]; edges = kg.get_edge_data(*(s, o))
                try: edges.keys()
                except AttributeError: edges = kg.get_edge_data(*(o, s))
                for e in edges.keys():
                    s_cut, s_label = str(s).split('/')[-1], node_metadata[str(s)]['label']
                    e_label = node_metadata[str(e)]['label']
                    # TODO -- remove this line once the metadata are fixed
                    e_label = 'causally related to' if e_label == 'is substance that treats' else e_label
                    o_cut, o_label = str(o).split('/')[-1], node_metadata[str(o)]['label']
                    if verbose:
                        print('>>> {} ({}) - {} - {} ({})'.format(s_label, s_cut, e_label, o_label, o_cut))
                        metadata_func(s_cut, o_cut, metadata_dict)
                    else: print('>>> {} ({}) - {} - {} ({})'.format(s_label, s_cut, e_label, o_label, o_cut))
            print('*' * 100); print('\n')

    return None


def nx_ancestor_search(kg: nx.multidigraph.MultiDiGraph, nodes: List, prefix: str, anc_list: Optional[List] = None) ->\
        Union[Callable, List]:
    """Returns all ancestors nodes reachable through a direct edge. The returned list is ordered by seniority.

    Args:
        kg: A networkx MultiDiGraph object.
        nodes: A list of RDFLib URIRef objects or None.
        prefix: A string containing an ontology prefix (e.g., MONDO).
        anc_list: A list that is empty or that contains RDFLib URIRef objects.

    Returns:
        anc_list: A list of period-delimited strings, where each string represents a path
    """

    ancestor_list = [] if anc_list is None else anc_list

    if len(nodes) == 0: return ancestor_list
    else:
        node = nodes.pop(); node_list = list(kg.neighbors(node))
        neighborhood = [a for b in [[[i, n] for j in [kg.get_edge_data(*(node, n)).keys()]
                                     for i in j] for n in node_list] for a in b]
        ancestors = [x[1] for x in neighborhood if (prefix in str(x[1]) and x[0] == RDFS.subClassOf)]
        if len(ancestors) > 0:
            ancestor_list += [[str(x) for x in ancestors]]
            nodes += [x for x in ancestors if x not in nodes]
        return nx_ancestor_search(kg, nodes, prefix, ancestor_list)


def processes_ancestor_path_list(path_list: List) -> Dict:
    """Processes a nested list of ancestor paths into a dictionary.

    Args:
        path_list: A nested list of ontology URLs, where each list represents a set of ancestors.

    Returns:
        ancestors: A dictionary where keys are ints formatted as strings and values are sets of URL strings for each
            concept that was found at that level. The level is the distance in the hierarchy from the searched node.
    """

    anc_dict: Dict = dict()
    for path in path_list:
        for x in path:
            idx = max([i for i, j in enumerate(path_list) if x in j])
            if str(idx) in anc_dict.keys(): anc_dict[str(idx)] |= {x}
            else: anc_dict[str(idx)] = {x}

    return anc_dict


def nudge(pos: Dict, x_shift: int, y_shift: int) -> Dict:
    """Function just moves the node labels on the plot so they don't overlap the arrows.

    Args:
        pos: A dictionary containing x and x axis information for each node in a graph.
        x_shift: An integer specifying the amount of x-axis shift.
        y_shift: An integer specifying the amount of y-axis shift.

    Returns:
        A shifted dictionary.
    """

    return {n: (x + x_shift, y + y_shift) for n, (x, y) in pos.items()}


def hierarchy_pos(g, root=None, width=1., vert_gap=0.2, vert_loc=0, xcenter=0.5):
    """From Joel's answer at https://stackoverflow.com/a/29597209/2966723.
    Licensed under Creative Commons Attribution-Share Alike
    If the graph is a tree this will return the positions to plot this in a hierarchical layout.

    Args:
        g: the graph (must be a tree)
        root: the root node of current branch
        - if the tree is directed and this is not given, the root will be found and used
        - if the tree is directed and this is given, then the positions will be just for the descendants of this node.
        - if the tree is undirected and not given, then a random choice will be used.
        width: horizontal space allocated for this branch - avoids overlap with other branches
        vert_gap: gap between levels of hierarchy
        vert_loc: vertical location of root
        xcenter: horizontal location of root
    """

    if root is None:
        if isinstance(g, nx.DiGraph):
            root = next(iter(nx.topological_sort(g)))  # allows back compatibility with nx version 1.11
        else:
            root = random.choice(list(g.nodes))

    def _hierarchy_pos(g0, root_node, wid=1., v_gap=0.2, v_loc=0, xcent=0.5, pos=None, parent=None):
        """see hierarchy_pos docstring for most arguments

        Args:
            pos: a dict saying where all nodes go if they have been assigned
            parent: parent of this branch. - only affects it if non-directed
        """

        if pos is None: pos = {root: (xcent, v_loc)}
        else: pos[root_node] = (xcent, v_loc)
        children = list(g0.neighbors(root_node))
        if not isinstance(g0, nx.DiGraph) and parent is not None: children.remove(parent)
        if len(children) != 0:
            dx = width / len(children); nextx = xcent - wid / 2 - dx / 2
            for child in children:
                nextx += dx
                pos = _hierarchy_pos(g0, child, wid=dx, v_gap=v_gap,
                                     v_loc=v_loc - v_gap, xcent=nextx,
                                     pos=pos, parent=root)
        return pos

    return _hierarchy_pos(g, root, width, vert_gap, vert_loc, xcenter)


def visualize_ancestor_tree(node_list: List) -> None:
    """Takes a nested list of ancestor information and creates a hierarchical tree visualization.

    Args:
        node_list: A nested list of ancestor information.

    Returns:
        None.
    """

    g_list = []; n_set = node_list[::-1]
    for x in range(len(n_set) - 1):
        for i in n_set[x]:
            for j in n_set[x + 1]:
                g_list.append((i.split('(')[0].rstrip(), j.split('(')[0].rstrip()))
    # convert to graph object
    g = nx.DiGraph(); g.add_edges_from(g_list)
    # visualize graph
    pos = hierarchy_pos(g); pos_labels = nudge(pos, 0, 0)
    fig = plt.figure(figsize=(14, 14)); ax = plt.subplot(1, 1, 1); plt.margins(0.1)
    # render image
    nx.draw(g, pos=pos, ax=ax, with_labels=False, arrows=True, arrowsize=20, edge_color='gray')
    nx.draw_networkx_nodes(g, pos=pos, node_color='lightblue', node_size=600)
    label_pos = nx.draw_networkx_labels(g, pos=pos_labels, font_weight='bold', ax=ax)

    return None


def pyvis_visualizer(node_info: list, edge_info: list) -> Network:
    """Visualizes processed network data using pyvis.

    Args:
        node_info: A nested list of node information.
        edge_info: A nested list of edge information.

    Return:
        None.
    """

    # process graph
    ids, titles, labels, values = node_info
    edge_list, edge_titles = edge_info

    # create graph
    g = Network(height="750px", width="100%", notebook=True, bgcolor="#222222", font_color="white")
    g.add_nodes(ids, value=values, title=titles, label=labels)
    for x in range(len(edge_list)):
        g.add_edge(edge_list[x][0], edge_list[x][1], title=edge_titles[x])

    return g


def visualize_kg_output(kg: nx.multidigraph.MultiDiGraph, node_list: list, node_metadata: dict) -> Network:
    """Processes a nested list of nodes in order to visualize them.

    Args:
        kg: A networkx MultiDiGraph object.
        node_list: A nested list of ancestor information.
        node_metadata: A nested dictionary containing node attributes.

    Return:
        None.
    """

    # process edges
    edge_list = []; edge_titles = []
    for x in range(len(node_list) - 1):
        for i in node_list[x]:
            for j in node_list[x + 1]:
                edge_list.append([i, j])
                s, o = URIRef(i.rpartition('(')[1].strip(')')), URIRef(j.rpartition('(')[1].strip(')'))
                try:
                    edges = list(kg.get_edge_data(*(s, o)).keys())[0]
                    edge_titles.append(node_metadata[str(edges)]['label'])
                except AttributeError:
                    edge_titles.append('subClassOf')
    # process nodes
    ids = []; titles = []; labels = []; values = []
    for x in node_list:
        for i in x:
            node = i.split('(')[0].rstrip(); uri = i.split('(')[1].strip(')'); ids.append(i)
            titles += [uri]; labels.append(node); values.append(len(kg.in_edges(URIRef(uri))))

    return pyvis_visualizer([ids, titles, labels, values], [edge_list, edge_titles])


def visualize_pheknowlator_schema() -> Network:
    """Visualizes the PheKnowLator v3.0.2 schema with node and edge count.

    Returns:
         pyvis Network object.
    """
    g = Network(height="750px", width="100%", notebook=True, bgcolor="#222222", font_color="white")
    g.add_nodes(['Phenotypes', 'Diseases', 'Chemicals', 'Variants', 'Transcripts', 'Genes', 'Molecular Functions',
                 'Biological Processes', 'Cellular Components', 'Cell Lines', 'Cells', 'Proteins',
                 'Anatomical Entities', 'Cofactors', 'Catalysts', 'Pathways'],
                title=['16,291', '22,334', '150,080', '145,156', '190,829', '26,532', '4,442', '12,329', '1,801',
                       '41,791', '2,368', '96,197', '14,181', '44', '3,749', '13,794'],
                label=['Phenotypes', 'Diseases', 'Chemicals', 'Variants', 'Transcripts', 'Genes', 'Molecular Functions',
                       'Biological Processes', 'Cellular Components', 'Cell Lines', 'Cells', 'Proteins',
                       'Anatomical Entities', 'Cofactors', 'Catalysts', 'Pathways'],
                color=['#fbafd1ff', '#e06666ff', '#bc376aff', '#e2d3e7ff', '#88d8ffff', '#b7b7b7ff', '#8e7cc3ff',
                       '#8e7cc3ff', '#8e7cc3ff', '#76a5afff', '#f6b26bff', '#6aa84fff', '#3d85c6ff', '#bc376aff',
                       '#bc376aff', '#e7e6e6ff'])
    g.add_edge('Chemicals', 'Diseases', title='causally related to (n=172,573)')
    g.add_edge('Chemicals', 'Genes', title='interacts with (n=16,708)')
    g.add_edge('Chemicals', 'Biological Processes', title='molecularly interacts with (n=288,873)')
    g.add_edge('Chemicals', 'Cellular Components', title='molecularly interacts with (n=47,716)')
    g.add_edge('Chemicals', 'Molecular Functions', title='molecularly interacts with (n=28,077)')
    g.add_edge('Chemicals', 'Pathways', title='participates in (n=29,988)')
    g.add_edge('Chemicals', 'Phenotypes', title='causally related to (n=110,898)')
    g.add_edge('Chemicals', 'Proteins', title='interacts with (n=71,679)')
    g.add_edge('Chemicals', 'Transcripts', title='interacts with (n=0)')
    g.add_edge('Diseases', 'Phenotypes', title='has phenotype (n=435,102)')
    g.add_edge('Genes', 'Diseases', title='causes or contributes to (n=12,842)')
    g.add_edge('Genes', 'Genes', title='genetically interacts with (n=1,694)')
    g.add_edge('Genes', 'Pathways', title='participates in (n=107,009)')
    g.add_edge('Genes', 'Phenotypes', title='cause or contributes to (n=24,760)')
    g.add_edge('Genes', 'Proteins', title='has gene product (n=19,521)')
    g.add_edge('Genes', 'Transcripts', title='transcribed to (n=182,692)')
    g.add_edge('Biological Processes', 'Pathways', title='realized in response to (n=672)')
    g.add_edge('Pathways', 'Cellular Components', title='has component (n=16,014)')
    g.add_edge('Pathways', 'Molecular Functions', title='has function (n=2,426)')
    g.add_edge('Proteins', 'Anatomical Entities', title='located in (n=30,681)')
    g.add_edge('Proteins', 'Catalysts', title='molecularly interacts with (n=25,136)')
    g.add_edge('Proteins', 'Cells', title='located in (n=75,313)')
    g.add_edge('Proteins', 'Cell Lines', title='located in (n=75,313)')
    g.add_edge('Proteins', 'Cofactors', title='molecularly interacts with (n=1,998)')
    g.add_edge('Proteins', 'Biological Processes', title='participates in (n=129,424)')
    g.add_edge('Proteins', 'Cellular Components', title='located in (n=82,526)')
    g.add_edge('Proteins', 'Molecular Functions', title='has function (n=69,801)')
    g.add_edge('Proteins', 'Pathways', title='participates in (n=117,813)')
    g.add_edge('Proteins', 'Proteins', title='molecularly interacts with (n=618,069)')
    g.add_edge('Transcripts', 'Anatomical Entities', title='located in (n=444,974)')
    g.add_edge('Transcripts', 'Cells', title='located in (n=65,180)')
    g.add_edge('Transcripts', 'Cell Lines', title='located in (n=65,180)')
    g.add_edge('Transcripts', 'Proteins', title='ribosomally translates to (n=44,205)')
    g.add_edge('Variants', 'Diseases', title='causes or contributes to (n=43,439)')
    g.add_edge('Variants', 'Genes', title='causally influences (n=145,129)')
    g.add_edge('Variants', 'Phenotypes', title='causes or contributes to (n=3,081)')

    return g


def visualize_pheknowlator_ontologies() -> Network:

    g = Network(height="750px", width="100%", notebook=True, bgcolor="#222222", font_color="white")
    g.add_nodes(['CLO', 'Mondo', 'Uberon', 'CL', 'PRO', 'GO', 'PW', 'ChEBI', 'HPO', 'SO', 'RO', 'VO'],
                title = ['Cell Line Ontology', 'Mondo Disease Ontology', 'Uber Anatomy Ontology', 'Cell Ontology',
                         'Protein Ontology', 'Gene Ontology', 'Pathway Ontology', 'Chemical Entities of Biological Interest', 'Human Phenotype Ontology', 'Sequence Ontology',
                         'Relations Ontology', 'Vaccine Ontology'],
                label = ['CLO', 'Mondo', 'Uberon', 'CL', 'PRO', 'GO', 'PW', 'ChEBI', 'HPO', 'SO', 'RO', 'VO'],
                color = ['#76a5afff', '#e06666ff', '#3d85c6ff', '#f6b26bff', '#6aa84fff', '#8e7cc3ff', '#ffe599ff',
                         '#bc376aff', '#fbafd1ff', '#d9ead3ff', '#666666ff', '#b59e7dff'])
    g.add_edge('HPO', 'Uberon', title='ontology import')
    g.add_edge('HPO', 'CL', title='ontology import')
    g.add_edge('HPO', 'GO', title='ontology import')
    g.add_edge('HPO', 'ChEBI', title='ontology import')
    g.add_edge('HPO', 'VO', title='ontology import')
    g.add_edge('HPO', 'PRO', title='ontology import')
    g.add_edge('CLO', 'PRO', title='ontology import')
    g.add_edge('CLO', 'RO', title='ontology import')
    g.add_edge('CLO', 'ChEBI', title='ontology import')
    g.add_edge('CLO', 'GO', title='ontology import')
    g.add_edge('CLO', 'CL', title='ontology import')
    g.add_edge('Mondo', 'SO', title='ontology import')
    g.add_edge('Mondo', 'RO', title='ontology import')
    g.add_edge('Mondo', 'GO', title='ontology import')
    g.add_edge('Mondo', 'ChEBI', title='ontology import')
    g.add_edge('Mondo', 'CL', title='ontology import')
    g.add_edge('Mondo', 'Uberon', title='ontology import')
    g.add_edge('Uberon', 'PRO', title='ontology import')
    g.add_edge('Uberon', 'ChEBI', title='ontology import')
    g.add_edge('Uberon', 'GO', title='ontology import')
    g.add_edge('Uberon', 'CL', title='ontology import')
    g.add_edge('CL', 'RO', title='ontology import')
    g.add_edge('CL', 'GO', title='ontology import')
    g.add_edge('CL', 'ChEBI', title='ontology import')
    g.add_edge('PRO', 'GO', title='ontology import')
    g.add_edge('PRO', 'ChEBI', title='ontology import')
    g.add_edge('GO', 'CL', title='ontology import')
    g.add_edge('GO', 'RO', title='ontology import')
    g.add_edge('GO', 'ChEBI', title='ontology import')
    g.add_edge('GO', 'VO', title='ontology import')
    g.add_edge('PW', 'GO', title='ontology import')
    g.add_edge('VO', 'ChEBI', title='ontology import')
    g.add_edge('VO', 'GO', title='ontology import')
    g.add_edge('VO', 'Uberon', title='ontology import')
    g.add_edge('VO', 'PRO', title='ontology import')

    return g
