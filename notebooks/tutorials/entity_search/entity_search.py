#!/usr/bin/env python
# -*- coding: utf-8 -*-

#######################################################################################################################
# purpose #
# This script serves as development space for exploring different ways to characterize the types of paths that exist
# between two input entities. This script works

#######################################################################################################################

# import needed libraries
# import needed libraries
import json
import matplotlib.pyplot as plt
import networkx as nx
import random

# from networkx.drawing.nx_pydot import graphviz_layout
from rdflib import URIRef  # type: ignore
from typing import Callable, Dict, List, Optional


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

        :return:
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


def chemical_disease_evidence(einfo: Dict):
    """

    Args:
        einfo: A dictionary of evidence.

    Returns:
        f: A formatted string of evidence.
    """

    ther_str = 'Therapeutic evidence (pmid(s): {})'
    mark_str = 'This chemical is correlated with or plays a role in the etiology of this disease (pmid(s): {})'
    inferred = []; dir_ther = []; dir_mark = []
    if 'Evidence' in einfo:
        for x in einfo['Evidence']:
            if x['DirectEvidence'] == 'therapeutic':
                pids = 'PMID(s):' + x['PubMedIDs']; dir_ther += [ther_str.format(pids)]
            elif x['DirectEvidence'] == 'marker/mechanism':
                pids = 'PMID(s):' + x['PubMedIDs']; dir_mark += [mark_str.format(pids)]
            else:
                scr = x['InferenceScore']
                inferred += ['{} (pmid(s): {})'.format(x['InferenceGeneSymbol'], x['PubMedIDs'])]
    # process evidence
    if len(dir_ther) > 0 and len(dir_mark) > 0 and len(inferred) > 0:
        ev_types = 'Direct (i.e., therapeutic and marker/mechanism) and Inferred'
        s1 = 'EVIDENCE: {}'.format(ev_types)
        i = '\n'.join(['\t-{}'.format(x) for x in inferred])
        t = '\n'.join(['\t-{}'.format(x) for x in dir_ther])
        m = '\n'.join(['\t-{}'.format(x) for x in dir_mark])
        s2 = '-Direct:\n{}\n{}\n-Inferred (score: {}):\n{}'.format(t, m, str(scr), i)
        return s1, s2
    elif (len(dir_ther) > 0 and len(inferred) > 0) and len(dir_mark) == 0:
        ev_types = 'Direct therapeutic and and Inferred'
        s1 = 'EVIDENCE: {}'.format(ev_types)
        i = '\n'.join(['\t-{}'.format(x) for x in inferred])
        t = '\n'.join(['\t-{}'.format(x) for x in dir_ther])
        s2 = '-Direct:\n{}\n-Inferred (score: {}):\n{}'.format(t, str(scr), i)
        return s1, s2
    elif (len(dir_mark) > 0 and len(inferred) > 0) and len(dir_ther) == 0:
        ev_types = 'Direct marker/mechanism and Inferred'
        s1 = 'EVIDENCE: {}'.format(ev_types)
        i = '\n'.join(['\t-{}'.format(x) for x in inferred])
        m = '\n'.join(['\t-{}'.format(x) for x in dir_mark])
        s2 = '-Direct:\n{}\n-Inferred (score: {}):\n{}'.format(m, str(scr), i)
        return s1, s2
    # elif (len(dir_ther) == 0 and len(dir_mark) == 0) and len(inferred) > 0:
    #     ev_types = 'Inferred'
    #     s1 = 'EVIDENCE: {}'.format(ev_types)
    #     i = '\n'.join(['\t-{}'.format(x) for x in dir_mark])
    #     s2 = '-Inferred (score: {}):\n{}\n{}'.format(str(scr), i)
    #     return s1, s2
    else: return None
