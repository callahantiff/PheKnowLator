#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

from sklearn.manifold import TSNE
from sklearn.decomposition import TruncatedSVD
from tqdm import tqdm


# set environment arguments
plt.style.use('ggplot')


def processes_integer_labeled_embeddings(embedding, node_label_dict, node_list):
    """Iterates over a list of embeddings and derives a dictionary where the keys are node types and the values are
    embeddings.

    Args:
        embedding (list): A nested list where each inner list is a vector of embeddings.
        node_label_dict (dict): A dictionary where keys are strings and values are integers.
        node_list (list): A list of node types.

    Returns:
        A nested list where each inner list contains the node type, node label, and embedding list..
    """

    print('\n' + '=' * 100)
    print('Converting Integer Embedding Labels to Strings')
    print('=' * 100 + '\n')

    embedding_info = []

    for row in tqdm(embedding):
        node = node_label_dict[int(row.split(' ')[0])].split('/')[-1]

        for node_label in node_list:
            embedding_info.append([node_label, node, [float(x) for x in row.strip('\n').split(' ')[1:]]])

    return embedding_info


def plots_embeddings(colors, names, groups, legend_arg, label_size, tsne_size, title, title_size):

    # set up plot
    fig, ax = plt.subplots(figsize=(15, 10))
    ax.margins(0.05)

    # iterate through groups to layer the plot
    for name, group in groups:
        ax.plot(group.x, group.y, marker='o', linestyle='', ms=6, label=names[name],
                color=colors[name], mec='none', alpha=0.8)

    plt.legend(handles=legend_arg[0], fontsize=legend_arg[1], frameon=False, loc=legend_arg[2], ncol=legend_arg[3])

    ax.tick_params(labelsize=label_size)
    plt.ylim(-(tsne_size + 5), tsne_size)
    plt.xlim(-tsne_size, tsne_size)
    plt.title(title, fontsize=title_size)
    plt.show()
    plt.close()


def main():

    # CONVERT EMBEDDINGS INTO PANDAS DATAFRAME
    # read in embedding file and re-generate node labels
    embedding_file = open('./resources/graphs/out.txt').readlines()[1:]
    node_labels = json.loads(open('./resources/graphs/KG_triples_ints_map.json').read())
    node_label_dict = {val: key for (key, val) in node_labels.items()}
    node_list = ['HP', 'CHEBI', 'VO', 'DOID', 'R-HSA', 'GO', 'geneid']

    # convert embeddings to df
    embeddings = processes_integer_labeled_embeddings(embedding_file, node_label_dict, node_list)
    embedding_data = pd.DataFrame(embeddings, columns=['node_type', 'node', 'embedding'])
    embedding_data.to_pickle('./resources/embeddings/PheKnowLator_embedding_dataframe')

    # DIMENSIONALITY REDUCTION
    x_reduced = TruncatedSVD(n_components=50, random_state=1).fit_transform(list(embedding_data['embeddings']))
    x_embedded = TSNE(n_components=2, random_state=1, verbose=True, perplexity=50.0).fit_transform(x_reduced)
    np.save('./resources/graphs/ALL_KG_res_tsne', x_embedded)

    # PLOT T-SNE
    # set up colors and legend labels
    colors = {'Diseases': '#009EFA',
              'Chemicals': 'indigo',
              'GO Concepts': '#F79862',
              'Genes': '#4fb783',
              'Pathways': 'orchid',
              'Phenotypes': '#A3B14B'}

    names = {key: key for key in colors.keys()}

    # create data frame to use for plotting data by node type
    df = pd.DataFrame(dict(x=x_embedded[:, 0], y=x_embedded[:, 1], group=list(embedding_data['node_type'])))
    groups = df.groupby('group')

    # create legend arguments
    dis = mpatches.Patch(color='#009EFA', label='Diseases')
    drg = mpatches.Patch(color='indigo', label='Drugs')
    go = mpatches.Patch(color='#F79862', label='GO Concepts')
    ge = mpatches.Patch(color='#4fb783', label='Genes')
    pat = mpatches.Patch(color='orchid', label='Pathways')
    phe = mpatches.Patch(color='#A3B14B', label='Phenotypes')

    legend_args = [[dis, drg, go, ge, pat, phe], 14, 'lower center', 3]
    title = 't-SNE: Biological Knowledge Graph'

    plots_embeddings(colors, names, groups, legend_args, 16, 100, title, 20)
