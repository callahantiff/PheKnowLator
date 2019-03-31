################################################################################################
# KGEmbeddingVis.py
# Purpose: script processes embeddings, performs and visualizes t-SNE dimensionality reduction
# version 1.0.0
# date: 06.01.2018
################################################################################################


## import module/script dependencies
import pandas as pd
from sklearn.manifold import TSNE
from sklearn.decomposition import TruncatedSVD
import matplotlib.pyplot as plt
import matplotlib
import json
matplotlib.style.use('ggplot')
import matplotlib.patches as mpatches
import numpy as np
import pickle

# read in embedding file and re-generate node labels
embed_file = open('./resources/graphs/out.txt').readlines()[1:]
node_labels = json.loads(open('./resources/graphs/KG_triples_ints_map.json').read())

nl_rev = {val:key for (key, val) in node_labels.items()}
nodes = []
embeddings = []
labels = []
edge_counter = 0
node_counter = 0

for row in embed_file:
    node = nl_rev[int(row.split(' ')[0])].split("/")[-1]
    embed = [float(x) for x in row.strip("\n").split(' ')[1:]]

    if node.startswith('RO_'):
        nodes.append(node)
        embeddings.append(embed)
        labels.append('RO')
        edge_counter += 1

    if node.startswith('BFO_'):
        nodes.append(node)
        embeddings.append(embed)
        labels.append('BFO')
        edge_counter += 1

    if node.startswith('DOID_'):
        nodes.append(node)
        embeddings.append(embed)
        labels.append('Diseases')
        node_counter += 1

    if node.startswith('R-HSA'):
        nodes.append(node)
        embeddings.append(embed)
        labels.append('Pathways')
        node_counter += 1

    if node.startswith('HP_'):
        nodes.append(node)
        embeddings.append(embed)
        labels.append('Phenotypes')
        node_counter += 1

    if node.startswith('GO_'):
        nodes.append(node)
        embeddings.append(embed)
        labels.append('Gene Ontology')
        node_counter += 1

    if 'mesh' in nl_rev[int(row.split(' ')[0])]:
        nodes.append(nl_rev[int(row.split(' ')[0])].split("/")[-1])
        embeddings.append(embed)
        labels.append('Drugs')
        node_counter += 1

    if 'geneid' in nl_rev[int(row.split(' ')[0])]:
        nodes.append(nl_rev[int(row.split(' ')[0])].split("/")[-1])
        embeddings.append(embed)
        labels.append('Genes')
        node_counter += 1


# create embeddings
dats = pd.DataFrame(dict(id=nodes, embeds=embeddings))
lab_dat = dats.merge(pd.DataFrame(dict(id=nodes, grp=labels)), left_on='id', right_on='id', how='left')
lab_dat.to_pickle('./resources/graphs/ALL_KG_res_df')
lab_dat = pd.read_pickle('./resources/graphs/ALL_KG_res_df')

# when saving file in lower protocol for use with python 2 and 3
with open('./resources/graphs/ALL_KG_res_df','wb') as f:
    pickle.dump(lab_dat,f,protocol=2.7)

lab_dat.groupby(lab_dat['grp']).size()

## DIMENSIONALITY REDUCTION
# reduce dimensions first since t-sne is not good with sparse data
X_reduced = TruncatedSVD(n_components=50, random_state=1).fit_transform(list(lab_dat['embeds']))
X_embedded = TSNE(n_components=2, random_state=1, verbose=True, perplexity=50.0).fit_transform(X_reduced)
np.save('./resources/graphs/ALL_KG_res_tsne', X_embedded)
# X_embedded = np.load('./resources/graphs/ALL_KG_res_tsne.npy')
# KL divergence = 90.516678

## Make plot
# set up colors and legend labels
colors = {
    # 'Edge': 'crimson',
    'Diseases': '#009EFA',
    'Drugs': 'indigo',
    'Gene Ontology': '#F79862',
    'Genes' : '#4fb783',
    'Pathways' : 'orchid',
    'Phenotypes': '#A3B14B'}

names = {
    # 'Edge':'Edge Relations',
    'Diseases': 'DOID Diseases',
    'Drugs': 'CTD Chemicals',
    'Gene Ontology': 'GO Concepts',
    'Genes': 'Entrez Genes',
    'Pathways': 'Reactome Pathways',
    'Phenotypes': 'HP Phenotypes'}

# create data frame that has the result of the MDS plus the cluster numbers and titles
df = pd.DataFrame(dict(x=X_embedded[:, 0], y=X_embedded[:, 1], group=list(lab_dat['grp'])))

# filter our edges for now
df = df[df.group != 'Edge']

# group data by label
groups = df.groupby('group')
df.groupby(df['group']).size()

# set up plot
fig, ax = plt.subplots(figsize=(15, 10))
ax.margins(0.05)  # Optional, just adds 5% padding to the autoscaling
# iterate through groups to layer the plot
for name, group in groups:
    if name in ['Gene Ontology', 'Phenotype', 'Genes']:
        ax.plot(group.x, group.y, marker='o', linestyle='', ms=8, label=names[name],
                color=colors[name], mec='none', alpha=0.8)

    if name not in ['Gene Ontology', 'Phenotype', 'Genes']:
        ax.plot(group.x, group.y, marker='o', linestyle='', ms=6, label=names[name],
                color=colors[name], mec='none', alpha=0.8)

# edge = mpatches.Patch(color='crimson', label='Edges')
dis = mpatches.Patch(color='#009EFA', label='Human Diseases')
drg = mpatches.Patch(color='indigo', label='Chemicals')
go = mpatches.Patch(color='#F79862', label='GO Concepts')
ge = mpatches.Patch(color='#4fb783', label='Entrez Genes')
pat = mpatches.Patch(color='orchid', label='Reactome Pathways')
phe = mpatches.Patch(color='#A3B14B', label='HP Phenotypes')

plt.legend(handles=[dis, drg, go, ge, pat, phe], fontsize=16, frameon=False, loc="lower center", ncol=3)

# ax.legend(numpoints=1, fontsize=17, frameon=False, loc="upper right", ncol=1)  # show legend with only 1 point
ax.tick_params(labelsize=15)
plt.ylim(-65, 65)
plt.xlim(-65, 65)
# plt.ylim(-100, 100)
# plt.xlim(-100, 100)
plt.title('t-SNE: Biomedical Knowledge Embeddings', fontsize=22)
# plt.savefig(str(plot_file), bbox_inches='tight')
plt.show()
plt.close()

















# read in ignorome lists
# ig_genes = open('./resources/graphs/PE_ignorome_know_both_lists.txt').readlines()[1:]
# res = pd.read_csv("./resources/graphs/results_data.txt", sep="\t", header=0)

# ignorome = [int(x.split('\t')[0]) for x in ig_genes if len(x.split('\t')[0]) > 1]
# both = [int(x.split('\t')[1]) for x in ig_genes if len(x.split('\t')[1]) > 1]
# know = [int(x.split('\t')[2]) for x in ig_genes]

# both_ig = ignorome+both
node_id =[]
node_labs= {}
embeddings = []

for row in embed_file:
    node = int(row.split(' ')[0])
    row = row.strip("\n")

    if node in node_labels.values():
        embed = [float(x) for x in row.split(' ')[1:]]

        node_id.append(node)
        embeddings.append(embed)

        if node in ignorome:
            node_labs[node] = 'Uncharacterized'
            node_id.append(node)
            embeddings.append(embed)

        if node in know:
            node_labs[node] = 'Characterized'
            node_id.append(node)
            embeddings.append(embed)

        if node in both:
            node_labs[node] = 'Both'
            node_id.append(node)
            embeddings.append(embed)


# create embeddings
dats = pd.DataFrame(dict(id=node_id, embeds=embeddings))
lab_dat = dats.merge(pd.DataFrame(dict(id=list(node_labs.keys()), lables=list(node_labs.values()), lan_num = [1 if x == "Characterized" else 0 for x in node_labs.values()])),left_on='id', right_on='id', how='left')



## KG APPLIED TO EXPRESSION DATA
# read in embedding file and re-generate node labels
embed_file = open('./resources/graphs/out.txt').readlines()[1:]
node_labels = json.loads(open('./resources/graphs/KG_triples_ints_map.json').read())

# read in ignorome lists
res = pd.read_csv("./resources/graphs/DEgenes_HbSS_HbSC.txt", sep="\t", header=0)
# res = pd.read_csv("./resources/graphs/CF_DEgenes.txt", sep="\t", header=0)
res = res.dropna(subset = ['Genes.ENTREZID'])

node_id = []
grp = []
nodes = []
genes = []
embeddings = []

for row in embed_file:
    node = int(row.split(' ')[0])
    row = row.strip("\n")

    if node in node_labels.values():
        if len(res[(res['Genes.ENTREZID'] == node)]) != 0:
            grp.append(str(res[(res['Genes.ENTREZID'] == node)]['ss_sc']).split("\n")[0].split("  ")[-1])
            # grp.append(str(res[(res['Genes.ENTREZID'] == node)]['grp']).split("\n")[0].split("  ")[-1])
            node_id.append(node)
            embeddings.append([float(x) for x in row.split(' ')[1:]])
            # node_labs[node] = [dir, dir1]
            nodes.append(node)
            genes.append([k for k, v in node_labels.items() if node == v][0].split('/')[-1])


dats = pd.DataFrame(dict(id=node_id, embeds=embeddings))
lab_dat = dats.merge(pd.DataFrame(dict(id=list(nodes), grp=grp)), left_on='id', right_on='id', how='left')

# lab_dat.to_csv('./resources/graphs/HbSSvsHbSC_res_embed_df.txt', sep='\t', encoding='utf-8', index=False)
# lab_dat = pd.read_csv('./resources/graphs/CF_res_embed_df.txt', sep="\t", header=0)
lab_dat = pd.read_csv('./resources/graphs/HbSS_res_embed_df.txt', sep="\t", header=0)
# lab_dat.to_csv('./resources/graphs/HbSSvsHbSC.txt', sep='\t', encoding='utf-8', index=False)

## DIMENSIONALITY REDUCTION

# reduce dimensions first since t-sne is not good with sparse data
X_reduced = TruncatedSVD(n_components=50, random_state=1).fit_transform(list(lab_dat['embeds']))
X_embedded = TSNE(n_components=2, random_state=1, verbose=True, perplexity=50.0).fit_transform(X_reduced)
# np.save('./resources/graphs/CF_res_tsne', X_embedded)
X_embedded = np.load('./resources/graphs/HbSS_res_tsne.npy')

X_embedded.shape

import matplotlib.patches as mpatches

colors = {"HbSS_Down": '#5AF158', "HbSS_Up": 'red', 'Control': '#F6C667'}
names = {'HbSS_Down': 'Downregulated', 'HbSS_Up': 'Upregulated','Control': 'Control'}

colors = {"CF_Down": '#5AF158', "CF_Up": 'red'}
names = {'CF_Down': 'Downregulated', 'CF_Up': 'Upregulated'}

ctrl = mpatches.Patch(color='red', label='Upregulated')
case = mpatches.Patch(color='#5AF158', label='Downregulated')


# create data frame that has the result of the MDS plus the cluster numbers and titles
df = pd.DataFrame(dict(x=X_embedded[:, 0], y=X_embedded[:, 1], group=[x.strip() for x in list(lab_dat['grp'])]))
groups = df.groupby('group')  # group by cond

# set up plot #2
fig, ax = plt.subplots(figsize=(14, 8))  # set size
# ax.margins(0.05)  # Optional, just adds 5% padding to the autoscaling

# iterate through groups to layer the plot
for name, group in groups:
    if name == 'HbSS_Up':
        ax.plot(group.x, group.y, marker='o', linestyle='', ms=9, label=names[name], color=colors[name], mec='none',
                alpha=0.6)

for name, group in groups:
    if name != 'HbSS_Up':
        ax.plot(group.x, group.y, marker='o', linestyle='', ms=7, label=names[name],
                color=colors[name], mec='none', alpha=0.6)

# ax.legend(numpoints=1, fontsize=18, frameon=False, loc="lower center", ncol=3)  # show legend with only 1 point
plt.legend(handles=[ctrl, case], fontsize=18, frameon=False, loc="lower center", ncol=5)
ax.tick_params(labelsize=16)
plt.ylim(-70,70)
plt.xlim(-70,70)
# plt.title("t-SNE Knowledge Graph Embeddings for Up and Down Regulated HbSS vs. HbSC DEGs", fontsize=20)
# plt.title("t-SNE Knowledge Graph Embeddings for Up and Down Regulated CF DEGs")
plt.show()
plt.close()





# create projection from PCA plot
# reduce high dimension data to 50 dimensions with PCA before running t-sne
pca_50 = PCA(n_components=50)
pca_result_50 = pca_50.fit_transform(vec)
'{}'.format(pca_50.explained_variance_ratio_)


svd = TruncatedSVD(n_components=50, n_iter=100)
svd.results = svd.fit_transform(vec)
print(svd.explained_variance_ratio_)
print(svd.explained_variance_ratio_.sum())

# run t-sne on Truncated SVD reduced dimensions
tsne = TSNE(n_components=2, perplexity=450, n_iter=300, metric='cosine')
result = tsne.fit_transform(svd.results)

# create a scatter plot of the projection
matplotlib.style.use('ggplot')
plt.scatter(group.x, group.y, edgecolor='', alpha=0.5, lw=2)
# create legend
green = mlines.Line2D([], [], color='green', marker='o', markersize=4, label='Ignorome (n=445)',
                      linestyle='')
# orange = mlines.Line2D([], [], color='orange', marker='o', markersize=4, label='Literature+DE (n=103)',
#                       linestyle='')
magenta = mlines.Line2D([], [], color='magenta', marker='o', markersize=4, label='Published (n=843)',
                        linestyle='')
plt.legend(handles=[green, magenta],scatterpoints=1,
           loc='lower right',
           fontsize=9.5, facecolor='white')
plt.ylim([-6,6])
plt.xlim([-6,6])
plt.title('t-SNE on Preeclampsia Ignorome and Published Genes', fontsize = 11.5)
plt.xlabel('x')
plt.ylabel('y')




# scatter plot 1
fig, ax = plt.subplots()
ax.scatter(X_embedded[:, 0], X_embedded[:, 1], alpha=0.6, marker='o', color='mediumseagreen')

plt.annotate(1, ((-25.00, -18.00)), fontsize=20)
plt.annotate(2, ((-18.00, 8.00)), fontsize=20)
plt.annotate(3, ((-9.00, 22.00)), fontsize=20)
plt.annotate(4, ((15.00, 5.00)), fontsize=20)
plt.annotate(5, ((-1.00, -6.00)), fontsize=20)
plt.annotate(6, ((8.00, -25.00)), fontsize=20)
plt.show()

# for i, txt in enumerate(list(lab_dat['id'])):
#     # print(txt)
#     if txt == 2321:
#         x = X_embedded[:, 0][i]
#         y = X_embedded[:, 1][i]
#         print(x,y)
#     if (x > 0 and x<22.0) and (y > -10.0 and y<20):
#         print(txt)
#     plt.annotate(txt, (X_embedded[:, 0][i], X_embedded[:, 1][i]), fontsize=8)
#
# plt.show()

import rdflib

g = rdflib.Graph()
hp = g.parse("resources/ontologies/hp_with_imports.owl")

myfile2 = open('DOID_Mapping.txt', 'w')
for subj, pred, obj in g:
    if "hasDbXref" in pred and "HP" in subj:
        myfile2.write(str(subj.split("/")[-1]) + "\t" + str(obj) + "\n")

myfile2.close()


g = rdflib.Graph()
doid = g.parse("resources/ontologies/doid_with_imports.owl")

myfile2 = open('DOID_Mapping.txt', 'w')
for subj, pred, obj in g:
    if "hasDbXref" in pred and "DOID" in subj:
        myfile2.write(str(subj.split("/")[-1]) + "\t" + str(obj) + "\n")

myfile2.close()








