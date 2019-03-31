##########################################################################################
# EmbeddingProcessing.py
# Purpose: script processes embedding results generated from running DeepWalk
# version 1.0.0
# date: 11.28.2017
# Python 3.6.2
##########################################################################################


# import needed libraries
import json
import tensorflow as tf


def NodeLabeler(embed_file, node_labels):
    """
    The function takes two strings as inputs representing semantic embedding results from running DeepWalk and a
    mapping between the node integers and corresponding node labels. With this information the function relabels the
    nodes with their string label and returns this information as a list. NEED TO IMPROVE THIS IT TAKES FOREVER TO
    RE-LABEL ~100K NODES

    :param
        embed_file (str): a string containing the file path/name of the embedding results

        node_labels (str): a string containing the file path/name of the node label mapping

    :return:
        labeled_nodes (list): a list of strings were each string represents a node and its embedding

    """
    labeled_nodes = []
    count = 0

    outfile = open('./resources/graphs/out_label.txt', "w")

    # loop over nodes and relabel nodes
    for i in embed_file:
        node = i.split(' ')[0]
        embed = ' '.join(i.strip('\n').split(' ')[1:])

        print(count)

        # retrieve node label
        key = [k for k,v in node_labels.items() if node in str(v)][0]

        # append label back to embeddings
        labeled_nodes.append(str(key) + ' ' + str(embed))
        outfile.write(str(key) + ' ' + str(embed) + "\n")

        count += 1

    outfile.close()

    # verify that the re-labeling worked
    # ASSUMPTION: we should have a label for every node
    if len(embed_file) != len(labeled_nodes):
        raise Exception('ERROR: Some of the nodes could not be relabeled')
    else:
        return labeled_nodes


def CosineSimilarity(filtered_nodes):
    """
    Function takes a dictionary of vectorized embeddings and calculates cosine similarity. Here, we calculate cosine
    similarity as cos=(A*B/||A||_2||B||_2), where the denominator is calculating L2 norm (square every dimension of
    the vector, sum the squares and take to root of the sum) for both A and B. Best practice to normalize the vectors
    by dividing the embeddings by L2 norm. The function calculates the cosine similarity between all nodes and
    returns a list of nodes and their corresponding cosine similarity ot all other nodes

    :param
        filtered_nodes (dict): a dictionary of nodes that matched the node_pattern (keys:nodes, values:embeddings)

    :return:
        cosine_sim (list): list of nodes and their corresponding cosine similarity ot all other nodes

    """

    # loop over all nodes and compute pairwise cosine similarity
    for node, emb in filtered_nodes.items():
        print(node, emb)

        A = filtered_nodes['673']
        B = filtered_nodes['6654']

        embeddings = [A,B]


        np.dot(A, B)/(np.linalg.norm(A) * np.linalg.norm(B))



def SimCalc(labeled_nodes, node_pattern):
    """
    Function takes a list of strings were each string represents a node and its embedding and a string containing a
    patter to filter labeled nodes by. The function uses the pattern to filter out nodes from the embedding list.
    Once filtered, the function returns a dictionary of nodes that matched the node_pattern (keys:nodes,
    values:embeddings)

    :param
        labeled_nodes (list): a list of strings were each string represents a node and its embedding

        node_pattern (str): a string containing a pattern to filter nodes on

    :return:
        filtered_nodes (dict): a dictionary of nodes that matched the node_pattern (keys:nodes, values:embeddings)

    """
    filtered_nodes = {}

    # loop over labeled nodes and filter by pattern
    for node in labeled_nodes:
        if node_pattern in node.split(' ')[0]:
            # add node to dictionary and convert embeddings list from string to float
            filtered_nodes[node.split(' ')[0].split('/')[-1]] = [float(x) for x in node.split(' ')[1:]]

    return filtered_nodes









def main():

    # read in embedding file and re-generate node labels
    embed_file = open('./resources/graphs/out.txt').readlines()[1:]
    node_labels = json.loads(open('./resources/graphs/KG_triples_ints_map.json').read())

    # Relabel nodes
    labeled_nodes = NodeLabeler(embed_file, node_labels)

    # calculate similarity between nodes - think that we only want to do this for genes
    node_pattern = 'http://purl.uniprot.org/geneid/'
    filtered_nodes = SimCalc(labeled_nodes, node_pattern)

    # calculate cosine similarity








if __name__ == "__main__":
    main()