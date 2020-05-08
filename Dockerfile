#!/usr/local/bin/docker
# -*- version: 19.03.8, build afacb8b -*-

FROM alpine:3.7

## INSTALL JAVA -- base container
RUN apk update \
&& apk upgrade \
&& apk add --no-cache bash \
&& apk add --no-cache --virtual=build-dependencies unzip \
&& apk add --no-cache curl \
&& apk add --no-cache openjdk8-jre


## INSTALL PYTHON -- copy docker
COPY --from=python:3.6.2 / /


## SET-UP PHEKNOWLATOR
RUN mkdir /PheKnowLator
COPY pkt_kg /PheKnowLator/
COPY Main.py /PheKnowLator/
COPY requirements.txt /PheKnowLator/

# install needed python libraries
RUN pip install --upgrade pip setuptools
RUN pip install -r /PheKnowLator/requirements.txt


## DOCKER DEPENDENCIES
COPY entrypoint.sh /PheKnowLator/


## SET-UP LOCAL DIRECTORIES AND COPY NEEDED DATA
# copy directories
COPY resources/construction_approach/ /PheKnowLator/construction_approach
COPY resources/knowledge_graphs/ /PheKnowLator/knowledge_graphs
COPY resources/node_data /PheKnowLator/resources/node_data
COPY resources/owl_decoding/ /PheKnowLator/owl_decoding
COPY resources/processed_data/ /PheKnowLator/resources/processed_data
COPY resources/relations_data /PheKnowLator/resources/relations_data

# copy local files
#COPY resources/construction_approach/*.pkl /PheKnowLator/resources/
#COPY resources/knowledge_graphs/PheKnowLator_MergedOntologies*.owl /PheKnowLator/resources/knowledge_graphs/
#COPY resources/owl_decoding/*.txt /PheKnowLator/resources/
#COPY resources/processed_data/MESH_*.txt /PheKnowLator/resources/
#COPY resources/processed_data/DISEASE_*.txt /PheKnowLator/resources/processed_data/
#COPY resources/processed_data/PHENOTYPE_*.txt /PheKnowLator/resources/processed_data/
#COPY resources/processed_data/ENTREZ_GENE_PRO_*.txt /PheKnowLator/resources/processed_data/
#COPY resources/processed_data/GENE_SYMBOL_*.txt /PheKnowLator/resources/processed_data/
#COPY resources/processed_data/ENSEMBL_GENE_*.txt /PheKnowLator/resources/processed_data/
#COPY resources/processed_data/UNIPROT_ACCESSION_*.txt /PheKnowLator/resources/processed_data/
#COPY resources/processed_data/HPA_GTEx_TISSUE_*.txt /PheKnowLator/resources/processed_data/
#COPY resources/processed_data/STRING_*.txt /PheKnowLator/resources/processed_data/


# HANDLE DEPENDENCIES
RUN chmod -R 755 /PheKnowLator


# SET OWLTOOLS MEMORY (SET HIGH, USES WHATEVER IS AVAILABLE)
ENV OWLTOOLS_MEMORY=500g
RUN echo $OWLTOOLS_MEMORY


ENTRYPOINT ["/PheKnowLator/entrypoint.sh"]
CMD ["-h"]
