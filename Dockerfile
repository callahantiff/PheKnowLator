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


## DOWNLOAD PHEKNOWLATOR - clone library from GitHub
RUN git clone https://github.com/callahantiff/PheKnowLator.git

# install needed python libraries
RUN pip install --upgrade pip setuptools
RUN pip install -r /PheKnowLator/requirements.txt


## SET-UP LOCAL DIRECTORIES AND COPY NEEDED DATA
# copy directories
COPY resources/node_data /PheKnowLator/resources/node_data
COPY resources/relations_data /PheKnowLator/resources/relations_data

# copy local files
COPY resources/construction_approach/subclass_construction_map.pkl /PheKnowLator/resources
COPY resources/knowledge_graphs/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl /PheKnowLator/resources/knowledge_graphs/
COPY resources/owl_decoding/OWL_NETS_Property_Types.txt /PheKnowLator/resources/
COPY resources/processed_data/MESH_CHEBI_MAP.txt /PheKnowLator/resources/
COPY resources/processed_data/DISEASE_DOID_MAP.txt /PheKnowLator/resources/processed_data/
COPY resources/processed_data/PHENOTYPE_HPO_MAP.txt /PheKnowLator/resources/processed_data/
COPY resources/processed_data/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt /PheKnowLator/resources/processed_data/
COPY resources/processed_data/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt /PheKnowLator/resources/processed_data/
COPY resources/processed_data/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt /PheKnowLator/resources/processed_data/
COPY resources/processed_data/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt /PheKnowLator/resources/processed_data/
COPY resources/processed_data/HPA_GTEx_TISSUE_CELL_MAP.txt /PheKnowLator/resources/processed_data/
COPY resources/processed_data/STRING_PRO_ONTOLOGY_MAP.txt /PheKnowLator/resources/processed_data/


# HANDLE DEPENDENCIES
RUN chmod -R 755 /PheKnowLator


# SET OWLTOOLS MEMORY (SET HIGH, USES WHATEVER IS AVAILABLE)
ENV OWLTOOLS_MEMORY=500g
RUN echo $OWLTOOLS_MEMORY


ENTRYPOINT ["/PheKnowLator/entrypoint.sh"]
CMD ["-h"]
