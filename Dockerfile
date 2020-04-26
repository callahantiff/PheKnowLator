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
COPY resources/construction_approach/subclass_construction_map.pkl /PheKnowLator/resources/construction_approach/subclass_construction_map.pkl
COPY resources/knowledge_graphs/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl /PheKnowLator/resources/knowledge_graphs/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl
COPY resources/owl_decoding/OWL_NETS_Property_Types.txt /PheKnowLator/resources/owl_decoding/OWL_NETS_Property_Types.txt
COPY resources/processed_data/MESH_CHEBI_MAP.txt /PheKnowLator/resources/processed_data/MESH_CHEBI_MAP.txt
COPY resources/processed_data/DISEASE_DOID_MAP.txt /PheKnowLator/resources/processed_data/DISEASE_DOID_MAP.txt
COPY resources/processed_data/PHENOTYPE_HPO_MAP.txt /PheKnowLator/resources/processed_data/PHENOTYPE_HPO_MAP.txt
COPY resources/processed_data/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt /PheKnowLator/resources/processed_data/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt
COPY resources/processed_data/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt /PheKnowLator/resources/processed_data/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt
COPY resources/processed_data/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt /PheKnowLator/resources/processed_data/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt
COPY resources/processed_data/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt /PheKnowLator/resources/processed_data/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt
COPY resources/processed_data/HPA_GTEx_TISSUE_CELL_MAP.txt /PheKnowLator/resources/processed_data/HPA_GTEx_TISSUE_CELL_MAP.txt
COPY resources/processed_data/STRING_PRO_ONTOLOGY_MAP.txt /PheKnowLator/resources/processed_data/STRING_PRO_ONTOLOGY_MAP.txt

# HANDLE DEPENDENCIES
RUN chmod -R 755 /PheKnowLator
RUN chmod +x /PheKnowLator/pkt_kg/libs/owltools


ENTRYPOINT ["python",  "/PheKnowLator/Main.py", \
            "--onts", "/PheKnowLator/resources/ontology_source_list.txt", \
            "--edg", "/PheKnowLator/resources/edge_source_list.txt", \
            "--res", "/PheKnowLator/resources/resource_info.txt", \
            "--out", "/PheKnowLator/resources/knowledge_graphs"]

CMD ["-h"]
