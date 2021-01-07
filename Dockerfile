#!/usr/local/bin/docker
# -*- version: 19.03.8, build afacb8b -*-

############################################
## MULTI-STAGE CONTAINER CONFIGURATION ##
FROM python:3.6.2
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    software-properties-common \
    unzip \
    curl
RUN wget -O- https://apt.corretto.aws/corretto.key | apt-key add - && \
    add-apt-repository 'deb https://apt.corretto.aws stable main' && \
    apt-get update && \
    apt-get install -y java-1.8.0-amazon-corretto-jdk


############################################
## PHEKNOWLATOR (PKT_KG) PROJECT SETTINGS ##
# create needed project directories
#WORKDIR /PheKnowLator
#RUN mkdir /PheKnowLator
RUN mkdir /PheKnowLator/resources
RUN mkdir /PheKnowLator/resources/edge_data
RUN mkdir /PheKnowLator/resources/knowledge_graphs
RUN mkdir /PheKnowLator/resources/ontologies

# copy pkt_kg scripts
COPY pkt_kg /PheKnowLator/pkt_kg

# copy scripts/files needed to run pkt_kg
COPY Main.py /PheKnowLator
COPY setup.py /PheKnowLator
COPY README.rst /PheKnowLator
COPY resources /PheKnowLator/resources

# install needed python libraries
RUN pip install --upgrade pip setuptools
WORKDIR /PheKnowLator
RUN pip install .


############################################
## GLOBAL ENVRIONMENT SETTINGS ##
# copy files needed to run docker container
COPY entrypoint.sh /PheKnowLator

# update permissions for all files
RUN chmod -R 755 /PheKnowLator

# set OWlTools memory (set to a high value, system will only use available memory)
ENV OWLTOOLS_MEMORY=500g
RUN echo $OWLTOOLS_MEMORY

# set python envrionment encoding
RUN export PYTHONIOENCODING=utf-8

# create a volume to enable persistent data
VOLUME ["/PheKnowLator/resources"]


############################################
## CONTAINER ACCESS ##
# call bash script which contains entrypoint parameters
ENTRYPOINT ["/PheKnowLator/entrypoint.sh"]
CMD ["-h"]
