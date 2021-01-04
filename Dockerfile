#!/usr/local/bin/docker
# -*- version: 19.03.8, build afacb8b -*-

############################################
## MULTI-STAGE CONTAINER CONFIGURATION ##
# use linux as base container
FROM alpine:3.7

# install java
RUN apk update \
&& apk upgrade \
&& apk add --no-cache bash \
&& apk add --no-cache --virtual=build-dependencies unzip \
&& apk add --no-cache curl \
&& apk add --no-cache openjdk8-jre

# install python
COPY --from=python:3.6.2 / /


############################################
## PHEKNOWLATOR (PKT_KG) PROJECT SETTINGS ##
# create needed project directories
RUN mkdir /PheKnowLator
RUN mkdir /PheKnowLator/resources
RUN mkdir /PheKnowLator/resources/edge_data
RUN mkdir /PheKnowLator/resources/knowledge_graphs
RUN mkdir /PheKnowLator/resources/ontologies

# copy pkt_kg scripts
COPY pkt_kg /PheKnowLator/pkt_kg

# copy scripts/files needed to run pkt_kg
COPY Main.py /PheKnowLator
COPY setup.py /PheKnowLator
COPY resources /PheKnowLator/resources

# install needed python libraries
RUN pip install --upgrade pip setuptools
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
