#!/usr/local/bin/docker
# -*- version: 20.10.2 -*-

# import phase 3 container
FROM callahantiff/pheknowlator:latest

# allow statements and log messages to immediately appear in logs
ENV PYTHONUNBUFFERED True

# copying local scripts to container
COPY *.py *.txt /PheKnowLator/builds/

# set wroking directory in container
WORKDIR /PheKnowLator/builds/

# download
RUN wget https://github.com/callahantiff/PheKnowLator/raw/master/pkt_kg/libs/owltools && \
    chmod 755 owltools

# install requirements
RUN pip install -r build_requirements.txt

# set OWlTools memory (set to a high value, system will only use available memory)
ENV OWLTOOLS_MEMORY=100g
RUN echo $OWLTOOLS_MEMORY

# set python envrionment encoding
RUN export PYTHONIOENCODING=utf-8

ENTRYPOINT ["python", "phases1_2_entrypoint.py"]