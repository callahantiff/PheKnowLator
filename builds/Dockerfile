# import phase 3 container
FROM callahantiff/pheknowlator:latest

# allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# copying local scripts to container
COPY *.py *.txt /PheKnowLator/builds/

# set wroking directory in container
WORKDIR /PheKnowLator/builds/

# download
RUN wget https://github.com/callahantiff/PheKnowLator/raw/kg_construction_fixes/pkt_kg/libs/owltools && \
    chmod 755 owltools

# install requirements
RUN pip install -r build_requirements.txt

ENTRYPOINT ["python", "entrypoint.py"]