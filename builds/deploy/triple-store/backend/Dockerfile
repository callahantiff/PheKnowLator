FROM openkbs/blazegraph:latest
USER root

# the following supplants the existing RWStore.properties file
COPY RWStore.properties /home/developer/blazegraph/conf
COPY log4j.properties /home/developer/blazegraph/conf
COPY run-loader.sh /
COPY pom-blazegraph-loader.xml /

RUN chmod 755 /run-loader.sh && \
    mkdir /blazegraph-data && \
    ln -s /usr/jdk1.8.0_191/bin/java /usr/bin/java

# install the dependencies for the blazegraph loader
RUN mvn package -f /pom-blazegraph-loader.xml

RUN mkdir /pkt-data
WORKDIR /pkt-data

# download & load the pheknowlator n-triples
RUN wget https://storage.googleapis.com/pheknowlator/current_build/knowledge_graphs/instance_builds/inverse_relations/owlnets/PheKnowLator_v2.0.0_full_instance_inverseRelations_noOWL_OWLNETS.nt
RUN /run-loader.sh -z /home/developer/blazegraph/conf/log4j.properties -g file://pheknowlator -f ntriples -r kb -p /home/developer/blazegraph/conf/RWStore.properties -m mvn -l /pkt-data/PheKnowLator_v2.0.0_full_instance_inverseRelations_noOWL_OWLNETS.nt
RUN rm /pkt-data/PheKnowLator_v2.0.0_full_instance_inverseRelations_noOWL_OWLNETS.nt

# add file needed to ensure endpoint is read-only
COPY override-web.xml /home/developer/

RUN chown -R developer:developer /blazegraph-data
RUN chown -R developer:developer /home/developer
USER developer
CMD ["java", "-server", "-Xmx32g", "-jar", "-Dbigdata.propertyFile=/home/developer/blazegraph/conf/RWStore.properties", "-Djetty.start.timeout=3600000" , "-Djetty.overrideWebXml=/home/developer/override-web.xml", "/home/developer/blazegraph/blazegraph.jar"]
