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

# Download & load the pheknowlator n-triples
RUN wget http://purl.obolibrary.org/obo/cl.owl
RUN /run-loader.sh -z /home/developer/blazegraph/conf/log4j.properties -g file://pheknowlator -f rdfxml -r pheknowlator -p /home/developer/blazegraph/conf/RWStore.properties -m mvn -l /pkt-data/cl.owl

RUN chown -R developer:developer /blazegraph-data
USER developer
CMD ["java", "-server", "-Xmx10g", "-jar", "-Dbigdata.propertyFile=/home/developer/blazegraph/conf/RWStore.properties", "/home/developer/blazegraph/blazegraph.jar"]