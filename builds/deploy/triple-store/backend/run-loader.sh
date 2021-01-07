#!/bin/bash

# NOTE: input arguments must be absolute paths

function print_usage {
    echo "Usage:"
    echo "$(basename $0) [OPTIONS]"
    echo "  [-g <default graph>]: The default graph to use for this load."
    echo "  [-f <format>]: The format of the incoming triples."
    echo "  [-r <repository name>]: The name of the repository (in blazegraph terminology, the 'namespace')"
    echo "  [-p <blazegraph properties file>]: MUST BE ABSOLUTE PATH. The path to the blazegraph properties file."
    echo "  [-l <file-to-load>]: MUST BE ABSOLUTE PATH. The path to the file to load. Note this can be multiple files space-delimited and can also be a directory."
    echo "  [-z <log4j properties file>]: MUST BE ABSOLUTE PATH. The path to the log4j properties file."
    echo "  [-m <maven>]: MUST BE ABSOLUTE PATH. The path to the mvn command."
}

while getopts "z:g:f:r:p:l:m:h" OPTION; do
    case $OPTION in
        # The default graph to use for this load
        g) DEFAULT_GRAPH=$OPTARG
           ;;
        # The format of the incoming triples
        f) FORMAT=$OPTARG
           ;;
        # the name of the repository (in blazegraph terminology, the 'namespace')
        r) REPO_NAME=$OPTARG
           ;;
        # the path to the blazegraph properties file
        p) BLAZEGRAPH_PROPERTIES_FILE=$OPTARG
           ;;
        # the path to the file to load. Note this can be multiple files space-delimited and can also be a directory
        l) FILE_TO_LOAD=$OPTARG
           ;;
        # The path to the Apache Maven command
        m) MAVEN=$OPTARG
           ;;
        # The path to the log4j properties file
        z) LOG4J_PROPERTIES_FILE=$OPTARG
           ;;
        # HELP!
        h) print_usage; exit 0
           ;;
    esac
done

if [[ -z ${DEFAULT_GRAPH} || -z ${FORMAT} || -z ${REPO_NAME} || -z ${BLAZEGRAPH_PROPERTIES_FILE} || -z ${FILE_TO_LOAD} || -z ${MAVEN} || -z ${LOG4J_PROPERTIES_FILE} ]]; then
	echo "missing input arguments!!!!!"
	echo "default graph: ${DEFAULT_GRAPH}"
	echo "format: ${FORMAT}"
	echo "repo name: ${REPO_NAME}"
	echo "blazegraph properties file: ${BLAZEGRAPH_PROPERTIES_FILE}"
	echo "file-to-load: ${FILE_TO_LOAD}"
    echo "log4j properties file: ${LOG4J_PROPERTIES_FILE}"
	echo "maven: ${MAVEN}"
    print_usage
    exit 1
fi

#if ! [[ -e README.md ]]; then
#    echo "Please run from the root of the project."
#    exit 1
#fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$MAVEN -e -f ${DIR}/pom-blazegraph-loader.xml exec:exec \
        -Ddefault_graph=${DEFAULT_GRAPH} \
        -Dformat=${FORMAT} \
        -Drepo_name=${REPO_NAME} \
        -Dproperty_file=${BLAZEGRAPH_PROPERTIES_FILE} \
        -Dfile_to_load=${FILE_TO_LOAD} \
        -Dlog4j_properties_file=${LOG4J_PROPERTIES_FILE}