# PheKnowLator SPARQL Endpoint

The files in this directory facilitate the hosting of a PheKnowLator knowledge graph via a SPARQL Endpoint. The [DBCLS SPARQL Proxy Web Application](https://github.com/dbcls/sparql-proxy) is used as the front end, and the data is served from a [Blazegraph](https://github.com/blazegraph/database) triple store.

## Requirements
* [docker-compose](https://docs.docker.com/compose)

## Installation
1. Check out this repository
```
git clone https://github.com/callahantiff/PheKnowLator.git ./pheknowlator.git
cd ./pheknowlator.git/builds/deploy/triple-store
```
1. Build & launch the application
```
docker-compose build
docker-compose up
```

## Access
Once installed, visit `http://HOST_URL/sparql`, where `HOST_URL` is the URL of the host machine. If running locally, this will likely be http://localhost/sparql. If needed, the host port can be configured in the docker-compose.yml file.