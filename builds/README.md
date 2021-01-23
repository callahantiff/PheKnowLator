# PheKnowLator Builds  
**Current Release:** [`v2.0.0`](https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0)  

This directory stores the scripts and files needed for the automatic monthly builds. Our continuous integration (CI)
/Continuous deployment (CD) pipeline is managed entirely using [GitHub Actions](https://github.com/actions) and [Google's AI-Platform](https://cloud.google.com/ai-platform). Builds are triggered on the first of each month and consist of three separate asynchronous phases (each phase is briefly described below):  
1. Data Download 
2. Data Processing and Quality Control   
3. Knowledge Graph Construction  
 
## Phase 1: Download Build Data 
**Script(s):** `build_phase_1.py`  

This phase is triggered on the same date each month and consists of the following three steps:    
1. **Create Google Cloud Storage Bucket:** Current builds are stored in a new Google Cloud Storage bucket under the current release in a directory called `current_build` (e.g. `release_v2.0.0/current_build`). Each subsequent build is also archived and saved with the build date under the `archived_builds` directory for the current release (e.g. `release_v2.0.0/archived_builds/build_27DEC2020`). Under this directory, the rest of the needed build directories are created. See example below:
   ```
    release_V*.0.0
    |---- curated_data/
    |---- current_build/
    |     |---- data/
    |     |     |---- original_data/
    |     |     |---- processed_data/   
    |     |---- knowledge_graphs/  
    |     |     |---- subclass_builds/
    |     |     |     |---- relations_only/
    |     |     |     |     |---- owl/
    |     |     |     |     |---- owlnets/     
    |     |     |     |---- inverse_relations/
    |     |     |     |     |---- owl/
    |     |     |     |     |---- owlnets/     
    |     |     |---- instance_builds/
    |     |     |     |---- relations_only/
    |     |     |     |     |---- owl/
    |     |     |     |     |---- owlnets/     
    |     |     |     |---- inverse_relations/
    |     |     |     |     |---- owl/
    |     |     |     |     |---- owlnets/
    |---- archived_builds
    |     |---- *build_<<date>>/
    |     |     |---- data/
    |     |     |     |---- original_data/
    |     |     |     |---- processed_data/   
    |     |     |---- knowledge_graphs/  
    |     |     |     |---- subclass_builds/
    |     |     |     |     |---- relations_only/
    |     |     |     |     |     |---- owl/
    |     |     |     |     |     |---- owlnets/     
    |     |     |     |     |---- inverse_relations/
    |     |     |     |     |     |---- owl/
    |     |     |     |     |     |---- owlnets/     
    |     |     |     |---- instance_builds/
    |     |     |     |     |---- relations_only/
    |     |     |     |     |     |---- owl/
    |     |     |     |     |     |---- owlnets/     
    |     |     |     |     |---- inverse_relations/
    |     |     |     |     |     |---- owl/
    |     |     |     |     |     |---- owlnets/        
   ```
   <<date>> is the date of download.  
   curated_data is a directory of hand-curated resources utilized in the builds.  
   
2. **Download Knowledge Graph Data:** Using the `data_to_download.txt` text file, all needed build data are downloaded to the `data/original_data` directory associated with the current build. In this file, each source to download is provided as a single URL per row.   
3. **Upload Local Build Data:** Generate `download_metadata.txt` a document that lives in the `original_data` directory and provides provenance information on each downloaded data source.

<br>

## Phase 2: Preprocess Downloaded Build Data       
**Script(s):** `build_phase_2.py`; `pkt_kg/data_preprocessing.py`; `pkt_kg/ontology_cleaning.py`   
**Data:** `genomic_typing_dict.pkl`

This phase is triggered upon the successful completion of [Phase 1](#Phase-1:-Download-Build-Data) and consists of the following three steps:  
1. **Preprocess Linked Open Data:** Runs the `pkt_kg/data_preprocessing.py` script, which preprocesses and prepares all Linked Open Data sources (i.e. non-ontology data) needed to the build the knowledge graphs. The cleaned data are output to the `data/processed_data` directory associated with the current build along with a metadata document providing provenance information on the preprocessed and cleaned documents.      
2. **Preprocess Ontology Data:** Runs the `pkt_kg/ontology_cleaning.py` script, which preprocesses and prepares all ontology data needed to the build the knowledge graphs. The cleaned data are output to the `data/processed_data` directory associated with the current build along with a metadata document providing provenance information on the preprocessed and cleaned documents.  
3. **Update Input Build Dependencies:** The URLS referenced in the knowledge graph [input dependency documents](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies) `resource_info.txt`, `edge_source_list.txt`, and `ontology_source_list.txt` documents are updated with the Google Cloud Storage bucket URLs for each associated preprocessed document located in the `data/processed_data` directory associated with the current build.  
4. **Upload Local Build Data:** Generate `preprocessed_build_metadata.txt` a document that lives in the 
   `processed_data` directory and provides provenance information on each downloaded data source. Also uploads the ontology data cleaning results (`ontology_cleaning_report.txt`), which provides additional insight into the errors that were cleaned for each ontology.

<br>

## Phase 3: Build Knowledge Graph    
**Script(s):** `build_phase_3.py`; `complete_build.py`  

This phase is triggered by the successful completion of [Phase 2](#Phase-2:-Preprocess-Downloaded-Build-Data) and is the primary step responsible for constructing the PheKnowLator knowledge graphs and consists of the following seven steps:  
1. **Downloads Input Build Dependencies:** The `resource_info.txt`, `edge_source_list.txt`, and 
   `ontology_source_list.txt` documents updated in [Phase 2](#Phase-2:-Preprocess-Downloaded-Build-Data) are downloaded locally and committed to the Master branch of the GitHub repository.     
2. **Downloads Processed Data:** All data processed during [Phase 2](#Phase-2:-Preprocess-Downloaded-Build-Data) are downloaded in preparation of constructing the build Docker container.  
3. **Build Docker Container:** The primary build Docker container is built and published to Docker Hub.  
4. **Container Parameterization and Deployment:** GitHub Actions communicates with Google Cloud Run to duplicate the constructor container and parameterize it for each of the PheKnowLator builds allowing for the knowledge graphs to be constructed in parallel.  
5. **Completes Build:** Waits for each container to complete and then uploads associated data to the correct Google Cloud Storage bucket associated with the current build. The table below maps the names of each [GitHub Action Workflow](https://github.com/callahantiff/PheKnowLator/blob/master/.github/workflows/kg-build.yml) job to each build type.
6. **Update Public Endpoints:** After a successful build, knowledge graphs are pushed to:   
    - Blazegraph SPARQL Endpoint: [http://sparql.pheknowlator.com](http://sparql.pheknowlator.com/)  
    - Neo4J Endpoint and User Interface: [http://neo4j.pheknowlator.com]()  -- *COMING SOON*   

**GitHUb Actions - Phase 3 Build Job Names**
GitHub   Action Job | Job Name | Construction   Approach | Relations | OWL Decoding
:--: | -- | :--: | :--: | :--:
1 | Phase   3 - Job 1 (Subclass + RelationsOnly + OWL) | Subclass | Relations   Only | OWL
2 | Phase   3 - Job 1 (Subclass + RelationsOnly + No OWL) | Subclass | Relations   Only | No   OWL
3 | Phase   3 - Job 1 (Subclass +InverseRelations + OWL) | Subclass | Inverse   Relations | OWL
4 | Phase   3 - Job 1 (Subclass + InverseRelations + No OWL) | Subclass | Inverse   Relations | No   OWL
5 | Phase   3 - Job 1 (Instance + RelationsOnly + OWL) | Instance | Relations   Only | OWL
6 | Phase   3 - Job 1 (Instance + RelationsOnly + No OWL) | Instance | Relations   Only | No   OWL
7 | Phase   3 - Job 1 (Instance +InverseRelations + OWL) | Instance | Inverse   Relations | OWL
8 | Phase   3 - Job 1 (Instance + InverseRelations + No OWL) | Instance | Inverse   Relations | No   OWL

____

## Important Notes  
- The contents in this directory are updated for every release and as needed to prepare bugs and unexpected issues. Any important to changes to the workflow and associated scripts will be documented her as they arise.

- Any issues that may arise during any of the Phases described above will be pushed to a build log and saved under the current build's directory in the Google Cloud Storage bucket.  
  
