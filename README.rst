pkt_kg
=========================================================================================

.. image:: https://api.codacy.com/project/badge/Grade/e06f0f13736e49c588244075122c8b94
   :alt: Codacy Badge
   :target: https://app.codacy.com/gh/callahantiff/PheKnowLator?utm_source=github.com&utm_medium=referral&utm_content=callahantiff/PheKnowLator&utm_campaign=Badge_Grade

|travis| |sonar_quality| |sonar_maintainability| |codacy|  |coveralls| |sonar_coverage|
|ABRA| 

.. |pip| |downloads|


PheKnowLator (Phenotype Knowledge Translator) or ``pkt_kg`` is a fully automated Python 3 library explicitly designed for optimized construction of semantically-rich, large-scale, biomedical knowledge graphs from complex heterogeneous data. Detailed information regarding this project can be found on the project `Wiki`_.

**This is a Reproducible Research Repository:** For detailed information on how we use GitHub as a reproducible research platform, click `here`_.

|

**Prelimary Results presented at the 2020 annual International Conference on Intelligent Systems for Molecular Biology (ISMB) are available:**

Callahan TJ, Tripodi IJ, Hunter LE, Baumgartner WA. A Framework for Automated Construction of Heterogeneous Large-Scale Biomedical Knowledge Graphs. 2020; BioRxiv `DOI: https://doi.org/10.1101/2020.04.30.071407 <https://doi.org/10.1101/2020.04.30.071407>`__

    
|

Releases
----------------------------------------------

All code and output for each release are free to download, see `Wiki <https://github.com/callahantiff/PheKnowLator/wiki>`__ for full release archive.

**Current Release:**  

- ``v2.0.0`` ➞ data and code can be directly downloaded `here <https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0>`__.

**Prior Releases:**  

- ``v1.0.0`` ➞ data and code can be directly downloaded `here <https://github.com/callahantiff/PheKnowLator/wiki/v1.0.0>`__.

|

Important Updates and Notifications  
----------------------------------------------

- *10/01/2020:*  We are hard at work on release ``v2.0.0`` and will announce here when the release is ready for public consumption!  
- *11/16/2020:* Have been alerted about minor issues with ``Data_Preparation.ipynb`` and ``Ontology_Cleaning.ipynb`` notebooks. We will be re-working some of the content in these notebooks to depend less on the source data providers. Look for new versions with the ``v2.0.0`` release content. Thanks for your patience!

|

Getting Started
----------------------------------------------

**Install Library**   

This program requires Python version 3.6. To install the library from PyPI, run:

.. code:: shell

  pip install pkt_kg

|

You can also clone the repository directly from GitHub by running:

.. code:: shell

  git clone https://github.com/callahantiff/PheKnowLator.git

|
|

**Set-Up Environment**     

The ``pkt_kg`` library requires a specific project directory structure.  

- If you plan to run the code from a cloned version of this repository, then no additional steps are needed.  
- If you are planning to utilize the library without cloning the library, please make sure that your project directory includes the following sub-directories:  

.. code:: shell

    PheKnowLator/  
        |
        |---- resources/
        |         |
        |     construction_approach/
        |         |
        |     edge_data/
        |         |
        |     knowledge_graphs/
        |         |
        |     node_data/
        |         |
        |     ontologies/
        |         |
        |     owl_decoding/
        |         |
        |     relations_data/

|
|

**Create Input Dependencies**   

Several input documents must be created before the ``pkt_kg`` library can be utilized. Each of the input documents are listed below by knowledge graph build step:  

*DOWNLOAD DATA*  

This code requires three documents within the ``resources`` directory to run successfully. For more information on these documents, see `Document Dependencies`_:
  
* `resources/resource_info.txt`_  
* `resources/ontology_source_list.txt`_  
* `resources/edge_source_list.txt`_

For assistance in creating these documents, please run the following from the root directory:

.. code:: bash

    python3 pkt/generates_dependency_documents.py

Prior to running this step, make sure that all mapping and filtering data referenced in `resources/resource_info.txt`_ have been created. Please see the `Data_Preparation.ipynb`_ Jupyter Notebook for detailed examples of the steps used to build the `v2.0.0 knowledge graph <https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0>`__.
  
*Note.* To ensure reproducibility, after downloading data, a metadata file is output for the ontologies (`ontology_source_metadata.txt`_) and edge data sources (`edge_source_metadata.txt`_). 

|

*CONSTRUCT KNOWLEDGE GRAPH*  

The `KG Construction`_ Wiki page provides a detailed description of the knowledge construction process (please see the knowledge graph `README`_ for more information). Please make sure you have created the documents listed below prior to constructing a knowledge graph. Click on each document for additional information.
  
* `resources/construction_approach/subclass_construction_map.pkl`_  
* `resources/Master_Edge_List_Dict.json`_ ➞ *automatically created after edge list construction*  
* `resources/node_data/*.txt`_ ➞ *if adding metadata for new edges to the knowledge graph*   
* `resources/knowledge_graphs/PheKnowLator_MergedOntologies*.owl`_ ➞ *see* `ontology README`_ *for information*
* `resources/owl_decoding/OWL_NETS_Property_Types.txt`_ 
* `resources/relations_data/RELATIONS_LABELS.txt`_  
* `resources/relations_data/INVERSE_RELATIONS.txt`_ ➞ *if including inverse relations*

|
|
      
**Running the pkt Library**

There are several ways to run ``pkt_kg``. An example workflow is provided below.

.. code:: python

 from pkt import downloads, edge_list, knowledge_graph

 # DOWNLOAD DATA
 # ontology data
 ont = pkt.OntData('resources/ontology_source_list.txt')
 ont.downloads_data_from_url()
 ont.writes_source_metadata_locally()

 # edge data sources
 edges = pkt.LinkedData('resources/edge_source_list.txt')
 edges.downloads_data_from_url()
 edges.writes_source_metadata_locally()

 # CREATE MASTER EDGE LIST
 combined_edges = dict(edges.data_files, **ont.data_files)

 # initialize edge dictionary class
 master_edges = pkt.CreatesEdgeList(combined_edges, './resources/resource_info.txt')
 master_edges.creates_knowledge_graph_edges()

 # BUILD KNOWLEDGE GRAPH
 # full build, subclass construction approach, with inverse relations and node metadata, and decode owl
 kg = PartialBuild(kg_version='v2.0.0',
                   write_location='./resources/knowledge_graphs',
                   construction='subclass,
                   edge_data='./resources/Master_Edge_List_Dict.json',
                   node_data='yes,
                   inverse_relations='yes',
                   decode_owl='yes',
                   kg_metadata_flag='yes')

 kg.construct_knowledge_graph()  

|
|

This repo provides 3 different of ways to run ``pkt_kg``:  

*COMMAND LINE* ➞ `Main.py`_

.. code:: bash

    python3 Main.py -h
    usage: Main.py [-h] -g ONTS -e EDG -a APP -t RES -b KG -o OUT -n NDE -r REL -s OWL -m KGM

    PheKnowLator: This program builds a biomedical knowledge graph using Open Biomedical Ontologies
    and linked open data. The program takes the following arguments:

    optional arguments:
    -h, --help            show this help message and exit
    -g ONTS, --onts ONTS  name/path to text file containing ontologies
    -e EDG,  --edg EDG    name/path to text file containing edge sources
    -a APP,  --app APP    construction approach to use (i.e. instance or subclass
    -t RES,  --res RES    name/path to text file containing resource_info
    -b KG,   --kg KG      the build, can be "partial", "full", or "post-closure"
    -o OUT,  --out OUT    name/path to directory where to write knowledge graph
    -n NDE,  --nde NDE    yes/no - adding node metadata to knowledge graph
    -r REL,  --rel REL    yes/no - adding inverse relations to knowledge graph
    -s OWL,  --owl OWL    yes/no - removing OWL Semantics from knowledge graph
    -m KGM,  --kgm KGM    yes/no - adding node metadata to knowledge graph      

|
|

*JUPYTER NOTEBOOK* ➞ `main.ipynb`_

|
|

*DOCKER*  

``pkt_kg`` can be run using a Docker instance. In order to utilize the Dockerized version of the code, please make sure that you have downloaded the newest version of `Docker <https://docs.docker.com/get-docker/>`__.

There are two ways to utilize Docker with this repository:  

- Obtain the pre-built Docker container from `DockerHub <https://docs.docker.com/get-docker/>`__  
- Build the Container  

|

*Build the Container*   

To build the ``pkt_kg`` Docker container:  

- Download a stable release of this repository or clone this repository to get the most up-to-date version  
- Unpack the repository downloaded (if necessary), then execute the following commands to build the container:

.. code:: bash

    cd /path/to/PheKnowLator (Note, this is the directory containing the Dockerfile file)
    docker build -t pkt:[VERSION] .

*NOTES:* When building a container using new data sources, the only files that you should have to update are the ``pkt_kg`` input dependency documents (i.e. ``PheKnowLator/resources/resource_info.txt``, ``PheKnowLator/resources/edge_source_list.txt``, and ``PheKnowLator/resources/ontology_source_list.txt``) and the ``PheKnowLatpr/.dockerignore`` (i.e. updating the sources listed under the ``## DATA NEEDED TO BUILD KNOWLEDGE GRAPH ##`` comment, to make sure they match the file paths for all datasets used to map indeitifers listed in the ``PheKnowLator/resources/resource_info.txt`` document).

|

*Run the Container*  

The following code can be used to run ``pkt_kg`` from outside of the container (after obtaining a prebuilt container or after building the container locally). In:  

.. code:: bash

    docker run --name [DOCKER CONTAINER NAME] -it pkt:[VERSION] --app subclass --kg full --nde yes --rel yes --owl no --kgm yes

|

*NOTES*:  

- The example shown above builds a full version of the knowledge graph using the subclass construction approach with node metadata, inverse relations, and decoding of OWL classes. See the **Running the pkt Library** section for more information on the parameters that can be passed to ``pkt_kg``  
- The Docker container cannot write to an encrypted filesystem, however, so please make sure ``/local/path/to/PheKnowLator/resources/knowledge_graphs`` references a directory that is not encrypted   

|

**Finding Data Inside Docker Container**  

In order to enable persistent data, a volume is mounted within the ``Dockerfile``. By default, Docker names volumes using a hash. In order to find the correctly mounted volume, you can run the following:  

*Command 1:* Obtains the volume hash:

.. code:: bash

    docker inspect --format='{{json .Mounts}}' [DOCKER CONTAINER NAME] | python -m json.tool   
    

*Command 2:* View data written to the volume:
 
.. code:: bash

    sudo ls /var/lib/docker/volumes/[VOLUME HASH]/_data  


--------------

--------------

|

Contributing
------------

Please read `CONTRIBUTING.md`_ for details on our code of conduct, and the process for submitting pull requests to us.

|

License
--------------

This project is licensed under Apache License 2.0 - see the `LICENSE.md`_ file for details.

|

Citing this Work
--------------

..

   @misc{callahan_tj_2019_3401437,
     author       = {Callahan, TJ},
     title        = {PheKnowLator},
     month        = mar,
     year         = 2019,
     doi          = {10.5281/zenodo.3401437},
     url          = {https://doi.org/10.5281/zenodo.3401437}
   }

|

Contact
--------------

We’d love to hear from you! To get in touch with us, please `create an issue`_ or `send us an email`_ 💌


.. |ABRA| image:: https://img.shields.io/badge/ReproducibleResearch-AbraCollaboratory-magenta.svg
   :target: https://github.com/callahantiff/Abra-Collaboratory   

.. |travis| image:: https://travis-ci.com/callahantiff/PheKnowLator.png
   :target: https://travis-ci.com/callahantiff/PheKnowLator
   :alt: Travis CI build

.. |sonar_quality| image:: https://sonarcloud.io/api/project_badges/measure?project=callahantiff_pkt_kg&metric=alert_status
    :target: https://sonarcloud.io/dashboard/index/callahantiff_pkt_kg
    :alt: SonarCloud Quality

.. |sonar_maintainability| image:: https://sonarcloud.io/api/project_badges/measure?project=callahantiff_pkt_kg&metric=sqale_rating
    :target: https://sonarcloud.io/dashboard/index/callahantiff_pkt_kg
    :alt: SonarCloud Maintainability

.. |sonar_coverage| image:: https://sonarcloud.io/api/project_badges/measure?project=callahantiff_pkt_kg&metric=coverage
    :target: https://sonarcloud.io/dashboard/index/callahantiff_pkt_kg
    :alt: SonarCloud Coverage

.. |coveralls| image:: https://coveralls.io/repos/github/callahantiff/PheKnowLator/badge.svg?branch=master
    :target: https://coveralls.io/github/callahantiff/PheKnowLator?branch=master
    :alt: Coveralls Coverage

.. |pip| image:: https://badge.fury.io/py/pkt_kg.svg
    :target: https://badge.fury.io/py/pkt_kg
    :alt: Pypi project

.. |downloads| image:: https://pepy.tech/badge/pkt_kg
    :target: https://pepy.tech/badge/pkt_kg
    :alt: Pypi total project downloads

.. |codacy| image:: https://api.codacy.com/project/badge/Grade/2cfa4ef5f9b6498da56afea0f5dadeed
    :target: https://www.codacy.com/manual/callahantiff/PheKnowLator?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=callahantiff/PheKnowLator&amp;utm_campaign=Badge_Grade
    :alt: Codacy Maintainability

.. |code_climate_maintainability| image:: https://api.codeclimate.com/v1/badges/29b7199d02f90c80130d/maintainability
    :target: https://codeclimate.com/github/callahantiff/PheKnowLator/maintainability
    :alt: Maintainability

.. |code_climate_coverage| image:: https://api.codeclimate.com/v1/badges/29b7199d02f90c80130d/test_coverage
    :target: https://codeclimate.com/github/callahantiff/PheKnowLator/test_coverage
    :alt: Code Climate Coverage
    
.. _Wiki: https://github.com/callahantiff/PheKnowLater/wiki

.. _here: https://github.com/callahantiff/Abra-Collaboratory/wiki/Using-GitHub-as-a-Reproducible-Research-Platform

.. _v2.0.0: https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0

.. _`Document Dependencies`: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies

.. _`Data_Preparation.ipynb`: https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb

.. _`resources/resource_info.txt`: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#master-resources

.. _`resources/ontology_source_list.txt`: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#ontology-data

.. _`resources/edge_source_list.txt`: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#edge-data

.. _`ontology_source_metadata.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/ontology_source_metadata.txt

.. _`edge_source_metadata.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/edge_data/edge_source_metadata.txt

.. _`KG Construction`: https://github.com/callahantiff/PheKnowLator/wiki/KG-Construction

.. _`README`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/knowledge_graphs/README.md

.. _`resources/construction_approach/subclass_construction_map.pkl`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/construction_approach/README.md

.. _`resources/Master_Edge_List_Dict.json`: https://www.dropbox.com/s/t8sgzd847t1rof4/Master_Edge_List_Dict.json?dl=1

.. _`resources/node_data/*.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/node_data/README.md

.. _`resources/knowledge_graphs/PheKnowLator_MergedOntologies*.owl`: https://www.dropbox.com/s/75lkod7vzpgjdaq/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl?dl=1

.. _`ontology README`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/README.md

.. _`resources/owl_decoding/OWL_NETS_Property_Types.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/owl_decoding/README.md

.. _`resources/relations_data/RELATIONS_LABELS.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/relations_data/README.md

.. _`resources/relations_data/INVERSE_RELATIONS.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/relations_data/README.md

.. _`main.ipynb`: https://github.com/callahantiff/pheknowlator/blob/master/main.ipynb

.. _`Main.py`: https://github.com/callahantiff/pheknowlator/blob/master/Main.py

.. _CONTRIBUTING.md: https://github.com/callahantiff/pheknowlator/blob/master/CONTRIBUTING.md

.. _LICENSE.md: https://github.com/callahantiff/pheknowlator/blob/master/LICENSE

.. _`create an issue`: https://github.com/callahantiff/PheKnowLator/issues/new/choose

.. _`send us an email`: https://mail.google.com/mail/u/0/?view=cm&fs=1&tf=1&to=callahantiff@gmail.com
