|logo| 


|github_action| |pip|  

|sonar_quality| |code_climate_maintainability| |codacy| |code_climate_coverage| |coveralls|

|ABRA|

***********************
What is PheKnowLator?
***********************

PheKnowLator (Phenotype Knowledge Translator) or ``pkt_kg`` is the first fully customizable knowledge graph (KG) construction framework enabling users to build complex KGs that are Semantic Web compliant and amenable to automatic Web Ontology Language (OWL) reasoning, generate contemporary property graphs, and are importable by today’s popular graph toolkits. Please see the project `Wiki <https://github.com/callahantiff/PheKnowLator/wiki>`__ for additional information.

What Does This Repository Provide?
===================================
1. **A Knowledge Graph Sharing Hub:** Prebuilt KGs and associated metadata. Each KG is provided as triple edge lists, OWL API-formatted ``RDF/XML`` and NetworkX graph-pickled MultiDiGraphs. We also make text files available containing node and relation metadata.
2. **A Knowledge Graph Building Framework:** An automated ``Python 3`` library designed for optimized construction of semantically-rich, large-scale biomedical KGs from complex heterogeneous data. The framework also includes Jupyter Notebooks to greatly simplify the generation of required input dependencies.

*NOTE.* A table listing and describing all output files generated for each build along with example output from each
file can be found `here <https://github.com/callahantiff/PheKnowLator/wiki/KG-Construction#table-knowledge-graph-build-output>`__.

How do I Learn More?
===================================
- Join and/or start a `Discussion`_
- The Project `Wiki`_ for available knowledge graphs, `pkt_kg` data sources, and the knowledge graph construction process
- Preliminary results from our 2020 ISMB presentation are available `here <https://doi.org/10.1101/2020.04.30.071407>`__

|

--------------------------------------------

************************************
Important Updates and Notifications
************************************

- *July 2021:*

  - Public SPARQL Endpoint: `http://sparql.pheknowlator.com/ <http://sparql.pheknowlator.com/>`__

    - Current Build: ``July 2021``  

    - Build Type: ``OWL-NETS``  instance-based + inverse relations and no metadata

  - Access build data via a dedicated Google Cloud Storage bucket (see details under ``Releases``)
  
  - New Jupyter Notebooks:

    - Applying ``OWL-NETS``: `OWLNETS_Example_Application.ipynb <https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/OWLNETS_Example_Application.ipynb>`__

    - Exploring ``pkt_kg`` knowledge graphs and other ontologies: `RDF_Graph_Processing_Example.ipynb <https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/RDF_Graph_Processing_Example.ipynb>`__

Releases
=========
All data and output for each release are free to download from our dedicated Google Cloud Storage Bucket (GCS). All data can be downloaded from the `PheKnowLator GCS Bucket <https://console.cloud.google.com/storage/browser/pheknowlator?project=pheknowlator>`__, which is organized by release and build.

Current Release
----------------
- ``v2``

  - `Build Documentation <https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0>`__
  - `Data Access <https://console.cloud.google.com/storage/browser/pheknowlator/release_v2.0.0?project=pheknowlator>`__

Prior Releases
-----------------
- ``v1.0.0``

  - `Build Documentation <https://github.com/callahantiff/PheKnowLator/wiki/v1.0.0>`__
  - `Data Access <https://console.cloud.google.com/storage/browser/pheknowlator/release_v1.0.0?project=pheknowlator>`__

|

----------------------------------

************************
Getting Started
************************

Install Library
================

This program requires Python version 3.6. To install the library from `PyPI <https://pypi.org/project/pkt-kg/>`_, run:

.. code:: shell

  pip install pkt_kg

|

You can also clone the repository directly from GitHub by running:

.. code:: shell

  git clone https://github.com/callahantiff/PheKnowLator.git

|

**Note.** Sometimes ``OWLTools``, which comes with the cloned/forked repository (``./pkt_kg/libs/owltools``) loses "executable" permission. To avoid any potential issues, I recommend running the following in the terminal from the PheKnowLator directory:

.. code:: shell

    chmod +x pkt_kg/libs/owltools

|

Set-Up Environment
===================
The ``pkt_kg`` library requires a specific project directory structure.

- If you plan to run the code from a cloned version of this repository, then no additional steps are needed.
- If you are planning to utilize the library without cloning the library, please make sure that your project directory matches the following:

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

Dependencies
-------------
Several input documents must be created before the ``pkt_kg`` library can be utilized. Each of the input documents are listed below by knowledge graph build step:

*DOWNLOAD DATA*
^^^^^^^^^^^^^^^^
This code requires three documents within the ``resources`` directory to run successfully. For more information on these documents, see `Document Dependencies`_:

* `resources/resource_info.txt`_
* `resources/ontology_source_list.txt`_
* `resources/edge_source_list.txt`_

For assistance in creating these documents, please run the following from the root directory:

.. code:: bash

    python3 generates_dependency_documents.py

Prior to running this step, make sure that all mapping and filtering data referenced in `resources/resource_info.txt`_ have been created or downloaded for an existing build from the `PheKnowLator GCS Bucket <https://console.cloud.google.com/storage/browser/pheknowlator?project=pheknowlator>`__. To generate these data yourself, please see the `Data_Preparation.ipynb`_ Jupyter Notebook for detailed examples of the steps used to build the `v2.0.0 knowledge graph <https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0>`__.

*Note.* To ensure reproducibility, after downloading data, a metadata file is output for the ontologies (`ontology_source_metadata.txt`_) and edge data sources (`edge_source_metadata.txt`_).

*CONSTRUCT KNOWLEDGE GRAPH*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The `KG Construction`_ Wiki page provides a detailed description of the knowledge construction process (please see the knowledge graph `README`_ for more information). Please make sure the documents listed below are presented in the specified location prior to constructing a knowledge graph. Click on each document for additional information. Note, that cloning this library will include a version of these documents that points to the current build. If you use this version then there is no need to download anything prior to running the program.

* `resources/construction_approach/subclass_construction_map.pkl`_
* `resources/Master_Edge_List_Dict.json`_ ➞ *automatically created after edge list construction*
* `resources/node_data/node_metadata_dict.pkl <https://github.com/callahantiff/PheKnowLator/blob/master/resources/node_data/README.md>`__ ➞ *if adding metadata for new edges to the knowledge graph*
* `resources/knowledge_graphs/PheKnowLator_MergedOntologies*.owl`_ ➞ *see* `ontology README`_ *for information*
* `resources/relations_data/RELATIONS_LABELS.txt`_
* `resources/relations_data/INVERSE_RELATIONS.txt`_ ➞ *if including inverse relations*

|

----------------------------------

************************
Running the pkt Library
************************

``pkt_kg`` can be run via the provided `main.py`_ script or using the `main.ipynb`_ Jupyter Notebook or using a Docker container.

Main Script or Jupyter Notebook
==========================================
The program can be run locally using the `main.py`_ script or using the `main.ipynb`_ Jupyter Notebook. An example of the workflow used in both of these approaches is shown below.

.. code:: python

 import psutil
 import ray
 from pkt import downloads, edge_list, knowledge_graph

 # initialize ray
 ray.init()

 # determine number of cpus available
 available_cpus = psutil.cpu_count(logical=False)

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
 master_edges = pkt.CreatesEdgeList(data_files=combined_edges, source_file='./resources/resource_info.txt')
 master_edges.runs_creates_knowledge_graph_edges(source_file'./resources/resource_info.txt',
                                                 data_files=combined_edges,
                                                 cpus=available_cpus)

 # BUILD KNOWLEDGE GRAPH
 # full build, subclass construction approach, with inverse relations and node metadata, and decode owl
 kg = PartialBuild(kg_version='v2.0.0',
                   write_location='./resources/knowledge_graphs',
                   construction='subclass,
                   node_data='yes,
                   inverse_relations='yes',
                   cpus=available_cpus,
                   decode_owl='yes')

 kg.construct_knowledge_graph()
 ray.shutdown()

``main.py``
-----------
The example below provides the details needed to run ``pkt_kg`` using ``./main.py``.

.. code:: bash

    python3 main.py -h
    usage: main.py [-h] [-p CPUS] -g ONTS -e EDG -a APP -t RES -b KG -o OUT -n NDE -r REL -s OWL -m KGM

    PheKnowLator: This program builds a biomedical knowledge graph using Open Biomedical Ontologies
    and linked open data. The program takes the following arguments:

    optional arguments:
    -h, --help            show this help message and exit
    -p CPUS, --cpus CPUS  # workers to use; defaults to use all available cores
    -g ONTS, --onts ONTS  name/path to text file containing ontologies
    -e EDG,  --edg EDG    name/path to text file containing edge sources
    -a APP,  --app APP    construction approach to use (i.e. instance or subclass
    -t RES,  --res RES    name/path to text file containing resource_info
    -b KG,   --kg KG      the build, can be "partial", "full", or "post-closure"
    -o OUT,  --out OUT    name/path to directory where to write knowledge graph
    -r REL,  --rel REL    yes/no - adding inverse relations to knowledge graph
    -s OWL,  --owl OWL    yes/no - removing OWL Semantics from knowledge graph

``main.ipynb``
---------------
The ``./main.ipynb`` Jupyter notebook provides detailed instructions for how to run the ``pkt_kg`` algorithm and build a knowledge graph from scratch.

|

Docker Container
=================
``pkt_kg`` can be run using a Docker instance. In order to utilize the Dockerized version of the code, please make sure that you have downloaded the newest version of `Docker <https://docs.docker.com/get-docker/>`__. There are two ways to utilize Docker with this repository:

- Obtain Pre-Built Container from `DockerHub <https://hub.docker.com/repository/docker/callahantiff/pheknowlator>`__
- Build the Container (see details below)

Obtaining a Container
----------------------
*Obtain Pre-Built Containiner:* A pre-built containers can be obtained directly from `DockerHub <https://hub.docker.com/repository/docker/callahantiff/pheknowlator/general>`__.

*Build Container:* To build the ``pkt_kg`` download a stable release of this repository (or fork/clone it repository). Once downloaded, you will have everything needed to build the container, including the ``./Dockerfile`` and ``./dockerignore``. The code shown below builds the container. Make sure to replace ``[VERSION]`` with the current ``pkt_kg`` version before running the code.

.. code:: bash

    cd /path/to/PheKnowLator (Note, this is the directory containing the Dockerfile file)
    docker build -t pkt:[VERSION] .

Notes:
^^^^^^
- Update ``PheKnowLator/resources/resource_info.txt``, ``PheKnowLator/resources/edge_source_list.txt``, and ``PheKnowLator/resources/ontology_source_list.txt``
- Building the container "as-is" off of DockerHub will include a download of the data used in the latest releases. No need to update any scripts or pre-download any data.

Running a Container
--------------------
The following code can be used to run ``pkt_kg`` from outside of the container (after obtaining a prebuilt container or after building the container locally). In:

.. code:: bash

    docker run --name [DOCKER CONTAINER NAME] -it pkt:[VERSION] --app subclass --kg full --nde yes --rel yes --owl no --kgm yes

Notes:
^^^^^^
- The example shown above builds a full version of the knowledge graph using the subclass construction approach with node metadata, inverse relations, and decoding of OWL classes. See the **Running the pkt Library** section for more information on the parameters that can be passed to ``pkt_kg``
- The Docker container cannot write to an encrypted filesystem, however, so please make sure ``/local/path/to/PheKnowLator/resources/knowledge_graphs`` references a directory that is not encrypted

Finding Data Inside a Container
------------------------------------
In order to enable persistent data, a volume is mounted within the ``Dockerfile``. By default, Docker names volumes using a hash. In order to find the correctly mounted volume, you can run the following:

**Command 1:** Obtains the volume hash:

.. code:: bash

    docker inspect --format='{{json .Mounts}}' [DOCKER CONTAINER NAME] | python -m json.tool


**Command 2:** View data written to the volume:

.. code:: bash

    sudo ls /var/lib/docker/volumes/[VOLUME HASH]/_data

|

---------------------------------

******************************
Get In Touch or Get Involved
******************************

Contribution
=============
Please read `CONTRIBUTING.md`_ for details on our code of conduct, and the process for submitting pull requests to us.

Contact Us
===========
We’d love to hear from you! To get in touch with us, please join or start a new `Discussion`_, `create an issue`_
or `send us an email`_ 💌

|

*************
Attribution
*************

Licensing
==========
This project is licensed under Apache License 2.0 - see the `LICENSE.md`_ file for details.

Citing this Work
=================

**ISMB Conference Pre-print:**  

Callahan TJ, Tripodi IJ, Hunter LE, Baumgartner WA. `A Framework for Automated Construction of Heterogeneous Large-Scale Biomedical Knowledge Graphs <https://www.biorxiv.org/content/10.1101/2020.04.30.071407v1.abstract>`_. bioRxiv. 2020 Jan 1.


**Zenodo**

.. code:: bash

   @misc{callahan_tj_2019_3401437,
     author       = {Callahan, TJ},  
     title        = {PheKnowLator},  
     year         = 2019,      
     doi          = {10.5281/zenodo.3401437},  
     url          = {https://doi.org/10.5281/zenodo.3401437}}


.. |logo| image:: https://user-images.githubusercontent.com/8030363/106306246-01df9100-621b-11eb-81c3-d1f2c2e124a6.png
   :target: https://github.com/callahantiff/PheKnowLator
   
.. |ABRA| image:: https://img.shields.io/badge/ReproducibleResearch-AbraCollaboratory-magenta.svg
   :target: https://github.com/callahantiff/Abra-Collaboratory

.. |github_action| image:: https://github.com/callahantiff/PheKnowLator/workflows/Rosey%20the%20Robot/badge.svg
   :target: https://github.com/callahantiff/PheKnowLator/actions?query=workflow%3A%22Rosey+the+Robot%22
   :alt: GitHub Action Rosey the Robot

.. |mypy| image:: http://www.mypy-lang.org/static/mypy_badge.svg
   :target: http://mypy-lang.org/
   :alt: Linted with MyPy

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

.. |pip| image:: https://pypip.in/v/pkt-kg/badge.png
    :target: https://pypi.org/project/pkt-kg/
    :alt: PyPI project

.. |downloads| image:: https://pepy.tech/badge/pkt_kg
    :target: https://pepy.tech/badge/pkt_kg
    :alt: Pypi total project downloads

.. |codacy| image:: https://app.codacy.com/project/badge/Grade/2cfa4ef5f9b6498da56afea0f5dadeed
    :target: https://www.codacy.com/gh/callahantiff/PheKnowLator/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=callahantiff/PheKnowLator&amp;utm_campaign=Badge_Grade
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

.. _`Data_Preparation.ipynb`: https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/Data_Preparation.ipynb

.. _`resources/resource_info.txt`: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#master-resources

.. _`resources/ontology_source_list.txt`: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#ontology-data

.. _`resources/edge_source_list.txt`: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#edge-data

.. _`ontology_source_metadata.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/ontology_source_metadata.txt

.. _`edge_source_metadata.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/edge_data/edge_source_metadata.txt

.. _`KG Construction`: https://github.com/callahantiff/PheKnowLator/wiki/KG-Construction

.. _`README`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/knowledge_graphs/README.md

.. _`resources/construction_approach/subclass_construction_map.pkl`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/construction_approach/README.md

.. _`resources/Master_Edge_List_Dict.json`: https://www.dropbox.com/s/t8sgzd847t1rof4/Master_Edge_List_Dict.json?dl=1

.. _`resources/node_data/node_metadata_dict.pkl`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/node_data/README.md

.. _`resources/knowledge_graphs/PheKnowLator_MergedOntologies*.owl`: https://www.dropbox.com/s/75lkod7vzpgjdaq/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl?dl=1

.. _`ontology README`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/README.md

.. _`resources/owl_decoding/OWL_NETS_Property_Types.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/owl_decoding/README.md

.. _`resources/relations_data/RELATIONS_LABELS.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/relations_data/README.md

.. _`resources/relations_data/INVERSE_RELATIONS.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/relations_data/README.md

.. _`main.ipynb`: https://github.com/callahantiff/pheknowlator/blob/master/main.ipynb

.. _`main.py`: https://github.com/callahantiff/pheknowlator/blob/master/main.py

.. _CONTRIBUTING.md: https://github.com/callahantiff/pheknowlator/blob/master/CONTRIBUTING.md

.. _LICENSE.md: https://github.com/callahantiff/pheknowlator/blob/master/LICENSE

.. _`create an issue`: https://github.com/callahantiff/PheKnowLator/issues/new/choose

.. _`send us an email`: https://mail.google.com/mail/u/0/?view=cm&fs=1&tf=1&to=callahantiff@gmail.com

.. _`Discussion`: https://github.com/callahantiff/PheKnowLator/discussions
