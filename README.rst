pkt_kg
=========================================================================================

|travis| |sonar_quality| |sonar_maintainability| |codacy|  |coveralls| |sonar_coverage|
|ABRA| 

.. |pip| |downloads|


PheKnowLator (Phenotype Knowledge Translator) is a fully automated Python 3 library explicitly designed for optimized
construction of semantically-rich, large-scale, biomedical KGs from complex heterogeneous data. Detailed information
regarding this project can be found on the project `Wiki`_.

**This is a Reproducible Research Repository:** For detailed information on how we use GitHub as a reproducible research platform, click `here`_.


    
|

Releases
----------------------------------------------

All code and output for each release are free to download, see `Wiki <https://github.com/callahantiff/PheKnowLator/wiki>`__ for full release archive.

**Current Release:** ``v1.0.0``. Data and code can be directly downloaded `here <https://github.com/callahantiff/PheKnowLator/wiki/v1.0.0>`__.

⚠️ **New Release in Progress:** ``v2.0.0`` in progress and will be released by the end of April 2020. The `PyPi` package will not be completely functional until this release is finalized.

|

Getting Started
----------------------------------------------

**Install Library**   

This program requires Python version 3.6.

To install the library from PyPy, run:

.. code:: shell

  pip install pkt_kg

|

You can also clone the repository directly from GitHub by running:

.. code:: shell

  git clone https://github.com/owlcollab/owltools.git

|
|

**Set-Up Environment**     

The `pkt_kg` library requires a specific project directory structure. If you plan to run the code from a cloned version of this repository, then no additional steps are needed. If you are planning to utilize the library without cloning the library, please make sure that your project directory includes the following sub-directories:  

.. code:: shell

    project_directory/  
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

Several input documents must be created before the `pkt_kg` library can be utilized. Each of the input documents are listed below by knowledge graph build step:  

*Download Data*  

This code requires three documents within the `resources` directory to run successfully. For more information on these documents, see `Document Dependencies`_:
  
* `resources/resource_info.txt`_  
* `resources/ontology_source_list.txt`_  
* `resources/edge_source_list.txt`_

For assistance in creating these documents, please run the following from the root directory:

.. code:: bash

    python3 pkt/generates_dependency_documents.py

Prior to running this step, make sure that all mapping and filtering data referenced in `resources/resource_info.txt`_ have been created. Please see the `Data_Preparation.ipynb`_ Jupyter notebook for detailed examples of the steps used to build the `v2.0.0 knowledge graph <https://github.com/callahantiff/PheKnowLator/wiki/v1.0.0>`__.
  
*Note.* To ensure reproducibility, after downloading data, a metadata file is output for the ontologies (`ontology_source_metadata.txt`_) and edge data sources (`edge_source_metadata.txt`_). 

|

*Construct Knowledge Graph*  

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

There are several ways to run `pkt_kg`. An example workflow is provided below.

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

This repo provides 3 different examples of ways that the `pkt_kg` can be run:  

*Command Line* ➞ `Main.py`_

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

*Jupyter Notebook* ➞ `main.ipynb`_

*Docker Instance*  

Finally, `pkt_kg` can be run using a Docker instance.  <<ADD MORE INFO HERE>>.


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

.. |travis| image:: https://travis-ci.org/callahantiff/PheKnowLator.png
   :target: https://travis-ci.org/callahantiff/PheKnowLator
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

.. _`README`: https://github.com/callahantiff/blob/PheKnowLator/master/resources/knowledge_graphs/README.md

.. _`resources/construction_approach/subclass_construction_map.pkl`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/construction_approach/README.md

.. _`resources/Master_Edge_List_Dict.json`: https://www.dropbox.com/s/w4l9yffnn4tyk2e/Master_Edge_List_Dict.json?dl=1

.. _`resources/node_data/*.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/node_data/README.md

.. _`resources/knowledge_graphs/PheKnowLator_MergedOntologies*.owl`: https://www.dropbox.com/s/75lkod7vzpgjdaq/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl?dl=1

.. _`ontology README`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/README.md

.. _`resources/owl_decoding/OWL_NETS_Property_Types.txt`: https://github.com/callahantiff/PheKnowLator/blob/documentation_updates/resources/owl_decoding/README.md

.. _`resources/relations_data/RELATIONS_LABELS.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/relations_data/README.md

.. _`resources/relations_data/INVERSE_RELATIONS.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/relations_data/README.md

.. _`main.ipynb`: https://github.com/callahantiff/pheknowlator/blob/master/main.ipynb

.. _`Main.py`: https://github.com/callahantiff/pheknowlator/blob/master/main.py

.. _CONTRIBUTING.md: https://github.com/callahantiff/pheknowlator/blob/master/CONTRIBUTING.md

.. _LICENSE.md: https://github.com/callahantiff/pheknowlator/blob/master/LICENSE

.. _`create an issue`: https://github.com/callahantiff/PheKnowLator/issues/new/choose

.. _`send us an email`: https://mail.google.com/mail/u/0/?view=cm&fs=1&tf=1&to=callahantiff@gmail.com
