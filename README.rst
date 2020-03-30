pkt_kg
=========================================================================================

|travis| |sonar_quality| |sonar_maintainability| |codacy|
|code_climate_maintainability| 

.. |pip| |downloads|


PheKnowLator (Phenotype Knowledge Translator), a fully automated Python 3 library explicitly designed for optimized
construction of semantically-rich, large-scale biomedical KGs from complex heterogeneous data. Detailed information
regarding this project can be found on the project `Wiki`_.

|DOI|

|

**This is a Reproducible Research Repository:** 

This repository contains more than just code, it provides a detailed and transparent narrative of our research process. For detailed information on how we use GitHub as a reproducible research platform, click `here`_.

|ABRA| 
    
|

Tests Coverage
----------------------------------------------

|coveralls| |sonar_coverage|

|

Releases
----------------------------------------------

All code and output for each release are free to download, see `Wiki <https://github.com/callahantiff/PheKnowLator/wiki>`__ for full release archive.

**Current Release:** `v2.0.0`_. Data and code can be directly downloaded `here <https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0#generated-output>`__.

|

Getting Started
----------------------------------------------

This program requires Python version 3.6. To install it, run the following:

.. code:: shell

    pip install pkt_kg


🛑 Dependencies 🛑
~~~~~~~~~~~~~~~~~~~~

* **Mapping, Filtering, and Labeling Data:** Run the `data_preparation.ipynb`_ notebook to build mapping, filtering, and labeling datasets.


* **Important.** This code depends on several documents in order to run successfully. See *STEP 1: Prepare Input Documents* below for more details.


* **Data Sources:** This knowledge graph is built entirely on publicly available linked open data and `Open Biomedical Ontologies`_.  🙏 🙇‍♀ Please see the `Data Source`_ Wiki page for information.


* **Running Code:** This program can be run using a Jupyter Notebook (`main.ipynb`_) or from the command line (`main.py`_) by:

  .. code:: bash
  

     python3 Main.py -h
     usage: Main.py [-h] -g ONTS -c CLS -i INST -t RES -b KG -o OUT -n NDE -r REL -s OWL

     PheKnowLator: This program builds a biomedical knowledge graph using Open Biomedical
     Ontologies and linked open data. The programs takes the following arguments:
  
     optional arguments:
       -h,      --help        show this help message and exit
       -g ONTS, --onts ONTS  name/path to text file containing ontologies
       -c CLS,  --cls CLS    name/path to text file containing class sources
       -i INST, --inst INST  name/path to text file containing instance sources
       -t RES,  --res RES    name/path to text file containing resource_info
       -b KG,   --kg KG      the build, can be "partial", "full", or "post-closure"
       -o OUT,  --out OUT    name/path to directory where to write knowledge graph
       -n NDE,  --nde NDE    yes/no - adding node metadata to knowledge graph
       -r REL,  --rel REL    yes/no - adding inverse relations to knowledge graph
       -s OWL,  --owl OWL    yes/no - removing OWL Semantics from knowledge graph
  
|
|

Workflow
--------------

The `KG Construction`_ Wiki page provides a detailed description of the knowledge construction process. A brief overview of this process is also provided provided below.


STEP 0: Select the Build and Construction Type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The knowledge graph build algorithm has been designed to run from three different stages of development (i.e. build types):
``full``, ``partial``, and ``post-closure``. It can also be built using two different construction types: ``instance-based`` and ``subclass-based``. Please see the knowledge graph `README`_ for more information.  

STEP 1: Prepare Input Documents
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This code depends on four documents in order to run successfully. For information on what’s included in these documents, see the `Document Dependencies`_ Wiki page.

For assistance in creating these documents, please run the following from the root directory:
.. code:: bash
python3 pkt/generates_dependency_documents.py


STEP 2: Download and Preprocess Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*PREPROCESS DATA:*  

* **Create Mapping, Filtering, and Labeling Data**: The `data_preparation.ipynb`_ assists with the downloading and processing of all data needed to help build the knowledge graph.

*DOWNLOAD DATA:* 

* **Download Ontologies**: Downloads ontologies with or without imports from the `ontology_source_list.txt`_. Metadata information from each ontology is saved to `ontology_source_metadata.txt`_ directory.  

* **Download Edge Data**: Downloads data that is used to create connections between ontology concepts treated as classes and instance data from the `edge_source_list.txt`_ file. Metadata information from each source is saved to `edge_source_metadata.txt`_ directory.


STEP 3: Process Ontology Data and Build Edge Lists  
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Process ontologies to verify they are error free, consistent, and normalized prior to constructing the knowledge graph
(see the
`Ontology README`_ for more information).

* Create new edges between ontology classes and edge data sources.


STEP 4: Build Knowledge Graph
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Merge ontologies used as classes.
2. Add new edges to merged ontologies.
3. Deductively close knowledge graph using `Elk reasoner`_
4. Remove edges that are not clinically meaningful (OWL-NETS).
5. Write edges (as triples) to local directory.
6. Convert original edges to integers and write to local directory (required input format for generating embeddings).

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



.. _Wiki: https://github.com/callahantiff/PheKnowLater/wiki

.. _here: https://github.com/callahantiff/Abra-Collaboratory/wiki/Using-GitHub-as-a-Reproducible-Research-Platform

.. _v2.0.0: https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0

.. _data_preparation.ipynb: https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb

.. _OWLTools: https://github.com/owlcollab/owltools

.. _n1-standard1: https://cloud.google.com/compute/vm-instance-pricing#n1_predefined

.. _`Open Biomedical Ontologies`: http://obofoundry.org/

.. _`Data Source`: https://github.com/callahantiff/PheKnowLator/wiki/Data-Sources

.. _main.ipynb: https://github.com/callahantiff/pheknowlator/blob/master/main.ipynb

.. _main.py: https://github.com/callahantiff/pheknowlator/blob/master/main.py

.. _`KG Construction`: https://github.com/callahantiff/PheKnowLator/wiki/KG-Construction

.. _`Ontology README`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/README.md

.. _`README`: https://github.com/callahantiff/blob/PheKnowLator/master/resources/knowledge_graphs/README.md

.. _`Document Dependencies`: https://github.com/callahantiff/PheKnowLator/wiki/Dependencies

.. _`data_preparation.ipynb`: https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb

.. _`ontology_source_list.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontology_source_list.txt

.. _`ontology_source_metadata.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/ontology_source_metadata.txt

.. _`edge_source_list.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/edge_source_list.txt

.. _`edge_source_metadata.txt`: https://github.com/callahantiff/PheKnowLator/blob/master/resources/edge_data/edge_source_metadata.txt

.. _`Elk reasoner`: https://www.cs.ox.ac.uk/isg/tools/ELK/

.. _CONTRIBUTING.md: https://github.com/callahantiff/pheknowlator/blob/master/CONTRIBUTING.md

.. _LICENSE.md: https://github.com/callahantiff/pheknowlator/blob/master/LICENSE

.. _`create an issue`: https://github.com/callahantiff/PheKnowLator/issues/new/choose

.. _`send us an email`: https://mail.google.com/mail/u/0/?view=cm&fs=1&tf=1&to=callahantiff@gmail.com

   
.. |DOI| image:: https://zenodo.org/badge/DOI/10.5281/34014365.svg
   :target: https://doi.org/10.5281/34014365
   
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
