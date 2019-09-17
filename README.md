## PheKnowLator

A repository for building biomedical knowledge graphs of human disease mechanisms. Detailed information regarding this project can be found on the associated [Wiki](https://github.com/callahantiff/PheKnowLater/wiki).

<img src="https://zenodo.org/badge/DOI/10.5281/zenodo.3401437.svg"> 

**This is a Reproducible Research Repository:** This repository contains more than just code, it provides a detailed and transparent narrative of our research process. For detailed information on how we use GitHub as a reproducible research platform, click [here](https://github.com/callahantiff/Abra-Collaboratory/wiki/Using-GitHub-as-a-Reproducible-Research-Platform).

<img src="https://img.shields.io/badge/ReproducibleResearch-AbraCollaboratory-magenta.svg?style=flat-square" alt="git-AbraCollaboratory"> 

<br>  

**Project Stats:** ![GitHub contributors](https://img.shields.io/github/contributors/callahantiff/PheKnowLater.svg?color=yellow&style=flat-square) ![Github all releases](https://img.shields.io/github/downloads/callahantiff/PheKnowLater/total.svg?color=dodgerblue&style=flat-square)

***

### Releases  
All code and output for each release are free to download, see [Wiki](https://github.com/callahantiff/PheKnowLator/wiki) for full release archive.  

**Current Release:** [v2.0.0](https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0). Data and code can be directly downloaded [here](https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0#generated-output)

*** 

### Getting Started

This program was written on a system running OS X Sierra. Successful execution of this program requires Python version 3.6.

  * Python
    * Version 3.6
    * Modules are described under [*Installation*](#Installation)

<br>

#### Installation

**Requirements:**  
To install and execute the program designate the cloned project folder as the current working directory. Place any outside files within the working directory prior to executing the program.

```
pip install -r requirements.txt
```

<br>

**Dependencies:**  
- [x] ‼ **Important:** This code also depends on four documents in order to run successfully. For information on 
what's 
included in these documents, see the [Wiki](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies).
- [x] This program depends on the [OWLTools](https://github.com/owlcollab/owltools) library. Please download it to 
`resources/lib/` prior to running `main.py`.  
   

<br>

#### Running Code

- This program can be run using a Jupyter Notebook ([`main.ipynb`](https://github.com/callahantiff/pheknowlator/blob/master/main.ipynb)) 
- This program can be also be run from the command line ([`main.py`](https://github.com/callahantiff/pheknowlator/blob/master/main.py)) by:

```
python3 Main.py -h

usage: Main.py [-h] -o ONTS -c CLS -i INST

PheKnowLator: This program builds a biomedical knowledge graph using Open
Biomedical Ontologies and linked open data. The programs takes the following arguments:

optional arguments:
  -h, --help            show this help message and exit
  -o ONTS, --onts ONTS  name/path to text file containing ontologies
  -c CLS, --cls CLS     name/path to text file containing class sources
  -i INST, --inst INST  name/path to text file containing instance sources
```   

#### Workflow   
**STEP 1: Download Data**
 - <u>Download Ontologies</u>: Downloads ontologies with or without imports from the `ontology_source_list.txt` file.
  Once the ontology has downloaded, metadata information from each ontology will be saved to `ontology_source_metadata.txt`, which is located within the `resources/ontologies` directory.
 - <u>Download Class Data</u>: Downloads data that is used to create connections between ontology concepts treated as
  classes and instance data from the `class_source_list.txt` file. Once 
    the data has downloaded, metadata information from each source will be saved to `class_source_metadata.txt`, which is located within the `resources/text_files` directory. 
 - <u>Download Instance Data</u>: Downloads data from the `instance_source_list.txt` file. Once the data has downloaded, metadata information from each source will be saved to `instance_source_metadata.txt`, which is located within the `resources/text_files` directory.   

**STEP 2: Create Edge Lists**  
 - Run `python/NCBO_rest_api.py` script first. Note, that you will need to create an account with [BioPortal](http://basic-formal-ontology.org/) and place your API key in `resources/bioportal_api_key.txt`. 
   - When run from the command line, you will be asked to enter two ontologies (`source1=MESH`, `source2=CHEBI`).
   - This will generate a text file that contains mappings between identifiers from two ontologies specified and write the results to `resources/data_maps/source1_source2_map.txt`.  

 - Create edges between classes and instances of classes.  
 - Create edges between instances of classes and instances of data.  

**STEP 3: Build Knowledge Graph**  
1. Merge ontologies used as classes.  
2. Add class-instance and instance-instance edges to merged ontologies.  
3. Remove disjointness axioms.  
4. Deductively close knowledge graph using [Elk reasoner](https://www.cs.ox.ac.uk/isg/tools/ELK/).    
5. Write edges (as triples) to local directory.  
6. Convert original edges to integers and write to local directory (required input format for generating embeddings).

**STEP 4: Generate Mechanism Embeddings**  
 - A [modified](https://github.com/bio-ontology-research-group/walking-rdf-and-owl) version of the [DeepWalk 
 algorithm](https://github.com/bio-ontology-research-group/walking-rdf-and-owl) was implemented to generate molecular mechanism embeddings from the biomedical knowledge graph. 
   - ‼ **Note:** This library depends on the [C++ Boost library](https://www.pyimagesearch.com/2015/04/27/installing-boost-and-boost-python-on-osx-with-homebrew/) and [Boost Threadpool Header Files](http://threadpool.sourceforge.net/). For the Headers, the sub-directory called `Boost` at the top-level of the `walking-rdf-and-owl-master` directory. In order to compile and run `Deepwalk-RDF`, there are a few important changes that will need to be made:  
      - Change `TIME_UTC` to `TIME_UTC_` in the `boost/threadpool/task_adaptors.hpp`.  
      - Change the `-lboost_thread` argument to `-lboost_thread-mt` in the `walking-rdf-and-owl-master/Makefile` 
      - To troubleshoot incompatability issues between Deepwalk and Gensim, run tthe following in this order:  
        - `pip uninstall gensim`  
        - `pip uninstall deepwalk`  
        - `pip install gensim==0.10.2` 
        - `pip install deepwalk`  

<br>

***

### Contributing

Please read [CONTRIBUTING.md](https://github.com/callahantiff/pheknowlator/blob/master/CONTRIBUTING.md) for details on 
our code of conduct, and the process for submitting pull requests to us.


### License

This project is licensed under Apache License 2.0 - see the [LICENSE.md](https://github.com/callahantiff/pheknowlator/blob/master/LICENSE) file for details.
