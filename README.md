## PheKnowLater

A repository for developing methods to facilitate clinically and biologically meaningful translations between human and rodent phenotypes. Deatiled information regarding this project can be found on the associated [Wiki](https://github.com/callahantiff/PheKnowLater/wiki).
This repository uses Semantic Web technologies to build a biomedical knowledge graph using [Open Biomedical Ontologies](http://www.obofoundry.org/) and other sources of open biomedical data. The resulting knowledge graph is designed to provide the user with mechanistic connections between sets of user-specified entities.

#### This is a Reproducible Research Repository
This repository contains more than just code, it provides a detailed and transparent narrative of our research process. For detailed information on how we use GitHub as a reproducible research platform, click [here](https://github.com/callahantiff/Abra-Collaboratory/wiki/Using-GitHub-as-a-Reproducible-Research-Platform).

<img src="https://img.shields.io/badge/ReproducibleResearch-AbraCollaboratory-magenta.svg?style=flat-square" alt="git-AbraCollaboratory">

### Project Stats
![GitHub contributors](https://img.shields.io/github/contributors/callahantiff/PheKnowLater.svg?color=yellow&style=flat-square) ![Github all releases](https://img.shields.io/github/downloads/callahantiff/PheKnowLater/total.svg?color=dodgerblue&style=flat-square)

#### Accessing Code and Results
All code and results for each releases will be accessible through the project [Wiki](https://github.com/callahantiff/PheKnowLator/wiki), under the releases tab.

_____
### Getting Started

To build the knowledge graph run the `main.py` file. This file will perform the following tasks:
**Download Data**
 - <u>Download Ontologies</u>: Downloads ontologies with or without imports from the `ontology_source_list.txt` file.
  Once the ontology has downloaded, metadata information from each ontology will be saved to `ontology_source_metadata.txt`, which is located within the `resources/ontologies` directory.
 - <u>Download Class Data</u>: Downloads data from the `class_source_list.txt` file. Once 
    the data has downloaded, metadata information from each source will be saved to `class_source_metadata.txt`, which is located within the `resources/text_files` directory. 
 - <u>Download Instance Data</u>: Downloads data from the `instance_source_list.txt` file. Once the data has downloaded, metadata information from each source will be saved to `instance_source_metadata.txt`, which is located within the `resources/text_files` directory.   

**Create Edge Lists**  
 - Create edges between classes and instances of classes.  
 - Create  edges between instances of classes and data.  

**Build Knowledge Graph**  
 - Merge ontologies used as classes.  
 - Add class-instance and instance-instance edges to merged ontologies.  
 - Remove disjointness axioms.  
 - Deductively close knowledge graph using Elk reasoner.    
 - Write original edges to local directory.  
 - Convert original edges to integers and write to local directory.

**Generate Mechanism Embeddings**  
 - Run modified DeepWalk algorithm over 

<br>

#### Installation  

To use run, download zip file or fork the project repository. Additional instructions can be found under [*Installation*](#installation). For the program to run successfully the prerequisites must be satisfied.

#### Prerequisites

This program was written on a system running OS X Sierra. Successful execution of this program requires Python version 3.6.

  * Python
    * Version 3.6
    * Modules are described under [*Installation*](#Installation)

<br>

#### Installation

To install and execute the program designate the cloned project folder as the current working directory. Place any outside files within the working directory prior to executing the program.

```
pip install -r requirements.txt
```

<br>

#### Running Code

Running program from the command line by:

```
python3 Main.py -h

usage: Main.py [-h] -o ONTS -c CLS -i INST

OpenBioGraph: This program builds a biomedical knowledge graph using Open
Biomedical Ontologies and other sources of open biomedical data. Built on
Semantic Web Technologies, the programs takes the inputs specified below and
outputs

optional arguments:
  -h, --help            show this help message and exit
  -o ONTS, --onts ONTS  name/path to text file containing ontologies
  -c CLS, --cls CLS     name/path to text file containing class sources
  -i INST, --inst INST  name/path to text file containing instance sources
```

### Contributing

Please read [CONTRIBUTING.md](https://github.com/callahantiff/pheknowlator/blob/master/CONTRIBUTING.md) for details on 
our code of conduct, and the process for submitting pull requests to us.


### License

This project is licensed under 3-Clause BSD License - see the [LICENSE.md](https://github.com/callahantiff/pheknowlator/blob/master/LICENSE) file for details.
