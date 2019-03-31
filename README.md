# PheKnowLater

A repository for developing methods to facilitate clinically and biologically meaningful translations between human and rodent phenotypes. Deatiled information regarding this project can be found on the associated [Wiki](https://github.com/callahantiff/PheKnowLater/wiki).
This repository uses Semantic Web technologies to build a biomedical knowledge graph using [Open Biomedical Ontologies](http://www.obofoundry.org/) and other sources of open biomedical data. The resulting knowledge graph is designed to provide the user with mechanistic connections between sets of user-specified entities.

<br>

#### This is a Reproducible Research Repository
This repository contains more than just code, it provides a detailed and transparent narrative of our research process. For detailed information on how we use GitHub as a reproducible research platform, click [here](https://github.com/callahantiff/Abra-Collaboratory/wiki/Using-GitHub-as-a-Reproducible-Research-Platform).

<img src="https://img.shields.io/badge/ReproducibleResearch-AbraCollaboratory-magenta.svg?style=flat-square" alt="git-AbraCollaboratory">

### Project Stats

![GitHub contributors](https://img.shields.io/github/contributors/callahantiff/PheKnowLater.svg?color=yellow&style=flat-square) ![Github all releases](https://img.shields.io/github/downloads/callahantiff/PheKnowLater/total.svg?color=dodgerblue&style=flat-square)

<br>

______
### Getting Started

To use run, download zip file or fork the project repository. Additional instructions can be found under [*Installation*](#installation). For the program to run successfully the prerequisites must be satisfied.


### Prerequisites

This program was written on a system running OS X Sierra. Successful execution of this program requires Python version 2.7.

  * Python
    * Version 3.6
    * Modules are described under [*Installation*](#Installation)


### Installation

To install and execute the program designate the cloned project folder as the current working directory. Place any outside files within the working directory prior to executing the program.

```
pip install -r requirements.txt
```

## Constructing the Knowledge Graph

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

## Contributing

Please read [CONTRIBUTING.md](https://github.com/callahantiff/open-bio-graph/blob/master/CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

<!--## Versioning-->

<!--We use [SemVer](http://semver.org/) for versioning.-->

## Testing
We are in the process of developing tests for each module. We will create documentation as they are created.

## License

This project is licensed under 3-Clause BSD License - see the [LICENSE.md](https://github.com/callahantiff/open-bio-graph/blob/master/LICENSE) file for details.

<!--## Acknowledgments-->

<!--* README was generated from a modified markdown template originally created by **Billie Thompson [PurpleBooth](https://github.com/PurpleBooth)**.-->
