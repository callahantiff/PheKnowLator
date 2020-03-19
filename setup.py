import os
import re

# To use a consistent encoding
from codecs import open as copen
from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the relevant file
with copen(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


def read(*parts):
    with copen(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


__version__ = find_version("pkt_kg", "__version__.py")

test_deps = [
    "pytest",
    "pytest-cov",
    "coveralls",
    "validate_version_code",
    "codacy-coverage"
]

extras = {
    'test': test_deps,
}

setup(
    name='pkt_kg',
    version=__version__,
    description="A Python library for scalable knowledge semantic graph construction",
    long_description=long_description,
    url="https://github.com/callahantiff/PheKnowLator",
    keywords='knowledge-graph, ontologies, formal-logic, reasoner, deep-learning, biomedical-applications, '
             'mechanisms, translation-research, linked-open-data, owl, semantic-web, symbolic-ai, '
             'knowledge-representation',
    author="Tiffany J. Callahan, William A. Baumgartner, Jr.",
    author_email="tiffany.callahan@cusanchutz.edu, william.baumgartner@cusanchutz.edu",

    # Choose your license
    license='MIT',
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3'
    ],
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    tests_require=test_deps,

    # Add here the package dependencies
    install_requires=['argparse>=1.4.0',
                      'Cython==0.29.14',
                      'networkx',
                      'nose',
                      'numpy==1.18.1',
                      'Owlready2==0.14',
                      'pandas>=0.25.3',
                      'rdflib>=4.2.2',
                      'reactome2py>=0.0.8',
                      'requests',
                      'responses',
                      'tqdm'],
    extras_require=extras,
)
