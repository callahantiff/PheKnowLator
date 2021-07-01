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
    raise RuntimeError('Unable to find version string.')


__version__ = find_version('pkt_kg', '__version__.py')

test_deps = [
    'codacy-coverage',
    'coveralls',
    'mock',
    'mypy',
    'pytest',
    'pytest-cov',
    'validate_version_code'
]

extras = {
    'test': test_deps,
}

setup(
    name='pkt_kg',
    version=__version__,
    description='A Python library for scalable knowledge semantic graph construction',
    long_description=long_description,
    url='https://github.com/callahantiff/PheKnowLator',
    keywords='knowledge-graph, ontologies, formal-logic, biomedical-applications, '
             'mechanisms, translation-research, linked-open-data, owl, semantic-web, symbolic-ai, ',
    author='Tiffany J. Callahan, William A. Baumgartner, Jr.',
    author_email='tiffany.callahan@cuanschutz.edu, william.baumgartner@cuanschutz.edu',

    # Choose your license
    license='Apache 2.0',
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3'
    ],
    packages=find_packages(exclude=['contrib', 'docs', 'tests*', 'builds']),
    tests_require=test_deps,

    entry_points={
        'console_scripts': ['pkt = Main:main']
    },

    # Add here the package dependencies
    install_requires=['argparse',
                      'click',
                      'Cython>=0.29.14',
                      'more-itertools',
                      'networkx',
                      'numpy>=1.18.1',
                      'openpyxl>=3.0.3',
                      'pandas>=1.0.5',
                      'psutil',
                      'python-json-logger',
                      'ray',
                      'rdflib',
                      'reactome2py',
                      'requests',
                      'types-requests',
                      'responses==0.10.12',
                      'tqdm',
                      'urllib3'],
    extras_require=extras,
)
