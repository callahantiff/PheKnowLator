from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()


test_deps = [
    'codacy-coverage',
    'coveralls',
    'nose',
    'nose-cov',
    'validate_version_code',
    'pylint',
    'mypy'
]

extras = {
    'test': test_deps,
}

setup(
    name='pkt',
    version='2.0.0',
    description='A Python library for scalable knowledge semantic graph construction',
    long_description=readme(),
    url='https://github.com/callahantiff/PheKnowLator',
    keywords='knowledge-graph, ontologies, formal-logic, reasoner, deep-learning, biomedical-applications, mechanisms,'
             'translation-research, linked-open-data, owl, semantic-web, symbolic-ai, knowledge-representation',
    author='Tiffany J. Callahan, William A. Baumgartner, Jr.',
    author_email='tiffany.callahan@cusanchutz.edu, william.baumgartner@cusanchutz.edu',
    license='Apache 2.0',
    packages=['pkt'],
    install_requires=[
        'argparse>=1.4.0',
        'Cython==0.29.14',
        'networkx',
        'nose',
        'numpy==1.18.1',
        'Owlready2==0.14',
        'pandas>=0.25.3',
        'rdflib>=4.2.2',
        'reactome2py>=0.0.8',
        'requests>=2.22.0',
        'tqdm'
    ],
    test_suite='nose.collector',
    tests_require=test_deps,
    include_package_data=True,
    zip_safe=False,
    extras_require=extras,
)
