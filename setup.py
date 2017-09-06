from setuptools import setup, find_packages

setup(
    name="bioluigi",
    version='0.0.1',
    author="Daniel Bunintg",
    author_email="daniel.bunting@jic.ac.uk",
    url="https://github.com/SaundersLab/bioluigi/",
    packages=find_packages(),
    install_requires=[
        'luigi', 'sqlalchemy', 'dill', 'multiprocessing_on_dill'
    ],
    dependency_links=[
        'git+ssh://git@github.com/dnlbunting/luigi.git#egg=luigi',
    ]
)
