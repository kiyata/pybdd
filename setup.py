from distutils.core import setup
import setuptools

setup(
    name='plics-qa',
    version='1.0',
    description='PLICS Automated Test',
    author='Charles Moga',
    packages=setuptools.find_packages(),
    install_requires=[
        'pytest-bdd',
        'pytest-html',
        'pylint',
        'pymysql',
        'pandas',
        'configparser',
        'xmlschema',
        'faker',
        'exrex',
        'lxml',
        'typer',
        'pycodestyle',
        'pyodbc',
        'autopep8',
        'python-dotenv',
        'fpdf',
        'bs4',
        'tabulate',
        'python-decouple',
        'python-dotenv'
    ]
)
