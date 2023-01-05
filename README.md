# setup
1. python setup.py install --user
2. In resources/config.json, customise database connection string and file locations. This file also contain all other settings at feed type level

# structure
1. Resources folder contain several important files, including input XML files, CSV out files (if not reading from remote server). It also includes the mapping file, which specifieds the maping of files from XML to DB and from XML to CSV.

# feature
This folder contains feature files for daily, weekly and matching. New tests may be added as required

# step_defs
This is where step definitions are located

# utils
This folder contains lots of utility library for various logics. Form example CommonUtil.py contains functions for comparisons, matching, etc

# how does it work
At the start of the test, the database is setup for each feed type; we create a table to store extracted XML for each feed type. We are also another table to data extracted from CSV. We use Sqlite3 and persist the data in plics.db
The test start by extracting the XML and storing the data sqlite3 so it can be queried and compared. I the then selects the data and for daily reads the data produced by SAS from the MSSQL database. It then compares row by row and field by field. For Weekly it first extracts the SCV and saves to Sqlite3 and queries from there. We obviously do a lot of processing like matching, pseudomisation, some transformations like hashing, age calculation, etc.
The result is writted to an HTML report

# running tests
python -m pytest --env=local --job=features.daily --html=report/report.html --css=report/report.css -rF --gherkin-terminal-reporter

#run with tag, where "intrec" is a tag
python -m pytest -m intrecweekly --html=report/report.html --css=report/report.css -rF --gherkin-terminal-reporter
python -m pytest -m apcmatching --html=report/report.html --css=report/report.css -rF --gherkin-terminal-reporter
python -m pytest -m mhpssummary

# format code
autopep8 --in-place --aggressive --aggressive --recursive .

# shell script for running test
. ./runner.sh intrec


