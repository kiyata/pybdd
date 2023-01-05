#!/bin/bash
# pass the test tag to run, for example . ./runner.sh apcmatching
python -m pytest -m "$1" --html=report/report.html --css=report/report.css -rF --gherkin-terminal-reporter