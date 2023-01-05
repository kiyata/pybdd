import os
import json
from conftest import Data


def get_property(section, property_item):
    job_type = Data.JobType
    directory = os.path.dirname(os.path.abspath(__file__))
    file = job_type + '-mappings.json'
    config_path = directory + '/' + file
    f = open(config_path)
    data = json.load(f)
    element = data[section][property_item]
    f.close()
    return element
