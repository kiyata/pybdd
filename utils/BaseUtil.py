from decimal import Decimal
import json
import os
from tabulate import tabulate
from decouple import config

# noinspection PyMethodMayBeStatic
class BaseUtil:

    def __init__(self):
        pass

    def get_data_type(self, data_type, min, max):
        if data_type == 'Text':
            return f"VARCHAR({max})"
        elif data_type == 'Decimal':
            return f"DECIMAL({min},{max})"
        else:
            return
    def get_xpath_depth(self, tier):
        depths = {
            1: "../@",
            2: "../../@",
            3: "../../../@",
            4: "../../../../@",
        }
        return depths[tier]



