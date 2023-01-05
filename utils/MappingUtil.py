import json
from decouple import config


# noinspection PyMethodMayBeStatic
class MappingUtil:
    resource_dir = ""
    mappings = {}

    def __init__(self):
        file_name = f"{config('LOCAL_RESOURCE_DIR')}/mappings.json"
        with open(file_name, 'r') as f:
            self.mappings = json.load(f)

    def get_field_mappings(self, feed_type, job_type, data_type):
        if data_type == 'activity':
            return self.mappings[job_type][feed_type]
        elif data_type == 'cost':
            return self.mappings["cost"][job_type]
        else:
            return self.mappings["matching"][feed_type]

    def get_matching_mappings(self, feed_type):
        return self.mappings["matching"][feed_type]

    def get_rec_mappings(self, feed_type, job_type):
        return self.mappings[job_type][feed_type]
