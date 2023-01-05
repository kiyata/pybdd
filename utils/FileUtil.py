import csv
import os.path
import glob
import os
from decouple import config


class FileUtil:
    csvBaseDir = ""
    acute = ["apc", "ec", "op", "si", "swc"]
    feed_type = ""
    LOCAL_RESOURCE_DIR = ""

    def __init__(self, feed_type):
        ENV = os.getenv('ENV')
        if ENV == 'local':
            self.csvBaseDir = f"{config('LOCAL_RESOURCE_DIR')}output"
        else:
            if feed_type in self.acute:
                self.csvBaseDir = f"\\\\ic\\IC_DME_DFS\\DME_DEV\\PLIC\\PLIC_HES\\OUTPUTS\\CSVforSEFT\\PLIC_{feed_type.upper()}"
            else:
                self.csvBaseDir = f"\\\\ic\\IC_DME_DFS\\DME_DEV\\PLIC\\PLIC_{feed_type.upper()}\\OUTPUTS\\CSVforSEFT"
        self.feed_type = feed_type.replace('"', '')
        self.LOCAL_RESOURCE_DIR = os.getenv('LOCAL_RESOURCE_DIR')

    def read_csv_file(self, file_type):
        folder = self.get_file_folder(file_type)
        file_pattern = self.get_file_name(file_type)
        file_path = f"{self.csvBaseDir}\\{folder}\\{file_pattern}"
        file_name = self.get_latest_file(file_path)

        with open(file_name) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
        return csv_reader

    def get_feed_xml(self):
        feed_data = f"{config('LOCAL_RESOURCE_DIR')}/feedData.csv"
        files = []
        with open(feed_data) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                if row[0] == self.feed_type:
                    files.append(row[1])
        return files

    def get_file_folder(self, file_type):
        if file_type == 'activity' or file_type == 'cost':
            return f"PLICS_${self.feed_type.upper()}"

        elif file_type == 'summary':
            return f"PLICS_${self.feed_type.upper()}_Summary"
        else:
            return f"PLICS_${file_type.upper()}"

    def get_file_name(self, file_type):
        if file_type == 'activity':
            return "*Activity*"
        elif file_type == 'cost':
            return "*Cost*"
        elif file_type == 'intrec' or file_type == 'rec' or file_type == 'ambrec':
            return "*Reconciliation*"
        else:
            return "*Data*"

    def get_latest_file(self, file_name):
        list_of_files = glob.glob(file_name)
        return max(list_of_files, key=os.path.getctime)

    def get_file_path(self, data_type, batch_number):
        FinYR = os.getenv('FINYR')
        batch = batch_number.replace('"', "")
        if self.feed_type.upper() not in ['INTREC', 'AMBREC', 'IAPTREC']:
            file = f"{self.csvBaseDir}\\{str(batch).zfill(3)}_{data_type}_{self.feed_type.upper()}_{FinYR}_X_XXX.csv"
            return file.replace('\\\\', '\\')
        else:
            file = f"{self.csvBaseDir}\\{str(batch).zfill(3)}_Reconciliation_{self.feed_type.upper()}_{FinYR}_XXX.csv"
            return file.replace('\\\\', '\\')
