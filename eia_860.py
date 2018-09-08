import os.path
import shutil
import pickle
import urllib.request
import zipfile
import datetime
import numpy as np
import pandas as pd

def unzip(zip_file_path, directory_to_extract_to):
    # function to unzip a file
    with zipfile.ZipFile(zip_file_path, "r") as z:
        z.extractall(directory_to_extract_to)


class SetupDataEIA(object):
    def __init__(self):
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        self.data_path = os.path.abspath('data')
        self.acquire_eia860()
        # self.setup_860_stuff

    def acquire_eia860(self):
        if not os.path.exists(self.data_path + '/3_1_Generator_Y2015.xlsx'):
            print('downloading EIA 860', str(datetime.datetime.now().time()))
            urllib.request.urlretrieve('https://www.eia.gov/electricity/data/eia860/xls/eia8602015.zip',
                                       self.data_path + '/tmp/EIA860.zip')
            print('unzipping EIA 860', str(datetime.datetime.now().time()))
            unzip(self.data_path + '/tmp/EIA860.zip', self.data_path + '/tmp/EIA860')
            os.remove(self.data_path + '/tmp/EIA860.zip')
            os.rename(self.data_path + '/tmp/EIA860/3_1_Generator_Y2015.xlsx',
                      self.data_path + '/3_1_Generator_Y2015.xlsx')
            if not os.path.exists(self.data_path + '/3_2_Wind_Y2015.xlsx'):
                os.rename(self.data_path + '/tmp/EIA860/3_2_Wind_Y2015.xlsx',
                          self.data_path + '/3_2_Wind_Y2015.xlsx')
            if not os.path.exists(self.data_path + '/3_3_Solar_Y2015.xlsx'):
                os.rename(self.data_path + '/tmp/EIA860/3_3_Solar_Y2015.xlsx',
                          self.data_path + '/3_3_Solar_Y2015.xlsx')
            if not os.path.exists(self.data_path + '/3_3_Solar_Y2015.xlsx'):
                os.rename(self.data_path + '/tmp/EIA860/3_4_Multifuel_Y2015.xlsx',
                          self.data_path + '/3_4_Multifuel_Y2015.xlsx')
            shutil.rmtree(self.data_path + '/tmp/EIA860')