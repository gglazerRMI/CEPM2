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



class SetupData(object):
    '''
    Class for setting up all the data, this is a less than perfect use for a class
    thinking we sort of use it like a function that can share certain parts of itself
    '''
    def __init__(self):
        # Create data and tmp directories
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        self.data_path = os.path.abspath('data')
        self.acquire_eia923()
        self.setup_923_monthly()

    def acquire_eia923(self):
        # download EIA 923 if relevant files are not in 'data' directory
        if not os.path.exists(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx'):
            print('downloading EIA 923', str(datetime.datetime.now().time()))
            urllib.request.urlretrieve('https://www.eia.gov/electricity/data/eia923/archive/xls/f923_2016.zip',
                                       self.data_path + '/tmp/EIA923.zip')
            print('unzipping EIA 923', str(datetime.datetime.now().time()))
            unzip(self.data_path + '/tmp/EIA923.zip', self.data_path + '/tmp/EIA923')
            os.remove(self.data_path + '/tmp/EIA923.zip')
            os.rename(self.data_path + '/tmp/EIA923/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
                      self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx')
            if not os.path.exists(self.data_path + '/EIA923_Schedule_8_Annual_Environmental_Information_'
                                                   '2016_Final_Revision.xlsx'):
                os.rename(self.data_path + '/tmp/EIA923/EIA923_Schedule_8_Annual_Environmental_Information_'
                                           '2016_Final_Revision.xlsx',
                          self.data_path + '/EIA923_Schedule_8_Annual_Environmental_Information_'
                                           '2016_Final_Revision.xlsx')
            if not os.path.exists(self.data_path + '/EIA923_Schedules_6_7_NU_SourceNDisposition_'
                                                   '2016_Final_Revision.xlsx'):
                os.rename(self.data_path + '/tmp/EIA923/EIA923_Schedules_6_7_NU_SourceNDisposition_'
                                           '2016_Final_Revision.xlsx',
                          self.data_path + '/EIA923_Schedules_6_7_NU_SourceNDisposition_2016_Final_Revision.xlsx')
            shutil.rmtree(self.data_path + '/tmp/EIA923')

    def setup_923_monthly(self):
        # this code loads EIA 923 monthly energy generation and will eventually calculate other things too
        print('setting up EIA 923 dataframe ' + str(datetime.datetime.now().time()))

        monthly_energy_full = pd.read_excel(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
                                       sheet_name=0, header=5, usecols="A:B,D:G,I,K:N,P,CB:CM,CR:CS")
        monthly_energy_full = monthly_energy_full.replace(to_replace='.', value=0)
        # test_df = pd.read_excel(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
        #                                sheet_name=0, header=5, usecols="A,CB:CM,CR")

        # monthly_energy_full['Plant Code'] = pd.Series(np.ones(len(monthly_energy_full['Plant Id'])),
        #                                               index=monthly_energy_full.index)
        monthly_energy = pd.DataFrame(columns=monthly_energy_full.columns)
        monthly_energy['Plant Code'] = 'NaN'
        new_counter = -1
        for i in range(len(monthly_energy_full)):
            if monthly_energy_full.iloc[i]['Plant Id'] not in monthly_energy['Plant Id'].values:
                monthly_energy = monthly_energy.append(monthly_energy_full.loc[i, :])
                if monthly_energy_full.loc[i, ['AER\nFuel Type Code']].item() == 'COL' or 'WOC':
                    monthly_energy.loc[i, ['Plant Code']] = 'CoalST'
                elif monthly_energy_full.loc[i, ['AER\nFuel Type Code']].item() == 'NG':
                    if monthly_energy_full.loc[i, ['Reported\nPrime Mover']].item() == 'CA' or 'CS' or 'CT':
                        monthly_energy.loc[i, ['Plant Code']] = 'NGCC'
                    else:
                        monthly_energy.loc[i, ['Plant Code']] = 'NGCT'
                elif monthly_energy_full.loc[i, ['AER\nFuel Type Code']].item() == 'SUN':
                        monthly_energy.loc[i, ['Plant Code']] = 'Solar'
                elif monthly_energy_full.loc[i, ['AER\nFuel Type Code']].item() == 'WND':
                        monthly_energy.loc[i, ['Plant Code']] = 'Wind'
                elif monthly_energy_full.loc[i, ['AER\nFuel Type Code']].item() == 'NUC':
                    monthly_energy.loc[i, ['Plant Code']] = 'Nuke'
                else:
                    monthly_energy.loc[i, ['Plant Code']] = 'Other'
                new_counter += 1
            else:
                monthly_energy.iloc[new_counter, 12:24] += monthly_energy_full.iloc[i, 12:24]

        print('dataframe ready! ' + str(datetime.datetime.now().time()))

        # test_df = test_df.groupby(['Plant Id'].sum())
        # print(test_df.loc[:3, :])

        print(monthly_energy_full.loc[:10, :])
        print(monthly_energy.loc[:10, :])

        # # remove columns for prime mover and fuel type (below)
        # monthly_energy = monthly_energy.drop(columns=['Reported\nPrime Mover', 'AER\nFuel Type Code'])
        return monthly_energy
