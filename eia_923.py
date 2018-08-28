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
        # self.ferc_gross_load = self.setup_ferc_gross_load()

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
        print('dataframe ready! ' + str(datetime.datetime.now().time()))
        monthly_energy = pd.DataFrame(columns=monthly_energy_full.columns)
        new_counter = -1
        # print(len(df))
        for i in range(len(monthly_energy_full)):
            if monthly_energy_full.iloc[i]['Plant Id'] not in monthly_energy['Plant Id'].values:
                monthly_energy = monthly_energy.append(monthly_energy_full.loc[i, :])
                new_counter += 1
            else:
                # print('New--should be current plant ID ')
                # print(monthly_energy.iloc[new_counter, monthly_energy.columns.get_loc('Netgen\nJanuary')])
                # print('Original -- should be the 2nd+ entry with given plant ID ')
                # print(monthly_energy_full.iloc[i]['Netgen\nJanuary'])
                # monthly_energy.iloc[new_counter, monthly_energy.columns.get_loc('Netgen\nJanuary')] += \
                #     float(monthly_energy_full.iloc[i]['Netgen\nJanuary'])
                monthly_energy.iloc[new_counter, 12:23] += monthly_energy_full.iloc[i, 12:23]




        #-----------------------------------------
        # i = 0
        # for plant in monthly_energy_full['Plant Id']:
        #     if plant not in monthly_energy['Plant Id']:
        #         new_row = monthly_energy_full[plant]
        #         monthly_energy.loc[i] = new_row
        #         i += 1
        #     else:
        #         monthly_energy[plant, 'Netgen\nJanuary'] += monthly_energy_full[plant, 'Netgen\nJanuary']
        #
        # # for plant in monthly_energy_full['Plant Id']:
        # #     monthly_energy(plant, 'Plant Id') = monthly_energy_full(plant, 'Plant Id')
        # #     monthly_energy(plant, 'Netgen\nJanuary') += monthly_energy_full.loc[plants[plant], 'Netgen\nJanuary']
        print(monthly_energy_full.loc[:10, :])
        print(monthly_energy.loc[:3, :])
        # # print('Testing: what does monthly_energy_full[Plant Id] give? ' + monthly_energy_full['Plant Id'])
        # # remove columns for prime mover and fuel type (below)
        # monthly_energy = monthly_energy.drop(columns=['Reported\nPrime Mover', 'AER\nFuel Type Code'])
        return monthly_energy
    #
    # def setup_ferc_gross_load(self):
    #     # this code loads FERC 714 demand data and produces a dataframe
    #     # of hourly load data, columns for each respondent
    #     temp_gross_load_df = pd.read_csv(self.data_path + '/Part 3 Schedule 2 - Planning Area Hourly Demand.csv')
    #     temp_gross_load_df = temp_gross_load_df.ix[:, 0:31]
    #     temp_gross_load_index = temp_gross_load_df.set_index('respondent_id')
    #     all_ferc_ids = list(set(list(temp_gross_load_index.index.values)))
    #     temp_gross_load_df = temp_gross_load_df.drop(['report_yr', 'report_prd',
    #                                                   'spplmnt_num', 'row_num',
    #                                                   'timezone'], axis=1)
    #     temp_gross_load_df = temp_gross_load_df.set_index(['respondent_id', 'plan_date'])
    #     gross_load_df = reshape_ferc(temp_gross_load_df, all_ferc_ids)
    #     del temp_gross_load_df
    #     max_load = gross_load_df.max(axis=0).T
    #     return gross_load_df

