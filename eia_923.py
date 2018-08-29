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
        """This code loads EIA 923 monthly energy generation and assigns each plant type to one of:
        SUN (solar PV and thermal), COL (coal and waste coal), NGCC (natural gas combined cycle),
        NGCT (natural gas combustion turbine, NUC (nuclear), WND (wind), FF (other fossil fuel), or
        ALT (other alternative fuel). The assignment is stored in the column 'Plant Type'."""

        print('setting up EIA 923 dataframe ' + str(datetime.datetime.now().time()))
        # import dataframe from excel file
        monthly_energy_full = pd.read_excel(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
                                       sheet_name=0, header=5, usecols="A:B,D:G,I,K:N,P,CB:CM,CR:CS")
        monthly_energy_full = monthly_energy_full.replace(to_replace='.', value=0)
        # replace AER codes for fossil fuels
        monthly_energy_full['AER\nFuel Type Code'].replace(['DFO', 'OOG', 'PC', 'RFO', 'WOO'], ['FF', 'FF', 'FF', 'FF',
                                                                                                'FF'], inplace=True)
        # replace AER codes for alternative fuels
        monthly_energy_full['AER\nFuel Type Code'].replace(['GEO', 'HPS', 'HYC', 'MLG', 'ORW', 'OTH', 'WWW'],
                                                ['ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT'], inplace=True)
        # relabel waste coal as coal
        monthly_energy_full['AER\nFuel Type Code'].replace(['WOC'], ['COL'], inplace=True)
        # change all natural gas listings to NGCT
        monthly_energy_full.loc[(monthly_energy_full['AER\nFuel Type Code'] == 'NG'),
                                'AER\nFuel Type Code'] = 'NGCT'
        # replace 'CA, CS, CT' prime movers as 'CC' (combined cycle)
        monthly_energy_full['Reported\nPrime Mover'].replace(['CA', 'CS', 'CT'], ['CC', 'CC', 'CC'], inplace=True)
        # rename CC natural gas listings as NGCC
        monthly_energy_full.loc[(monthly_energy_full['AER\nFuel Type Code'] == 'NGCT') &
                                (monthly_energy_full['Reported\nPrime Mover'] == 'CC'), 'AER\nFuel Type Code'] = 'NGCC'

        monthly_energy_full.rename(columns={'AER\nFuel Type Code': 'Plant Type'}, inplace=True)

        # test_df = pd.read_excel(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
        #                                sheet_name=0, header=5, usecols="A,CB:CM,CR")

        # monthly_energy_full['Plant Code'] = pd.Series(np.ones(len(monthly_energy_full['Plant Id'])),
        #                                               index=monthly_energy_full.index)
        # monthly_energy = monthly_energy_full.groupby(by='Plant Id')['Netgen\nJanuary', 'Netgen\nFebruary',
        #                                                             'Netgen\nMarch', 'Netgen\nApril', 'Netgen\nMay',
        #                                                             'Netgen\nJune', 'Netgen\nJuly', 'Netgen\nAugust',
        #                                                             'Netgen\nSeptember', 'Netgen\nOctober',
        #                                                             'Netgen\nNovember', 'Netgen\nDecember',
        #                                                             'Net Generation\n(Megawatthours)'].sum()

        # monthly_energy = pd.DataFrame(columns=monthly_energy_full.columns)
        # monthly_energy['Plant Code'] = 'NaN'
        # new_counter = -1
        # for i in range(len(monthly_energy_full)):
        #     if monthly_energy_full.iloc[i]['Plant Id'] not in monthly_energy['Plant Id'].values:
        #         monthly_energy = monthly_energy.append(monthly_energy_full.loc[i, :])
        #         new_counter += 1
        #     else:
        #         monthly_energy.iloc[new_counter, 12:24] += monthly_energy_full.iloc[i, 12:24]

        print('dataframe ready! ' + str(datetime.datetime.now().time()))

        # test_df = test_df.groupby(['Plant Id'].sum())
        # print(test_df.loc[:3, :])

        print(monthly_energy_full.loc[:10, :])
        # print(monthly_energy.loc[:10])
        # print(monthly_energy_full['Plant Id'].value_counts())
        # # remove columns for prime mover and fuel type (below)
        # monthly_energy = monthly_energy.drop(columns=['Reported\nPrime Mover', 'AER\nFuel Type Code'])
        return monthly_energy_full
