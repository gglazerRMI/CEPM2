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
        self.acquire_eia860()
        df923 = self.setup_eia923()
        df860 = self.setup_eia860()
        self.setup_dfpp(df923, df860)

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
            if not os.path.exists(self.data_path + '/2___Plant_Y2015.xlsx'):
                os.rename(self.data_path + '/tmp/EIA860/2___Plant_Y2015.xlsx',
                          self.data_path + '/2___Plant_Y2015.xlsx')
            # if not os.path.exists(self.data_path + '/3_2_Wind_Y2015.xlsx'):
            #     os.rename(self.data_path + '/tmp/EIA860/3_2_Wind_Y2015.xlsx',
            #               self.data_path + '/3_2_Wind_Y2015.xlsx')
            # if not os.path.exists(self.data_path + '/3_3_Solar_Y2015.xlsx'):
            #     os.rename(self.data_path + '/tmp/EIA860/3_3_Solar_Y2015.xlsx',
            #               self.data_path + '/3_3_Solar_Y2015.xlsx')
            # if not os.path.exists(self.data_path + '/3_3_Solar_Y2015.xlsx'):
            #     os.rename(self.data_path + '/tmp/EIA860/3_4_Multifuel_Y2015.xlsx',
            #               self.data_path + '/3_4_Multifuel_Y2015.xlsx')
            # shutil.rmtree(self.data_path + '/tmp/EIA860')

    def setup_eia923(self):
        """This code loads EIA 923 monthly energy generation and assigns each plant type to one of:
        SUN (solar PV and thermal), COL (coal and waste coal), NGCC (natural gas combined cycle),
        NGCT (natural gas combustion turbine, NUC (nuclear), WND (wind), FF (other fossil fuel), or
        ALT (other alternative fuel). The assignment is stored in the column 'Plant Type'."""

        print('setting up EIA 923 dataframe ' + str(datetime.datetime.now().time()))
        # import dataframe from excel file
        df923 = pd.read_excel(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
                                       sheet_name=0, header=5, usecols="A:B,D:G,I,K:N,P,CB:CM,CR:CS")
        ## took out df923= and added inplace
        df923.replace(to_replace='.', value=0, inplace=True)
        # replace AER codes for fossil fuels
        df923['AER\nFuel Type Code'].replace(['DFO', 'OOG', 'PC', 'RFO', 'WOO'], ['FF', 'FF', 'FF', 'FF',
                                                                                                'FF'], inplace=True)
        # replace AER codes for alternative fuels
        df923['AER\nFuel Type Code'].replace(['GEO', 'HPS', 'HYC', 'MLG', 'ORW', 'OTH', 'WWW'],
                                                ['ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT'], inplace=True)
        # relabel waste coal as coal
        df923['AER\nFuel Type Code'].replace(['WOC'], ['COL'], inplace=True)
        # replace 'CA, CS, CT' prime movers as 'CC' (combined cycle)
        df923['Reported\nPrime Mover'].replace(['CA', 'CS', 'CT'], ['CC', 'CC', 'CC'], inplace=True)

        # ## trying to figure out if there are any CC prime movers that are not NGCC
        # for i in df923.loc[('Reported\nPrime Mover' == 'CC') & (df923('AER\nFuel Type Code' != 'NG'))]:
        #     print(i)
        # print('shouldve been above here!')

        # label CC in AER fuel column
        df923.loc[(df923['Reported\nPrime Mover'] == 'CC'), 'AER\nFuel Type Code'] = 'NGCC'
        # change remaining natural gas listings to NGCT
        df923.loc[(df923['AER\nFuel Type Code'] == 'NG'),
                  'AER\nFuel Type Code'] = 'NGCT'
        df923.rename(columns={'AER\nFuel Type Code': 'Plant Type'}, inplace=True)

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

        df923test = df923.groupby(['Plant Id', 'Plant Type']).sum()

        print('dataframe ready! ' + str(datetime.datetime.now().time()))

        f = open('eia923txt.txt', 'w')
        f.write(df923test.loc[:30, :])

        # print(df923.loc[:10, :])
        # print(df923test.loc[:30, :])

        # print(monthly_energy_full['Plant Id'].value_counts())
        # # remove columns for prime mover and fuel type (below)
        # monthly_energy = monthly_energy.drop(columns=['Reported\nPrime Mover', 'AER\nFuel Type Code'])
        return df923test

    def setup_eia860(self):
        """This method loads the EIA 860-Generators form as a dataframe from the excel download. The dataframe is
        organized where each index is a unique 'power plant', as defined for the purposes of the CEPM tool.
        Each power plant of uniform plant type is a unique entry, and power plants with multiple plant types are listed
        as separate entries."""
        print('setting up EIA 860 dataframe ' + str(datetime.datetime.now().time()))
        # import dataframe from excel file
        df860gen = pd.read_excel(self.data_path + '/3_1_Generator_Y2015.xlsx',
                              sheet_name=0, header=1, usecols="A:D,H:I,P,Z:AA,AH")
        df860gen.replace(to_replace='.', value=0, inplace=True)
        df860gen.drop(df860gen.tail(1).index, inplace=True)
        # replace Energy Source 1 codes for fossil fuels
        df860gen['Energy Source 1'].replace(['DFO', 'JF', 'KER', 'PC', 'PG', 'RFO', 'SGP', 'WO'],
                                             ['FF', 'FF', 'FF', 'FF', 'FF', 'FF', 'FF', 'FF'], inplace=True)
        # replace Energy Source 1 codes for alternative fuels
        df860gen['Energy Source 1'].replace(['AB', 'MSW', 'OBS', 'WDS', 'OBL', 'SLW', 'BLQ', 'WDL', 'LFG', 'OBG',
                                              'GEO', 'WAT', 'PUR', 'WH', 'TDF', 'MWH', 'OTH'],
                                             ['ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT',
                                              'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT'], inplace=True)
        # replace Energy Source 1 codes for coal
        df860gen['Energy Source 1'].replace(['ANT', 'BIT', 'LIG', 'SGC', 'SUB', 'WC', 'RC'],
                                             ['COL', 'COL', 'COL', 'COL', 'COL', 'COL', 'COL'], inplace=True)
        # replace Energy Source 1 codes for natural gas
        df860gen['Energy Source 1'].replace(['BFG', 'OG'], ['NG', 'NG'], inplace=True)
        # replace 'CA, CS, CT' prime movers as 'CC' (combined cycle)
        df860gen['Prime Mover'].replace(['CA', 'CS', 'CT'], ['CC', 'CC', 'CC'], inplace=True)
        # rename CC natural gas listings as NGCC
        df860gen.loc[(df860gen['Energy Source 1'] == 'NG') &
                 (df860gen['Prime Mover'] == 'CC'), 'Energy Source 1'] = 'NGCC'
        # change all remaining natural gas listings to NGCT
        df860gen['Energy Source 1'].replace(['NG'], ['NGCT'], inplace=True)

        # change names and types of variables to match df923
        df860gen.rename(columns={'Energy Source 1': 'Plant Type'}, inplace=True)
        df860gen.rename(columns={'Plant Code': 'Plant Id'}, inplace=True)
        df860gen['Plant Id'] = df860gen['Plant Id'].astype(int)

        # group to match df923
        df860test = df860gen.groupby(['Plant Id', 'Plant Type']).sum()
        print('dataframe ready! ' + str(datetime.datetime.now().time()))
        f = open('eia860txt.txt', 'w')
        f.write(df860test.loc[:30, :])
        # print(df860gen.loc[:30, :])
        # print(df860test.loc[:30, :])

        return df860test

    def setup_dfpp(self, df923test, df860test):
        """This method combines df923 and df860gen into one cumulative power plant dataframe."""
        print('setting up Power Plant dataframe ' + str(datetime.datetime.now().time()))
        dfpp = pd.merge(df923test, df860test, how='outer', on=['Plant Id', 'Plant Type'])

        print('dataframe ready! ' + str(datetime.datetime.now().time()))
        f = open('powerplants_txt.txt', 'w')
        f.write(dfpp)
        return dfpp
