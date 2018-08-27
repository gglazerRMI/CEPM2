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
        # self.eia923_forecast = self.setup_eia923_forecast()
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

        monthly_energy = pd.read_excel(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
                                       sheet_name=0, header=5, usecols="A,D:G,M:N,P,CB:CM,CR")
        # temp_demand_forecast = temp_demand_forecast.iloc[:, 0:10]
        # temp_demand_forecast['forecast'] = np.nan
        # for i in list(temp_demand_forecast.index):
        #     w = temp_demand_forecast.loc[i, 'winter_forecast']
        #     s = temp_demand_forecast.loc[i, 'summer_forecast']
        #     if w > s:
        #         d = w
        #     else:
        #         d = s
        #     temp_demand_forecast.set_value(i, 'forecast', d)
        # temp_demand_forecast = temp_demand_forecast.loc[temp_demand_forecast['report_yr'] == 2015]
        # temp_demand_forecast = temp_demand_forecast.set_index(['respondent_id',
        #                                                        'plan_year'])
        # temp_demand_forecast = temp_demand_forecast.drop(['report_yr', 'report_prd',
        #                                 'spplmnt_num', 'row_num',
        #                                 'summer_forecast', 'winter_forecast',
        #                                 'net_energy_forecast', 'plan_year_f'], axis=1)
        # demand_forecast = temp_demand_forecast.unstack(level=[1])
        # demand_forecast.columns = demand_forecast.columns.droplevel(0)
        # del temp_demand_forecast
        # demand_forecast['load_growth'] = np.nan
        # for i in list(demand_forecast.index):
        #     first = demand_forecast.get_value(i, 2016)
        #     last = demand_forecast.get_value(i, 2025)
        #     try:
        #         growth = calc_cagr(first, last, 9)
        #         demand_forecast.set_value(i, 'load_growth', growth)
        #     except ZeroDivisionError:
        #         print('Error calculating demand growth for ', i, ' because initial value is zero')

        print('dataframe ready! ' + str(datetime.datetime.now().time()))
        print(monthly_energy.loc[:1, :])
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

