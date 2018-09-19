import os.path
import shutil
import pickle
import urllib.request
import zipfile
import datetime
# import csv
# from ftplib import FTP
# import copy
# from multiprocessing import Pool
# import sys
import numpy as np
import pandas as pd
# import pandas.io.formats.excel
# from scipy import optimize
# import matplotlib.pyplot as plt
# from matplotlib.backends.backend_pdf import PdfPages
# import matplotlib.dates as mdates
# import seaborn as sns
# import xlsxwriter
# import pyiso
# from pudl import pudl, ferc1, eia923
# from pudl import models, models_ferc1, models_eia923
# from pudl import settings, constants

def load_pickle(name):
    # function to load an object from a pickle
    with open(str(name) + '.pkl', 'rb') as f:
        temp = pickle.load(f)
    return temp

def save_pickle(contents, name):
    # function to save to an object as a pickle
    with open(str(name) + '.pkl', 'wb') as output:
        pickle.dump(contents, output, pickle.HIGHEST_PROTOCOL)

def calc_cagr(first_value, last_value, num_years, value_type):
    # calculates the compound annual growth rate
    expo = float(1)/float(num_years)
    if value_type == 'int':
        if first_value == 0:
            cagr = 0
        else:
            base = np.divide(last_value, first_value)
            to_the = np.power(base, expo)
            cagr = np.subtract(to_the, 1)
    else:
        first_value.replace({0: np.nan}, inplace=True)
        base = np.divide(last_value, first_value)
        to_the = np.power(base, expo)
        cagr = np.subtract(to_the, 1)
    return cagr

def reshape_ferc(df, resps):
    # function to reshape FERC 714 load or lambda data into pandas timeseries
    for resp in resps:
        # print(resp, end=", ", flush=True)
        tdf = df.loc[resp]
        # print('Temporary data frame ' + str(resp))
        # print(tdf)
        adf = tdf.values
        nadf = np.reshape(adf, adf.size)
        ind = list(tdf.index.values)
        # print('Index ' + str(resp))
        # print(ind)
        per = len(ind) * 24
        temp_index = pd.date_range(ind[0], periods=per, freq='H')
        # print('Temporary Index ' + str(resp))
        # print(temp_index)
        tdf = pd.DataFrame(nadf, index=temp_index, columns=[str(resp)])
        if np.sum(nadf) > 0:
            try:
                all_ldf = pd.concat([all_ldf, tdf], axis=1)
            except NameError:
                all_ldf = tdf
    all_ldf = all_ldf.loc[:, (all_ldf != 0).any(axis=0)]
    all_ldf = all_ldf.dropna(axis=1, how='all')
    return all_ldf

def get_rps(df, rps_state):
    # function to find the year and renewable energy percentage given by a state's RPS
    try:
        print('RPS for ' + rps_state)
        print(df.loc[rps_state])
        re_frac = df.loc[rps_state, 'RPS RE%']
        rps_yr = df.loc[rps_state, 'Year']
    except KeyError:
        if rps_state == 'TX':
            print('Texas requires 10,000 MW of renewable capacity by 2025, this will be handled elsewhere in the '
                  'script.\n')
            re_frac = float('nan')
            rps_yr = float('nan')
        else:
            print('State does not have an RPS, assume constant mix of renewable energy sources.')
            re_frac = float('nan')
            rps_yr = float('nan')
    return re_frac, rps_yr

def unzip(zip_file_path, directory_to_extract_to):
    # function to unzip a file
    with zipfile.ZipFile(zip_file_path, "r") as z:
        z.extractall(directory_to_extract_to)

class SetupDataL(object):
    '''
    Class for setting up all the data, this is a less than perfect use for a class
    thinking we sort of use it like a function that can share certain parts of itself
    '''
    def __init__(self,  respondent_id, state, forecast_year, curr_year=2016, cagr_range=10, export_all=True):
        # Create data and tmp directories, /Users/gglazer/PycharmProjects
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        self.data_path = os.path.abspath('data')
        self.export_all = export_all
        self.acquire_ferc()
        demand_forecast = self.setup_ferc_forecast(curr_year, cagr_range)
        ferc_gross_load = self.setup_ferc_gross_load(demand_forecast, forecast_year, curr_year)
        # renewable_8760 = self.setup_renewable_8760(respondent_id, state)
        # self.calculate_net_load(ferc_gross_load, respondent_id, curr_year)

    def acquire_ferc(self):
        # download FERC 714 if relevant files are not in 'data' directory
        if not os.path.exists(self.data_path + '/Part 3 Schedule 2 - Planning Area Hourly Demand.csv'):
            print('downloading FERC 714', str(datetime.datetime.now().time()))
            urllib.request.urlretrieve('https://www.ferc.gov/docs-filing/forms/form-714/data/form714-database.zip',
                                       self.data_path + '/tmp/FERC.zip')
            print('unzipping FERC 714', str(datetime.datetime.now().time()))
            unzip(self.data_path + '/tmp/FERC.zip', self.data_path + '/tmp/FERC')
            os.remove(self.data_path + '/tmp/FERC.zip')
            os.rename(self.data_path + '/tmp/FERC/Part 3 Schedule 2 - Planning Area Hourly Demand.csv',
                      self.data_path + '/Part 3 Schedule 2 - Planning Area Hourly Demand.csv')
            if not os.path.exists(self.data_path + '/Part 3 Schedule 2 - Planning Area Forecast Demand.csv'):
                os.rename(self.data_path + '/tmp/FERC/Part 3 Schedule 2 - Planning Area Forecast Demand.csv',
                          self.data_path + '/Part 3 Schedule 2 - Planning Area Forecast Demand.csv')
            if not os.path.exists(self.data_path + '/Respondent IDs.csv'):
                os.rename(self.data_path + '/tmp/FERC/Respondent IDs.csv',
                          self.data_path + '/Respondent IDs.csv')
            shutil.rmtree(self.data_path + '/tmp/FERC')

    def setup_ferc_forecast(self, curr_year, cagr_range):
        # -----------TO FIX------------ #
        # get_value and set_value are both deprecated, use at[] or iat[]

        # this code loads FERC 714 demand forecasts and calculates the implied
        # annual load growth for each respondent
        temp_demand_forecast = pd.read_csv(self.data_path + '/Part 3 Schedule 2 - Planning Area Forecast Demand.csv')
        temp_demand_forecast = temp_demand_forecast.iloc[:, 0:10]
        # temp_demand_forecast['forecast'] = np.nan
        # for i in list(temp_demand_forecast.index):
        #     w = temp_demand_forecast.loc[i, 'winter_forecast']
        #     s = temp_demand_forecast.loc[i, 'summer_forecast']
        #     if w > s:
        #         d = w
        #     else:
        #         d = s
        #     temp_demand_forecast.set_value(i, 'forecast', d)
        temp_demand_forecast = temp_demand_forecast.loc[temp_demand_forecast['report_yr'] == curr_year]
        temp_demand_forecast = temp_demand_forecast.set_index(['respondent_id',
                                                               'plan_year'])
        temp_demand_forecast = temp_demand_forecast.drop(['report_yr', 'report_prd',
                                                          'spplmnt_num', 'row_num',
                                                          'summer_forecast', 'winter_forecast',
                                                          'plan_year_f'], axis=1)
        demand_forecast = temp_demand_forecast.unstack(level=[1])
        demand_forecast.columns = demand_forecast.columns.droplevel(0)
        del temp_demand_forecast
        demand_forecast['load_growth'] = np.nan
        print(demand_forecast.iloc[:10, :])
        for i in list(demand_forecast.index):
            # first = demand_forecast.loc[i, (curr_year + 1)]
            first = demand_forecast.loc[i, 2017]
            last = demand_forecast.loc[i, 2026]
            try:
                growth = calc_cagr(first, last, 9, 'int')
                demand_forecast.loc[i, 'load_growth'] = growth
            except ZeroDivisionError:
                print('Error calculating demand growth for ' + str(i) + ' because initial value is zero\n')
        if self.export_all:
            demand_forecast.to_csv(self.data_path + '/tmp/demand_forecast.csv')
        save_pickle(demand_forecast, 'demand_forecast_pickle')
        return demand_forecast

    def setup_ferc_gross_load(self, demand_forecast, forecast_year, curr_year):
        # -------------TO FIX------------ #
        # need to grow the load by the growth rate in demand_forecast 'load_growth' to the forecast year
        # this code loads FERC 714 demand data and produces a dataframe
        # of hourly load data, columns for each respondent
        temp_gross_load_df = pd.read_csv(self.data_path + '/Part 3 Schedule 2 - Planning Area Hourly Demand.csv')
        temp_gross_load_df = temp_gross_load_df.ix[:, 0:31]
        temp_gross_load_index = temp_gross_load_df.set_index('respondent_id')
        all_ferc_ids = list(set(list(temp_gross_load_index.index.values)))
        temp_gross_load_df = temp_gross_load_df.drop(['report_yr', 'report_prd',
                                                      'spplmnt_num', 'row_num',
                                                      'timezone'], axis=1)
        temp_gross_load_df = temp_gross_load_df.set_index(['respondent_id', 'plan_date'])
        gross_load_df = reshape_ferc(temp_gross_load_df, all_ferc_ids)
        del temp_gross_load_df
        max_load = gross_load_df.max(axis=0).T
        if self.export_all:
            gross_load_df.to_csv(self.data_path + '/tmp/gross_load.csv')
            max_load.to_csv(self.data_path + '/tmp/max_load.csv')

        # to clear room so I can see past the error messages
        for i in range(10):
            print('extra space\n')
        # data frame of the gross load for all respondents in the current year
        gross_load_df = gross_load_df[gross_load_df.index.year == curr_year]
        print(gross_load_df)
        # add load growth column to gross load data frame
        # demand_forecast.reset_index(inplace=True)
        # gross_load_df = gross_load_df.merge(demand_forecast[['respondent_id', 'load_growth']],
        #                                                       on='respondent_id', how='left')
        # gross_load_df['load_growth'] = gross_load_df['load_growth'].fillna(0)
        print('This is the gross load data frame for the current year: \n')
        print(gross_load_df.iloc[:10, :])

        save_pickle(gross_load_df, 'gross_load_pickle')

        return gross_load_df

    def setup_renewable_8760(self, respondent_id, state):
        # This code determines the share of renewable energy for a given respondent
        # We need to multiply a normalized RE 8760 by the total capacity of RE for the respondent before subtracting
        # from the gross 8760

        # need to consider growth given RPS

        dfpp = load_pickle('/Users/gglazer/PyCharmProjects/CEP1/pppickle')
        dfpp.reset_index(inplace=True)
        df_re = dfpp.loc[((dfpp['Plant Type'] == 'WND') | (dfpp['Plant Type'] == 'SUN')) & (
                    dfpp['Respondent Id'] == respondent_id) & (dfpp['Nameplate Capacity (MW)'] > 0)]
        df_total = dfpp.loc[(dfpp['Respondent Id'] == respondent_id) & (dfpp['Nameplate Capacity (MW)'] > 0)]
        re_sum = df_re['Nameplate Capacity (MW)'].sum()
        print('total renewables = ' + str(re_sum))
        total_sum = df_total['Nameplate Capacity (MW)'].sum()
        print('total capacity = ' + str(total_sum))
        renewable_share = re_sum / total_sum
        print('renewables make up ' + str(renewable_share * 100) + '% of this respondents capacity')


        re_8760 = [0]

        return re_8760

    def calculate_net_load(self, gross_load_df, respondent_id, curr_year):
        # This code calculates the top 50 hours of net load



        # need to sort after subtracting
        gross_load_df = gross_load_df[gross_load_df.index.year == curr_year]
        gross_load_resp = gross_load_df[[str(respondent_id)]].sort_values(by=[str(respondent_id)], ascending=False)

        # Take the top 50 hours of gross load
        top_50 = gross_load_resp.iloc[:50, :]
        top_50.to_csv(self.data_path + '/tmp/top_50.csv')
        print(gross_load_df.iloc[:24, :10])
        print(gross_load_resp.iloc[:24, :])

        return gross_load_resp


# class CEPCase(object):
#     """
#     An instance of this class is a CEP case
#     """
#     def __init__(self,
#                  name,
#                  util,
#                  capacity,
#                  year,
#                  more_args,
#                  export_all=False,
#                  load_old=True):
#         self.name = name
#         self.util = util
#         self.capacity = capacity
#         self.year = year
#         self.export_all = export_all
#         self.load_old = load_old
#         self.more_args = more_args
#         # lots more stuff and settings
#
#         # this allows us to not have to load all the individual data sources again
#         # more an example than a neccesity, this code might be better somewhere else
#         if self.load_old:
#             try:
#                 self.case_data = load_pickle('data/'+str(self.name))
#             except FileNotFoundError:
#                 self.case_data = self.import_data()
#                 save_pickle(self.case_data, 'data/'+str(self.name))
#         else:
#             self.case_data = self.import_data()
#             save_pickle(self.case_data, 'data/' + str(self.name))
#
#         # putting this function up here under __init__ means it runs when
#         # an instance of the class is created
#         self.prepare_matrices()
#
#     def import_data(self):
#         # this function imports the nicely formatted data from
#         # the setup function into the class
#
#     def calculate_net_load(self):
#         # this function calculates net load
#
#     def prepare_matrices(self):
#         # this function builds the matrices
#
#     def optimize_CEP(self):
#         # this function runs LP
#
#     def calculate_bau(self):
#         # this function calculates the PV for the BAU plant
#
#     def calculate_results(self):
#         # this function calculates all the significant metrics
#         # and exports in the form we want them
#
#     def make_figures(self):
#         # this function makes the figures in matplotlib and
#         # exports them as pdfs