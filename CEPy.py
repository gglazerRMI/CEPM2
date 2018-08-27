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
        print(resp, end=", ", flush=True)
        tdf = df.loc[resp]
        adf = tdf.as_matrix()
        nadf = np.reshape(adf, adf.size)
        ind = list(tdf.index.values)
        per = len(ind) * 24
        temp_index = pd.date_range(ind[0], periods=per, freq='H')
        tdf = pd.DataFrame(nadf, index=temp_index, columns=[str(resp)])
        if np.sum(nadf) > 0:
            try:
                all_ldf = pd.concat([all_ldf, tdf], axis=1)
            except NameError:
                all_ldf = tdf
    all_ldf = all_ldf.loc[:, (all_ldf != 0).any(axis=0)]
    all_ldf = all_ldf.dropna(axis=1, how='all')
    return all_ldf

def unzip(zip_file_path, directory_to_extract_to):
    # function to unzip a file
    with zipfile.ZipFile(zip_file_path, "r") as z:
        z.extractall(directory_to_extract_to)

class SetupData(object):
    '''
    Class for setting up all the data, this is a less than perfect use for a class
    thinking we sort of use it like a function that can share certain parts of itself
    '''
    def __init__(self, year, export_all=False):
        # Create data and tmp directories, /Users/gglazer/PycharmProjects
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        self.data_path = os.path.abspath('data')
        self.export_all = export_all
        self.acquire_ferc()
        self.ferc_forecast = self.setup_ferc_forecast(year)
        self.ferc_gross_load = self.setup_ferc_gross_load()
        # self.year = year

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

    def setup_ferc_forecast(self, year):
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
        temp_demand_forecast = temp_demand_forecast.loc[temp_demand_forecast['report_yr'] == year]
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
        for i in list(demand_forecast.index):
            first = demand_forecast.get_value(i, year + 1)
            last = demand_forecast.get_value(i, year + 10)
            try:
                growth = calc_cagr(first, last, 9, 'int')
                demand_forecast.set_value(i, 'load_growth', growth)
            except ZeroDivisionError:
                print('Error calculating demand growth for ', i, ' because initial value is zero')
        if self.export_all:
            demand_forecast.to_csv(self.data_path + '/tmp/demand_forecast.csv')
        return demand_forecast

    def setup_ferc_gross_load(self):
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
        return gross_load_df




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