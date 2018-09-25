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
    def __init__(self, curr_year=2016, export_all=True):
        # Create data and tmp directories, /Users/gglazer/PycharmProjects
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        self.data_path = os.path.abspath('data')
        self.export_all = export_all
        self.acquire_ferc()
        self.setup_ferc_forecast(curr_year)
        self.setup_ferc_gross_load(curr_year)
        self.setup_renewable_8760()

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

    def setup_ferc_forecast(self, curr_year):
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

    def setup_ferc_gross_load(self, curr_year):
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

        # data frame of the gross load for all respondents in the current year
        gross_load_df = gross_load_df[gross_load_df.index.year == curr_year]

        save_pickle(gross_load_df, 'gross_load_pickle')

        return gross_load_df

    def setup_renewable_8760(self):
        # Load the renewable portfolio standard data frame for all states
        df_rps = pd.read_csv(self.data_path + '/RPS_csv.csv')
        df_rps.set_index('State', inplace=True)
        df_rps.dropna(axis=0, subset=['RPS RE%'], inplace=True)
        save_pickle(df_rps, 'rps_pickle')

        return df_rps


class CEPCase(object):
    """
    An instance of this class is a CEP case
    """
    def __init__(self,
                 name,
                 util,
                 util2,
                 state,
                 region,
                 capacity,
                 current_year,
                 forecast_year,
                 more_args,
                 export_all=False,
                 load_old=True):
        self.data_path = os.path.abspath('data')
        self.name = name
        self.util = util
        self.util2 = util2
        self.state = state
        self.region = region
        self.capacity = capacity
        self.current_year = current_year
        self.forecast_year = forecast_year
        self.export_all = export_all
        # self.load_old = load_old
        # self.more_args = more_args
        [dfpp, demand_forecast, gross_load_df, df_norm_renewable_cap, df_rps] = self.import_data(region)
        [cagr, current_8760, norm_wind_8760, norm_solar_8760, current_wind_8760, current_solar_8760, current_wind_df,
         current_solar_df, dfpp_resp] = \
            self.prepare_case_data(dfpp, demand_forecast, gross_load_df, df_norm_renewable_cap, respondent_id=util,
                                   respondent_id_backup=util2)
        net_load_sorted = self.calculate_hourly_net_load(forecast_year, current_year, state, util, cagr,
                                                         norm_wind_8760,norm_solar_8760, current_wind_8760,
                                                         current_solar_8760, current_wind_df, current_solar_df,
                                                         current_8760, dfpp_resp, df_rps)
        # lots more stuff and settings

        # this allows us to not have to load all the individual data sources again
        # more an example than a neccesity, this code might be better somewhere else
        # if self.load_old:
        #     try:
        #         self.case_data = load_pickle('data/'+str(self.name))
        #     except FileNotFoundError:
        #         self.case_data = self.import_data()
        #         save_pickle(self.case_data, 'data/'+str(self.name))
        # else:
        #     self.case_data = self.import_data()
        #     save_pickle(self.case_data, 'data/' + str(self.name))
        # # putting this function up here under __init__ means it runs when
        # # an instance of the class is created
        # self.prepare_matrices()

    def import_data(self, region):
        # this function imports the nicely formatted data from
        # the setup function into the class

        dfpp = load_pickle(self.data_path + 'pppickle')
        dfpp.reset_index(inplace=True)
        demand_forecast = load_pickle(self.data_path + 'demand_forecast_pickle')
        gross_load_df = load_pickle(self.data_path + 'gross_load_pickle')
        df_rps = load_pickle(self.data_path + 'rps_pickle')

        # Load the renewable energy normalized 8760s from Reinventing Fire
        df_norm_renewable_cap = pd.read_excel(self.data_path + '/Region_Data.xlsm', sheet_name=region, usecols='A,Y:AA')
        df_norm_renewable_cap.drop(labels=[0, 1, 2, 3], inplace=True)
        df_norm_renewable_cap['Date'] = pd.to_datetime(df_norm_renewable_cap['Date'])
        df_norm_renewable_cap['Month'] = df_norm_renewable_cap['Date'].dt.month
        df_norm_renewable_cap['Day'] = df_norm_renewable_cap['Date'].dt.day
        df_norm_renewable_cap['Hour'] = df_norm_renewable_cap['Date'].dt.hour
        # df_norm_renewable_cap['Date'] = datetime.datetime.strptime(df_norm_renewable_cap['Date'], '%m-%d %h')
        # df_norm_renewable_cap['MM-DD'] = df_norm_renewable_cap['Date'].datetime.to_period('H')
        df_norm_renewable_cap.set_index(['Month', 'Day', 'Hour'], inplace=True)
        df_norm_renewable_cap['Solar'] = df_norm_renewable_cap[['Solar Fixed', 'Solar 1 Axis']].mean(axis=1)
        df_norm_renewable_cap.drop(columns=['Solar Fixed', 'Solar 1 Axis'], inplace=True)
        df_norm_renewable_cap.dropna(inplace=True)

        return dfpp, demand_forecast, gross_load_df, df_norm_renewable_cap, df_rps

    def prepare_case_data(self, dfpp, demand_forecast, gross_load_df, df_norm_renewable_cap, respondent_id, respondent_id_backup):
        # this function sets up necessary datasets in order to calculate net load for the chosen entity

        # power plant data frame for respondent
        try:
            dfpp_resp = dfpp.loc[(dfpp['Respondent Id'] == respondent_id) & (dfpp['Nameplate Capacity (MW)'] > 0)]
        except KeyError:
            print('Key Error: Respondent not in power plant data frame. Backup ID used.')
            dfpp_resp = dfpp.loc[
                (dfpp['Respondent Id'] == respondent_id_backup) & (dfpp['Nameplate Capacity (MW)'] > 0)]
        del dfpp

        # gross load data frame for respondent
        try:
            current_8760 = gross_load_df[[str(respondent_id)]]
        except KeyError:
            print('Key Error: Respondent not in gross load data frame. Backup ID used.')
            current_8760 = gross_load_df[[str(respondent_id_backup)]]

        # Reset FERC 8760 index to match the normalized renewables 8760s
        current_8760.reset_index(inplace=True)
        current_8760['Date'] = pd.to_datetime(current_8760['index'])
        current_8760['Month'] = current_8760['Date'].dt.month
        current_8760['Day'] = current_8760['Date'].dt.day
        current_8760['Hour'] = current_8760['Date'].dt.hour
        current_8760.set_index(['Month', 'Day', 'Hour'], inplace=True)
        current_8760.dropna(inplace=True)
        print('current 8760 raw input')
        print(current_8760)

        # Load growth for respondent from demand forecast
        cagr = max(0, demand_forecast.loc[respondent_id, 'load_growth'])

        # Normalized 8760s for renewable energy sources
        norm_wind_8760 = df_norm_renewable_cap[['Wind']]
        print('normalized re df: ')
        print(df_norm_renewable_cap)
        norm_solar_8760 = df_norm_renewable_cap[['Solar']]
        print('normalized solar df: ')
        print(norm_solar_8760)

        # Current wind energy 8760 from nameplate capacity and normalized 8760
        current_wind_df = dfpp_resp.loc[(dfpp_resp['Plant Type'] == 'WND')]
        current_wind_cap = current_wind_df['Nameplate Capacity (MW)'].sum()
        print('current wind capacity is ' + str(current_wind_cap))
        current_wind_8760 = norm_wind_8760 * current_wind_cap

        # Current solar energy 8760 from nameplate capacity and normalized 8760
        current_solar_df = dfpp_resp.loc[(dfpp_resp['Plant Type'] == 'SUN')]
        current_solar_cap = current_solar_df['Nameplate Capacity (MW)'].sum()
        print('current solar capacity is ' + str(current_solar_cap))
        current_solar_8760 = norm_solar_8760 * current_solar_cap

        print('Case data prepared! ' + str(datetime.datetime.now().time()))

        return cagr, current_8760, norm_wind_8760, norm_solar_8760, current_wind_8760, current_solar_8760, \
               current_wind_df, current_solar_df, dfpp_resp

    def calculate_hourly_net_load(self, forecast_year, curr_year, state, respondent_id, cagr, norm_wind_8760,
                                  norm_solar_8760, current_wind_8760, current_solar_8760, current_wind_df,
                                  current_solar_df, current_8760, dfpp_resp, df_rps):
        # This function calculates the top 50 hours of net load for the chosen entity
        regional_wind_yearly_cfh = norm_wind_8760['Wind'].sum()
        regional_solar_yearly_cfh = norm_solar_8760['Solar'].sum()
        print('normalized 8760 total annual solar energy (yearly solar CFH) is ' + str(regional_solar_yearly_cfh))

        # Current wind, solar, and total energy generation from EIA 923
        wind_energy_generation = current_wind_df['Annual Energy'].sum()
        solar_energy_generation = current_solar_df['Annual Energy'].sum()
        total_energy_generation = dfpp_resp['Annual Energy'].sum()

        wind_re_frac_curr = wind_energy_generation / (wind_energy_generation + solar_energy_generation)
        solar_re_frac_curr = solar_energy_generation / (wind_energy_generation + solar_energy_generation)
        re_frac_curr = (wind_energy_generation + solar_energy_generation) / total_energy_generation
        print('wind renewable frac curr = ' + str(wind_re_frac_curr))
        print('solar renewable frac curr = ' + str(solar_re_frac_curr))
        print('current renewable fraction = ' + str(re_frac_curr))
        [re_frac_rps, rps_year] = get_rps(df_rps, state)
        print('rps requirement = ' + str(re_frac_rps))

        growth_factor = pow((1 + cagr), (forecast_year - curr_year))
        future_8760 = current_8760[[str(respondent_id)]] * growth_factor

        # Why are these different?
        # going to use FERC numbers for now because we are using the future 8760 to build out net load 8760
        future_gen = future_8760[str(respondent_id)].sum()
        future_gen2 = total_energy_generation * growth_factor

        print('future_gen (FERC 714) is ' + str(future_gen))
        print('future gen 2 (EIA) is ' + str(future_gen2))
        print('EIA is ' + str((future_gen2 - future_gen) / future_gen2 * 100) + '% larger than FERC')

        if np.isnan(re_frac_rps):
            re_frac_fut = re_frac_curr
        elif re_frac_curr >= re_frac_rps:
            re_frac_fut = re_frac_curr
        else:
            re_frac_fut = re_frac_rps

        re_fut_gen = re_frac_fut * future_gen
        fut_wind_gen = wind_re_frac_curr * re_fut_gen
        fut_solar_gen = solar_re_frac_curr * re_fut_gen

        fut_wind_cap = fut_wind_gen / regional_wind_yearly_cfh
        print('total future wind capacity is ' + str(fut_wind_cap))
        fut_solar_cap = fut_solar_gen / regional_solar_yearly_cfh
        print('total future solar capacity is ' + str(fut_solar_cap))

        # For states with no projected renewable growth from RPS and no growth rate, assume renewable CFs match
        # historic, not the normalized 8760s from Reinventing Fire
        if re_frac_fut == re_frac_curr & cagr == 0:
            fut_wind_8760 = current_wind_8760
            fut_solar_8760 = current_solar_8760
        else:
            fut_wind_8760 = norm_wind_8760[['Wind']] * fut_wind_cap
            fut_solar_8760 = norm_solar_8760[['Solar']] * fut_solar_cap

        fut_re_8760 = pd.DataFrame(data=None, index=fut_wind_8760.index, columns=[str(respondent_id)])
        fut_re_8760[str(respondent_id)] = fut_wind_8760['Wind'] + fut_solar_8760['Solar']

        fut_net_load_8760 = future_8760[[str(respondent_id)]] - fut_re_8760[[str(respondent_id)]]
        fut_net_load_8760.dropna(inplace=True)
        net_load_sorted = fut_net_load_8760.sort_values(by=str(respondent_id), ascending=False)
        net_load_sorted.reset_index(inplace=True)

        return net_load_sorted



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