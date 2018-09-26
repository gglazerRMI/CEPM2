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

def unzip(zip_file_path, directory_to_extract_to):
    # function to unzip a file
    with zipfile.ZipFile(zip_file_path, "r") as z:
        z.extractall(directory_to_extract_to)

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
        if not os.path.exists('data/pickles'):
            os.makedirs('data/pickles')
        if not os.path.exists('data/results'):
            os.makedirs('data/results')
        self.data_path = os.path.abspath('data')
        self.export_all = export_all
        self.acquire_ferc()
        self.setup_ferc_forecast(curr_year)
        self.setup_ferc_gross_load(curr_year)
        self.setup_rps()

    print('Setting up hourly net load data ', str(datetime.datetime.now().time()))

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
        print('Setting up demand forecast ', str(datetime.datetime.now().time()))
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
            demand_forecast.to_csv(self.data_path + '/results/demand_forecast.csv')
        save_pickle(demand_forecast, self.data_path + '/pickles/demand_forecast_pickle')
        return demand_forecast

    def setup_ferc_gross_load(self, curr_year):
        # -------------TO FIX------------ #
        # need to grow the load by the growth rate in demand_forecast 'load_growth' to the forecast year
        # this code loads FERC 714 demand data and produces a dataframe
        # of hourly load data, columns for each respondent

        print('Setting up gross load ', str(datetime.datetime.now().time()))

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
            gross_load_df.to_csv(self.data_path + '/results/gross_load.csv')
            max_load.to_csv(self.data_path + '/results/max_load.csv')

        # data frame of the gross load for all respondents in the current year
        gross_load_df = gross_load_df[gross_load_df.index.year == curr_year]

        save_pickle(gross_load_df, self.data_path + '/pickles/gross_load_pickle')

        return gross_load_df

    def setup_rps(self):
        # Load the renewable portfolio standard data frame for all states

        # ------TO FIX------- #
        # add for Texas

        print('Setting up RPS ', str(datetime.datetime.now().time()))

        df_rps = pd.read_csv(self.data_path + '/RPS_csv.csv')
        df_rps.set_index('State', inplace=True)
        df_rps.dropna(axis=0, subset=['RPS RE%'], inplace=True)
        save_pickle(df_rps, self.data_path + '/pickles/rps_pickle')

        return df_rps


class CEPCase(object):
    """
    An instance of this class is a CEP case
    """
    def __init__(self,
                 # name,
                 util,
                 util2,
                 state,
                 region,
                 # capacity,
                 current_year,
                 forecast_year,
                 # more_args,
                 export_all=False,
                 # load_old=True
                 save_results=True
                 ):
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        if not os.path.exists('data/pickles'):
            os.makedirs('data/pickles')
        if not os.path.exists('data/results'):
            os.makedirs('data/results')
        self.data_path = os.path.abspath('data')
        self.util = util
        self.util2 = util2
        self.state = state
        self.region = region
        self.current_year = current_year
        self.forecast_year = forecast_year
        self.export_all = export_all
        [dfpp, demand_forecast, gross_load_df, df_rps] = self.import_general_data()
        [norm_wind_8760, norm_solar_8760] = self.import_regional_data(region)
        [current_8760, current_wind_8760, current_solar_8760, current_wind_df, current_solar_df, dfpp_resp, re_frac_curr
            , wind_re_frac_curr, solar_re_frac_curr] = \
            self.prepare_current_case_data(dfpp, gross_load_df, norm_wind_8760, norm_solar_8760, util, util2)
        [fut_re_8760, future_8760] = self.prepare_future_case_data(demand_forecast, forecast_year, current_year, current_8760, util, df_rps,
                                 state, re_frac_curr, wind_re_frac_curr, solar_re_frac_curr, norm_wind_8760,
                                 norm_solar_8760, current_wind_8760, current_solar_8760)
        net_load_sorted = self.calculate_hourly_net_load(util, fut_re_8760, future_8760, save_results)


    print('Running case ', str(datetime.datetime.now().time()))

    def import_general_data(self):
        # this function imports the nicely formatted data from the setup function into the class
        print('importing case data ', str(datetime.datetime.now().time()))

        dfpp = load_pickle(self.data_path + '/pickles/pppickle')
        dfpp.reset_index(inplace=True)
        demand_forecast = load_pickle(self.data_path + '/pickles/demand_forecast_pickle')
        gross_load_df = load_pickle(self.data_path + '/pickles/gross_load_pickle')
        df_rps = load_pickle(self.data_path + '/pickles/rps_pickle')

        return dfpp, demand_forecast, gross_load_df, df_rps

    def import_regional_data(self, region):
        # Load the renewable energy normalized 8760s from Reinventing Fire
        df_norm_renewable_cap = pd.read_excel(self.data_path + '/Region_Data.xlsm', sheet_name=region, usecols='A,Y:AA')
        df_norm_renewable_cap.drop(labels=[0, 1, 2, 3], inplace=True)
        df_norm_renewable_cap['Date'] = pd.to_datetime(df_norm_renewable_cap['Date'])
        df_norm_renewable_cap['Month'] = df_norm_renewable_cap['Date'].dt.month
        df_norm_renewable_cap['Day'] = df_norm_renewable_cap['Date'].dt.day
        df_norm_renewable_cap['Hour'] = df_norm_renewable_cap['Date'].dt.hour
        df_norm_renewable_cap.set_index(['Month', 'Day', 'Hour'], inplace=True)
        df_norm_renewable_cap['Solar'] = df_norm_renewable_cap[['Solar Fixed', 'Solar 1 Axis']].mean(axis=1)
        df_norm_renewable_cap.drop(columns=['Solar Fixed', 'Solar 1 Axis'], inplace=True)
        df_norm_renewable_cap.dropna(inplace=True)

        # Normalized 8760s for renewable energy sources from Reinventing Fire
        norm_wind_8760 = df_norm_renewable_cap[['Wind']]
        norm_solar_8760 = df_norm_renewable_cap[['Solar']]

        return norm_wind_8760, norm_solar_8760

    def prepare_current_case_data(self, dfpp, gross_load_df, norm_wind_8760, norm_solar_8760, respondent_id,
                                  respondent_id_backup):
        # this function sets up necessary datasets in order to calculate net load for the chosen entity

        print('preparing case data ', str(datetime.datetime.now().time()))

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
        del gross_load_df

        # Reset FERC 8760 index to match the normalized renewables 8760s
        current_8760.reset_index(inplace=True)
        current_8760['Date'] = pd.to_datetime(current_8760['index'])
        current_8760['Month'] = current_8760['Date'].dt.month
        current_8760['Day'] = current_8760['Date'].dt.day
        current_8760['Hour'] = current_8760['Date'].dt.hour
        current_8760.set_index(['Month', 'Day', 'Hour'], inplace=True)
        current_8760.dropna(inplace=True)

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

        # Current wind, solar, and total energy generation from EIA 923
        wind_energy_generation = current_wind_df['Annual Energy'].sum()
        solar_energy_generation = current_solar_df['Annual Energy'].sum()
        total_energy_generation = dfpp_resp['Annual Energy'].sum()

        wind_re_frac_curr = wind_energy_generation / (wind_energy_generation + solar_energy_generation)
        solar_re_frac_curr = solar_energy_generation / (wind_energy_generation + solar_energy_generation)
        re_frac_curr = (wind_energy_generation + solar_energy_generation) / total_energy_generation
        print('current wind renewable fraction = ' + str(wind_re_frac_curr))
        print('current solar renewable fraction = ' + str(solar_re_frac_curr))
        print('current total renewable fraction = ' + str(re_frac_curr))

        print('Current case data prepared! ' + str(datetime.datetime.now().time()))

        return current_8760, current_wind_8760, current_solar_8760, current_wind_df, current_solar_df, dfpp_resp, \
               re_frac_curr, wind_re_frac_curr, solar_re_frac_curr

    def prepare_future_case_data(self, demand_forecast, forecast_year, curr_year, current_8760, respondent_id, df_rps,
                                 state, re_frac_curr, wind_re_frac_curr, solar_re_frac_curr, norm_wind_8760,
                                 norm_solar_8760, current_wind_8760, current_solar_8760):
        # Load growth for respondent from demand forecast
        cagr = max(0.0, demand_forecast.loc[respondent_id, 'load_growth'])
        growth_factor = pow((1 + cagr), (forecast_year - curr_year))
        future_8760 = current_8760[[str(respondent_id)]] * growth_factor
        future_gen = future_8760[str(respondent_id)].sum()
        print('future annual energy generation (MWh) = ' + str(future_gen))

        # Future renewable generation for respondent
        [re_frac_rps, rps_year] = get_rps(df_rps, state)
        print('rps requirement = ' + str(re_frac_rps))

        if np.isnan(re_frac_rps):
            re_frac_fut = re_frac_curr
        elif re_frac_curr >= re_frac_rps:
            re_frac_fut = re_frac_curr
        else:
            re_frac_fut = re_frac_rps

        re_fut_gen = re_frac_fut * future_gen
        fut_wind_gen = wind_re_frac_curr * re_fut_gen
        fut_solar_gen = solar_re_frac_curr * re_fut_gen

        # Average capacity factor x 8760 hours for region, used in later calculations
        regional_wind_yearly_cfh = norm_wind_8760['Wind'].sum()
        regional_solar_yearly_cfh = norm_solar_8760['Solar'].sum()

        fut_wind_cap = fut_wind_gen / regional_wind_yearly_cfh
        print('total future wind capacity is ' + str(fut_wind_cap))
        fut_solar_cap = fut_solar_gen / regional_solar_yearly_cfh
        print('total future solar capacity is ' + str(fut_solar_cap))

        # For states with no projected renewable growth from RPS and no growth rate, assume renewable CFs match
        # historic, not the normalized 8760s from Reinventing Fire
        if re_frac_fut == re_frac_curr:
            if cagr == 0.0:
                fut_wind_8760 = current_wind_8760
                fut_solar_8760 = current_solar_8760
        else:
            fut_wind_8760 = norm_wind_8760[['Wind']] * fut_wind_cap
            fut_solar_8760 = norm_solar_8760[['Solar']] * fut_solar_cap

        fut_re_8760 = pd.DataFrame(data=None, index=fut_wind_8760.index, columns=[str(respondent_id)])
        fut_re_8760[str(respondent_id)] = fut_wind_8760['Wind'] + fut_solar_8760['Solar']

        return fut_re_8760, future_8760

    def calculate_hourly_net_load(self, respondent_id, fut_re_8760, future_8760, save_results):
        # This function calculates the top 50 hours of net load for the chosen entity

        print('Calculating hourly net load ', str(datetime.datetime.now().time()))

        fut_net_load_8760 = future_8760[[str(respondent_id)]] - fut_re_8760[[str(respondent_id)]]
        fut_net_load_8760.dropna(inplace=True)

        save_pickle(fut_net_load_8760, self.data_path + '/future_net_8760_pickle')

        # Calculate hour of max load added
        df_8760f = fut_net_load_8760[[str(respondent_id)]]
        print('8760f:')
        print(df_8760f)
        df_8760f.reset_index(inplace=True)
        df_delta = pd.DataFrame(data=None, index=df_8760f.index)

        df_8760first = df_8760f.drop(df_8760f.index[-1]).reset_index()
        df_8760last = df_8760f.drop(df_8760f.index[0]).reset_index()

        df_delta[str(respondent_id)] = df_8760last[str(respondent_id)] - df_8760first[str(respondent_id)]
        df_delta[str(respondent_id)] = df_delta[str(respondent_id)].shift(1)

        df_delta[str(respondent_id)].loc[df_delta.index[0]] = df_8760f[str(respondent_id)].loc[df_8760f.index[0]] - \
                                                              df_8760f[str(respondent_id)].loc[df_8760f.index[-1]]

        df_delta.sort_values(by=str(respondent_id), ascending=False, inplace=True)
        print('delta:')

        print(df_delta)
        max_hour = df_delta.loc[[df_delta.index[0]]]
        print('max hr:')

        print(max_hour)

        net_load_sorted = fut_net_load_8760.sort_values(by=str(respondent_id), ascending=False)
        net_load_sorted.reset_index(inplace=True)

        save_pickle(net_load_sorted, self.data_path + '/pickles/net_load_sorted')
        save_pickle(max_hour, self.data_path + '/pickles/max_hour')

        print('net load sorted:')

        print(net_load_sorted)

        if save_results:
            net_load_sorted.to_csv(self.data_path + '/results/net_load_sorted.csv')
            max_hour.to_csv(self.data_path + '/results/max_hour.csv')

        return net_load_sorted



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