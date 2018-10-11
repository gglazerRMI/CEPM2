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


def divide(dividend, divisor, err=0.0):
    try:
        quotient = dividend / divisor
    except Warning:
        quotient = err
    if np.isnan(quotient):
        quotient = 0.0
    return quotient


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


class CEPCase(object):
    """
    An instance of this class is a CEP case
    """
    def __init__(self,
                 # name,
                 util,
                 state,
                 region,
                 capacity,
                 type,
                 current_year,
                 forecast_year,
                 # more_args,
                 util2=102,
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
        # Parameters
        self.data_path = os.path.abspath('data')
        self.util = util
        self.util2 = util2
        self.plant_type = type
        self.state = state
        self.region = region
        self.capacity = capacity
        self.curr_year = current_year
        self.fut_year = forecast_year
        # Settings
        self.export_all = export_all
        self.save_results = save_results
        self.min_size = 100
        self.top_hours = 50
        # Data Structures
        self.dfpp = pd.DataFrame()
        self.current_8760 = pd.DataFrame()
        self.demand_forecast = pd.DataFrame()
        # do i use this?
        self.df_rps = pd.DataFrame()
        self.df_norm_renewable_cap = pd.DataFrame()
        self.re_8760 = pd.DataFrame()
        # Attributes
        self.current_re_fraction = 0.0
        self.cagr = 0.0
        self.rps_frac = 0.0
        self.rps_year = 0.0
        # Methods
        self.load_data()
        self.calculate_net_load()
        # self.calculate_monthly_energy()
        # [dfpp, demand_forecast, gross_load_df, df_rps] = self.import_general_data()
        # [norm_wind_8760, norm_solar_8760] = self.import_regional_data(region)
        # [current_8760, current_wind_8760, current_solar_8760, current_wind_df, current_solar_df, dfpp_resp, re_frac_curr
        #     , wind_re_frac_curr, solar_re_frac_curr] = \
        #     self.prepare_current_case_data(dfpp, gross_load_df, norm_wind_8760, norm_solar_8760, util, util2)
        # [fut_re_8760, future_8760] = self.prepare_future_case_data(demand_forecast, forecast_year, current_year, current_8760, util, df_rps,
        #                          state, re_frac_curr, wind_re_frac_curr, solar_re_frac_curr, norm_wind_8760,
        #                          norm_solar_8760, current_wind_8760, current_solar_8760)
        # net_load_sorted = self.calculate_hourly_net_load(util, fut_re_8760, future_8760, save_results)

    def load_data(self):
        print('loading data')

        # Load and save power plant data frame for case
        dfpp = load_pickle(self.data_path + '/pickles/pppickle')
        dfpp.reset_index(inplace=True)
        # power plant data frame for respondent
        try:
            dfpp_resp = dfpp.loc[(dfpp['Respondent Id'] == self.util) & (dfpp['Nameplate Capacity (MW)'] > 0)]
        except KeyError:
            print('Key Error: Respondent not in power plant data frame. Backup ID used.')
            dfpp_resp = dfpp.loc[(dfpp['Respondent Id'] == self.util2) & (dfpp['Nameplate Capacity (MW)'] > 0)]
        del dfpp
        self.dfpp = dfpp_resp

        # Load gross_load_df to save the most current FERC 714 8760 for case
        gross_load_df = load_pickle(self.data_path + '/pickles/gross_load_pickle')
        try:
            util_8760 = gross_load_df[[str(self.util)]]
        except KeyError:
            print('Key Error: Respondent not in FERC gross load data frame. Backup ID used.')
            util_8760 = gross_load_df[[str(self.util2)]]
        del gross_load_df
        util_8760 = util_8760.dropna()
        max_year = max(util_8760.index.year)
        # Change the 'current year' to match the FERC data
        ### SPECIFY FERC OR EIA YEAR (see Alex's code) ###
        self.curr_year = max_year
        current_8760 = util_8760.loc[util_8760.index.year == max_year]
        if int(max_year) % 4 == 0:
            # current_8760.drop(current_8760.loc[(current_8760.index.month == 2) & (current_8760.index.day == 29)], inplace=True)
            # current_8760 = current_8760.drop(current_8760.index[pd.to_datetime((str(max_year)+'-02-29 00:00:00')):pd.to_datetime((str(max_year)+'-02-29 23:00:00'))])
            current_8760 = current_8760.drop(current_8760.index[1416:1440])
        # Reset index for eventual merge with re_8760s
        current_8760.reset_index(inplace=True)
        print(current_8760.head())
        current_8760['Month'] = current_8760['index'].dt.month.apply('{:0>2}'.format)
        current_8760['Day'] = current_8760['index'].dt.day.apply('{:0>2}'.format)
        current_8760['Hour'] = current_8760['index'].dt.hour.apply('{:0>2}'.format)
        current_8760.set_index(['Month', 'Day', 'Hour'], inplace=True)
        print(current_8760.head())
        current_8760.dropna(inplace=True)
        self.current_8760 = current_8760

        # Load demand_forecast data frame for use in calculating load growth
        self.demand_forecast = load_pickle(self.data_path + '/pickles/demand_forecast_pickle')
        try:
            self.cagr = max(0.0, self.demand_forecast.loc[self.util, 'load_growth'])
        except KeyError:
            print('Key Error: Respondent not in FERC demand forecast data frame. Backup ID used.')
            self.cagr = max(0.0, self.demand_forecast.loc[self.util2, 'load_growth'])


        # ------- SEE ALEX'S CODE FOR THESE ------ #
        # Load RPS data to save RPS fraction and year
        df_rps = load_pickle(self.data_path + '/pickles/rps_pickle')
        [rps_frac, rps_year] = get_rps(df_rps, self.state)
        self.rps_frac = rps_frac
        self.rps_year = rps_year

        # Load and save normalized renewable energy 8760s
        re_8760 = load_pickle(self.data_path + '/pickles/' + str(self.region) + '_pickle')
        re_8760['Date'] = pd.to_datetime(re_8760['Time'])
        re_8760['Month'] = re_8760['Date'].dt.month.apply('{:0>2}'.format)
        re_8760['Day'] = re_8760['Date'].dt.day.apply('{:0>2}'.format)
        re_8760['Hour'] = re_8760['Date'].dt.hour.apply('{:0>2}'.format)
        re_8760.set_index(['Month', 'Day', 'Hour'], inplace=True)
        re_8760['Solar'] = re_8760[['Solar_Fixed', 'Solar_Tracking']].mean(axis=1)
        re_8760.drop(columns=['Solar_Fixed', 'Solar_Tracking'], inplace=True)
        re_8760.dropna(inplace=True)
        self.re_8760 = re_8760

        # Load and save end-use maxima 8760s

        # return self

    print('Running case ', str(datetime.datetime.now().time()))

    def calculate_net_load(self):

        print('preparing case data ', str(datetime.datetime.now().time()))
        # Access the power plant data frame for the respondent
        dfpp_resp = self.dfpp

        # Import FERC 714 8760 data
        current_8760 = self.current_8760

        # Current wind energy 8760 from nameplate capacity and normalized 8760
        current_wind_df = dfpp_resp.loc[(dfpp_resp['Plant Type'] == 'WND')]
        current_wind_cap = current_wind_df['Nameplate Capacity (MW)'].sum()
        print('current wind capacity is ' + str(current_wind_cap))
        current_wind_8760 = self.re_8760['Wind'] * current_wind_cap

        # Current solar energy 8760 from nameplate capacity and normalized 8760
        current_solar_df = dfpp_resp.loc[(dfpp_resp['Plant Type'] == 'SUN')]
        current_solar_cap = current_solar_df['Nameplate Capacity (MW)'].sum()
        print('current solar capacity is ' + str(current_solar_cap))
        current_solar_8760 = self.re_8760['Solar'] * current_solar_cap

        # Current wind, solar, and total energy generation from EIA 923
        wind_energy_generation = current_wind_df['Annual Energy'].sum()
        print('eia wind energy generation is ' + str(wind_energy_generation))
        solar_energy_generation = current_solar_df['Annual Energy'].sum()
        print('eia solar energy generation is ' + str(solar_energy_generation))
        total_energy_generation = dfpp_resp['Annual Energy'].sum()
        print('eia total energy generation is ' + str(total_energy_generation))

        wind_re_frac_curr = divide(wind_energy_generation, (wind_energy_generation + solar_energy_generation))
        solar_re_frac_curr = divide(solar_energy_generation, (wind_energy_generation + solar_energy_generation))
        re_frac_curr = divide((wind_energy_generation + solar_energy_generation), total_energy_generation)
        print('current wind renewable fraction = ' + str(wind_re_frac_curr))
        print('current solar renewable fraction = ' + str(solar_re_frac_curr))
        print('current total renewable fraction = ' + str(re_frac_curr))

        print('Current case data prepared! ' + str(datetime.datetime.now().time()))

        # Load growth for respondent from demand forecast
        cagr = max(self.cagr, self.demand_forecast.loc[self.util, 'load_growth'])
        self.cagr = cagr
        # Note here that 'curr_year' is set by the latest available FERC 714 data for the case
        growth_factor = pow((1 + cagr), (self.fut_year - self.curr_year))
        print('growth factor is ' + str(growth_factor) + ' of type ' + str(type(growth_factor)))
        future_8760 = current_8760[[str(self.util)]] * growth_factor
        future_gen = future_8760[str(self.util)].sum()
        print('future annual energy generation (MWh) = ' + str(future_gen))

        # Future renewable generation for respondent
        print('rps requirement = ' + str(self.rps_frac))

        if np.isnan(self.rps_frac):
            re_frac_fut = re_frac_curr
        elif re_frac_curr >= self.rps_frac:
            re_frac_fut = re_frac_curr
        else:
            re_frac_fut = self.rps_frac

        print('future renewable energy fraction is ' + str(re_frac_fut))
        # print(future_gen)
        re_fut_gen = re_frac_fut * future_gen
        fut_wind_gen = wind_re_frac_curr * re_fut_gen
        fut_solar_gen = solar_re_frac_curr * re_fut_gen

        # Average capacity factor x 8760 hours for region, used in later calculations
        regional_wind_yearly_cfh = self.re_8760['Wind'].sum()
        regional_solar_yearly_cfh = self.re_8760['Solar'].sum()

        fut_wind_cap = fut_wind_gen / regional_wind_yearly_cfh
        print('total future wind capacity is ' + str(fut_wind_cap))
        fut_solar_cap = fut_solar_gen / regional_solar_yearly_cfh
        print('total future solar capacity is ' + str(fut_solar_cap))

        # For states with no projected renewable growth from RPS and no growth rate, assume renewable CFs match
        # historic, not the normalized 8760s from Reinventing Fire
        if (re_frac_fut == re_frac_curr) & (cagr == 0.0):
                fut_wind_8760 = current_wind_8760
                fut_solar_8760 = current_solar_8760
                print('No renewables growth')
        else:
            fut_wind_8760 = self.re_8760[['Wind']] * fut_wind_cap
            fut_solar_8760 = self.re_8760[['Solar']] * fut_solar_cap
            print('Renewables growth')

        fut_re_8760 = pd.DataFrame(data=None, index=future_8760.index, columns=[str(self.util)])
        fut_re_8760[str(self.util)] = fut_wind_8760['Wind'] + fut_solar_8760['Solar']

        # This section calculates the top 50 hours of net load for the chosen entity
        print('Calculating hourly net load ', str(datetime.datetime.now().time()))
        # fut_net_load_8760 = pd.DataFrame(data=None, index=future_8760.index, columns=['Net Load', 'Delta'])
        fut_net_load_8760 = future_8760[[str(self.util)]] - fut_re_8760[[str(self.util)]]
        fut_net_load_8760.rename(columns={str(self.util): 'Net Load'}, inplace=True)
        fut_net_load_8760.dropna(inplace=True)
        fut_net_load_8760.reset_index(inplace=True)
        # Create a new 8760 where each index represents the current hour for all hours of the 365-day year
        # Each value is the change in net load (MW) from the previous hour to the current hour
        df_delta = pd.DataFrame(data=None, index=fut_net_load_8760.index, columns=['Delta'])
        df_8760first = fut_net_load_8760[['Net Load']].drop(fut_net_load_8760.index[-1]).reset_index()
        df_8760last = fut_net_load_8760[['Net Load']].drop(fut_net_load_8760.index[0]).reset_index()
        df_delta['Delta'] = df_8760last['Net Load'] - df_8760first['Net Load']
        df_delta['Delta'] = df_delta['Delta'].shift(1)
        # Perform the same for the 8760th - 1st hour
        df_delta['Delta'].loc[df_delta.index[0]] = fut_net_load_8760['Net Load'].loc[fut_net_load_8760.index[0]] - \
                                                    fut_net_load_8760['Net Load'].loc[fut_net_load_8760.index[-1]]
        fut_net_load_8760['Delta'] = df_delta['Delta']
        fut_net_load_8760['Datetime'] = pd.date_range(start=('1/1/'+str(self.fut_year)), periods=8760, freq='H')
        # print(fut_net_load_8760)
        save_pickle(fut_net_load_8760, self.data_path + '/future_net_8760_pickle')

        if self.save_results:
            fut_net_load_8760.to_csv(self.data_path + '/results/future_net_load.csv')

        # return self



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

    def calculate_monthly_energy(self):
        """This method calculates the monthly energy requirements given a nameplate capacity and FERC Respondent ID.
        The monthly energy capacity factor hours for all plant types operated by a FERC Respondent ID are averaged and
        multiplied by the nameplate capacity.
        Only plants larger than 100 MW are included in the averaging."""

        print('Calculating monthly energy constraint ', str(datetime.datetime.now().time()))

        cols = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Annual Energy',
                'JanCFH', 'FebCFH', 'MarCFH', 'AprCFH', 'MayCFH', 'JunCFH', 'JulCFH', 'AugCFH', 'SepCFH', 'OctCF',
                'NovCF', 'DecCF', 'YearCFH']
        monthly_cfh = ['JanCFH', 'FebCFH', 'MarCFH', 'AprCFH', 'MayCFH', 'JunCFH', 'JulCFH', 'AugCFH', 'SepCFH', 'OctCF'
                       , 'NovCF', 'DecCF']

        # load power plant data frame
        dfpp = load_pickle(self.data_path + '/pickles/pppickle')

        # reset index to enable lookup by respondent ID and plant type
        dfpp.reset_index(inplace=True)
        # select only entries corresponding to the correct plant type within a service area
        df_selected_plants = dfpp.loc[(dfpp['Respondent Id'] == self.util) & (dfpp['Plant Type'] == self.plant_type)
                                      & (dfpp['Nameplate Capacity (MW)'] > self.min_size)]
        # Calculate average monthly capacity factor hours
        df_average_plant = df_selected_plants.groupby(['Respondent Id', 'Plant Type'])[cols].agg('mean')

        # Calculate MWh needed for each month
        monthly_energy = df_average_plant[monthly_cfh] * self.capacity
        monthly_energy.rename(columns={'JanCFH': 'Jan_MWh', 'FebCFH': 'Feb_MWh', 'MarCFH': 'Mar_MWh',
                                         'AprCFH': 'Apr_MWh', 'MayCFH': 'May_MWh', 'JunCFH': 'JunMWh',
                                         'JulCFH': 'JulMWh', 'AugCFH': 'Aug_MWh', 'SepCFH': 'Sep_MWh',
                                         'OctCF': 'Oct_MWh', 'NovCF': 'Nov_MWh', 'DecCF': 'Dec_MWh'}, inplace=True)
        monthly_energy['Nameplate Capacity (MW)'] = self.capacity

        save_pickle(monthly_energy, self.data_path + '/pickles/monthly_energy')

        if self.save_results:
            monthly_energy.to_csv(self.data_path + '/results/monthly_energy.csv')

        if self.export_all:
            # Write eligible plants to Excel file
            writer = pd.ExcelWriter('selected_plants_df.xlsx', engine='xlsxwriter')
            df_selected_plants.to_excel(writer)
            writer.save()

            # Write plant averages to Excel file
            writer = pd.ExcelWriter('plant_averages_df.xlsx', engine='xlsxwriter')
            df_average_plant.to_excel(writer)
            writer.save()

            # Write monthly energies to Excel file
            writer = pd.ExcelWriter('monthly_energies.xlsx', engine='xlsxwriter')
            monthly_energy.to_excel(writer)
            writer.save()

        print('Monthly Energies')
        print(monthly_energy)

        # return monthly_energy

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