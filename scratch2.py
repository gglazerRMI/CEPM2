import os.path
import shutil
import pickle
import urllib.request
import zipfile
import datetime
import numpy as np
import pandas as pd
from io import BytesIO
import os
import sys
import subprocess
import re
from urllib.request import urlopen

print('Begin script ' + str(datetime.datetime.now().time()))

# --------- Settings --------- #
pd.set_option('display.max_columns', 70)
export_all = False


# --------- Functions --------- #


def load_pickle(name):
    # function to load an object from a pickle
    with open(str(name) + '.pkl', 'rb') as f:
        temp = pickle.load(f)
    return temp


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


# --------- Parameters --------- #
# respondent_id = 171
# state = 'FL'
# region = 'South'
# cagr = .01
# forecast_year = 2020
# curr_year = 2016

# 243 - Sacramento MUD (wind), RPS
# 228 - PacifiCorp - East (wind), multistate (Utah, Wyoming, Idaho, Oregon, California, Washington -- descending)
# 209 - Nebraska Public Power District (wind), no RPS

respondent_id = 209
respondent_id_backup = 209
state = 'NE'
region = 'MidWest'
forecast_year = 2020
# Will skip leap days if leap year selected
curr_year = 2016


# # --------- Load Data (this has been ported into hourly_net_load--------- #
print('Loading data ' + str(datetime.datetime.now().time()))

# Load power plant data frame for all respondents
dfpp = load_pickle('/Users/gglazer/PyCharmProjects/CEP1/pppickle')
dfpp.reset_index(inplace=True)

# Load gross load data frame (8760 for 'current year') for all respondents
gross_load_df = load_pickle('/Users/gglazer/PyCharmProjects/CEP1/gross_load_pickle')

# Load demand forecast data frame for all respondents
demand_forecast = load_pickle('/Users/gglazer/PyCharmProjects/CEP1/demand_forecast_pickle')

# Load the renewable portfolio standard data frame for all states
df_rps = pd.read_excel('/Users/gglazer/Downloads/State RE Projection.xlsx', header=1)
df_rps.set_index('State', inplace=True)
df_rps.dropna(axis=0, subset=['RPS RE%'], inplace=True)
df_rps.drop(labels=['TX'], inplace=True)

# Load the renewable energy normalized 8760s from Reinventing Fire
df_norm_renewable_cap = pd.read_excel('/Users/gglazer/Documents/Clean Energy Portfolio/Model/Region_Data.xlsm',
                                      sheet_name=region, usecols='A,Y:AA')
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
print(df_norm_renewable_cap)

print('Data loaded! ' + str(datetime.datetime.now().time()))


# --------- Prepare Case Data --------- #
print('Preparing case data ' + str(datetime.datetime.now().time()))

# power plant data frame for respondent
try:
    dfpp_resp = dfpp.loc[(dfpp['Respondent Id'] == respondent_id) & (dfpp['Nameplate Capacity (MW)'] > 0)]
except KeyError:
    print('Key Error: Respondent not in power plant data frame. Backup ID used.')
    dfpp_resp = dfpp.loc[(dfpp['Respondent Id'] == respondent_id_backup) & (dfpp['Nameplate Capacity (MW)'] > 0)]
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

## BUILD OUT TRY EXCEPT HERE TOO (maybe do try/except earlier on)
# Load growth for respondent from demand forecast
cagr = max(0, demand_forecast.loc[respondent_id, 'load_growth'])
del demand_forecast


# Normalized 8760s for renewable energy sources
norm_wind_8760 = df_norm_renewable_cap[['Wind']]
print('normalized re df: ')
print(df_norm_renewable_cap)
regional_wind_yearly_cfh = norm_wind_8760['Wind'].sum()
print('normalized 8760 total annual wind energy (yearly wind CFH) is ' + str(regional_wind_yearly_cfh))
norm_solar_8760 = df_norm_renewable_cap[['Solar']]
print('normalized solar df: ')
print(norm_solar_8760)
regional_solar_yearly_cfh = norm_solar_8760['Solar'].sum()
print('normalized 8760 total annual solar energy (yearly solar CFH) is ' + str(regional_solar_yearly_cfh))

# Current wind energy 8760 from nameplate capacity and normalized 8760
current_wind_df = dfpp_resp.loc[(dfpp_resp['Plant Type'] == 'WND')]
current_wind_cap = current_wind_df['Nameplate Capacity (MW)'].sum()
print('current wind capacity is ' + str(current_wind_cap))
# current_wind_8760 = norm_wind_8760 * current_wind_cap

# Current solar energy 8760 from nameplate capacity and normalized 8760
current_solar_df = dfpp_resp.loc[(dfpp_resp['Plant Type'] == 'SUN')]
current_solar_cap = current_solar_df['Nameplate Capacity (MW)'].sum()
print('current solar capacity is ' + str(current_solar_cap))
# current_solar_8760 = norm_solar_8760 * current_solar_cap

# Current wind, solar, and total energy generation from EIA 923
wind_energy_generation = current_wind_df['Annual Energy'].sum()
solar_energy_generation = current_solar_df['Annual Energy'].sum()
total_energy_generation = dfpp_resp['Annual Energy'].sum()


print('Case data prepared! ' + str(datetime.datetime.now().time()))

# --------- Calculations --------- #
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
print('EIA is ' + str((future_gen2-future_gen)/future_gen2*100) + '% larger than FERC')

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

fut_wind_8760 = norm_wind_8760[['Wind']] * fut_wind_cap
fut_solar_8760 = norm_solar_8760[['Solar']] * fut_solar_cap
fut_re_8760 = pd.DataFrame(data=None, index=fut_wind_8760.index, columns=[str(respondent_id)])
print(fut_wind_8760)
print(fut_solar_8760)
fut_re_8760[str(respondent_id)] = fut_wind_8760['Wind'] + fut_solar_8760['Solar']
print('Future Renewable 8760')
print(fut_re_8760[:10])
print('Current 8760')
print(current_8760[:10])
print('Future 8760')
print(future_8760[:10])

# fut_net_load_8760 = pd.DataFrame(data=None, index=future_8760.index, columns=['Net Load', 'Date'])
# hour_list = pd.date_range(start=(str(forecast_year)+'-01-01'), periods=8760, freq='H')
# fut_net_load_8760['Date'] = hour_list
fut_net_load_8760 = future_8760[[str(respondent_id)]] - fut_re_8760[[str(respondent_id)]]
fut_net_load_8760.dropna(inplace=True)
net_load_sorted = fut_net_load_8760.sort_values(by=str(respondent_id), ascending=False)
print('future net load')
print(fut_net_load_8760[:10])
print('future net load sorted')
print(net_load_sorted[:10])
net_load_sorted.reset_index(inplace=True)
# net_load_sorted['Date'] = str((net_load_sorted['Month']).apply('{:0>2}'.format)) +
# net_load_sorted['Date'] = net_load_sorted['Month'].astype(str) + net_load_sorted['Day'].astype(str)

    # str(forecast_year) + '-' + str(net_load_sorted['Month']) + '-' + str(net_load_sorted['Day']) \
    #                       + ' ' + str(net_load_sorted['Hour'])
print(net_load_sorted)

print('current wind energy generation by 923 = ' + str(wind_energy_generation))
print('current solar energy generation by 923 = ' + str(solar_energy_generation))
print('load growth for respondent ' + str(respondent_id) + ': ' + str(cagr))
print('growth factor for respondent ' + str(respondent_id) + ': ' + str(growth_factor))
print('future renewable energy fraction for respondent ' + str(respondent_id) + ' is: ' + str(re_frac_fut))
print('future wind gen = ' + str(fut_wind_gen))
print('future solar gen = ' + str(fut_solar_gen))
print('future renewable energy = ' + str(re_fut_gen))
print('current wind cap = ' + str(current_wind_cap))
print('current solar cap = ' + str(current_solar_cap))
print('future wind cap = ' + str(fut_wind_cap))
print('future solar cap = ' + str(fut_solar_cap))




# print(gross_load_df.iloc[:10, :])


#
#
#
# print('current 8760')
# print(current_8760[:10])
# print('forecast 8760')
# print(forecast_8760[:10])
#
# forecast_total_energy = forecast_8760.sum()
# print('total energy forecast = ' + str(forecast_total_energy))
# forecast_renewable_energy = forecast_total_energy * re_percent
# print('total renewable energy forecast = ' + str(forecast_renewable_energy))
#
# current_total_energy = current_8760.sum()
# print('current total energy = ' + str(current_total_energy))





# if export_all:
#     writer = pd.ExcelWriter('renewables.xlsx', engine='xlsxwriter')
#     df_re.to_excel(writer)
#     writer.save()
#
#     writer = pd.ExcelWriter('total.xlsx', engine='xlsxwriter')
#     df_total.to_excel(writer)
#     writer.save()

