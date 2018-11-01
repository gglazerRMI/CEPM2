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
from energy_constraint import *

print('Begin script ' + str(datetime.datetime.now().time()))

# --------- Settings --------- #
pd.set_option('display.max_columns', 70)
np.set_printoptions(precision=4, threshold=20)
export_all = False


# --------- Functions --------- #


def load_pickle(name):
    # function to load an object from a pickle
    with open(str(name) + '.pkl', 'rb') as f:
        temp = pickle.load(f)
    return temp


def save_pickle(contents, name):
    # function to save to an object as a pickle
    with open(str(name) + '.pkl', 'wb') as output:
        pickle.dump(contents, output, pickle.HIGHEST_PROTOCOL)


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

state='AZ'
region='Southwest'
# case_list = pd.read_excel('/Users/gglazer/PycharmProjects/RMI/RMI_gridproj/data/Case_List.xlsx')
# states = case_list.groupby(['State']).count()
# # states.apply(lambda x: x['State'].set_index())
# # # states.reset_index(inplace=True)
# print(states)

# state_data = pd.read_excel('/Users/gglazer/PycharmProjects/RMI/RMI_gridproj/data/State_Data.xlsx', index_col=0,
#                            header=[0, 1])
# state_data = state_data.loc[state]
# # state_data.reset_index(inplace=True)
# print(state_data)
#
# rps_frac = state_data.loc['RPS', 'Target']
# print('target rps fraction is: ' + str(rps_frac))

# l_matrix = pd.read_csv('/Users/gglazer/PycharmProjects/RMI/RMI_gridproj/data/L.csv')

num_hours = 5
ramp_ran = 5
region = 'Southwest'


def idxmax(s, w):
    i = 0
    while i + w <= len(s):
        yield(s.iloc[i:i+w].idxmax())
        i += 1


future_net_8760 = load_pickle('/Users/gglazer/PycharmProjects/CEP1/data/future_net_8760_pickle')
all_EU = load_pickle('/Users/gglazer/PycharmProjects/RMI/RMI_gridproj/data/all_EU')
all_RE = load_pickle('/Users/gglazer/PycharmProjects/RMI/RMI_gridproj/data/all_RE')
eu_matrix = all_EU[region].reset_index()
eu_matrix.sort_index(inplace=True)
eu_matrix.drop(columns='index', inplace=True)
re_matrix = all_RE[region].reset_index()

solar_list = ['Solar_Tracking', 'Solar_Fixed']

# ## things to make 'self'
# change re_matrix to self.re
# change ramp_ran to self.ramp_ran
# re_hours = re_matrix.copy()
max_ramp = pd.DataFrame()


def calc_ramping(cols):
    for col in cols:
        re_hours = re_matrix.copy()
        re_hours['Rolling Max ' + col] = re_hours[col].rolling(window=ramp_ran).max()
        re_hours['First Hour ' + col] = pd.Series(idxmax(re_matrix[col], ramp_ran), re_hours.index[ramp_ran-1:])
        re_hours.fillna(0, inplace=True)
        re_hours['First Hour ' + col] = re_hours['First Hour ' + col].astype(int)
        re_hours['Delta ' + col] = re_hours[col] - re_hours['Rolling Max ' + col]
        re_hours['Num Hours ' + col] = re_hours.index - re_hours['First Hour ' + col]
        # if export_all:
        #     re_hours.to_csv()
        if col == 'Solar_Fixed':
            max_ramp_fixed = re_hours.loc[re_hours['Delta ' + col] == min(re_hours['Delta ' + col])]
        if col == 'Solar_Tracking':
            max_ramp_tracking = re_hours.loc[re_hours['Delta ' + col] == min(re_hours['Delta ' + col])]
    return max_ramp_tracking, max_ramp_fixed


# if (max_ramp_fixed.index.values).astype(int) == 6736:
#     print((max_ramp_fixed.index.values).astype(int))
#     print('poopy')


[max_tracking, max_fixed] = calc_ramping(solar_list)


# change [max_fixed] to self.max_fixed
def find_flex_value(matrix, source, pv_type='fixed'):
    if pv_type == 'fixed':
        value = matrix[source][max_fixed.index.values].values - \
                matrix[source][max_fixed.index.values - max_fixed['Num Hours Solar_Fixed']].values
    if pv_type == 'tracking':
        value = matrix[source][max_tracking.index.values].values - \
                matrix[source][max_tracking.index.values - max_tracking['Num Hours Solar_Tracking']].values
    return value

# A_flex values for PVs
a_fixed = -max_fixed['Delta Solar_Fixed'].values
a_track = -max_tracking['Delta Solar_Tracking'].values
# A_flex values for wind sources
a_wind_fix = find_flex_value(re_matrix, 'Wind', 'fixed')
a_wind_tra = find_flex_value(re_matrix, 'Wind', 'tracking')
a_windoff_fix = find_flex_value(re_matrix, 'Wind_Offshore', 'fixed')
a_windoff_tra = find_flex_value(re_matrix, 'Wind_Offshore', 'tracking')
# A_flex values for energy storage
a_es4f = 2
a_es4t = 2
a_es6f = 2
a_es6t = 2
# A_flex values for energy efficiency
a_ee_fix = eu_matrix.iloc[max_fixed.index.values, :].values - \
                eu_matrix.iloc[max_fixed.index.values - max_fixed['Num Hours Solar_Fixed'], :].values
a_ee_tra = eu_matrix.iloc[max_tracking.index.values, :].values - \
                eu_matrix.iloc[max_tracking.index.values - max_tracking['Num Hours Solar_Tracking'], :].values
# A_flex values for demand response
a_dr_fix = eu_matrix.iloc[max_fixed.index.values, :].values
a_dr_tra = eu_matrix.iloc[max_tracking.index.values, :].values

# subtr = eu_matrix.iloc[max_fixed.index.values - max_fixed['Num Hours Solar_Fixed'], :].values


A_flex = [[a_fixed, 0, a_wind_fix, a_windoff_fix, a_es4f, a_es6f, a_ee_fix, a_dr_fix],
          [0, a_track, a_wind_tra, a_windoff_tra, a_es4t, a_es6t, a_ee_tra, a_dr_tra]]
A_flex = np.asarray(A_flex)
print(A_flex)
# print(A_flex)
# np.savetxt('/Users/gglazer/PycharmProjects/RMI/RMI_gridproj/data/a_flex.csv', A_flex, delimiter=',')

# solar_fixed = re_matrix['Solar_Fixed'].tolist()
# print(solar_fixed[:15])
# print(len(solar_fixed))
# first_delta = solar_fixed[1:] - solar_fixed[0:len(solar_fixed)-1]
# max_delta = []
# max_index = []
# for i in range(ramp_ran, len(solar_fixed)):
#     max_delta[i] = solar_fixed[i] - max(solar_fixed[(i-ramp_ran):i])
#     max_index[i] = max(solar_fixed[(i-ramp_ran):i]).index
# print(max_delta[:15])
# print(max_delta.shape)
# print(max_index[:15])
# print(max_index.shape)
# print(re_matrix.head())

# print(eu_matrix.head())
# print(re_matrix.head())
# print(future_net_8760)

# # Find top hour of added load
# maxes = future_net_8760.sort_values('Delta', ascending=False)
# max_hour = maxes.iloc[[0]]
# del maxes
# # Sort by net load, keep top hours, add in max hour of added load
# fut_sorted = future_net_8760.sort_values(['Net Load'], ascending=False)
# fut_sorted = fut_sorted[:num_hours]
# fut_sorted = pd.concat([max_hour, fut_sorted])
# fut_sorted.reset_index(inplace=True)
# # Count how many times each day appears in top hours
# fut_sorted['MonthDay'] = fut_sorted[['Month', 'Day']].apply(''.join, axis=1)
# counts = fut_sorted[['MonthDay']]
# counts = counts.groupby(by=['MonthDay'])['MonthDay'].agg('count')
# counts = counts.to_frame()
# counts.rename(columns={'MonthDay': 'Counts'}, inplace=True)
# counts.reset_index(inplace=True)
# fut_sorted = pd.merge(fut_sorted, counts, how='left', on=['MonthDay'])
# # Merge RE, EU matrices into the top hours for net load constraint
# fut_sorted.set_index('index', inplace=True)
# fut_sorted = fut_sorted.merge(re_matrix, how='left', left_index=True, right_index=True)
# fut_sorted = fut_sorted.merge(eu_matrix, how='left', left_index=True, right_index=True)
# print(fut_sorted)
# fut_sorted.to_csv('/Users/gglazer/PycharmProjects/CEP1/data/fut_sorted.csv')
# print(counts)
#
# # Indexing for months
# Jan = 0
# Feb = Jan + 31*24
# Mar = Feb + 28*24
# Apr = Mar + 31*24
# May = Apr + 30*24
# Jun = May + 31*24
# Jul = Jun + 30*24
# Aug = Jul + 31*24
# Sep = Aug + 31*24
# Oct = Sep + 30*24
# Nov = Oct + 31*24
# Dec = Nov + 30*24
# #
# months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
# # months = list(range(1, 13))
# re_8760s = pd.ExcelFile('/Users/gglazer/PycharmProjects/CEP1/data/RE.xlsx', usecols='A:E').parse('Midwest')
# L_matrix = pd.DataFrame(data=0, index=re_8760s.index, columns=months)
# L_matrix['Datetime'] = L_matrix.index
# for month in months:
#     # print(L_matrix[month])
#     L_matrix[month].loc[L_matrix['Time'].dt.month == month] = 1
# if L_matrix['Datetime'].month == 1:
#     L_matrix['Jan'] = 1
#
# L = np.zeros((8760, 12))
# months = [Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec]
# for i in range(len(months)):
#     if i == 11:
#         L[months[i]:, i] = np.ones((1, len(L)-months[i])).astype(int)
#     else:
#         L[months[i]:months[i+1], i] = np.ones((1, months[i+1]-months[i])).astype(int)
# # print(L.T.dot(L))
# # np.savetxt('/Users/gglazer/PycharmProjects/RMI/L.csv', L, delimiter=',')
# save_pickle(L, '/Users/gglazer/PycharmProjects/RMI/RMI_gridproj/data/L')
# print(L)
#
# re_8760s = pd.ExcelFile('/Users/gglazer/PycharmProjects/CEP1/data/RE.xlsx', usecols='A:E')
# save_pickle(re_8760s, '/Users/gglazer/PycharmProjects/CEP1/data/pickles/re_8760s_pickle')

# print(df_norm_renewable_cap)
# LHSConstraints('West')

