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


# gross_load_df = load_pickle('/Users/gglazer/PycharmProjects/CEP1/data/pickles/gross_load_pickle')
# print(gross_load_df[:10])
# util_8760 = gross_load_df[['105']]
# util_8760 = util_8760.dropna()
# print(util_8760)
# max_year = max(util_8760.index.year)
# print('max year = ' + str(max_year))
# current_8760 = util_8760.loc[util_8760.index.year == max_year]
# print(current_8760.size)
# # Feb = 2.as_type(Int64Index)
#
# if int(max_year) % 4 == 0:
#     # current_8760.drop(current_8760.loc[(current_8760.index.month == 2) & (current_8760.index.day == 29)], inplace=True)
#     # current_8760 = current_8760.drop(current_8760.index[pd.to_datetime((str(max_year)+'-02-29 00:00:00')):pd.to_datetime((str(max_year)+'-02-29 23:00:00'))])
#     current_8760 = current_8760.drop(current_8760.index[1416:1440])
#
# print(current_8760[1414:1441])
# print(current_8760.size)
print(sys.version)
# regions = ['Midwest', 'Northcentral', 'Northeast', 'Southeast', 'Southwest', 'Texas', 'West']
# # df_midwest = pd.read_excel('/Users/gglazer/PycharmProjects/CEP1/data/RE.xlsx', sheet_name='Midwest', usecols='A:E')
# # print(df_midwest)
# # df = pd.DataFrame(index=df_midwest.index)
# for i in range(len(regions)):
#     df = pd.read_excel('/Users/gglazer/PycharmProjects/CEP1/data/RE.xlsx', sheet_name=regions[i], usecols='A:E')
#     save_pickle(df, '/Users/gglazer/PycharmProjects/CEP1/data/pickles/' + str(regions[i]) + '_pickle')
print(pd.__version__)
print('End script', str(datetime.datetime.now().time()))
#
# re_8760s = pd.ExcelFile('/Users/gglazer/PycharmProjects/CEP1/data/RE.xlsx', usecols='A:E')
# save_pickle(re_8760s, '/Users/gglazer/PycharmProjects/CEP1/data/pickles/re_8760s_pickle')

# print(df_norm_renewable_cap)
# LHSConstraints('West')

