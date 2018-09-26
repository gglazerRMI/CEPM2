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


df_8760f = load_pickle('/Users/gglazer/PycharmProjects/CEP1/data/future_net_8760_pickle')
df_8760f.reset_index(inplace=True)
df_delta = pd.DataFrame(data=None, index=df_8760f.index)
# print(df_delta)

df_8760first = df_8760f.drop(df_8760f.index[-1]).reset_index()
df_8760last = df_8760f.drop(df_8760f.index[0]).reset_index()
print(df_8760first)
print(df_8760last)

df_delta['209'] = df_8760last['209'] - df_8760first['209']
df_delta['209'] = df_delta['209'].shift(1)
print(df_delta)

df_delta['209'].loc[df_delta.index[0]] = df_8760f['209'].loc[df_8760f.index[0]] - df_8760f['209'].loc[df_8760f.index[-1]]

print(df_8760f)
print(df_delta)
# print(df_8760last)
# print(df_8760last['209'] - df_8760first['209'])
