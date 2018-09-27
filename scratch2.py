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

LHSConstraints('West')

