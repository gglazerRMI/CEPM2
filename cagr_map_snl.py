import pandas as pd
import numpy as np
import zipfile
import os.path
import datetime
import urllib.request


pd.set_option('display.max_columns', 60)
np.set_printoptions(precision=4, threshold=20)

data_path = os.path.abspath('cagr_data')
csvs_path = os.path.abspath('csvs')
df_dx = pd.read_excel(data_path + '/snl_dxassetsforcagr.xls', header=[2], skiprows=[3, 4, 5])
df_dx.rename(columns={'State': 'Corp State'}, inplace=True)
# print(df_snl.head())
df_dx['State'] = df_dx['Electric Distribution'].copy()
df_dx['State'].fillna(value=df_dx['Electric States of Operation'], inplace=True)
df_dx['State'].fillna(value=df_dx['States of Operation'], inplace=True)
df_dx['State'].fillna(value=df_dx['Corp State'], inplace=True)
df_dx.dropna(subset=['State'], inplace=True)
df_dx.drop(columns=['States of Operation', 'Electric States of Operation', 'Electric Distribution', 'Corp State'])

df_eands = pd.read_excel(data_path + '/snl_energyandstate.xls', header=[2], skiprows=[3, 4, 5])
# print(df_eands.shape)
ener_agg = {
    '2016 Total Retail Electric Customers, Total\n(actual)': 'sum',
    '2010 Total Retail Electric Customers, Total\n(actual)': 'sum',
    '2016 Total Retail Electric Volume, Total\n(MWh)': 'sum',
    '2010 Total Retail Electric Volume, Total\n(MWh)': 'sum'

}
energy_totals = df_eands.groupby(by='Institution Key').agg(ener_agg)
energy_totals.rename(columns={'2016 Total Retail Electric Customers, Total\n(actual)': '2016 Total Customers',
                              '2010 Total Retail Electric Customers, Total\n(actual)': '2010 Total Customers',
                              '2016 Total Retail Electric Volume, Total\n(MWh)': '2016 Total MWh',
                              '2010 Total Retail Electric Volume, Total\n(MWh)': '2010 Total MWh'}, inplace=True)
# print(energy_totals.shape)

df_eands = df_eands.join(energy_totals, on='Institution Key', how='left')
# print(df_states.head())
# print(df_eands.shape)
df_eands['2016 Sales Fraction'] = df_eands['2016 Total Retail Electric Volume, Total\n(MWh)'] / df_eands['2016 Total MWh']
df_eands['2010 Sales Fraction'] = df_eands['2010 Total Retail Electric Volume, Total\n(MWh)'] / df_eands['2010 Total MWh']
df_eands.dropna(subset=['2016 Sales Fraction', '2010 Sales Fraction'], inplace=True)
# df_states.to_csv(data_path + '/df_states.csv')
# df_dx.to_csv(data_path + '/df_snl.csv')

df_dxfrac = pd.merge(df_eands, df_dx, how='left', left_on='Institution Key', right_on='SNL Institution Key')
df_dxfrac.to_csv(data_path + '/df_dxfrac.csv')
print(df_dxfrac.shape)
df_dxfrac.dropna(subset=['2017 Total Distribution Plant: EOY\n($000)'], inplace=True)
print(df_dxfrac.shape)
# df_dxfrac.to_csv(data_path + '/df_dxfrac1.csv')

df_dxfrac['2017 Dx Assets'] = df_dxfrac['2016 Sales Fraction'] * df_dxfrac['2017 Total Distribution Plant: EOY\n($000)']
df_dxfrac['2016 Dx Assets'] = df_dxfrac['2016 Sales Fraction'] * df_dxfrac['2016 Total Distribution Plant: EOY\n($000)']
df_dxfrac['2010 Dx Assets'] = df_dxfrac['2010 Sales Fraction'] * df_dxfrac['2010 Total Distribution Plant: EOY\n($000)']
# print('total 2017 dx assets =', str(df_dxfrac['2017 Dx Assets'].sum()))
# print('total 2016 dx assets =', str(df_dxfrac['2016 Dx Assets'].sum()))
# print('total 2010 dx assets =', str(df_dxfrac['2010 Dx Assets'].sum()))

state_agg = {
    '2016 Total Retail Electric Customers, Total\n(actual)': 'sum',
    '2010 Total Retail Electric Customers, Total\n(actual)': 'sum',
    '2016 Total Retail Electric Volume, Total\n(MWh)': 'sum',
    '2010 Total Retail Electric Volume, Total\n(MWh)': 'sum',
    '2017 Dx Assets': 'sum',
    '2016 Dx Assets': 'sum',
    '2010 Dx Assets': 'sum',
}

df_states = df_dxfrac.groupby(by='State of Operation').agg(state_agg)
df_states.rename(columns={'2016 Total Retail Electric Customers, Total\n(actual)': '2016 Customers SNL',
                          '2010 Total Retail Electric Customers, Total\n(actual)': '2010 Customers SNL',
                          '2016 Total Retail Electric Volume, Total\n(MWh)': '2016 Retail MWh SNL',
                          '2010 Total Retail Electric Volume, Total\n(MWh)': '2010 Retail MWh SNL'
                          }, inplace=True)
df_states.reset_index(inplace=True)
df_eia10 = pd.read_excel(csvs_path + '/eia8612010_grouped.xlsx', usecols='A,C:G')
df_eia16 = pd.read_excel(csvs_path + '/eia8612016_grouped.xlsx', usecols='A,C:G')
df_eia17 = pd.read_excel(csvs_path + '/eia8612017_grouped.xlsx', usecols='A,C:G')
df_eia = pd.merge(df_eia10, df_eia16, on='State')
df_eia = pd.merge(df_eia, df_eia17, on='State')

df_states = pd.merge(df_states, df_eia, how='inner', left_on='State of Operation', right_on='State')
print(df_states.head(10))
df_states.to_csv(csvs_path + '/df_states_updated.csv')
