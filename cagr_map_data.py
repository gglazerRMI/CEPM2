import pandas as pd
import numpy as np
import zipfile
import os.path
import datetime
import urllib.request



pd.set_option('display.max_columns', 20)
np.set_printoptions(precision=4, threshold=20)


def unzip(zip_file_path, directory_to_extract_to):
    # function to unzip a file
    with zipfile.ZipFile(zip_file_path, "r") as z:
        z.extractall(directory_to_extract_to)


if not os.path.exists('cagr_data'):
    os.makedirs('cagr_data')
if not os.path.exists('csvs'):
    os.makedirs('csvs')
data_path_csvs = os.path.abspath('csvs')
data_path = os.path.abspath('cagr_data')
# for year in range(2010, 2018):
# ------ NOTES ON ENTERING YEARS ------ #
# When year is early:
# 'Thousand Dollars' should be 'Thousands Dollars'
# Do not skip footer
# '.xls' files
year = 2017
# unzip(data_path + '/eia_861_zips/f861'+str(year)+'.zip', data_path)
if year == 2010:
    eia861_sales = pd.read_excel(data_path + '/Sales_Ult_Cust_'+str(year)+'.xls', sheet_name='States', header=[2],
                                 usecols='A:D, G, U:W', skipfooter=0)
    eia861_sales.rename(columns={'Thousands Dollars': 'Thousand Dollars'}, inplace=True)
    eia861_peak = pd.read_excel(data_path + '/file1_2010.xls', sheet_name='file1_states', header=7, usecols='A:D, G:I')
    eia861_peak.rename(columns={'MAIL_STATE': 'State', 'PEAK_DEMAND_SUMMER': 'Summer Peak Demand', 'PEAK_DEMAND_WINTER':
                                'Winter Peak Demand', 'NET_GENERATION': 'Net Generation'}, inplace=True)
else:
    eia861_sales = pd.read_excel(data_path + '/Sales_Ult_Cust_' + str(year) + '.xlsx', sheet_name='States', header=[2],
                                 usecols='A:D, G, V:X', skipfooter=1)
    eia861_peak = pd.read_excel(data_path + '/Operational_Data_'+str(year)+'.xlsx', sheet_name='States', header=2, usecols='A:E, G:I')
eia861_sales.replace(to_replace='.', value=0, inplace=True)
eia861_peak.replace(to_replace='.', value=0, inplace=True)
state_revenues = pd.DataFrame()
state_revenues['Revenues (Thousands Dollars)'] = eia861_sales['Thousand Dollars']
state_revenues['State'] = eia861_sales['State']

'''
"To calculate a state or the US total, sum Parts (A,B,C & D) for Revenue, but only Parts (A,B & D) for Sales and 
Customers. To avoid double counting of customers, the aggregated customer counts for the states and US do not include 
the customer count for respondents with ownership code 'Behind the Meter'.
This group consists of Third Party Owners of rooftop solar systems."																							
'''

if year < 2013:
    sales = eia861_sales
else:
    sales = eia861_sales.loc[(eia861_sales['Part'] == 'A') | (eia861_sales['Part'] == 'B') |
                             (eia861_sales['Part'] == 'D')]

sales_agg = {
    'Data Year': 'first',
    'Megawatthours': 'sum',
    'Count': 'sum'
}
rev_agg = {
    'Revenues (Thousands Dollars)': 'sum'
}
peak_agg = {
    'Summer Peak Demand': 'sum',
    'Winter Peak Demand': 'sum'
}
sales = sales.groupby(by=['State']).agg(sales_agg)
state_revenues = state_revenues.groupby(by=['State']).agg(rev_agg)
peaks = eia861_peak.groupby(by=['State']).agg(peak_agg)
sales = pd.merge(sales, state_revenues, how='left', left_index=True, right_index=True)
sales = pd.merge(sales, peaks, how='left', left_index=True, right_index=True)
sales.rename(columns={'Megawatthours': str(year)+' MWh sold EIA',
                      'Count': str(year)+' Customers EIA',
                      'Revenues (Thousands Dollars)': str(year)+' Revenues ($1000) EIA',
                      'Summer Peak Demand': str(year)+' Summer Peak EIA',
                      'Winter Peak Demand': str(year)+' Winter Peak EIA'
                      }, inplace=True)
writer = pd.ExcelWriter(data_path_csvs + '/eia861'+str(year)+'_grouped.xlsx', engine='xlsxwriter')
sales.to_excel(writer)
writer.save()
print(sales.head())
