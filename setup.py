import os.path
import shutil
import pickle
import urllib.request
import zipfile
import datetime
import numpy as np
import pandas as pd


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


class SetupDataE(object):
    '''
    Class for setting up all the data, this is a less than perfect use for a class
    thinking we sort of use it like a function that can share certain parts of itself
    '''
    def __init__(self, export_all=False):
        # Create data and tmp directories
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        if not os.path.exists('data/pickles'):
            os.makedirs('data/pickles')
        if not os.path.exists('data/results'):
            os.makedirs('data/results')
        self.data_path = os.path.abspath('data')
        self.acquire_eia923()
        self.acquire_eia860()
        self.acquire_FERC_key()
        ferc_key = self.setup_FERC_key()
        df923 = self.setup_eia923()
        df860 = self.setup_eia860()
        self.setup_dfpp(df923, df860, ferc_key, export_all)

    def acquire_eia923(self):
        # download EIA 923 if relevant files are not in 'data' directory
        if not os.path.exists(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx'):
            print('downloading EIA 923', str(datetime.datetime.now().time()))
            urllib.request.urlretrieve('https://www.eia.gov/electricity/data/eia923/archive/xls/f923_2016.zip',
                                       self.data_path + '/tmp/EIA923.zip')
            print('unzipping EIA 923', str(datetime.datetime.now().time()))
            unzip(self.data_path + '/tmp/EIA923.zip', self.data_path + '/tmp/EIA923')
            os.remove(self.data_path + '/tmp/EIA923.zip')
            os.rename(self.data_path + '/tmp/EIA923/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
                      self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx')
            if not os.path.exists(self.data_path + '/EIA923_Schedule_8_Annual_Environmental_Information_'
                                                   '2016_Final_Revision.xlsx'):
                os.rename(self.data_path + '/tmp/EIA923/EIA923_Schedule_8_Annual_Environmental_Information_'
                                           '2016_Final_Revision.xlsx',
                          self.data_path + '/EIA923_Schedule_8_Annual_Environmental_Information_'
                                           '2016_Final_Revision.xlsx')
            if not os.path.exists(self.data_path + '/EIA923_Schedules_6_7_NU_SourceNDisposition_'
                                                   '2016_Final_Revision.xlsx'):
                os.rename(self.data_path + '/tmp/EIA923/EIA923_Schedules_6_7_NU_SourceNDisposition_'
                                           '2016_Final_Revision.xlsx',
                          self.data_path + '/EIA923_Schedules_6_7_NU_SourceNDisposition_2016_Final_Revision.xlsx')
            shutil.rmtree(self.data_path + '/tmp/EIA923')

    def acquire_eia860(self):
        if not os.path.exists(self.data_path + '/3_1_Generator_Y2015.xlsx'):

            ## DEBUG THIS, CHECK THAT IT'S NOT A ZIP
            print('downloading EIA 860', str(datetime.datetime.now().time()))
            urllib.request.urlretrieve('https://www.eia.gov/electricity/data/eia860/archive/xls/eia8602015.zip',
                                       self.data_path + '/tmp/EIA860.zip')
            print('unzipping EIA 860', str(datetime.datetime.now().time()))
            unzip(self.data_path + '/tmp/EIA860.zip', self.data_path + '/tmp/EIA860')
            os.remove(self.data_path + '/tmp/EIA860.zip')
            os.rename(self.data_path + '/tmp/EIA860/3_1_Generator_Y2015.xlsx',
                      self.data_path + '/3_1_Generator_Y2015.xlsx')
            if not os.path.exists(self.data_path + '/2___Plant_Y2015.xlsx'):
                os.rename(self.data_path + '/tmp/EIA860/2___Plant_Y2015.xlsx',
                          self.data_path + '/2___Plant_Y2015.xlsx')
            # if not os.path.exists(self.data_path + '/3_2_Wind_Y2015.xlsx'):
            #     os.rename(self.data_path + '/tmp/EIA860/3_2_Wind_Y2015.xlsx',
            #               self.data_path + '/3_2_Wind_Y2015.xlsx')
            # if not os.path.exists(self.data_path + '/3_3_Solar_Y2015.xlsx'):
            #     os.rename(self.data_path + '/tmp/EIA860/3_3_Solar_Y2015.xlsx',
            #               self.data_path + '/3_3_Solar_Y2015.xlsx')
            # if not os.path.exists(self.data_path + '/3_3_Solar_Y2015.xlsx'):
            #     os.rename(self.data_path + '/tmp/EIA860/3_4_Multifuel_Y2015.xlsx',
            #               self.data_path + '/3_4_Multifuel_Y2015.xlsx')
            shutil.rmtree(self.data_path + '/tmp/EIA860')

    def acquire_FERC_key(self):
        if not os.path.exists(self.data_path + '/Respondent IDs.csv'):
            print('downloading FERC Key', str(datetime.datetime.now().time()))
            urllib.request.urlretrieve('https://www.ferc.gov/docs-filing/forms/form-714/data/form714-database.zip',
                                       self.data_path + '/tmp/FERC.zip')
            print('unzipping FERC 714', str(datetime.datetime.now().time()))
            unzip(self.data_path + '/tmp/FERC.zip', self.data_path + '/tmp/FERC')
            os.remove(self.data_path + '/tmp/FERC.zip')
            os.rename(self.data_path + '/tmp/FERC/Respondent IDs.csv',
                      self.data_path + '/Respondent IDs.csv')
            shutil.rmtree(self.data_path + '/tmp/FERC')

    def setup_FERC_key(self):
        ferc_key = pd.read_csv(self.data_path + '/Respondent IDs.csv')
        ferc_key.rename(columns={'eia_code': 'Operator Id'}, inplace=True)
        return ferc_key

    def setup_eia923(self):
        """This code loads EIA 923 monthly energy generation and assigns each plant type to one of:
        SUN (solar PV and thermal), COL (coal and waste coal), NGCC (natural gas combined cycle),
        NGCT (natural gas combustion turbine, NUC (nuclear), WND (wind), FF (other fossil fuel), or
        ALT (other alternative fuel). The assignment is stored in the column 'Plant Type'."""

        # -------------Things to fix---------- #
        #
        print('setting up EIA 923 dataframe ' + str(datetime.datetime.now().time()))
        # import dataframe from excel file
        df923 = pd.read_excel(self.data_path + '/EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx',
                                       sheet_name=0, header=5, usecols="A:B,D:G,I,K:N,P,CB:CM,CR:CS")
        ## took out df923= and added inplace
        df923.replace(to_replace='.', value=0, inplace=True)
        # replace AER codes for fossil fuels
        df923['AER\nFuel Type Code'].replace(['DFO', 'OOG', 'PC', 'RFO', 'WOO'], ['FF', 'FF', 'FF', 'FF', 'FF'],
                                             inplace=True)
        # replace AER codes for alternative fuels
        df923['AER\nFuel Type Code'].replace(['GEO', 'HPS', 'HYC', 'MLG', 'ORW', 'OTH', 'WWW'], [
            'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT'], inplace=True)
        # relabel waste coal as coal
        df923['AER\nFuel Type Code'].replace(['WOC'], ['COL'], inplace=True)
        # replace 'CA, CS, CT' prime movers as 'CC' (combined cycle)
        df923['Reported\nPrime Mover'].replace(['CA', 'CS', 'CT'], ['CC', 'CC', 'CC'], inplace=True)
        # label CC in AER fuel column
        df923.loc[(df923['Reported\nPrime Mover'] == 'CC'), 'AER\nFuel Type Code'] = 'NGCC'
        # change remaining natural gas listings to NGCT
        df923.loc[(df923['AER\nFuel Type Code'] == 'NG'),
                  'AER\nFuel Type Code'] = 'NGCT'
        # change column name for future indexing use
        df923.rename(columns={'AER\nFuel Type Code': 'Plant Type', 'Plant State': 'State'}, inplace=True)
        # simplify names for monthly energy
        df923.rename(columns={'Netgen\nJanuary': 'Jan', 'Netgen\nFebruary': 'Feb', 'Netgen\nMarch':'Mar',
                              'Netgen\nApril': 'Apr', 'Netgen\nMay': 'May', 'Netgen\nJune': 'Jun', 'Netgen\nJuly':
                                  'Jul', 'Netgen\nAugust': 'Aug', 'Netgen\nSeptember': 'Sep', 'Netgen\nOctober': 'Oct',
                              'Netgen\nNovember': 'Nov', 'Netgen\nDecember': 'Dec', 'Net Generation\n(Megawatthours)':
                                  'Annual Energy'}, inplace=True)

        # Group generation data from 923 data frame
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Annual Energy']
        df923gen = df923.groupby(['Plant Id', 'Plant Type', 'Operator Id', 'Operator Name', 'Plant Name', 'State'
                                  ])[months].agg('sum')

        # Group plant data from 923 data frame
        aggregations = {
            'YEAR': 'mean',
            'Combined Heat And\nPower Plant': 'first',
            'NERC Region': 'first', 'NAICS Code': 'first', 'EIA Sector Number': 'first'
        }
        df923data = df923.groupby(['Plant Id', 'Plant Type', 'Operator Id', 'Operator Name', 'Plant Name', 'State']).\
            agg(aggregations)

        # print(df923gen.loc[:10, :])
        # print(df923data.loc[:10, :])

        # merge with df923data
        df923group = pd.merge(df923gen, df923data, how='outer', on=['Plant Id', 'Plant Type', 'Operator Id',
                                                                    'Operator Name', 'Plant Name', 'State'])

        print('dataframe ready! ' + str(datetime.datetime.now().time()))

        return df923group

    def setup_eia860(self):
        """This method loads the EIA 860-Generators form as a dataframe from the excel download. The dataframe is
        organized where each index is a unique 'power plant', as defined for the purposes of the CEPM tool.
        Each power plant of uniform plant type is a unique entry, and power plants with multiple plant types are listed
        as separate entries."""
        # -------------Things to fix------------- #
        # Only looks at Energy Source 1, despite the fact that some plants use multiple sources of energy. This may lead
        # to inaccurate measurements for similar plant types.
        # Should I delete temporary dataframes?
        print('setting up EIA 860 dataframe ' + str(datetime.datetime.now().time()))
        # import dataframe from excel file
        df860gen = pd.read_excel(self.data_path + '/3_1_Generator_Y2015.xlsx',
                              sheet_name=0, header=1, usecols="A:E,H:I,P,Z:AA,AH")
        df860gen.replace(to_replace='.', value=0, inplace=True)
        df860gen.drop(df860gen.tail(1).index, inplace=True)
        # replace Energy Source 1 codes for fossil fuels
        df860gen['Energy Source 1'].replace(['DFO', 'JF', 'KER', 'PC', 'PG', 'RFO', 'SGP', 'WO'],
                                             ['FF', 'FF', 'FF', 'FF', 'FF', 'FF', 'FF', 'FF'], inplace=True)
        # replace Energy Source 1 codes for alternative fuels
        df860gen['Energy Source 1'].replace(['AB', 'MSW', 'OBS', 'WDS', 'OBL', 'SLW', 'BLQ', 'WDL', 'LFG', 'OBG',
                                              'GEO', 'WAT', 'PUR', 'WH', 'TDF', 'MWH', 'OTH'],
                                             ['ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT',
                                              'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT', 'ALT'], inplace=True)
        # replace Energy Source 1 codes for coal
        df860gen['Energy Source 1'].replace(['ANT', 'BIT', 'LIG', 'SGC', 'SUB', 'WC', 'RC'],
                                             ['COL', 'COL', 'COL', 'COL', 'COL', 'COL', 'COL'], inplace=True)
        # replace Energy Source 1 codes for natural gas
        df860gen['Energy Source 1'].replace(['BFG', 'OG'], ['NG', 'NG'], inplace=True)
        # replace 'CA, CS, CT' prime movers as 'CC' (combined cycle)
        df860gen['Prime Mover'].replace(['CA', 'CS', 'CT'], ['CC', 'CC', 'CC'], inplace=True)
        # rename CC natural gas listings as NGCC
        df860gen.loc[(df860gen['Energy Source 1'] == 'NG') &
                     (df860gen['Prime Mover'] == 'CC'), 'Energy Source 1'] = 'NGCC'
        # change all remaining natural gas listings to NGCT
        df860gen['Energy Source 1'].replace(['NG'], ['NGCT'], inplace=True)

        # change names and types of variables to match df923
        df860gen.rename(columns={'Energy Source 1': 'Plant Type', 'Plant Code': 'Plant Id', 'Utility ID': 'Operator Id',
                                 'Utility Name': 'Operator Name'}, inplace=True)
        df860gen['Plant Id'] = df860gen['Plant Id'].astype(int)
        df860gen['Operating Month'] = df860gen['Operating Month'].astype(int)
        df860gen['Operating Year'] = df860gen['Operating Year'].astype(int)

        # group relevant plant info for 860 data frame
        df860info = df860gen.groupby(['Plant Id', 'Plant Type', 'Operator Id', 'Operator Name', 'Plant Name', 'State'
                                      ]).agg({'Technology': 'first',
                                              'Operating Year': ['mean', 'min', 'max'],
                                              'Operating Month': ['mean', 'min', 'max']})

        # group capacity for 860 data frame
        df860cap = df860gen.groupby(['Plant Id', 'Plant Type', 'Operator Id', 'Operator Name', 'Plant Name', 'State'
                                     ]).agg({'Nameplate Capacity (MW)': 'sum'})
        # # Merging between different levels can give an unintended result
        # merge 860 data and capacity
        df860group = pd.merge(df860cap, df860info, how='outer', on=['Plant Id', 'Plant Type', 'Operator Id',
                                                                    'Operator Name', 'Plant Name', 'State'])

        # print(df860group.loc[:10, :])
        print('dataframe ready! ' + str(datetime.datetime.now().time()))

        return df860group

    def setup_dfpp(self, df923group, df860group, ferc_key, export_all):
        """This method combines df923 and df860gen into one cumulative power plant dataframe."""
        # -------------Things to fix---------- #
        # We lose indices that aren't in both df923 and df860
        # CHP(Y/N), NERC Region, NAICS Code, EIA Sector Number, YEAR, Operating Month, Operating Year
        # Make zeros in 'Nameplate Capacity (MW)' NaNs
        # Label each index with Plant Id in a column, drop the first index
        print('setting up Power Plant dataframe ' + str(datetime.datetime.now().time()))
        ## I think something may be resolvable with the on= command
        dfpp = pd.merge(df923group, df860group, how='outer', on=['Plant Id', 'Plant Type', 'Operator Id',
                                                                 'Operator Name', 'Plant Name', 'State'])
        # dfpp['Average Annual Capacity Factor'] = dfpp['Annual Energy'].divide(
        #     dfpp['Nameplate Capacity (MW)']*8760)
        dfpp.sort_index(inplace=True)

        # Calculate Capacity Factor Hours (C.F.*hours/time period) for months and year
        dfpp[['JanCFH', 'FebCFH', 'MarCFH', 'AprCFH', 'MayCFH', 'JunCFH', 'JulCFH', 'AugCFH', 'SepCFH', 'OctCF',
              'NovCF', 'DecCF', 'YearCFH']] = dfpp[['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                                                    'Oct', 'Nov', 'Dec', 'Annual Energy'
                                                    ]].div(dfpp['Nameplate Capacity (MW)'].values, axis=0)

        # Add column for FERC key
        cols = ['Operator Id']
        dfpp = dfpp.join(ferc_key.set_index(cols), on=cols)
        dfpp['respondent_id'] = dfpp['respondent_id'].fillna(0).astype(int)
        dfpp.rename(columns={'respondent_id': 'Respondent Id'}, inplace=True)

        print('dataframe ready! ' + str(datetime.datetime.now().time()))
        # print(dfpp.loc[:10, :])

        save_pickle(dfpp, self.data_path + '/pickles/pppickle')

        if export_all:
            # Write data frame to Excel file
            writer = pd.ExcelWriter('powerplant_df.xlsx', engine='xlsxwriter')
            dfpp.to_excel(writer)
            writer.save()

        return dfpp


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
        # gross_load_df = gross_load_df[gross_load_df.index.year == curr_year]

        save_pickle(gross_load_df, self.data_path + '/pickles/gross_load_pickle')
        # print(gross_load_df)
        return self

    def setup_rps(self):
        # Load the renewable portfolio standard data frame for all states

        # ------TO FIX------- #
        # add for Texas

        print('Setting up RPS ', str(datetime.datetime.now().time()))

        df_rps = pd.read_csv(self.data_path + '/RPS_csv.csv')
        df_rps.set_index('State', inplace=True)
        df_rps.dropna(axis=0, subset=['RPS RE%'], inplace=True)
        save_pickle(df_rps, self.data_path + '/pickles/rps_pickle')

        return self

    def import_re_8760s(self):

        # Load the regional renewable energy normalized 8760s from Reinventing Fire

        regions = ['Midwest', 'Northcentral', 'Northeast', 'Southeast', 'Southwest', 'Texas', 'West']
        for i in range(len(regions)):
            df = pd.read_excel('/Users/gglazer/PycharmProjects/CEP1/data/RE.xlsx', sheet_name=regions[i], usecols='A:E')
            save_pickle(df, '/Users/gglazer/PycharmProjects/CEP1/data/pickles/' + str(regions[i]) + '_pickle')

        return self
