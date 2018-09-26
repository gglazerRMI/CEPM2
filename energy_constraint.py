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


class CalculateMonthlyEnergy(object):
    '''
    Class for monthly energy calcs
    '''
    def __init__(self, respondent_id, plant_type, nameplate, save_results=True, export_all=False):
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        if not os.path.exists('data/pickles'):
            os.makedirs('data/pickles')
        if not os.path.exists('data/results'):
            os.makedirs('data/results')
        self.data_path = os.path.abspath('data')
        self.calculate_monthly_energy(respondent_id, plant_type, nameplate, save_results, export_all)

    def calculate_monthly_energy(self, respondent_id, plant_type, nameplate, save_results, export_all):
        """This method calculates the monthly energy requirements given a nameplate capacity and FERC Respondent ID.
        The monthly energy capacity factor hours for all plant types operated by a FERC Respondent ID are averaged and
        multiplied by the nameplate capacity.
        Only plants larger than 100 MW are included in the averaging."""

        print('Calculating monthly energy constraint ', str(datetime.datetime.now().time()))

        min_plant_size = 100
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
        df_selected_plants = dfpp.loc[(dfpp['Respondent Id'] == respondent_id) & (dfpp['Plant Type'] == plant_type)
                                      & (dfpp['Nameplate Capacity (MW)'] > min_plant_size)]
        # Calculate average monthly capacity factor hours
        df_average_plant = df_selected_plants.groupby(['Respondent Id', 'Plant Type'])[cols].agg('mean')

        # Calculate MWh needed for each month
        monthly_energy = df_average_plant[monthly_cfh] * nameplate
        monthly_energy.rename(columns={'JanCFH': 'Jan_MWh', 'FebCFH': 'Feb_MWh', 'MarCFH': 'Mar_MWh',
                                         'AprCFH': 'Apr_MWh', 'MayCFH': 'May_MWh', 'JunCFH': 'JunMWh',
                                         'JulCFH': 'JulMWh', 'AugCFH': 'Aug_MWh', 'SepCFH': 'Sep_MWh',
                                         'OctCF': 'Oct_MWh', 'NovCF': 'Nov_MWh', 'DecCF': 'Dec_MWh'}, inplace=True)
        monthly_energy['Nameplate Capacity (MW)'] = nameplate

        save_pickle(monthly_energy, self.data_path + '/pickles/monthly_energy')

        if save_results:
            monthly_energy.to_csv(self.data_path + '/results/monthly_energy.csv')

        if export_all:
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

            print(df_selected_plants.loc[:10, :])
            print('Averages')
            print(df_average_plant)
            print('Monthly Energies')
            print(monthly_energy)

        return monthly_energy
