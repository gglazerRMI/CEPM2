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


class LHSConstraints(object):
    def __init__(self, region):
        if not os.path.exists('data'):
            os.makedirs('data')
        self.data_path = os.path.abspath('data')
        self.build_a(region)

    def build_a(self, region):
        EU = pd.read_excel(self.data_path + '/EU.xlsx', sheet_name=str(region), index_col=0)
        print(EU)

        A = 0

        return A


class CalculateMonthlyEnergy(object):
    '''
    Class for RHS monthly energy calcs
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
