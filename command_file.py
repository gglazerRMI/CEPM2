from setup import *
# from energy_constraint import *
from CEPCase import *


# --------- Settings --------- #
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 70)

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

respondent_id = 209     # util
respondent_id_backup = 209  # util2
state = 'NE'    # state
region1 = 'MidWest'  # region1
region = 'Midwest'
plant_type = 'NGCC'
nameplate = 800         # nameplate capacity of proposed plant
forecast_year = 2020    # forecast_year
# Will skip leap days if leap year selected
curr_year = 2016    # current_year
save_results = True
export_all = False

# --------- Setup Data --------- #
# SetupDataE(export_all=export_all)
# SetupDataL(export_all=export_all)
CEPCase(util=101, util2=102, type='NGCC', state='AL', region='Southeast', capacity=1000, current_year=2016, forecast_year=2025)
# SetupDataL(respondent_id=102, forecast_year=2026, state='AL', export_all=False)
# SetupDataEIA()
# SetupDataE(102, 'NGCT', 800)

# --------- Calculate Constraints --------- #
# CalculateMonthlyEnergy(respondent_id=respondent_id, plant_type=plant_type, nameplate=nameplate)
# CEPCase(current_year=curr_year, forecast_year=forecast_year, util=respondent_id, util2=respondent_id_backup,
#         state=state, region1=region1)

print('le done')