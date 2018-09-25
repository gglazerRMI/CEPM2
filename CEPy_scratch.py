from energy_constraint import *
from hourly_net_load_constraint import *
# from eia_860 import *


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

respondent_id = 209
respondent_id_backup = 209
state = 'NE'
region = 'MidWest'
forecast_year = 2020
# Will skip leap days if leap year selected
curr_year = 2016

# --------- Setup Data --------- #
# SetupDataE(102, 'NGCC', 1000)
SetupDataL()
# SetupDataL(respondent_id=102, forecast_year=2026, state='AL', export_all=False)
# SetupDataEIA()
# SetupDataE(102, 'NGCT', 800)

# --------- Calculate Constraints --------- #
# Calculate_Monthly_Energy(dfpp, 10, 'NGCT', 1000)


