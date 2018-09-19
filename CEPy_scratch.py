from energy_constraint import *
from hourly_net_load_constraint import *
# from eia_860 import *
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 30)
# SetupDataE(102, 'NGCC', 1000)
SetupDataL(respondent_id=102, forecast_year=2026, state='AL', export_all=True)
# SetupDataEIA()
# SetupDataE(102, 'NGCT', 800)
# Calculate_Monthly_Energy(dfpp, 10, 'NGCT', 1000)

# Tests for debugging 860 + 923 merge
# dfpp_outer = pd.read_excel('/Users/gglazer/PycharmProjects/CEP1/powerplant_df.xlsx')
# dfpp_outer_nullcap = dfpp_outer.loc[pd.isnull(dfpp_outer["('Nameplate Capacity (MW)', 'sum')"])]
# # dfpp_outer_nullcap.sort_values('Annual Energy', axis=0, ascending=False, inplace=True)
# print(dfpp_outer_nullcap)
#
# writer = pd.ExcelWriter('nullcapacity_df.xlsx', engine='xlsxwriter')
# dfpp_outer_nullcap.to_excel(writer)
# writer.save()
# test.acquire_ferc()

