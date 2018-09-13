from energy_constraint import *
# from CEPy import *
# from eia_860 import *
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 30)
# SetupData(2016)
# SetupDataEIA()
SetupData(102, 'NGCT', 800)
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

