from CEPy import *
from eia_923 import *

pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 30)
# test = SetupData(2015)
test = SetupData().setup_923_monthly()

# test.acquire_ferc()