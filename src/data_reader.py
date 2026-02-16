import pandas as pd

wind_crk = pd.read_excel("../data/RWE_creek.xlsx", sheet_name="Sheet1")
desc = pd.read_excel("../data/RWE_creek.xlsx", sheet_name="Sheet2")
wind_crk["TTimeStamp"]= pd.to_datetime(wind_crk["TTimeStamp"],format='%Y-%m-%d %H:%M:%S.%f', errors="raise")