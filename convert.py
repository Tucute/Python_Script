import pandas as pd
from transform import create_table_B_SS00_SS02_03_IT_TCURF_LATEST_VALS 

def read_csv_and_print(file_path):
    try:
        A_TCURF = pd.read_csv(file_path, sep='\t')
        print("DATA FRAMES:")
        print(A_TCURF)
        print(A_TCURF.head())
        
        newTable = create_table_B_SS00_SS02_03_IT_TCURF_LATEST_VALS(A_TCURF)
        newTable.to_csv('B_SS00_SS02_01_TT_TCURF_FILT_ORDER_BY_NEW.csv', sep='\t', index=False)
        # return A_TCURF
        print("NEW DATA:")
        print(newTable.head())
    except Exception as e:
        print("Error:", e)

file_path = "RUN_D01/A_PHASE_RUN_SQLITE/A_TCURF.csv"
read_csv_and_print(file_path)