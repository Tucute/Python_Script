import pandas as pd
from transform import create_table_1, create_table_2

def read_csv_and_print(file_path_1, file_path_2):
    try:
        A_TCURF = pd.read_csv(file_path_1, sep='\t')
        newTable = create_table_1(A_TCURF)
        print(newTable)
        newTable.to_csv('B_SS00_SS02_01_TT_TCURF_ORDER_BY.csv', sep='\t', index=False)
     
        A_TCURX = pd.read_csv(file_path_2, sep='\t')
        newTable = create_table_2(A_TCURX)
        newTable.to_csv('B_SS00_SS02_01B_IT_TCURX.csv', sep='\t', index=False)
        print(newTable)

    except Exception as e:
        print("Error:", e)

file_path_1 = "RUN_D01/A_PHASE_RUN_SQLITE/A_TCURF.csv"
file_path_2 = "RUN_D01/A_PHASE_RUN_SQLITE/A_TCURX.csv"
read_csv_and_print(file_path_1, file_path_2)