import pandas as pd
from transform import create_table 

def read_csv_and_print(file_path):
    try:
        # Đọc tệp CSV và lưu vào DataFrame
        A_TCURF = pd.read_csv(file_path, sep='\t')
        print("DataFrame:")
        print(A_TCURF)
        print(A_TCURF.head())
        
        
        newTable = create_table(A_TCURF)
        newTable.to_csv('B_SS00_SS02_01_TT_TCURF_FILT_ORDER_BY_NEW.csv', sep='\t', index=False)
        # return A_TCURF
        print("Data Mới:")
        # print(newTable)
        print(newTable.head())
    except Exception as e:
        print("Lỗi đây này:", e)

file_path = "RUN_D01/A_PHASE_RUN_SQLITE/A_TCURF.csv"
read_csv_and_print(file_path)