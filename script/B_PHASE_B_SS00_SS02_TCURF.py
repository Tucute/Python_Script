import pandas as pd

def create_table_B_SS00_SS02_01_TT_TCURF_FILT(A_TCURF):
    df = pd.DataFrame(A_TCURF)
    df['TCURF_FCURR'] = df['FCURR'].str.strip()
    df['TCURF_TCURR'] = df['TCURR'].str.strip()
    df['TCURF_GDATU'] = 99999999 - df['GDATU']
    df['TCURF_FFACT'] = df['FFACT']
    df['TCURF_TFACT'] = df['TFACT']
    df = df[df['KURST'] == 'M']
    return df[['TCURF_FCURR', 'TCURF_TCURR', 'TCURF_GDATU', 'TCURF_FFACT', 'TCURF_TFACT']]

def create_table_B_SS00_SS02_02_TT_TCURF_ORDER_BY(B_SS00_SS02_01_TT_TCURF_FILT):
    B_SS00_SS02_02_TT_TCURF_ORDER_BY = pd.DataFrame({'TCURF_FCURR': B_SS00_SS02_01_TT_TCURF_FILT['TCURF_FCURR'], 'TCURF_TCURR': B_SS00_SS02_01_TT_TCURF_FILT['TCURF_TCURR'], 'TCURF_FFACT': B_SS00_SS02_01_TT_TCURF_FILT['TCURF_FFACT'], 'TCURF_TFACT': B_SS00_SS02_01_TT_TCURF_FILT['TCURF_TFACT'], 'TCURF_GDATU': B_SS00_SS02_01_TT_TCURF_FILT['TCURF_GDATU']})
    B_SS00_SS02_02_TT_TCURF_ORDER_BY.sort_values(by=['TCURF_FCURR', 'TCURF_TCURR', 'TCURF_GDATU'], inplace=True)
    B_SS00_SS02_01_TT_TCURF_FILT.drop(B_SS00_SS02_01_TT_TCURF_FILT.index, inplace=True)
    return B_SS00_SS02_02_TT_TCURF_ORDER_BY

# def create_table_B_SS00_SS02_03_IT_TCURF_LATEST_VALS(B_SS00_SS02_02_TT_TCURF_ORDER_BY):
#     # Add a new column with row numbers
#     B_SS00_SS02_02_TT_TCURF_ORDER_BY['rnk'] = B_SS00_SS02_02_TT_TCURF_ORDER_BY.groupby(['TCURF_FCURR', 'TCURF_TCURR']).cumcount() + 1
    
#     # Filter for rows with rank = 1
#     filtered = B_SS00_SS02_02_TT_TCURF_ORDER_BY[B_SS00_SS02_02_TT_TCURF_ORDER_BY['rnk'] == 1]
    
#     # Sort the table by specified columns
#     sorted_table = filtered.sort_values(by=['TCURF_FCURR', 'TCURF_TCURR', 'TCURF_FFACT', 'TCURF_TFACT', 'TCURF_GDATU'])
    
#     # Drop the 'rnk' column
#     sorted_table.drop(columns=['rnk'], inplace=True)
    
#     # Return the final table
#     return sorted_table

def create_table_B_SS00_SS02_03_IT_TCURF_LATEST_VALS(df):
    # Sắp xếp DataFrame theo 'TCURF_GDATU' giảm dần để đảm bảo dòng gần nhất được xử lý đầu tiên
    df_sorted = df.sort_values(by='TCURF_GDATU', ascending=False)
    
    # Thêm một cột 'rnk' với giá trị đếm dựa trên nhóm ['TCURF_FCURR', 'TCURF_TCURR'], tương tự như ROW_NUMBER() trong SQL
    df_sorted['rnk'] = df_sorted.groupby(['TCURF_FCURR', 'TCURF_TCURR']).cumcount() + 1
    
    # Lọc ra các dòng có 'rnk' = 1, tức là giữ lại dòng mới nhất cho mỗi cặp tiền tệ
    latest_vals = df_sorted[df_sorted['rnk'] == 1]
    
    # Bỏ cột 'rnk' vì không cần thiết cho kết quả cuối cùng
    latest_vals.drop(columns=['rnk'], inplace=True)
    
    # Sắp xếp lại DataFrame theo các trường được yêu cầu
    final_table = latest_vals.sort_values(by=['TCURF_FCURR', 'TCURF_TCURR', 'TCURF_FFACT', 'TCURF_TFACT', 'TCURF_GDATU'])
    
    return final_table