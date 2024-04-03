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

def create_table_B_SS00_SS02_03_IT_TCURF_LATEST_VALS(B_SS00_SS02_02_TT_TCURF_ORDER_BY):
     # Create table with columns
    B_SS00_SS02_03_IT_TCURF_LATEST_VALS = pd.DataFrame(columns=['TCURF_FCURR', 'TCURF_TCURR', 'TCURF_FFACT', 'TCURF_TFACT', 'TCURF_GDATU'])
    
    # Select columns from another table
    selected_cols = B_SS00_SS02_02_TT_TCURF_ORDER_BY[['TCURF_FCURR', 'TCURF_TCURR', 'TCURF_FFACT', 'TCURF_TFACT', 'TCURF_GDATU']]
    
    # Add a new column with row numbers
    selected_cols['rnk'] = selected_cols.groupby(['TCURF_FCURR', 'TCURF_TCURR']).cumcount() + 1
    
    # Filter for rows with rank = 1
    filtered = selected_cols[selected_cols['rnk'] == 1]
    
    # Sort the table by specified columns
    sorted_table = filtered.sort_values(by=['TCURF_FCURR', 'TCURF_TCURR', 'TCURF_FFACT', 'TCURF_TFACT', 'TCURF_GDATU'])
    
    # Rename the table
    sorted_table.rename(columns={'TCURF_FCURR': 'TCURF_FCURR', 'TCURF_TCURR': 'TCURF_TCURR', 'TCURF_FFACT': 'TCURF_FFACT', 'TCURF_TFACT': 'TCURF_TFACT', 'TCURF_GDATU': 'TCURF_GDATU'}, inplace=True)
    
    # Drop the temporary table
    B_SS00_SS02_02_TT_TCURF_ORDER_BY.drop()
    
    # Return the final table
    return sorted_table
    