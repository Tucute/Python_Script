import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def create_latest_table(A_TCURR):
    Updated_A_TCURR = update_table_ATCURR(A_TCURR)
    B_SS00_SS01_00_01_TT_TCURR_SORT_A = create_table_B_SS00_SS01_00_01_TT_TCURR_SORT_A(Updated_A_TCURR)
    B_SS00_SS01_00_02_TT_TCURR_SORT_D = create_table_B_SS00_SS01_00_02_TT_TCURR_SORT_D(Updated_A_TCURR)
    B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP = create_table_B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP(B_SS00_SS01_00_01_TT_TCURR_SORT_A)
    B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP = create_table_B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP(B_SS00_SS01_00_02_TT_TCURR_SORT_D)
    B_SS00_SS01_01_TT_TCURR = create_table_B_SS00_SS01_01_TT_TCURR(A_TCURR)
    B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP_2 = create_table_B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP_2(B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP)
    B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP_2 = create_table_B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP_2(B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP)
    B_SS00_SS01_02_TT_TCURR_SORT = create_table_B_SS00_SS01_02_TT_TCURR_SORT(B_SS00_SS01_01_TT_TCURR)
    B_SS00_SS01_03_TT_TCURR = create_table_B_SS00_SS01_03_TT_TCURR(B_SS00_SS01_02_TT_TCURR_SORT)
    B_SS00_SS01_03_TT_TCURR_DAYSTART = create_table_B_SS00_SS01_03_TT_TCURR_DAYSTART(B_SS00_SS01_03_TT_TCURR)
    B_SS00_SS01_04_TT_TCURR = create_table_B_SS00_SS01_04_TT_TCURR(B_SS00_SS01_03_TT_TCURR_DAYSTART, B_SS00_SS01_03_TT_TCURR)
    return B_SS00_SS01_04_TT_TCURR
#   Step 0/ list of currencies and start and end dates, for oppending to currency table
#   Step 0.1/ Load TCURR so that we can do order by 

def update_table_ATCURR(A_TCURR):
    A_TCURR['ZF_BEG_DATE'] = None
    A_TCURR['ZF_END_DATE'] = None
    A_TCURR['ZF_GDATU'] = None
    A_TCURR['ZF_TCURR_UKURS'] = None

    A_TCURR['ZF_BEG_DATE'] = A_TCURR['ZF_BEG_DATE'].apply(lambda x: datetime.strptime(x, '%Y%m%d'))
    A_TCURR['ZF_END_DATE'] = A_TCURR['ZF_END_DATE'].apply(lambda x: datetime.strptime(x, '%Y%m%d'))
    A_TCURR['ZF_TCURR_UKURS'] = A_TCURR['UKURS'].apply(lambda x: float(x.replace('-', '')) if x[-1] == '-' else float(x))
    A_TCURR['ZF_GDATU'] = A_TCURR['GDATU'].apply(lambda x: 99999999 - x)
    
    A_TCURR['ZF_BEG_DATE'] = '19940101'
    A_TCURR['ZF_END_DATE'] = '20230202'
    return A_TCURR

def create_table_B_SS00_SS01_00_01_TT_TCURR_SORT_A(A_TCURR):
    # Step 0.2/ Order by ascending date for obtaining the first exchange rate
    B_SS00_SS01_00_01_TT_TCURR_SORT_A = A_TCURR.sort_values(by=['FCURR', 'TCURR', 'ZF_GDATU'])
    return B_SS00_SS01_00_01_TT_TCURR_SORT_A
    
def create_table_B_SS00_SS01_00_02_TT_TCURR_SORT_D(A_TCURR):
    # Step 0.3/ Order by descending for obtaining the last exchange rate
    B_SS00_SS01_00_02_TT_TCURR_SORT_D = A_TCURR.sort_values(by=['FCURR', 'TCURR', 'ZF_GDATU'], ascending=False)
    return B_SS00_SS01_00_02_TT_TCURR_SORT_D

def create_table_B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP(B_SS00_SS01_00_01_TT_TCURR_SORT_A):
    # Step 0.4/ Get one record per currency pair with earliest exchange rate and beginning date
    # Add key to look up in main currency table to see if not exists (in lower step)
    B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP = B_SS00_SS01_00_01_TT_TCURR_SORT_A[(B_SS00_SS01_00_01_TT_TCURR_SORT_A['ZF_GDATU'] >= 19940101) & (B_SS00_SS01_00_01_TT_TCURR_SORT_A['ZF_GDATU'] <= 20230202) & (B_SS00_SS01_00_01_TT_TCURR_SORT_A['KURST'] == 'M') & (B_SS00_SS01_00_01_TT_TCURR_SORT_A['TCURR'] == 'USD')].groupby(['FCURR', 'TCURR', 'KURST', 'ZF_BEG_DATE']).agg({'ZF_TCURR_UKURS': 'min'}).reset_index()
    B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP['ZF_TCURR_FCURR_BEGD'] = B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP['FCURR'].str.strip() + B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP['TCURR'].str.strip() + B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP['ZF_BEG_DATE'].str.strip()
    return B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP


# Step 0.5/ Get one record per currency pair with latest exchange rate and latest date
# Add key to look up in main currency table to see if not exists (in lower step)
def create_table_B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP(B_SS00_SS01_00_02_TT_TCURR_SORT_D):
    B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP = B_SS00_SS01_00_02_TT_TCURR_SORT_D[(B_SS00_SS01_00_02_TT_TCURR_SORT_D['ZF_GDATU'] >= 19940101) & (B_SS00_SS01_00_02_TT_TCURR_SORT_D['ZF_GDATU'] <= 20230202) & (B_SS00_SS01_00_02_TT_TCURR_SORT_D['KURST'] == 'M') & (B_SS00_SS01_00_02_TT_TCURR_SORT_D['TCURR'] == 'USD')].groupby(['FCURR', 'TCURR', 'KURST', 'ZF_END_DATE']).agg({'ZF_TCURR_UKURS': 'min'}).reset_index()
    B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP['ZF_TCURR_FCURR_ENDD'] = B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP['FCURR'].str.strip() + B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP['TCURR'].str.strip() + B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP['ZF_END_DATE'].str.strip()
    return B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP

# -- Step 1/ Currencies table
# -- Filter the currencies table on the beginning and ending date
# -- Add beginning and ending dates, for each combination of from and to currencies
def create_table_B_SS00_SS01_01_TT_TCURR(A_TCURR):
    B_SS00_SS01_01_TT_TCURR = pd.DataFrame(columns=['TCURR_FCURR', 'TCURR_FFACT', 'TCURR_GDATU', 'TCURR_KURST', 'TCURR_TCURR', 'TCURR_TFACT', 'ZF_TCURR_SORT_KEY', 'ZF_TCURR_FCURR_BEGD', 'ZF_TCURR_FCURR_ENDD', 'TCURR_UKURS'])
    
    B_SS00_SS01_01_TT_TCURR['TCURR_FCURR'] = A_TCURR['FCURR']
    B_SS00_SS01_01_TT_TCURR['TCURR_FFACT'] = A_TCURR['FFACT']
    B_SS00_SS01_01_TT_TCURR['TCURR_GDATU'] = A_TCURR['ZF_GDATU']
    B_SS00_SS01_01_TT_TCURR['TCURR_KURST'] = A_TCURR['KURST']
    B_SS00_SS01_01_TT_TCURR['TCURR_TCURR'] = A_TCURR['TCURR']
    B_SS00_SS01_01_TT_TCURR['TCURR_TFACT'] = A_TCURR['TFACT']
    
    B_SS00_SS01_01_TT_TCURR['ZF_TCURR_SORT_KEY'] = A_TCURR['FCURR'] + A_TCURR['TCURR'] + A_TCURR['ZF_GDATU']
    B_SS00_SS01_01_TT_TCURR['ZF_TCURR_FCURR_BEGD'] = A_TCURR['FCURR'] + A_TCURR['TCURR'] + A_TCURR['ZF_GDATU']
    B_SS00_SS01_01_TT_TCURR['ZF_TCURR_FCURR_ENDD'] = A_TCURR['FCURR'] + A_TCURR['TCURR'] + A_TCURR['ZF_GDATU']
    B_SS00_SS01_01_TT_TCURR = B_SS00_SS01_01_TT_TCURR[(B_SS00_SS01_01_TT_TCURR['ZF_GDATU'] >= 19940101) & (B_SS00_SS01_01_TT_TCURR['ZF_GDATU'] <= 20230202) & (B_SS00_SS01_01_TT_TCURR['KURST'] == 'M') & (B_SS00_SS01_01_TT_TCURR['TCURR'] == 'USD')]
    return B_SS00_SS01_01_TT_TCURR

# -- // Add the records relating to the start and end dates,
# -- // but only if the beginning or ending date does not exist 
# -- // for the from and to currencies, in the currencies table
def create_table_B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP_2(B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP):
    B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP_2 = pd.DataFrame(columns=['TCURR_FCURR', 'TCURR_FFACT', 'TCURR_GDATU', 'TCURR_KURST', 'TCURR_TCURR', 'TCURR_TFACT', 'ZF_TCURR_SORT_KEY', 'ZF_TCURR_FCURR_BEGD', 'ZF_TCURR_FCURR_ENDD', 'TCURR_UKURS'])
    B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP_2 = B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP
    B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP_2.rename(columns={'ZF_TCURR_FCURR_BEGD': 'ZF_TCURR_SORT_KEY'}, inplace=True)
    return B_SS00_SS01_00_03_IT_TCURR_BEG_DATE_TEMP_2


def create_table_B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP_2(B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP):
    B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP_2 = pd.DataFrame(columns=['TCURR_FCURR', 'TCURR_FFACT', 'TCURR_GDATU', 'TCURR_KURST', 'TCURR_TCURR', 'TCURR_TFACT', 'ZF_TCURR_SORT_KEY', 'ZF_TCURR_FCURR_BEGD', 'ZF_TCURR_FCURR_ENDD', 'TCURR_UKURS'])
    B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP_2 = B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP
    B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP_2.rename(columns={'ZF_TCURR_FCURR_ENDD': 'ZF_TCURR_SORT_KEY'}, inplace=True)
    return B_SS00_SS01_00_04_IT_TCURR_END_DATE_TEMP_2

# -- Step 2/ Sort by date to get exchange rates from newest to oldest
def create_table_B_SS00_SS01_02_TT_TCURR_SORT(B_SS00_SS01_01_TT_TCURR):
    B_SS00_SS01_02_TT_TCURR_SORT = []
    for row in B_SS00_SS01_01_TT_TCURR:
        B_SS00_SS01_02_TT_TCURR_SORT.append(row)
    B_SS00_SS01_02_TT_TCURR_SORT.sort(key=lambda x: x['ZF_TCURR_SORT_KEY'])
    # del B_SS00_SS01_01_TT_TCURR
    return B_SS00_SS01_02_TT_TCURR_SORT

#     -- Step 3/ Obtain the previous date and the previous exchange rate
# -- if it is a new currency key, or the first record, otherwise take date of the line
def create_table_B_SS00_SS01_03_TT_TCURR(B_SS00_SS01_02_TT_TCURR_SORT):
    B_SS00_SS01_03_TT_TCURR = B_SS00_SS01_02_TT_TCURR_SORT.copy()

    # add new columns
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_FCURR'] = ''
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_TCURR'] = ''
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_GDATU'] = ''
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_UKURS'] = ''

    # use LAG function to get previous values
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_FCURR'] = B_SS00_SS01_03_TT_TCURR['TCURR_FCURR'].shift(1)
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_TCURR'] = B_SS00_SS01_03_TT_TCURR['TCURR_TCURR'].shift(1)
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_GDATU'] = B_SS00_SS01_03_TT_TCURR['TCURR_GDATU'].shift(1)
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_UKURS'] = B_SS00_SS01_03_TT_TCURR['TCURR_UKURS'].shift(1)

    # use IIF function to check if current values are equal to previous values and if so, assign previous values to new columns
    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_GDATU'] = np.where((B_SS00_SS01_03_TT_TCURR['TCURR_FCURR'] == B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_FCURR']) & (B_SS00_SS01_03_TT_TCURR['TCURR_TCURR'] == B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_TCURR']) & (B_SS00_SS01_03_TT_TCURR['TCURR_GDATU'] > B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_GDATU']), B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_GDATU'], '')

    B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_UKURS'] = np.where((B_SS00_SS01_03_TT_TCURR['TCURR_FCURR'] == B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_FCURR']) & (B_SS00_SS01_03_TT_TCURR['TCURR_TCURR'] == B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_TCURR']), B_SS00_SS01_03_TT_TCURR['ZF_PRE_TCURR_UKURS'], '')

    # order by ZF_TCURR_SORT_KEY
    B_SS00_SS01_03_TT_TCURR = B_SS00_SS01_03_TT_TCURR.sort_values(by='ZF_TCURR_SORT_KEY')
        # drop table B_SS00_SS01_02_TT_TCURR_SORT
    # B_SS00_SS01_02_TT_TCURR_SORT = None
    return B_SS00_SS01_03_TT_TCURR
    
# -- Step 4/ Fill down date for where there are missing dates
def create_table_B_SS00_SS01_03_TT_TCURR_DAYSTART(B_SS00_SS01_03_TT_TCURR):
    B_SS00_SS01_03_TT_TCURR_DAYSTART = []

    # Loop through each row in B_SS00_SS01_03_TT_TCURR
    for row in B_SS00_SS01_03_TT_TCURR:
        # Extract year, month, and day from ZF_PRE_TCURR_GDATU
        year = row['ZF_PRE_TCURR_GDATU'][0:4]
        month = row['ZF_PRE_TCURR_GDATU'][4:6]
        day = row['ZF_PRE_TCURR_GDATU'][6:8]

        # Create new column ZF_PRE_TCURR_GDATU_DAYSTART with date format
        row['ZF_PRE_TCURR_GDATU_DAYSTART'] = year + '-' + month + '-' + day

        # Extract year, month, and day from TCURR_GDATU
        year = row['TCURR_GDATU'][0:4]
        month = row['TCURR_GDATU'][4:6]
        day = row['TCURR_GDATU'][6:8]

        # Create new column TCURR_GDATU_DAYSTART with date format
        row['TCURR_GDATU_DAYSTART'] = year + '-' + month + '-' + day

        # Add row to B_SS00_SS01_03_TT_TCURR_DAYSTART
        B_SS00_SS01_03_TT_TCURR_DAYSTART.append(row)
    return B_SS00_SS01_03_TT_TCURR_DAYSTART

def create_table_B_SS00_SS01_04_TT_TCURR(B_SS00_SS01_03_TT_TCURR_DAYSTART, B_SS00_SS01_03_TT_TCURR):
    B_SS00_SS01_04_TT_TCURR = []

    # Create recursive function
    def b_ss00_ss01_03_tt_tcurr_while(row):
        # Add row to B_SS00_SS01_04_TT_TCURR
        B_SS00_SS01_04_TT_TCURR.append(row)

        # Check if ZF_TCURR_GDATU is less than TCURR_GDATU_DAYSTART - 1 day
        if row['ZF_TCURR_GDATU'] < row['TCURR_GDATU_DAYSTART'] - timedelta(days=1):
            # Create new row with incremented date
            new_row = {
                'TCURR_FCURR': row['TCURR_FCURR'],
                'TCURR_FFACT': row['TCURR_FFACT'],
                'TCURR_GDATU': row['TCURR_GDATU'],
                'TCURR_KURST': row['TCURR_KURST'],
                'TCURR_TCURR': row['TCURR_TCURR'],
                'TCURR_TFACT': row['TCURR_TFACT'],
                'ZF_TCURR_SORT_KEY': row['ZF_TCURR_SORT_KEY'],
                'ZF_TCURR_FCURR_BEGD': row['ZF_TCURR_FCURR_BEGD'],
                'ZF_TCURR_FCURR_ENDD': row['ZF_TCURR_FCURR_ENDD'],
                'TCURR_UKURS': row['TCURR_UKURS'],
                'ZF_PRE_TCURR_FCURR': row['ZF_PRE_TCURR_FCURR'],
                'ZF_PRE_TCURR_TCURR': row['ZF_PRE_TCURR_TCURR'],
                'ZF_PRE_TCURR_GDATU': row['ZF_PRE_TCURR_GDATU'],
                'ZF_PRE_TCURR_UKURS': row['ZF_PRE_TCURR_UKURS'],
                'ZF_PRE_TCURR_GDATU_DAYSTART': row['ZF_PRE_TCURR_GDATU_DAYSTART'],
                'TCURR_GDATU_DAYSTART': row['TCURR_GDATU_DAYSTART'],
                'ZF_TCURR_GDATU': row['ZF_TCURR_GDATU'] + timedelta(days=1)
            }

            # Call recursive function with new row
            b_ss00_ss01_03_tt_tcurr_while(new_row)

    # Loop through each row in B_SS00_SS01_03_TT_TCURR_DAYSTART
    for row in B_SS00_SS01_03_TT_TCURR_DAYSTART:
        # Call recursive function with current row
        b_ss00_ss01_03_tt_tcurr_while(row)
    
    # create a new table
    B_SS00_SS01_04_TT_TCURR_ADD = []
    
    # create a new column with a ranking based on the values in TCURR_FCURR and TCURR_TCURR, ordered by TCURR_GDATU in descending order
    B_SS00_SS01_03_TT_TCURR['rnk'] = B_SS00_SS01_03_TT_TCURR.groupby(['TCURR_FCURR', 'TCURR_TCURR'])['TCURR_GDATU'].rank(ascending=False)
    
    # select all rows where the ranking is equal to 1
    RankedRows = B_SS00_SS01_03_TT_TCURR[B_SS00_SS01_03_TT_TCURR['rnk'] == 1]
    
    # group the selected rows by TCURR_FCURR and TCURR_TCURR
    GroupedRows = RankedRows.groupby(['TCURR_FCURR', 'TCURR_TCURR'])
    
    # insert the grouped rows into the new table
    B_SS00_SS01_04_TT_TCURR_ADD = GroupedRows
    
    # create a new table with the same columns as the original table
    B_SS00_SS01_04_TT_TCURR = pd.DataFrame(columns=['TCURR_FCURR', 'TCURR_FFACT', 'TCURR_GDATU', 'TCURR_KURST', 'TCURR_TCURR', 'TCURR_TFACT', 'ZF_TCURR_SORT_KEY', 'ZF_TCURR_FCURR_BEGD', 'ZF_TCURR_FCURR_ENDD', 'TCURR_UKURS', 'ZF_PRE_TCURR_FCURR', 'ZF_PRE_TCURR_TCURR', 'ZF_PRE_TCURR_UKURS', 'ZF_PRE_TCURR_GDATU', 'ZF_TCURR_GDATU'])
    
    # insert the values from the grouped rows into the new table
    B_SS00_SS01_04_TT_TCURR['TCURR_FCURR'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_FCURR']
    B_SS00_SS01_04_TT_TCURR['TCURR_FFACT'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_FFACT']
    B_SS00_SS01_04_TT_TCURR['TCURR_GDATU'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_GDATU']
    B_SS00_SS01_04_TT_TCURR['TCURR_KURST'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_KURST']
    B_SS00_SS01_04_TT_TCURR['TCURR_TCURR'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_TCURR']
    B_SS00_SS01_04_TT_TCURR['TCURR_TFACT'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_TFACT']
    B_SS00_SS01_04_TT_TCURR['ZF_TCURR_SORT_KEY'] = B_SS00_SS01_04_TT_TCURR_ADD['ZF_TCURR_SORT_KEY']
    B_SS00_SS01_04_TT_TCURR['ZF_TCURR_FCURR_BEGD'] = B_SS00_SS01_04_TT_TCURR_ADD['ZF_TCURR_FCURR_BEGD']
    B_SS00_SS01_04_TT_TCURR['ZF_TCURR_FCURR_ENDD'] = B_SS00_SS01_04_TT_TCURR_ADD['ZF_TCURR_FCURR_ENDD']
    B_SS00_SS01_04_TT_TCURR['TCURR_UKURS'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_UKURS']
    B_SS00_SS01_04_TT_TCURR['ZF_PRE_TCURR_FCURR'] = B_SS00_SS01_04_TT_TCURR_ADD['ZF_PRE_TCURR_FCURR']
    B_SS00_SS01_04_TT_TCURR['ZF_PRE_TCURR_TCURR'] = B_SS00_SS01_04_TT_TCURR_ADD['ZF_PRE_TCURR_TCURR']
    B_SS00_SS01_04_TT_TCURR['ZF_PRE_TCURR_UKURS'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_UKURS']
    B_SS00_SS01_04_TT_TCURR['ZF_PRE_TCURR_GDATU'] = B_SS00_SS01_04_TT_TCURR_ADD['ZF_PRE_TCURR_GDATU']
    
    # create a new column with the date in the format YYYY-MM-DD based on the values in TCURR_GDATU
    B_SS00_SS01_04_TT_TCURR['ZF_TCURR_GDATU'] = B_SS00_SS01_04_TT_TCURR_ADD['TCURR_GDATU'].apply(lambda x: datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d'))
    
    # drop the temporary table
    B_SS00_SS01_04_TT_TCURR_ADD = None
    
    return B_SS00_SS01_04_TT_TCURR