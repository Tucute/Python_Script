import pandas as pd
import numpy as np

# Step 1.1 COA per company code

def create_SKB1_TEMP(A_SKB1):
    SKB1_TEMP = pd.DataFrame()
    SKB1_TEMP['SKB1_BUKRS'] = A_SKB1['BUKRS']
    SKB1_TEMP['SKB1_ERDAT'] = A_SKB1['ERDAT']
    SKB1_TEMP['SKB1_ERNAM'] = A_SKB1['ERNAM']
    SKB1_TEMP['SKB1_HBKID'] = A_SKB1['HBKID']
    SKB1_TEMP['SKB1_HKTID'] = A_SKB1['HKTID']
    SKB1_TEMP['SKB1_SAKNR'] = A_SKB1['SAKNR'].astype(str).apply(lambda x: x.zfill(10) if isinstance(x, str) and x.isdigit() else x)
    SKB1_TEMP['SKB1_WAERS'] = A_SKB1['WAERS']
    SKB1_TEMP['SKB1_XGKON'] = A_SKB1['XGKON']
    SKB1_TEMP['SKB1_STEXT'] = A_SKB1['STEXT']
    SKB1_TEMP['SKB1_FIPLS'] = A_SKB1['FIPLS'].astype(str).str.zfill(3)  
    SKB1_TEMP['SKB1_XINTB'] = A_SKB1['XINTB']
    SKB1_TEMP['SKB1_XOPVW'] = A_SKB1['XOPVW']
    SKB1_TEMP['SKB1_BEWGP'] = A_SKB1['BEWGP']
    SKB1_TEMP['SKB1_XLGCLR'] = A_SKB1['XLGCLR']
    SKB1_TEMP['SKB1_MITKZ'] = A_SKB1['MITKZ']
    SKB1_TEMP['ZF_KEY_JN_USR21_SKB1'] = A_SKB1['ERNAM']
    SKB1_TEMP['ZF_KEY_JN_USER_ADDR_SKA1'] = A_SKB1['ERNAM']
    SKB1_TEMP['ZF_KEY_JN_T001_SKB1'] = A_SKB1['BUKRS'].astype(str).str.zfill(4)
    SKB1_TEMP['ZF_KEY_JN_T012K_SKB1'] = A_SKB1['BUKRS'].astype(str).str.zfill(4) + A_SKB1['SAKNR'].astype(str).apply(lambda x: x.zfill(10) if isinstance(x, str) and x.isdigit() else x)
    SKB1_TEMP['ZF_KEY_JN_REGUH_SKB1'] = A_SKB1['BUKRS'].astype(str).str.zfill(4) + A_SKB1['SAKNR'].astype(str).apply(lambda x: x.zfill(10) if isinstance(x, str) and x.isdigit() else x)
    SKB1_TEMP['ZF_KEY_JN_FEBKO_SKB1'] = A_SKB1['BUKRS'].astype(str).str.zfill(4) + A_SKB1['SAKNR'].astype(str).apply(lambda x: x.zfill(10) if isinstance(x, str) and x.isdigit() else x)
    
    return SKB1_TEMP

# Step 1.2/ Add user cost center and personnel number

def create_SKB1_TEMP_2(A_USR21):
    SKB1_TEMP_2 = pd.DataFrame()
    SKB1_TEMP_2['USR21_BNAME_SKB1'] = A_USR21['BNAME']
    SKB1_TEMP_2['USR21_KOSTL_SKB1'] = A_USR21['KOSTL']
    SKB1_TEMP_2['USR21_PERSNUMBER_SKB1'] = A_USR21['PERSNUMBER'].astype(str).apply(lambda x: x.zfill(10) if isinstance(x, str) and x.isdigit() else x)
    SKB1_TEMP_2['ZF_KEY_JN_USR21_SKB1'] = A_USR21['BNAME']
    SKB1_TEMP_2 = SKB1_TEMP_2.groupby('USR21_BNAME_SKB1').agg({'USR21_BNAME_SKB1': 'first', 'USR21_KOSTL_SKB1': 'first', 'USR21_PERSNUMBER_SKB1': 'first', 'ZF_KEY_JN_USR21_SKB1': 'first'})
    return SKB1_TEMP_2

# Step 1.3/ Add user name

def create_SKB1_TEMP_3(A_USER_ADDR):    
    SKB1_TEMP_3 = pd.DataFrame(A_USER_ADDR.groupby('BNAME').agg({'BNAME': 'first', 'NAME_TEXTC': 'first'}))
    SKB1_TEMP_3.rename(columns={'BNAME': 'USER_ADDR_BNAME_SKA1', 'NAME_TEXTC': 'USER_ADDR_NAME_TEXTC_SKA1'}, inplace=True)
    SKB1_TEMP_3['ZF_KEY_JN_USER_ADDR_SKA1'] = SKB1_TEMP_3['USER_ADDR_BNAME_SKA1']
    return SKB1_TEMP_3
# Step 1.4/ Add indicator that the account is found in house banks
def create_SKB1_TEMP_4(A_T012K):
    SKB1_TEMP_4= pd.DataFrame()
    print(A_T012K['HKONT'].dtype)

    SKB1_TEMP_4['T012K_BUKRS_SKB1']=A_T012K['BUKRS'].astype(str).apply(lambda x: x.zfill(4) if isinstance(x, str) and x.isdigit() else x)
    SKB1_TEMP_4['T012K_HKONT_SKB1'] = A_T012K['HKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_4['T012K_HBKID_SKB1']=A_T012K['HBKID']
    SKB1_TEMP_4['ZF_KEY_JN_T012K_SKB1'] = SKB1_TEMP_4['T012K_BUKRS_SKB1'] + SKB1_TEMP_4['T012K_HKONT_SKB1']
    SKB1_TEMP_4 = SKB1_TEMP_4.groupby(['T012K_BUKRS_SKB1','T012K_HKONT_SKB1']).agg({'T012K_BUKRS_SKB1': 'first', 'T012K_HKONT_SKB1': 'first','T012K_HBKID_SKB1':'first','ZF_KEY_JN_T012K_SKB1':'first'})

    return SKB1_TEMP_4
# Step 1.5/ Add indicator that the account is found in the payment program
def create_SKB1_TEMP_5(A_REGUH):
    SKB1_TEMP_5= pd.DataFrame()
    SKB1_TEMP_5['REGUH_ZBUKR_SKB1'] = A_REGUH['ZBUKR'].apply(lambda x: str(int(x)).zfill(4) if pd.notnull(x) and str(x).strip().isdigit() else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_5['REGUH_HKONT_SKB1'] = A_REGUH['HKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_5['ZF_REGUH_HKONT_COUNT'] = A_REGUH['HKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_5['ZF_KEY_JN_REGUH_SKB1'] = A_REGUH['ZBUKR'].apply(lambda x: str(int(x)).zfill(4) if pd.notnull(x) and str(x).strip().isdigit() else '' if pd.isnull(x) else str(x))+ A_REGUH['HKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_5 = SKB1_TEMP_5.groupby(['REGUH_ZBUKR_SKB1','REGUH_HKONT_SKB1']).agg({'REGUH_ZBUKR_SKB1': 'first', 'REGUH_HKONT_SKB1': 'first','ZF_REGUH_HKONT_COUNT':'first','ZF_KEY_JN_REGUH_SKB1':'first'})

    return SKB1_TEMP_5
# Step 1.6/ Add indicator that the account was found in electronic bank statements
def create_SKB1_TEMP_6(A_FEBKO):
    SKB1_TEMP_6= pd.DataFrame()
    SKB1_TEMP_6['FEBKO_BUKRS_SKB1'] = A_FEBKO['BUKRS'].apply(lambda x: str(int(x)).zfill(4) if pd.notnull(x) and str(x).strip().isdigit() else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_6['FEBKO_HKONT_SKB1'] = A_FEBKO['HKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_6['ZF_FEBKO_HKONT_COUNT'] = A_FEBKO['HKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_6['ZF_KEY_JN_FEBKO_SKB1'] = A_FEBKO['BUKRS'].apply(lambda x: str(int(x)).zfill(4) if pd.notnull(x) and str(x).strip().isdigit() else '' if pd.isnull(x) else str(x))+ A_FEBKO['HKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    SKB1_TEMP_6 = SKB1_TEMP_6.groupby(['FEBKO_BUKRS_SKB1','FEBKO_HKONT_SKB1']).agg({'FEBKO_BUKRS_SKB1': 'first', 'FEBKO_HKONT_SKB1': 'first','ZF_FEBKO_HKONT_COUNT':'first','ZF_KEY_JN_FEBKO_SKB1':'first'})

    return SKB1_TEMP_6
# Step 1.6/ Add the local currency key and the chart of accounts
# (Usually a company can only be in one chart of accounts)
def create_SKB1_TEMP_7(A_T001):
    SKB1_TEMP_7= pd.DataFrame()
    SKB1_TEMP_7['T001_BUKRS_SKB1'] = A_T001['BUKRS']
    SKB1_TEMP_7['T001_BUTXT_SKB1'] = A_T001['BUTXT']
    SKB1_TEMP_7['T001_KTOPL_SKB1'] = A_T001['KTOPL']
    SKB1_TEMP_7['T001_WAERS_SKB1'] = A_T001['WAERS']
    SKB1_TEMP_7['ZF_KEY_JN_T001_SKB1'] = A_T001['BUKRS']
    SKB1_TEMP_7 = SKB1_TEMP_7.groupby('T001_BUKRS_SKB1').agg({'T001_BUKRS_SKB1': 'first', 'T001_BUTXT_SKB1': 'first','T001_KTOPL_SKB1':'first','T001_WAERS_SKB1':'first','ZF_KEY_JN_T001_SKB1':'first'})

    return SKB1_TEMP_7

def create_B_SS00_SS04_01_TT_SKB1_ADD_INFO():
    path_to_A_SKB1 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_SKB1.csv"
    A_SKB1 = pd.read_csv(path_to_A_SKB1, sep='\t')
    
    path_to_A_USR21 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_USR21.csv"
    A_USR21 = pd.read_csv(path_to_A_USR21, sep='\t')
    
    path_to_A_USER_ADDR = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_USER_ADDR.csv"
    A_USER_ADDR = pd.read_csv(path_to_A_USER_ADDR, sep='\t')
    
    path_to_A_T012K = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T012K.csv"
    A_T012K = pd.read_csv(path_to_A_T012K, sep='\t')
    
    path_to_A_REGUH = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_REGUH.csv"
    A_REGUH = pd.read_csv(path_to_A_REGUH, sep='\t')
    
    path_to_A_FEBKO = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_FEBKO.csv"
    A_FEBKO = pd.read_csv(path_to_A_FEBKO, sep='\t')
    
    path_to_A_T001 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T001.csv"
    A_T001 = pd.read_csv(path_to_A_T001, sep='\t')
    
    SKB1_TEMP = create_SKB1_TEMP(A_SKB1)
    SKB1_TEMP_2 = create_SKB1_TEMP_2(A_USR21)
    SKB1_TEMP_3 = create_SKB1_TEMP_3(A_USER_ADDR)
    SKB1_TEMP_4 = create_SKB1_TEMP_4(A_T012K)
    SKB1_TEMP_5 = create_SKB1_TEMP_5(A_REGUH)
    SKB1_TEMP_6 = create_SKB1_TEMP_6(A_FEBKO)
    SKB1_TEMP_7 = create_SKB1_TEMP_7(A_T001)
    
    B_SS00_SS04_01_TT_SKB1_ADD_INFO = SKB1_TEMP.merge(SKB1_TEMP_2[['ZF_KEY_JN_USR21_SKB1', 'USR21_BNAME_SKB1', 'USR21_KOSTL_SKB1', 'USR21_PERSNUMBER_SKB1']], how='left', left_on='ZF_KEY_JN_USR21_SKB1', right_on='ZF_KEY_JN_USR21_SKB1') \
        .merge(SKB1_TEMP_3[['ZF_KEY_JN_USER_ADDR_SKA1', 'USER_ADDR_BNAME_SKA1', 'USER_ADDR_NAME_TEXTC_SKA1']], how='left', left_on='ZF_KEY_JN_USER_ADDR_SKA1', right_on='ZF_KEY_JN_USER_ADDR_SKA1') \
        .merge(SKB1_TEMP_4[['ZF_KEY_JN_T012K_SKB1', 'T012K_BUKRS_SKB1', 'T012K_HKONT_SKB1', 'T012K_HBKID_SKB1']], how='left', left_on='ZF_KEY_JN_T012K_SKB1', right_on='ZF_KEY_JN_T012K_SKB1') \
        .merge(SKB1_TEMP_5[['ZF_KEY_JN_REGUH_SKB1', 'REGUH_ZBUKR_SKB1', 'REGUH_HKONT_SKB1', 'ZF_REGUH_HKONT_COUNT']], how='left', left_on='ZF_KEY_JN_REGUH_SKB1', right_on='ZF_KEY_JN_REGUH_SKB1') \
        .merge(SKB1_TEMP_6[['ZF_KEY_JN_FEBKO_SKB1', 'FEBKO_BUKRS_SKB1', 'FEBKO_HKONT_SKB1', 'ZF_FEBKO_HKONT_COUNT']], how='left', left_on='ZF_KEY_JN_FEBKO_SKB1', right_on='ZF_KEY_JN_FEBKO_SKB1') \
        .merge(SKB1_TEMP_7[['ZF_KEY_JN_T001_SKB1', 'T001_BUKRS_SKB1', 'T001_BUTXT_SKB1', 'T001_KTOPL_SKB1', 'T001_WAERS_SKB1']], how='left', left_on='ZF_KEY_JN_T001_SKB1', right_on='ZF_KEY_JN_T001_SKB1')
    
    return B_SS00_SS04_01_TT_SKB1_ADD_INFO

B_SS00_SS04_01_TT_SKB1_ADD_INFO = create_B_SS00_SS04_01_TT_SKB1_ADD_INFO()


# B_SS00_SS04_01_TT_SKB1_ADD_INFO.to_csv('B_SS00_SS04_01_TT_SKB1_ADD_INFO.csv', sep='\t', index=False)

# Step 2/ Create key: chart of accounts & account

def create_SKB1_ADD_SKA1_TEMP(B_SS00_SS04_01_TT_SKB1_ADD_INFO):
    SKB1_ADD_SKA1_TEMP = B_SS00_SS04_01_TT_SKB1_ADD_INFO.copy()
    SKB1_ADD_SKA1_TEMP['ZF_KEY_JN_SKA1_SKB1'] = SKB1_ADD_SKA1_TEMP['T001_KTOPL_SKB1'] + SKB1_ADD_SKA1_TEMP['SKB1_SAKNR'].astype(str)
    return SKB1_ADD_SKA1_TEMP
SKB1_ADD_SKA1_TEMP = create_SKB1_ADD_SKA1_TEMP(B_SS00_SS04_01_TT_SKB1_ADD_INFO)
# SKB1_ADD_SKA1_TEMP.to_csv('SKB1_ADD_SKA1_TEMP.csv', sep='\t', index=False)

# Step 2.1/ Add the central account master information

def create_SKB1_ADD_SKA1_TEMP_1(A_SKA1):
    SKA1_ADD_SKA1_TEMP_1= pd.DataFrame()
    SKA1_ADD_SKA1_TEMP_1['SKA1_KTOKS'] = A_SKA1['KTOKS']
    SKA1_ADD_SKA1_TEMP_1['SKA1_FUNC_AREA'] = A_SKA1['FUNC_AREA']
    SKA1_ADD_SKA1_TEMP_1['SKA1_ERNAM'] = A_SKA1['ERNAM']
    SKA1_ADD_SKA1_TEMP_1['SKA1_ERDAT'] = A_SKA1['ERDAT']
    SKA1_ADD_SKA1_TEMP_1['SKA1_XBILK'] = A_SKA1['XBILK']
    SKA1_ADD_SKA1_TEMP_1['SKA1_GVTYP'] = A_SKA1['GVTYP']
    SKA1_ADD_SKA1_TEMP_1['SKA1_BILKT'] = A_SKA1['BILKT']
    SKA1_ADD_SKA1_TEMP_1['SKA1_XLOEV'] = A_SKA1['XLOEV']
    SKA1_ADD_SKA1_TEMP_1['SKA1_XSPEA'] = A_SKA1['XSPEA']
    SKA1_ADD_SKA1_TEMP_1['SKA1_XSPEB'] = A_SKA1['XSPEB']
    SKA1_ADD_SKA1_TEMP_1['SKA1_XSPEP'] = A_SKA1['XSPEP']

    SKA1_ADD_SKA1_TEMP_1['ZF_KEY_JN_SKA1_SKB1'] = A_SKA1['KTOPL'] + A_SKA1['SAKNR']
    SKA1_ADD_SKA1_TEMP_1['ZF_KEY_JN_SKAT_SKA1'] = A_SKA1['KTOPL'] + A_SKA1['SAKNR']
    SKA1_ADD_SKA1_TEMP_1['ZF_KEY_JN_T077Z_SKA1'] = A_SKA1['KTOPL'] + A_SKA1['KTOKS']
    SKA1_ADD_SKA1_TEMP_1['ZF_KEY_JN_TFKBT_SKA1'] = A_SKA1['FUNC_AREA'].astype(str)

    return SKA1_ADD_SKA1_TEMP_1

# Step 2.2/ Add the COA description
def create_SKB1_ADD_SKA1_TEMP_2(A_SKAT):
    filtered_A_SKAT = A_SKAT[A_SKAT['SPRAS'].isin(['E', 'EN'])]
    SKB1_ADD_SKA1_TEMP_2 = filtered_A_SKAT.groupby(['KTOPL', 'SAKNR']).agg({
    'SPRAS': 'first',
    'TXT20': 'first',
    'TXT50': 'first'
    }).reset_index()

    SKB1_ADD_SKA1_TEMP_2 = SKB1_ADD_SKA1_TEMP_2.rename(columns={
    'SPRAS': 'SKAT_SPRAS_SKA1',
    'TXT20': 'SKAT_TXT20_SKA1',
    'TXT50': 'SKAT_TXT50_SKA1'
    })
    SKB1_ADD_SKA1_TEMP_2['ZF_KEY_JN_SKA1_SKB1'] = SKB1_ADD_SKA1_TEMP_2['KTOPL'].astype(str) + SKB1_ADD_SKA1_TEMP_2['SAKNR'].astype(str)
    SKB1_ADD_SKA1_TEMP_2.drop(columns=['KTOPL', 'SAKNR'], inplace=True)

    return SKB1_ADD_SKA1_TEMP_2


# Step 2.3/ Add the description of the account group

def create_SKB1_ADD_SKA1_TEMP_3(A_T077Z):
    filtered_A_T077Z = A_T077Z[A_T077Z['SPRAS'].isin(['E', 'EN'])]
    SKB1_ADD_SKA1_TEMP_3 = filtered_A_T077Z.groupby(['KTOPL', 'KTOKS']).agg({
        'TXT30': 'first'
    }).reset_index()
    SKB1_ADD_SKA1_TEMP_3 = SKB1_ADD_SKA1_TEMP_3.rename(columns={
        'KTOPL': 'T077Z_KTOPL_SKA1',
        'KTOKS': 'T077Z_KTOKS_SKA1',
        'TXT30': 'T077Z_TXT30_SKA1'
    })
    SKB1_ADD_SKA1_TEMP_3['ZF_KEY_JN_T077Z_SKA1'] = SKB1_ADD_SKA1_TEMP_3['T077Z_KTOPL_SKA1'].astype(str) + SKB1_ADD_SKA1_TEMP_3['T077Z_KTOKS_SKA1'].astype(str)
    
    return SKB1_ADD_SKA1_TEMP_3

def create_SKB1_ADD_SKA1_TEMP_4(df_a_tfkbt):
    filtered_df = df_a_tfkbt[(df_a_tfkbt['SPRAS'] == 'E') | (df_a_tfkbt['SPRAS'] == 'EN')]
    selected_df = filtered_df[['FKBTX', 'FKBER']].copy()
    selected_df.rename(columns={'FKBTX': 'TFKBT_FKBTX_SKA1', 'FKBER': 'ZF_KEY_JN_TFKBT_SKA1'}, inplace=True)
    result_df = selected_df.drop_duplicates(subset=['ZF_KEY_JN_TFKBT_SKA1'])
    result_df.fillna({'TFKBT_FKBTX_SKA1': ''}, inplace=True)

    return result_df

def create_B_SS00_SS04_02_TT_SKB1_ADD_SKA1(): # done testing
    SKB1_ADD_SKA1_TEMP = create_SKB1_ADD_SKA1_TEMP(B_SS00_SS04_01_TT_SKB1_ADD_INFO)

    path_to_A_SKA1 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_SKA1.csv"
    A_SKA1 = pd.read_csv(path_to_A_SKA1, sep='\t')
    SKB1_ADD_SKA1_TEMP_1 = create_SKB1_ADD_SKA1_TEMP_1(A_SKA1)

    path_to_A_SKAT = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_SKAT.csv"
    A_SKAT = pd.read_csv(path_to_A_SKAT, sep='\t')
    SKB1_ADD_SKA1_TEMP_2 = create_SKB1_ADD_SKA1_TEMP_2(A_SKAT)

    path_to_A_T077Z = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T077Z.csv"
    A_T077Z = pd.read_csv(path_to_A_T077Z, sep='\t')
    SKB1_ADD_SKA1_TEMP_3 = create_SKB1_ADD_SKA1_TEMP_3(A_T077Z)

    path_to_A_TFKBT = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_TFKBT.csv"
    A_TFKBT = pd.read_csv(path_to_A_TFKBT, sep='\t')
    SKB1_ADD_SKA1_TEMP_4 = create_SKB1_ADD_SKA1_TEMP_4(A_TFKBT)

    SKB1_ADD_SKA1_TEMP['ZF_KEY_JN_SKA1_SKB1'] = SKB1_ADD_SKA1_TEMP['ZF_KEY_JN_SKA1_SKB1'].astype(str)
    SKB1_ADD_SKA1_TEMP_1['ZF_KEY_JN_SKA1_SKB1'] = SKB1_ADD_SKA1_TEMP_1['ZF_KEY_JN_SKA1_SKB1'].astype(str)
    SKB1_ADD_SKA1_TEMP_2['ZF_KEY_JN_SKA1_SKB1'] = SKB1_ADD_SKA1_TEMP_2['ZF_KEY_JN_SKA1_SKB1'].astype(str)
    SKB1_ADD_SKA1_TEMP_1['ZF_KEY_JN_T077Z_SKA1'] = SKB1_ADD_SKA1_TEMP_1['ZF_KEY_JN_T077Z_SKA1'].astype(str)
    SKB1_ADD_SKA1_TEMP_3['ZF_KEY_JN_T077Z_SKA1'] = SKB1_ADD_SKA1_TEMP_3['ZF_KEY_JN_T077Z_SKA1'].astype(str)
    SKB1_ADD_SKA1_TEMP_1['ZF_KEY_JN_TFKBT_SKA1'] = SKB1_ADD_SKA1_TEMP_1['ZF_KEY_JN_TFKBT_SKA1'].astype(str)
    SKB1_ADD_SKA1_TEMP_4['ZF_KEY_JN_TFKBT_SKA1'] = SKB1_ADD_SKA1_TEMP_4['ZF_KEY_JN_TFKBT_SKA1'].apply(lambda x: '' if pd.isna(x) or x=='nan' else x)

    merged_df = pd.merge(SKB1_ADD_SKA1_TEMP, SKB1_ADD_SKA1_TEMP_1,
                        left_on='ZF_KEY_JN_SKA1_SKB1', right_on='ZF_KEY_JN_SKA1_SKB1', how='left')
    merged_df['SKB1_BUKRS'] = merged_df['SKB1_BUKRS'].astype(str).str.zfill(4)
    merged_df['SKA1_ERDAT'] = merged_df['SKA1_ERDAT'].apply(lambda x: '' if pd.isna(x) else str(int(x)))
    merged_df['ZF_KEY_JN_TFKBT_SKA1'] = merged_df['ZF_KEY_JN_TFKBT_SKA1'].apply(lambda x: '' if pd.isna(x) or x=='nan' else str(int(float(x))).zfill(4))
    merged_df = pd.merge(merged_df, SKB1_ADD_SKA1_TEMP_2,
                     left_on='ZF_KEY_JN_SKA1_SKB1', right_on='ZF_KEY_JN_SKA1_SKB1', how='left')
    merged_df = pd.merge(merged_df, SKB1_ADD_SKA1_TEMP_3,
                     left_on='ZF_KEY_JN_T077Z_SKA1', right_on='ZF_KEY_JN_T077Z_SKA1', how='left')
    merged_df = pd.merge(merged_df, SKB1_ADD_SKA1_TEMP_4,
                     left_on='ZF_KEY_JN_TFKBT_SKA1', right_on='ZF_KEY_JN_TFKBT_SKA1', how='left')
    
    for index, row in merged_df.iterrows():
        if  pd.isna(row['T077Z_TXT30_SKA1']) and pd.isna(row['T077Z_KTOKS_SKA1']) and pd.isna(row['T077Z_KTOPL_SKA1']) :
            merged_df.at[index, 'TFKBT_FKBTX_SKA1'] = np.nan

    return merged_df

B_SS00_SS04_02_TT_SKB1_ADD_SKA1 = create_B_SS00_SS04_02_TT_SKB1_ADD_SKA1()

# B_SS00_SS04_02_TT_SKB1_ADD_SKA1.to_csv('B_SS00_SS04_02_TT_SKB1_ADD_SKA1.csv',sep='\t',index=False)


# Step 3/ Create lists of accounts for different functions
# Note: DISTINCT used in first Load for each concatenate
# because QLIK will concatenate first and then do DISTINCT

# 3 types of keys: KTOPL+SAKNR, BUKRS+SAKNR or SAKNR
# Step 3.1/ Provisions

def create_B_SS00_SS04_03A_TT_PROVS(): # Done testing
    file_paths = [
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C000.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C001.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C002.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C003.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C004.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C005.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C006.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C007.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C008.csv",
    "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_C009.csv"
]
    data_frames = []
    for path in file_paths:
        df = pd.read_csv(path, sep='\t')
        data_frames.append(df)

    combined_df = pd.concat(data_frames, ignore_index=True)

    result_df = combined_df[['KTOPL', 'SAKN2']]
    result_df['SAKN2'] = result_df['SAKN2'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    
    result_df.rename(columns={'KTOPL': 'ZF_C00X_KTOPL', 'SAKN2': 'ZF_C00X_SAKN2'}, inplace=True)
    result_df.sort_values(by=['ZF_C00X_KTOPL', 'ZF_C00X_SAKN2'], inplace=True)

    return result_df


B_SS00_SS04_03A_TT_PROVS = create_B_SS00_SS04_03A_TT_PROVS()
# B_SS00_SS04_03A_TT_PROVS.to_csv('B_SS00_SS04_03A_TT_PROVS.csv', sep='\t', index=False)

# Step 3.2/ FX adjustments

def create_B_SS00_SS04_03B_TT_FX_ADJ():  # Done testing 
    file_paths = [
        "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030D.csv",
        "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030E.csv",
        "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030H.csv",
        "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030HB.csv",
        "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030S.csv",
        "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030K.csv",
        "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030U.csv"
    ]
    
    data_frames = []
    
    for path in file_paths:
        df = pd.read_csv(path, sep='\t')
        data_frames.append(df)
    
    result_df = pd.DataFrame(columns=['ZF_T030X_KTOPL', 'ZF_T030X_SAKNR'])
    
    df_d = data_frames[0][['KTOPL', 'LKORR']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LKORR': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)

    df_d = data_frames[0][['KTOPL', 'KKORR']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KKORR': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)

    df_d = data_frames[0][['KTOPL', 'LSREA']].groupby(['KTOPL', 'LSREA']).first().reset_index().drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSREA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)

    df_d = data_frames[0][['KTOPL', 'LHREA']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHREA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'LSTRA']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'LHTRA']].groupby(['KTOPL', 'LHTRA']).first().reset_index().drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'KSTRA']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KSTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'KHTRA']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KHTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'LSBEW']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'KSBEW']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KSBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'KHBEW']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KHBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'LSTRV']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'LHTRV']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'KSTRV']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KSTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_d = data_frames[0][['KTOPL', 'KHTRV']].drop_duplicates()
    df_d.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KHTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_d], ignore_index=True)
    
    df_e = data_frames[1][['KTOPL', 'LKORR']].drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LKORR': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)

    df_e = data_frames[1][['KTOPL', 'LSREA']].drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSREA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)

    df_e = data_frames[1][['KTOPL', 'LHREA']].groupby(['KTOPL', 'LHREA']).first().reset_index().drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHREA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)
    
    df_e = data_frames[1][['KTOPL', 'LSTRA']].drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)
    
    df_e = data_frames[1][['KTOPL', 'LHTRA']].drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)

    df_e = data_frames[1][['KTOPL', 'LSBEW']].drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)
    
    df_e = data_frames[1][['KTOPL', 'LHBEW']].drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)
    
    df_e = data_frames[1][['KTOPL', 'LSTRV']].drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)
    
    df_e = data_frames[1][['KTOPL', 'LHTRV']].drop_duplicates()
    df_e.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_e], ignore_index=True)
    
    df_h = data_frames[2][['KTOPL', 'LSREA']].drop_duplicates()
    df_h.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSREA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_h], ignore_index=True)

    df_h = data_frames[2][['KTOPL', 'LHREA']].drop_duplicates()
    df_h.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHREA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_h], ignore_index=True)
    
    df_h = data_frames[2][['KTOPL', 'LSTRA']].drop_duplicates()
    df_h.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_h], ignore_index=True)
    
    df_h = data_frames[2][['KTOPL', 'LHTRA']].drop_duplicates()
    df_h.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_h], ignore_index=True)
    
    df_h = data_frames[2][['KTOPL', 'LSBEW']].drop_duplicates()
    df_h.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_h], ignore_index=True)
    
    df_h = data_frames[2][['KTOPL', 'LHBEW']].drop_duplicates()
    df_h.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_h], ignore_index=True)
    
    df_h = data_frames[2][['KTOPL', 'LSTRV']].groupby(['KTOPL', 'LSTRV']).first().reset_index().drop_duplicates()
    df_h.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_h], ignore_index=True)
    
    df_h = data_frames[2][['KTOPL', 'LHTRV']].drop_duplicates()
    df_h.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_h], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LKORR']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LKORR': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LSREA']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSREA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LHREA']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHREA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LSTRA']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LHTRA']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHTRA': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LSBEW']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LHBEW']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHBEW': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LSTRV']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LSTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_hb = data_frames[3][['KTOPL', 'LHTRV']].drop_duplicates()
    df_hb.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'LHTRV': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_hb], ignore_index=True)
    
    df_s = data_frames[4][['KTOPL', 'KSOLL']].drop_duplicates()
    df_s.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KSOLL': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_s], ignore_index=True)
    
    df_s = data_frames[4][['KTOPL', 'KHABN']].drop_duplicates()
    df_s.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'KHABN': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_s], ignore_index=True)
    
    df_s = data_frames[4][['KTOPL', 'GSOLL']].drop_duplicates()
    df_s.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'GSOLL': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_s], ignore_index=True)
    
    df_s = data_frames[4][['KTOPL', 'GHABN']].drop_duplicates()
    df_s.rename(columns={'KTOPL': 'ZF_T030X_KTOPL', 'GHABN': 'ZF_T030X_SAKNR'}, inplace=True)
    result_df = pd.concat([result_df, df_s], ignore_index=True)
    result_df = result_df[['ZF_T030X_KTOPL','ZF_T030X_SAKNR']].drop_duplicates()
    result_df.sort_values(by=['ZF_T030X_KTOPL', 'ZF_T030X_SAKNR'],na_position='first', inplace=True)
    result_df['ZF_T030X_SAKNR'] = result_df['ZF_T030X_SAKNR'].apply(lambda x: str(int(x)).zfill(10) if pd.notnull(x) else '')
    
    return result_df

B_SS00_SS04_03B_TT_FX_ADJ = create_B_SS00_SS04_03B_TT_FX_ADJ()

# B_SS00_SS04_03B_TT_FX_ADJ.to_csv('B_SS00_SS04_03B_TT_FX_ADJ.csv',sep='\t',index=False)


# Step 3.3/ Create a list of accounts for TAX

def create_B_SS00_SS04_03C_TT_TAX(df): # Done testing

    df_part1 = df[['KTOPL', 'KONTS']].drop_duplicates()
    df_part1.columns = ['T030K_KTOPL', 'ZF_T030K_KONTSH']

    df_part2 = df[['KTOPL', 'KONTH']].drop_duplicates()
    df_part2.columns = ['T030K_KTOPL', 'ZF_T030K_KONTSH']

    result_df = pd.concat([df_part1, df_part2], ignore_index=True)
    result_df = result_df.drop_duplicates()
    result_df['ZF_T030K_KONTSH'] = result_df['ZF_T030K_KONTSH'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    result_df.sort_values(by=['T030K_KTOPL', 'ZF_T030K_KONTSH'],na_position='first', inplace=True)

    return result_df


# Step 3.4/ Balance sheet transfer postings

def create_B_SS00_SS04_03D_TT_BS_TR_POST(df): # Done testing
    df_part1 = df[['KTOPL', 'KORRK']].drop_duplicates()
    df_part1.columns = ['ZF_T030U_KTOPL', 'ZF_T030U_KORRKZIELK']

    df_part2 = df[['KTOPL', 'ZIELK']].drop_duplicates()
    df_part2.columns = ['ZF_T030U_KTOPL', 'ZF_T030U_KORRKZIELK']

    result_df = pd.concat([df_part1, df_part2], ignore_index=True)

    result_df = result_df.drop_duplicates()
    result_df.sort_values(by=['ZF_T030U_KTOPL', 'ZF_T030U_KORRKZIELK'],na_position='first', inplace=True)

    result_df['ZF_T030U_KORRKZIELK'] = result_df['ZF_T030U_KORRKZIELK'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    return result_df

file_path_A_T030K = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030K.csv"

file_path_A_T030U = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030U.csv"

df_A_T030K = pd.read_csv(file_path_A_T030K,sep='\t')
df_A_T030U = pd.read_csv(file_path_A_T030U,sep='\t')

B_SS00_SS04_03C_TT_TAX = create_B_SS00_SS04_03C_TT_TAX(df_A_T030K)
B_SS00_SS04_03D_TT_BS_TR_POST = create_B_SS00_SS04_03D_TT_BS_TR_POST(df_A_T030U)

# df_B_SS00_SS04_03C_TT_TAX.to_csv('B_SS00_SS04_03C_TT_TAX.csv',sep='\t',index=False)
# df_B_SS00_SS04_03D_TT_BS_TR_POST.to_csv('B_SS00_SS04_03D_TT_BS_TR_POST.csv',sep='\t',index=False)


# Step 3.5/ Accounts for payment program

def create_B_SS00_SS04_03E_TT_PAY_PROG(A_T042I,A_T042IY,A_T042Y): # Done testing
   
    df_list = []

    df_part1 = A_T042I[['ZBUKR', 'UKONT']].drop_duplicates()
    df_part1.columns = ['ZF_T042X_ZBUKR', 'ZF_T042X_UVKONT']
    df_part2 = A_T042I[['ZBUKR', 'VKONT']].drop_duplicates()
    df_part2.columns = ['ZF_T042X_ZBUKR', 'ZF_T042X_UVKONT']
    df_list.append(df_part1)
    df_list.append(df_part2)

    df_part3 = A_T042IY[['ZBUKR', 'UKONT']].drop_duplicates()
    df_part3.columns = ['ZF_T042X_ZBUKR', 'ZF_T042X_UVKONT']
    df_part4 = A_T042IY[['ZBUKR', 'VKONT']].drop_duplicates()
    df_part4.columns = ['ZF_T042X_ZBUKR', 'ZF_T042X_UVKONT']
    df_list.append(df_part3)
    df_list.append(df_part4)

    df_part5 = A_T042Y[['ZBUKR', 'UKONT']].drop_duplicates()
    df_part5.columns = ['ZF_T042X_ZBUKR', 'ZF_T042X_UVKONT']
    df_part6 = A_T042Y[['ZBUKR', 'VKONT']].drop_duplicates()
    df_part6.columns = ['ZF_T042X_ZBUKR', 'ZF_T042X_UVKONT']
    df_list.append(df_part5)
    df_list.append(df_part6)

    result_df = pd.concat(df_list, ignore_index=True)

    result_df = result_df.drop_duplicates()
    result_df.sort_values(by=['ZF_T042X_ZBUKR', 'ZF_T042X_UVKONT'],na_position='first', inplace=True)

    result_df['ZF_T042X_UVKONT'] = result_df['ZF_T042X_UVKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    return result_df
file_path_A_T042I = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T042I.csv"
file_path_A_T042IY = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T042IY.csv"
file_path_A_T042Y = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T042Y.csv"

A_T042I = pd.read_csv(file_path_A_T042I,sep='\t')
A_T042IY = pd.read_csv(file_path_A_T042IY,sep='\t')
A_T042Y = pd.read_csv(file_path_A_T042Y,sep='\t')

B_SS00_SS04_03E_TT_PAY_PROG = create_B_SS00_SS04_03E_TT_PAY_PROG(A_T042I,A_T042IY,A_T042Y)
# B_SS00_SS04_03E_TT_PAY_PROG.to_csv('B_SS00_SS04_03E_TT_PAY_PROG.csv',sep='\t',index=False)

# Step 3.6/ Accounts for payment cards in payment program

def create_B_SS00_SS04_03F_TT_PAYCARDS(df): # Done testing
    result_df = df[['ZBUKR', 'UKONT']].drop_duplicates()
    result_df.columns = ['T042ICC_ZBUKR', 'T042ICC_UKONT']

    # result_df.sort_values(by=['T042ICC_ZBUKR', 'T042ICC_UKONT'],na_position='first', inplace=True)
    result_df['T042ICC_UKONT'] = result_df['T042ICC_UKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    return result_df

file_path_A_T042ICC = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T042ICC.csv"

A_T042ICC = pd.read_csv(file_path_A_T042ICC,sep='\t')
B_SS00_SS04_03F_TT_PAYCARDS = create_B_SS00_SS04_03F_TT_PAYCARDS(A_T042ICC)
# B_SS00_SS04_03F_TT_PAYCARDS.to_csv('B_SS00_SS04_03F_TT_PAYCARDS.csv',sep='\t',index=False)

# Step 3.7/ Accounts for HR payments 
def create_B_SS00_SS04_03G_TT_HRPAY(df): # Done testing
    result_df = df[['ZBUKR', 'UKONT']].drop_duplicates()
    result_df.columns = ['T042YP_ZBUKR', 'T042YP_UKONT']

    # result_df.sort_values(by=['T042ICC_ZBUKR', 'T042ICC_UKONT'],na_position='first', inplace=True)
    result_df['T042YP_UKONT'] = result_df['T042YP_UKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    return result_df

file_path_A_T042YP = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T042YP.csv"

A_T042YP = pd.read_csv(file_path_A_T042YP,sep='\t')
B_SS00_SS04_03G_TT_HRPAY = create_B_SS00_SS04_03G_TT_HRPAY(A_T042YP)
# B_SS00_SS04_03G_TT_HRPAY.to_csv('B_SS00_SS04_03G_TT_HRPAY.csv',sep='\t',index=False)

# Step 3.8/ Accounts for bank charges

def create_B_SS00_SS04_03H_TT_BNKCHRG(df): # Done testing
    result_df = df[['ZBUKR', 'CKONT']].drop_duplicates()
    result_df.columns = ['T042K_ZBUKR', 'ZF_T042K_CSKONT']
    
    df_second_part = df[['ZBUKR', 'SKONT']].drop_duplicates()
    df_second_part.columns = ['T042K_ZBUKR', 'ZF_T042K_CSKONT']
    
    result_df = pd.concat([result_df, df_second_part]).drop_duplicates().reset_index(drop=True)
    result_df.sort_values(by=['T042K_ZBUKR', 'ZF_T042K_CSKONT'],na_position='first', inplace=True)
    result_df['ZF_T042K_CSKONT'] = result_df['ZF_T042K_CSKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))

    return result_df

   
file_path_A_T042K = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T042K.csv"

A_T042K = pd.read_csv(file_path_A_T042K,sep='\t')
B_SS00_SS04_03H_TT_BNKCHRG = create_B_SS00_SS04_03H_TT_BNKCHRG(A_T042K)
# B_SS00_SS04_03H_TT_BNKCHRG.to_csv('B_SS00_SS04_03H_TT_BNKCHRG.csv',sep='\t',index=False)

# Step 3.9/ Account for cash management

def create_B_SS00_SS04_03I_TT_CASHMNGT(df): # Done testing
    result_df = df[['BUKRS', 'BNKKO']].drop_duplicates()
    result_df.columns = ['T035D_BUKRS', 'T035D_BNKKO']

    result_df.sort_values(by=['T035D_BUKRS', 'T035D_BNKKO'],na_position='first', inplace=True)
    result_df['T035D_BNKKO'] = result_df['T035D_BNKKO'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    return result_df

file_path_A_T035D = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T035D.csv"

A_T035D = pd.read_csv(file_path_A_T035D,sep='\t')
B_SS00_SS04_03I_TT_CASHMNGT = create_B_SS00_SS04_03I_TT_CASHMNGT(A_T035D)
# B_SS00_SS04_03I_TT_CASHMNGT.to_csv('B_SS00_SS04_03I_TT_CASHMNGT.csv',sep='\t',index=False)

# Step 3.10/ Account for bill of exchange

def create_B_SS00_SS04_03J_TT_BOE(df): # Done testing
    result_df = df[['BUKRS', 'GKON1']].drop_duplicates()
    result_df.columns = ['T045B_BUKRS', 'ZF_T045B_GHKON1']
    
    df_second_part = df[['BUKRS', 'HKON1']].drop_duplicates()
    df_second_part.columns = ['T045B_BUKRS', 'ZF_T045B_GHKON1']
    
    result_df = pd.concat([result_df, df_second_part]).drop_duplicates().reset_index(drop=True)
    result_df.sort_values(by=['T045B_BUKRS', 'ZF_T045B_GHKON1'],na_position='first', inplace=True)
    result_df['ZF_T045B_GHKON1'] = result_df['ZF_T045B_GHKON1'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))

    return result_df

file_path_A_T045B = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T045B.csv"

A_T045B = pd.read_csv(file_path_A_T045B,sep='\t')
B_SS00_SS04_03J_TT_BOE = create_B_SS00_SS04_03J_TT_BOE(A_T045B)
# B_SS00_SS04_03J_TT_BOE.to_csv('B_SS00_SS04_03J_TT_BOE.csv',sep='\t',index=False)


# Step 3.11/ Account for bill of exchange (per chart of accounts)

def create_B_SS00_SS04_03K_TT_BOECOA(df): # Done testing
    df1 = df[['KTOPL', 'HKONT']].rename(columns={'KTOPL': 'T045W_KTOPL', 'HKONT': 'ZF_T045W_HVKONT'})
    df2 = df[['KTOPL', 'VKONT']].rename(columns={'KTOPL': 'T045W_KTOPL', 'VKONT': 'ZF_T045W_HVKONT'})
    
    result_df = pd.concat([df1, df2], ignore_index=True)
    result_df.sort_values(by=['T045W_KTOPL', 'ZF_T045W_HVKONT'],na_position='first', inplace=True)
    result_df['ZF_T045W_HVKONT'] = result_df['ZF_T045W_HVKONT'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))

    return result_df
file_path_A_T045W = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T045W.csv"

A_T045W = pd.read_csv(file_path_A_T045W,sep='\t')
B_SS00_SS04_03K_TT_BOECOA = create_B_SS00_SS04_03K_TT_BOECOA(A_T045W)
# B_SS00_SS04_03K_TT_BOECOA.to_csv('B_SS00_SS04_03K_TT_BOECOA.csv',sep='\t',index=False)

# Step 3.12/ Account for returned bill of payables 


def create_B_SS00_SS04_03L_TT_RBOP(df): # Done testing
    result_df = df[['BUKRS', 'WKKON']].drop_duplicates()
    result_df.columns = ['T046A_BUKRS', 'T046A_WKKON']

    result_df.sort_values(by=['T046A_BUKRS', 'T046A_WKKON'],na_position='first', inplace=True)
    result_df['T046A_WKKON'] = result_df['T046A_WKKON'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))
    return result_df
file_path_A_T046A = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T046A.csv"

A_T046A = pd.read_csv(file_path_A_T046A,sep='\t')
B_SS00_SS04_03L_TT_RBOP = create_B_SS00_SS04_03L_TT_RBOP(A_T046A)
# B_SS00_SS04_03L_TT_RBOP.to_csv('B_SS00_SS04_03L_TT_RBOP.csv',sep='\t',index=False)


# Step 3.13/ Account for payment cards

def create_B_SS00_SS04_03M_TT_PAYCRDS(df): # Done testing
    result_df = df[['KTOPL', 'HKONT_V']].drop_duplicates()
    result_df.columns = ['TCCAA_KTOPL', 'ZF_TCCAA_HKONT_VN']
    
    df_second_part = df[['KTOPL', 'HKONT_N']].drop_duplicates()
    df_second_part.columns = ['TCCAA_KTOPL', 'ZF_TCCAA_HKONT_VN']
    
    result_df = pd.concat([result_df, df_second_part]).drop_duplicates().reset_index(drop=True)
    result_df.sort_values(by=['TCCAA_KTOPL', 'ZF_TCCAA_HKONT_VN'],na_position='first', inplace=True)
    result_df['ZF_TCCAA_HKONT_VN'] = result_df['ZF_TCCAA_HKONT_VN'].apply(lambda x:  str(int(x)).zfill(10) if pd.notnull(x) else '' if pd.isnull(x) else str(x))

    return result_df

file_path_A_TCCAA = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_TCCAA.csv"

A_TCCAA = pd.read_csv(file_path_A_TCCAA,sep='\t')
B_SS00_SS04_03M_TT_PAYCRDS = create_B_SS00_SS04_03M_TT_PAYCRDS(A_TCCAA)
# B_SS00_SS04_03M_TT_PAYCRDS.to_csv('B_SS00_SS04_03M_TT_PAYCRDS.csv',sep='\t',index=False)


# Step 3.14/ Accounts for fixed assets 
def create_B_SS00_SS04_03N_TT_FIXASS(A_T095, A_T095B, A_T095P): # Done testing
    dfs = []

    columns_A_T095 = [
        'KTANSW', 'KTANZA', 'KTERLW', 'KTMEHR', 'KTMAHK', 'KTMIND', 'KTREST', 'KTAPFG',
        'KTAUFW', 'KTAUFG', 'KTANSG', 'KTANZG', 'KTVBAB', 'KTVZU', 'KTVIZU', 'KTRIZU',
        'KTARIZ', 'KTVERK', 'KTCOAB', 'KTENAK', 'KTNAIB'
    ]

    for col in columns_A_T095:
        if col in A_T095.columns:
            df = A_T095[['KTOPL', col]].rename(columns={'KTOPL': 'T095_KTOPL', col: 'ZF_T095_KTXXX'}).drop_duplicates()
            dfs.append(df)

    columns_A_T095B = [
        'KTNAFB', 'KTNAFG', 'KTNAFU', 'KTSAFB', 'KTSAFG', 'KTSAFU', 'KTAAFB', 'KTAAFG',
        'KTZINB', 'KTZING', 'KTZINU', 'KTMAFB', 'KTMAFG', 'KTAUNB', 'KTAUNG', 'KTNZUS',
        'KTSZUS', 'KTAZUS', 'KTMZUS'
    ]

    for col in columns_A_T095B:
        if col in A_T095B.columns:
            df = A_T095B[['KTOPL', col]].rename(columns={'KTOPL': 'T095_KTOPL', col: 'ZF_T095_KTXXX'}).drop_duplicates()
            dfs.append(df)

    columns_A_T095P = [
        'KTSOPO', 'KTSEIN', 'KTNAUF', 'KTSAUF', 'KTSABG', 'KTZUSA', 'KTMEHR', 'KTMIND',
        'KTVBAB'
    ]

    for col in columns_A_T095P:
        if col in A_T095P.columns:
            df = A_T095P[['KTOPL', col]].rename(columns={'KTOPL': 'T095_KTOPL', col: 'ZF_T095_KTXXX'}).drop_duplicates()
            dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
    result_df['ZF_T095_KTXXX'] = result_df['ZF_T095_KTXXX'].apply(lambda x: str(int(x)).zfill(10) if pd.notnull(x) and isinstance(x, float) else '' if pd.isnull(x) else str(x) if isinstance(x, int) else x)
    result_df = result_df.drop_duplicates()
    result_df.sort_values(by=['T095_KTOPL', 'ZF_T095_KTXXX'], na_position='first', inplace=True)

    return result_df
    

file_path_A_T095 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T095.csv"
A_T095 = pd.read_csv(file_path_A_T095,sep='\t')
file_path_A_T095B = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T095B.csv"
A_T095B = pd.read_csv(file_path_A_T095B,sep='\t')
file_path_A_T095P = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T095P.csv"
A_T095P = pd.read_csv(file_path_A_T095P,sep='\t')


B_SS00_SS04_03N_TT_FIXASS = create_B_SS00_SS04_03N_TT_FIXASS(A_T095,A_T095B,A_T095P)
# B_SS00_SS04_03N_TT_FIXASS.to_csv('B_SS00_SS04_03N_TT_FIXASS.csv',sep='\t',index=False)

# Step 3.15/ Special GL accounts

def create_B_SS00_SS04_03O_TT_SPGL(df): # Done testing
    result_df = df[['KTOPL', 'HKONT']].drop_duplicates()
    result_df.columns = ['T074_KTOPL', 'ZF_T074_HSKONT']
    
    df_second_part = df[['KTOPL', 'SKONT']].drop_duplicates()
    df_second_part.columns = ['T074_KTOPL', 'ZF_T074_HSKONT']
    
    result_df = pd.concat([result_df, df_second_part]).drop_duplicates().reset_index(drop=True)
    result_df['ZF_T074_HSKONT'] = result_df['ZF_T074_HSKONT'].apply(lambda x: str(int(x)).zfill(10) if pd.notnull(x) and isinstance(x, float) else '' if pd.isnull(x) else str(x) if isinstance(x, int) else x)
    result_df= result_df.drop_duplicates()
    result_df.sort_values(by=['T074_KTOPL', 'ZF_T074_HSKONT'],na_position='first', inplace=True)

    return result_df

file_path_A_T074 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T074.csv"
A_T074 = pd.read_csv(file_path_A_T074,sep='\t')
B_SS00_SS04_03O_TT_SPGL = create_B_SS00_SS04_03O_TT_SPGL(A_T074)
# B_SS00_SS04_03O_TT_SPGL.to_csv('B_SS00_SS04_03O_TT_SPGL.csv',sep='\t',index=False)

# Step 4/ For tables that are only by company code,
#         add the chart of accounts
# (Usually only one chart of accounts per company code)

def create_B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_1(B_SS00_SS04_03E_TT_PAY_PROG): # Done testing
    result_df = B_SS00_SS04_03E_TT_PAY_PROG.copy()
    result_df['ZF_KEY_JN_T001_ACCDESC'] = result_df['ZF_T042X_ZBUKR']
    
    return result_df
B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_1 = create_B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_1(B_SS00_SS04_03E_TT_PAY_PROG)

# B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_1.to_csv('B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_1.csv',sep='\t',index=False)

def create_B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_2(df): # Done testing
    result_df = df.copy()
    
    result_df = result_df[['KTOPL', 'BUKRS']]
    result_df.columns = ['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC']
    
    result_df = result_df.groupby('ZF_KEY_JN_T001_ACCDESC').agg({'T001_KTOPL_ACCDESC': 'first'}).reset_index()
    result_df = result_df.reindex(columns=['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC'])

    return result_df

file_path_A_T001 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T001.csv"
A_T001 = pd.read_csv(file_path_A_T001,sep='\t')
B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_2 = create_B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_2(A_T001)
# B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_2.to_csv('B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_2.csv',sep='\t',index=False)

def create_B_SS00_SS04_03E_TT_PAY_PROG_2(B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_1, B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_2):
    result_df = pd.merge(B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_1, B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_2,
                         on='ZF_KEY_JN_T001_ACCDESC', how='left')

    return result_df
B_SS00_SS04_03E_TT_PAY_PROG_2 = create_B_SS00_SS04_03E_TT_PAY_PROG_2(B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_1,B_SS00_SS04_03E_TT_PAY_PROG_2_TEMP_2)
# B_SS00_SS04_03E_TT_PAY_PROG_2.to_csv('B_SS00_SS04_03E_TT_PAY_PROG_2.csv',sep='\t',index=False)


def create_B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_1 (B_SS00_SS04_03F_TT_PAYCARDS): # Done testing
    result_df = B_SS00_SS04_03F_TT_PAYCARDS.copy()
    result_df['ZF_KEY_JN_T001_ACCDESC'] = result_df['T042ICC_ZBUKR']
    
    return result_df

B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_1 = create_B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_1(B_SS00_SS04_03F_TT_PAYCARDS)
# B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_1.to_csv('B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_1.csv',sep='\t',index=False)

def create_B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_2(A_T001): # Done testing
    result_df = A_T001.copy()
    result_df['T001_KTOPL_ACCDESC'] = result_df['KTOPL'].apply(lambda x: str(x).split('|')[0] if isinstance(x, str) and '|' in x else x)
    result_df = result_df[['T001_KTOPL_ACCDESC', 'BUKRS']]
    result_df.columns = ['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC']
    
    result_df = result_df.groupby('ZF_KEY_JN_T001_ACCDESC').agg({'T001_KTOPL_ACCDESC': 'first'}).reset_index()
    result_df = result_df.reindex(columns=['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC'])

    return result_df

B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_2 = create_B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_2(A_T001)
# B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_2.to_csv('B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_2.csv',sep='\t',index=False)

def create_B_SS00_SS04_03F_TT_PAYCARDS_2(B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_1, B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_2): # Done testing
    merge_key = 'ZF_KEY_JN_T001_ACCDESC'
    
    result_df = pd.merge(B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_1, 
                         B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_2[[merge_key, 'T001_KTOPL_ACCDESC']], 
                         on=merge_key, 
                         how='left')
    
    return result_df

B_SS00_SS04_03F_TT_PAYCARDS_2 = create_B_SS00_SS04_03F_TT_PAYCARDS_2(B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_1, B_SS00_SS04_03F_TT_PAYCARDS_2_TEMP_2)
# B_SS00_SS04_03F_TT_PAYCARDS_2.to_csv('B_SS00_SS04_03F_TT_PAYCARDS_2.csv',sep='\t',index=False)

def create_B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1 (B_SS00_SS04_03G_TT_HRPAY): # Done testing
    result_df = B_SS00_SS04_03G_TT_HRPAY.copy()
    result_df['ZF_KEY_JN_T001_ACCDESC'] = result_df['T042YP_ZBUKR']
    
    return result_df

B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1 = create_B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1(B_SS00_SS04_03G_TT_HRPAY)
# B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1.to_csv('B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1.csv',sep='\t',index=False)

def create_B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2(A_T001): # Done testing
    result_df = A_T001.copy()
    result_df['T001_KTOPL_ACCDESC'] = result_df['KTOPL'].apply(lambda x: str(x).split('|')[0] if isinstance(x, str) and '|' in x else x)
    result_df = result_df[['T001_KTOPL_ACCDESC', 'BUKRS']]
    result_df.columns = ['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC']
    
    result_df = result_df.groupby('ZF_KEY_JN_T001_ACCDESC').agg({'T001_KTOPL_ACCDESC': 'first'}).reset_index()
    result_df = result_df.reindex(columns=['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC'])

    return result_df

B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2 = create_B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2(A_T001)
# B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2.to_csv('B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2.csv',sep='\t',index=False)

def create_B_SS00_SS04_03G_TT_HRPAY_2(B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1, B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2): # Done testing
    merge_key = 'ZF_KEY_JN_T001_ACCDESC'
    B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'].astype(str)
    B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'].astype(str)

    result_df = pd.merge(B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1, 
                         B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2[[merge_key, 'T001_KTOPL_ACCDESC']], 
                         on=merge_key, 
                         how='left')
    
    return result_df

B_SS00_SS04_03G_TT_HRPAY_2 = create_B_SS00_SS04_03G_TT_HRPAY_2(B_SS00_SS04_03G_TT_HRPAY_2_TEMP_1, B_SS00_SS04_03G_TT_HRPAY_2_TEMP_2)
# B_SS00_SS04_03G_TT_HRPAY_2.to_csv('B_SS00_SS04_03G_TT_HRPAY_2.csv',sep='\t',index=False)


def create_B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1 (B_SS00_SS04_03H_TT_BNKCHRG): # Done testing
    result_df = B_SS00_SS04_03H_TT_BNKCHRG.copy()
    result_df['ZF_KEY_JN_T001_ACCDESC'] = result_df['T042K_ZBUKR']
    
    return result_df

B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1 = create_B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1(B_SS00_SS04_03H_TT_BNKCHRG)
# B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1.to_csv('B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1.csv',sep='\t',index=False)

def create_B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2(A_T001): # Done testing
    result_df = A_T001.copy()
    result_df['T001_KTOPL_ACCDESC'] = result_df['KTOPL'].apply(lambda x: str(x).split('|')[0] if isinstance(x, str) and '|' in x else x)
    result_df = result_df[['T001_KTOPL_ACCDESC', 'BUKRS']]
    result_df.columns = ['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC']
    
    result_df = result_df.groupby('ZF_KEY_JN_T001_ACCDESC').agg({'T001_KTOPL_ACCDESC': 'first'}).reset_index()
    result_df = result_df.reindex(columns=['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC'])

    return result_df

B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2 = create_B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2(A_T001)
# B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2.to_csv('B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2.csv',sep='\t',index=False)

def create_B_SS00_SS04_03H_TT_BNKCHRG_2(B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1, B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2): # Done testing
    merge_key = 'ZF_KEY_JN_T001_ACCDESC'
    B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'].astype(str)
    B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'].astype(str)

    result_df = pd.merge(B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1, 
                         B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2[[merge_key, 'T001_KTOPL_ACCDESC']], 
                         on=merge_key, 
                         how='left')
    
    return result_df

B_SS00_SS04_03H_TT_BNKCHRG_2 = create_B_SS00_SS04_03H_TT_BNKCHRG_2(B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_1, B_SS00_SS04_03H_TT_BNKCHRG_2_TEMP_2)
# B_SS00_SS04_03H_TT_BNKCHRG_2.to_csv('B_SS00_SS04_03H_TT_BNKCHRG_2.csv',sep='\t',index=False)

def create_B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1 (B_SS00_SS04_03I_TT_CASHMNGT): # Done testing
    result_df = B_SS00_SS04_03I_TT_CASHMNGT.copy()
    result_df['ZF_KEY_JN_T001_ACCDESC'] = result_df['T035D_BUKRS']
    
    return result_df

B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1 = create_B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1(B_SS00_SS04_03I_TT_CASHMNGT)
# B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1.to_csv('B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1.csv',sep='\t',index=False)

def create_B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2(A_T001): # Done testing
    result_df = A_T001.copy()
    result_df['T001_KTOPL_ACCDESC'] = result_df['KTOPL'].apply(lambda x: str(x).split('|')[0] if isinstance(x, str) and '|' in x else x)
    result_df = result_df[['T001_KTOPL_ACCDESC', 'BUKRS']]
    result_df.columns = ['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC']
    
    result_df = result_df.groupby('ZF_KEY_JN_T001_ACCDESC').agg({'T001_KTOPL_ACCDESC': 'first'}).reset_index()
    result_df = result_df.reindex(columns=['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC'])

    return result_df

B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2 = create_B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2(A_T001)
# B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2.to_csv('B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2.csv',sep='\t',index=False)

def create_B_SS00_SS04_03I_TT_CASHMNGT_2(B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1, B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2): # Done testing
    merge_key = 'ZF_KEY_JN_T001_ACCDESC'
    B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'].astype(str)
    B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'].astype(str)

    result_df = pd.merge(B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1, 
                         B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2[[merge_key, 'T001_KTOPL_ACCDESC']], 
                         on=merge_key, 
                         how='left')
    
    return result_df

B_SS00_SS04_03I_TT_CASHMNGT_2 = create_B_SS00_SS04_03I_TT_CASHMNGT_2(B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_1, B_SS00_SS04_03I_TT_CASHMNGT_2_TEMP_2)
# B_SS00_SS04_03I_TT_CASHMNGT_2.to_csv('B_SS00_SS04_03I_TT_CASHMNGT_2.csv',sep='\t',index=False)


def create_B_SS00_SS04_03J_TT_BOE_2_TEMP_1(B_SS00_SS04_03J_TT_BOE):
    result_df = B_SS00_SS04_03J_TT_BOE.copy()
    result_df['ZF_KEY_JN_T001_ACCDESC'] = result_df['T045B_BUKRS']
    return result_df

B_SS00_SS04_03J_TT_BOE_2_TEMP_1 = create_B_SS00_SS04_03J_TT_BOE_2_TEMP_1(B_SS00_SS04_03J_TT_BOE)

def create_B_SS00_SS04_03J_TT_BOE_2_TEMP_2(A_T001):
    result_df = A_T001.copy()
    result_df['T001_KTOPL_ACCDESC'] = result_df['KTOPL'].apply(lambda x: str(x).split('|')[0] if isinstance(x, str) and '|' in x else x)
    result_df = result_df[['T001_KTOPL_ACCDESC', 'BUKRS']]
    result_df.columns = ['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC']
    result_df = result_df.groupby('ZF_KEY_JN_T001_ACCDESC').agg({'T001_KTOPL_ACCDESC': 'first'}).reset_index()
    result_df = result_df.reindex(columns=['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC'])
    return result_df

B_SS00_SS04_03J_TT_BOE_2_TEMP_2 = create_B_SS00_SS04_03J_TT_BOE_2_TEMP_2(A_T001)

def create_B_SS00_SS04_03J_TT_BOE_2(B_SS00_SS04_03J_TT_BOE_2_TEMP_1, B_SS00_SS04_03J_TT_BOE_2_TEMP_2):
    merge_key = 'ZF_KEY_JN_T001_ACCDESC'
    B_SS00_SS04_03J_TT_BOE_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03J_TT_BOE_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'].astype(str)
    B_SS00_SS04_03J_TT_BOE_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03J_TT_BOE_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'].astype(str)
    result_df = pd.merge(B_SS00_SS04_03J_TT_BOE_2_TEMP_1,
                         B_SS00_SS04_03J_TT_BOE_2_TEMP_2[['ZF_KEY_JN_T001_ACCDESC', 'T001_KTOPL_ACCDESC']],
                         on=merge_key,
                         how='left')
    return result_df

B_SS00_SS04_03J_TT_BOE_2 = create_B_SS00_SS04_03J_TT_BOE_2(B_SS00_SS04_03J_TT_BOE_2_TEMP_1, B_SS00_SS04_03J_TT_BOE_2_TEMP_2)
# B_SS00_SS04_03J_TT_BOE_2.to_csv('B_SS00_SS04_03J_TT_BOE_2.csv', sep='\t', index=False)


def create_B_SS00_SS04_03L_TT_RBOP_2_TEMP_1(B_SS00_SS04_03L_TT_RBOP):
    result_df = B_SS00_SS04_03L_TT_RBOP.copy()
    result_df['ZF_KEY_JN_T001_ACCDESC'] = result_df['T046A_BUKRS']
    return result_df

B_SS00_SS04_03L_TT_RBOP_2_TEMP_1 = create_B_SS00_SS04_03L_TT_RBOP_2_TEMP_1(B_SS00_SS04_03L_TT_RBOP)

def create_B_SS00_SS04_03L_TT_RBOP_2_TEMP_2(A_T001):
    result_df = A_T001.copy()
    result_df['T001_KTOPL_ACCDESC'] = result_df['KTOPL'].apply(lambda x: str(x).split('|')[0] if isinstance(x, str) and '|' in x else x)
    result_df = result_df[['T001_KTOPL_ACCDESC', 'BUKRS']]
    result_df.columns = ['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC']
    result_df = result_df.groupby('ZF_KEY_JN_T001_ACCDESC').agg({'T001_KTOPL_ACCDESC': 'first'}).reset_index()
    result_df = result_df.reindex(columns=['T001_KTOPL_ACCDESC', 'ZF_KEY_JN_T001_ACCDESC'])
    return result_df

B_SS00_SS04_03L_TT_RBOP_2_TEMP_2 = create_B_SS00_SS04_03L_TT_RBOP_2_TEMP_2(A_T001)

def create_B_SS00_SS04_03L_TT_RBOP_2(B_SS00_SS04_03L_TT_RBOP_2_TEMP_1, B_SS00_SS04_03L_TT_RBOP_2_TEMP_2):
    merge_key = 'ZF_KEY_JN_T001_ACCDESC'
    B_SS00_SS04_03L_TT_RBOP_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03L_TT_RBOP_2_TEMP_1['ZF_KEY_JN_T001_ACCDESC'].astype(str)
    B_SS00_SS04_03L_TT_RBOP_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'] = B_SS00_SS04_03L_TT_RBOP_2_TEMP_2['ZF_KEY_JN_T001_ACCDESC'].astype(str)
    result_df = pd.merge(B_SS00_SS04_03L_TT_RBOP_2_TEMP_1,
                         B_SS00_SS04_03L_TT_RBOP_2_TEMP_2[['ZF_KEY_JN_T001_ACCDESC', 'T001_KTOPL_ACCDESC']],
                         on=merge_key,
                         how='left')
    return result_df

B_SS00_SS04_03L_TT_RBOP_2 = create_B_SS00_SS04_03L_TT_RBOP_2(B_SS00_SS04_03L_TT_RBOP_2_TEMP_1, B_SS00_SS04_03L_TT_RBOP_2_TEMP_2)
# B_SS00_SS04_03L_TT_RBOP_2.to_csv('B_SS00_SS04_03L_TT_RBOP_2.csv', sep='\t', index=False)


#  Step 5/ Concatenate the above tables into one table 
#  Adding the key and the description

#  Create and populate the B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC table

# def create_B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC(B_SS00_SS04_03A_TT_PROVS, B_SS00_SS04_03B_TT_FX_ADJ,
#                                                B_SS00_SS04_03C_TT_TAX,B_SS00_SS04_03D_TT_BS_TR_POST,B_SS00_SS04_03E_TT_PAY_PROG_2,
#                                                B_SS00_SS04_03F_TT_PAYCARDS_2,B_SS00_SS04_03M_TT_PAYCRDS,B_SS00_SS04_03G_TT_HRPAY_2,
#                                                B_SS00_SS04_03H_TT_BNKCHRG_2,B_SS00_SS04_03I_TT_CASHMNGT_2,B_SS00_SS04_03J_TT_BOE_2,
#                                                B_SS00_SS04_03K_TT_BOECOA,B_SS00_SS04_03L_TT_RBOP_2,B_SS00_SS04_03N_TT_FIXASS,B_SS00_SS04_03O_TT_SPGL):
#     #  Data has differents position

#     dfs = []
#     columns_to_keep = ['ZF_ACCDESC_KTOPL', 'ZF_ACCDESC_SAKNR', 'ZF_SAKNR_CONFIG_DESC']

#     df1 = B_SS00_SS04_03A_TT_PROVS.rename(columns={'ZF_C00X_KTOPL': 'ZF_ACCDESC_KTOPL',
#                                                    'ZF_C00X_SAKN2': 'ZF_ACCDESC_SAKNR'})
#     df1['ZF_SAKNR_CONFIG_DESC'] = 'Provisions;'
#     dfs.append(df1[columns_to_keep])

#     df2 = B_SS00_SS04_03B_TT_FX_ADJ.rename(columns={'ZF_T030X_KTOPL': 'ZF_ACCDESC_KTOPL',
#                                                     'ZF_T030X_SAKNR': 'ZF_ACCDESC_SAKNR'})
#     df2['ZF_SAKNR_CONFIG_DESC'] = 'FXAdjustments;'
#     dfs.append(df2[columns_to_keep])

#     df3 = B_SS00_SS04_03C_TT_TAX.rename(columns={'T030K_KTOPL': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_T030K_KONTSH': 'ZF_ACCDESC_SAKNR'})
#     df3['ZF_SAKNR_CONFIG_DESC'] = 'Tax;'
#     dfs.append(df3[columns_to_keep])

#     df4 = B_SS00_SS04_03D_TT_BS_TR_POST.rename(columns={'ZF_T030U_KTOPL': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_T030U_KORRKZIELK': 'ZF_ACCDESC_SAKNR'})
#     df4['ZF_SAKNR_CONFIG_DESC'] = 'BalanceTransfer;'
#     dfs.append(df4[columns_to_keep])

#     df5 = B_SS00_SS04_03E_TT_PAY_PROG_2.rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_T042X_UVKONT': 'ZF_ACCDESC_SAKNR'})
#     df5['ZF_SAKNR_CONFIG_DESC'] = 'PaymentProgram;'
#     dfs.append(df5[columns_to_keep])

#     df6 = B_SS00_SS04_03F_TT_PAYCARDS_2.rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL',
#                                                  'T042ICC_UKONT': 'ZF_ACCDESC_SAKNR'})
#     df6['ZF_SAKNR_CONFIG_DESC'] = 'PaymentCards;'
#     dfs.append(df6[columns_to_keep])

#     df7 = B_SS00_SS04_03M_TT_PAYCRDS.rename(columns={'TCCAA_KTOPL': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_TCCAA_HKONT_VN': 'ZF_ACCDESC_SAKNR'})
#     df7['ZF_SAKNR_CONFIG_DESC'] = 'PaymentCards;'
#     dfs.append(df7[columns_to_keep])

#     df8 = B_SS00_SS04_03G_TT_HRPAY_2.rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL',
#                                                  'T042YP_UKONT': 'ZF_ACCDESC_SAKNR'})
#     df8['ZF_SAKNR_CONFIG_DESC'] = 'HRPayments;'
#     dfs.append(df8[columns_to_keep])


#     df9 = B_SS00_SS04_03H_TT_BNKCHRG_2.rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_T042K_CSKONT': 'ZF_ACCDESC_SAKNR'})
#     df9['ZF_SAKNR_CONFIG_DESC'] = 'BankCharges;'
#     dfs.append(df9[columns_to_keep])

    
#     df10 = B_SS00_SS04_03I_TT_CASHMNGT_2.rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL',
#                                                  'T035D_BNKKO': 'ZF_ACCDESC_SAKNR'})
#     df10['ZF_SAKNR_CONFIG_DESC'] = 'CashManagement;'
#     dfs.append(df10[columns_to_keep])

#     df11 = B_SS00_SS04_03J_TT_BOE_2.rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_T045B_GHKON1': 'ZF_ACCDESC_SAKNR'})
#     df11['ZF_SAKNR_CONFIG_DESC'] = 'BillOfExchange;'
#     dfs.append(df11[columns_to_keep])  

#     df12 = B_SS00_SS04_03K_TT_BOECOA.rename(columns={'T045W_KTOPL': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_T045W_HVKONT': 'ZF_ACCDESC_SAKNR'})
#     df12['ZF_SAKNR_CONFIG_DESC'] = 'BillOfExchange;'
#     dfs.append(df12[columns_to_keep])  

#     df13 = B_SS00_SS04_03L_TT_RBOP_2.rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL',
#                                                  'T046A_WKKON': 'ZF_ACCDESC_SAKNR'})
#     df13['ZF_SAKNR_CONFIG_DESC'] = 'BillOfPayables;'
#     dfs.append(df13[columns_to_keep])

#     df14 = B_SS00_SS04_03N_TT_FIXASS.rename(columns={'T095_KTOPL': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_T095_KTXXX': 'ZF_ACCDESC_SAKNR'})
#     df14['ZF_SAKNR_CONFIG_DESC'] = 'FixedAssets;'
#     dfs.append(df14[columns_to_keep])

#     df15 = B_SS00_SS04_03O_TT_SPGL.rename(columns={'T074_KTOPL': 'ZF_ACCDESC_KTOPL',
#                                                  'ZF_T074_HSKONT': 'ZF_ACCDESC_SAKNR'})
#     df15['ZF_SAKNR_CONFIG_DESC'] = 'SpecialGL;'
#     dfs.append(df15[columns_to_keep])

#     result_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
#     result_df.sort_values(by=['ZF_ACCDESC_KTOPL', 'ZF_ACCDESC_SAKNR','ZF_SAKNR_CONFIG_DESC'], na_position='first', inplace=True)

#     return result_df

# B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC = create_B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC(B_SS00_SS04_03A_TT_PROVS, B_SS00_SS04_03B_TT_FX_ADJ,
#                                                B_SS00_SS04_03C_TT_TAX,B_SS00_SS04_03D_TT_BS_TR_POST,B_SS00_SS04_03E_TT_PAY_PROG_2,
#                                                B_SS00_SS04_03F_TT_PAYCARDS_2,B_SS00_SS04_03M_TT_PAYCRDS,B_SS00_SS04_03G_TT_HRPAY_2,
#                                                B_SS00_SS04_03H_TT_BNKCHRG_2,B_SS00_SS04_03I_TT_CASHMNGT_2,B_SS00_SS04_03J_TT_BOE_2,
#                                                B_SS00_SS04_03K_TT_BOECOA,B_SS00_SS04_03L_TT_RBOP_2,B_SS00_SS04_03N_TT_FIXASS,B_SS00_SS04_03O_TT_SPGL)

# B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC.to_csv('B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC.csv', sep='\t', index=False)

def create_B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC(B_SS00_SS04_03A_TT_PROVS, B_SS00_SS04_03B_TT_FX_ADJ,
                                               B_SS00_SS04_03C_TT_TAX,B_SS00_SS04_03D_TT_BS_TR_POST,B_SS00_SS04_03E_TT_PAY_PROG_2,
                                               B_SS00_SS04_03F_TT_PAYCARDS_2,B_SS00_SS04_03M_TT_PAYCRDS,B_SS00_SS04_03G_TT_HRPAY_2,
                                               B_SS00_SS04_03H_TT_BNKCHRG_2,B_SS00_SS04_03I_TT_CASHMNGT_2,B_SS00_SS04_03J_TT_BOE_2,
                                               B_SS00_SS04_03K_TT_BOECOA,B_SS00_SS04_03L_TT_RBOP_2,B_SS00_SS04_03N_TT_FIXASS,B_SS00_SS04_03O_TT_SPGL):
    frames = [
    B_SS00_SS04_03A_TT_PROVS.assign(ZF_SAKNR_CONFIG_DESC='Provisions;')[['ZF_C00X_KTOPL', 'ZF_C00X_SAKN2', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'ZF_C00X_KTOPL': 'ZF_ACCDESC_KTOPL', 'ZF_C00X_SAKN2': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03B_TT_FX_ADJ.assign(ZF_SAKNR_CONFIG_DESC='FXAdjustments;')[['ZF_T030X_KTOPL', 'ZF_T030X_SAKNR', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'ZF_T030X_KTOPL': 'ZF_ACCDESC_KTOPL', 'ZF_T030X_SAKNR': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03C_TT_TAX.assign(ZF_SAKNR_CONFIG_DESC='Tax;')[['T030K_KTOPL', 'ZF_T030K_KONTSH', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T030K_KTOPL': 'ZF_ACCDESC_KTOPL', 'ZF_T030K_KONTSH': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03D_TT_BS_TR_POST.assign(ZF_SAKNR_CONFIG_DESC='BalanceTransfer;')[['ZF_T030U_KTOPL', 'ZF_T030U_KORRKZIELK', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'ZF_T030U_KTOPL': 'ZF_ACCDESC_KTOPL', 'ZF_T030U_KORRKZIELK': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03E_TT_PAY_PROG_2.assign(ZF_SAKNR_CONFIG_DESC='PaymentProgram;')[['T001_KTOPL_ACCDESC', 'ZF_T042X_UVKONT', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL', 'ZF_T042X_UVKONT': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03F_TT_PAYCARDS_2.assign(ZF_SAKNR_CONFIG_DESC='PaymentCards;')[['T001_KTOPL_ACCDESC', 'T042ICC_UKONT', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL', 'T042ICC_UKONT': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03M_TT_PAYCRDS.assign(ZF_SAKNR_CONFIG_DESC='PaymentCards;')[['TCCAA_KTOPL', 'ZF_TCCAA_HKONT_VN', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'TCCAA_KTOPL': 'ZF_ACCDESC_KTOPL', 'ZF_TCCAA_HKONT_VN': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03G_TT_HRPAY_2.assign(ZF_SAKNR_CONFIG_DESC='HRPayments;')[['T001_KTOPL_ACCDESC', 'T042YP_UKONT', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL', 'T042YP_UKONT': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03H_TT_BNKCHRG_2.assign(ZF_SAKNR_CONFIG_DESC='BankCharges;')[['T001_KTOPL_ACCDESC', 'ZF_T042K_CSKONT', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL', 'ZF_T042K_CSKONT': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03I_TT_CASHMNGT_2.assign(ZF_SAKNR_CONFIG_DESC='CashManagement;')[['T001_KTOPL_ACCDESC', 'T035D_BNKKO', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL', 'T035D_BNKKO': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03J_TT_BOE_2.assign(ZF_SAKNR_CONFIG_DESC='BillOfExchange;')[['T001_KTOPL_ACCDESC', 'ZF_T045B_GHKON1', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL', 'ZF_T045B_GHKON1': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03K_TT_BOECOA.assign(ZF_SAKNR_CONFIG_DESC='BillOfExchange;')[['T045W_KTOPL', 'ZF_T045W_HVKONT', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T045W_KTOPL': 'ZF_ACCDESC_KTOPL', 'ZF_T045W_HVKONT': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03L_TT_RBOP_2.assign(ZF_SAKNR_CONFIG_DESC='BillOfPayables;')[['T001_KTOPL_ACCDESC', 'T046A_WKKON', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T001_KTOPL_ACCDESC': 'ZF_ACCDESC_KTOPL', 'T046A_WKKON': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03N_TT_FIXASS.assign(ZF_SAKNR_CONFIG_DESC='FixedAssets;')[['T095_KTOPL', 'ZF_T095_KTXXX', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T095_KTOPL': 'ZF_ACCDESC_KTOPL', 'ZF_T095_KTXXX': 'ZF_ACCDESC_SAKNR'}),
    B_SS00_SS04_03O_TT_SPGL.assign(ZF_SAKNR_CONFIG_DESC='SpecialGL;')[['T074_KTOPL', 'ZF_T074_HSKONT', 'ZF_SAKNR_CONFIG_DESC']].rename(columns={'T074_KTOPL': 'ZF_ACCDESC_KTOPL', 'ZF_T074_HSKONT': 'ZF_ACCDESC_SAKNR'})
    ]

    B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC = pd.concat(frames)
    B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC=B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC.drop_duplicates()
    B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC.sort_values(by=['ZF_SAKNR_CONFIG_DESC'],inplace=True)

    return B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC

B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC = create_B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC(B_SS00_SS04_03A_TT_PROVS, B_SS00_SS04_03B_TT_FX_ADJ,
                                               B_SS00_SS04_03C_TT_TAX,B_SS00_SS04_03D_TT_BS_TR_POST,B_SS00_SS04_03E_TT_PAY_PROG_2,
                                               B_SS00_SS04_03F_TT_PAYCARDS_2,B_SS00_SS04_03M_TT_PAYCRDS,B_SS00_SS04_03G_TT_HRPAY_2,
                                               B_SS00_SS04_03H_TT_BNKCHRG_2,B_SS00_SS04_03I_TT_CASHMNGT_2,B_SS00_SS04_03J_TT_BOE_2,
                                               B_SS00_SS04_03K_TT_BOECOA,B_SS00_SS04_03L_TT_RBOP_2,B_SS00_SS04_03N_TT_FIXASS,B_SS00_SS04_03O_TT_SPGL)

# B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC.to_csv('B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC.csv', sep='\t', index=False)

# Step 6/ Make a list of descriptions from the T030 and T030W table

# Step 6.1/ Debit accounts with their descriptions of the transaction event key

def create_T030W_DESCS_D_TEMP_1(df):
    data = {
        'T030_KTOPL': df['KTOPL'],         
        'ZF_T030_KONTSH': df['KONTS'],     
        'T030_KTOSL': df['KTOSL'],         
        'T030_KOMOK': df['KOMOK'],         
        'ZF_KEY_JN_T030W_T030': df['KTOSL'] 
    }

    T030W_DESCS_D_TEMP_1 = pd.DataFrame(data)
    return T030W_DESCS_D_TEMP_1

def create_T030W_DESCS_D_TEMP_2(df):
    filtered_df = df[df['SPRAS'].isin(['E', 'EN'])]
    T030W_DESCS_D_TEMP_2 = filtered_df[['LTEXT', 'KTOSL']]
    T030W_DESCS_D_TEMP_2 = T030W_DESCS_D_TEMP_2.rename(columns={'LTEXT': 'T030W_LTEXT_T030', 'KTOSL': 'ZF_KEY_JN_T030W_T030'})

    return T030W_DESCS_D_TEMP_2
    

def create_B_SS00_SS04_05_TT_T030W_DESCS_D(T030W_DESCS_D_TEMP_1, T030W_DESCS_D_TEMP_2):
    B_SS00_SS04_05_TT_T030W_DESCS_D = pd.merge(T030W_DESCS_D_TEMP_1, T030W_DESCS_D_TEMP_2,
                                                on='ZF_KEY_JN_T030W_T030', how='left')
    # result_df = B_SS00_SS04_05_TT_T030W_DESCS_D[['T030_KTOPL', 'ZF_T030_KONTSH', 'T030_KTOSL', 'T030_KOMOK', 'ZF_KEY_JN_T030W_T030', 'T030W_LTEXT_T030']]

    return B_SS00_SS04_05_TT_T030W_DESCS_D


a_t030_df = pd.read_csv('C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030.csv',sep='\t')
a_t030w_df = pd.read_csv('C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030W.csv',sep='\t')

T030W_DESCS_D_TEMP_1 = create_T030W_DESCS_D_TEMP_1(a_t030_df)
# T030W_DESCS_D_TEMP_1.to_csv('T030W_DESCS_D_TEMP_1.csv', sep='\t', index=False)
T030W_DESCS_D_TEMP_2 = create_T030W_DESCS_D_TEMP_2(a_t030w_df)
# T030W_DESCS_D_TEMP_2.to_csv('T030W_DESCS_D_TEMP_2.csv', sep='\t', index=False)

B_SS00_SS04_05_TT_T030W_DESCS_D = create_B_SS00_SS04_05_TT_T030W_DESCS_D(T030W_DESCS_D_TEMP_1, T030W_DESCS_D_TEMP_2)

# B_SS00_SS04_05_TT_T030W_DESCS_D.to_csv('B_SS00_SS04_05_TT_T030W_DESCS_D.csv', sep='\t', index=False)


# Step 6.2 Credit accounts with their descriptions of the transaction event key


def create_T030W_DESCS_C_TEMP_1(df):
    data = {
        'T030_KTOPL': df['KTOPL'],         
        'ZF_T030_KONTSH': df['KONTH'],     
        'T030_KTOSL': df['KTOSL'],         
        'T030_KOMOK': df['KOMOK'],         
        'ZF_KEY_JN_T030W_T030': df['KTOSL'] 
    }

    T030W_DESCS_C_TEMP_1 = pd.DataFrame(data)
    return T030W_DESCS_C_TEMP_1

def create_T030W_DESCS_C_TEMP_2(df):
    filtered_df = df[df['SPRAS'].isin(['E', 'EN'])]
    T030W_DESCS_C_TEMP_2 = filtered_df[['LTEXT', 'KTOSL']]
    T030W_DESCS_C_TEMP_2 = T030W_DESCS_C_TEMP_2.rename(columns={'LTEXT': 'T030W_LTEXT_T030', 'KTOSL': 'ZF_KEY_JN_T030W_T030'})

    return T030W_DESCS_C_TEMP_2
    

def create_B_SS00_SS04_06_TT_T030W_DESCS_C(T030W_DESCS_C_TEMP_1, T030W_DESCS_C_TEMP_2):
    B_SS00_SS04_06_TT_T030W_DESCS_C = pd.merge(T030W_DESCS_C_TEMP_1, T030W_DESCS_C_TEMP_2,
                                                on='ZF_KEY_JN_T030W_T030', how='left')
    # result_df = B_SS00_SS04_06_TT_T030W_DESCS_C[['T030_KTOPL', 'ZF_T030_KONTSH', 'T030_KTOSL', 'T030_KOMOK', 'ZF_KEY_JN_T030W_T030', 'T030W_LTEXT_T030']]

    return B_SS00_SS04_06_TT_T030W_DESCS_C


a_t030_df = pd.read_csv('C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030.csv',sep='\t')
a_t030w_df = pd.read_csv('C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T030W.csv',sep='\t')

T030W_DESCS_C_TEMP_1 = create_T030W_DESCS_C_TEMP_1(a_t030_df)
# T030W_DESCS_C_TEMP_1.to_csv('T030W_DESCS_C_TEMP_1.csv', sep='\t', index=False)
T030W_DESCS_C_TEMP_2 = create_T030W_DESCS_C_TEMP_2(a_t030w_df)
# T030W_DESCS_C_TEMP_2.to_csv('T030W_DESCS_C_TEMP_2.csv', sep='\t', index=False)

B_SS00_SS04_06_TT_T030W_DESCS_C = create_B_SS00_SS04_06_TT_T030W_DESCS_C(T030W_DESCS_C_TEMP_1, T030W_DESCS_C_TEMP_2)

# B_SS00_SS04_06_TT_T030W_DESCS_C.to_csv('B_SS00_SS04_06_TT_T030W_DESCS_C.csv', sep='\t', index=False)

#  Step 6.3/ Concatenate debit and credit together

def create_T030W_DESCS_DC_TEMP_1(df):
    T030W_DESCS_DC_TEMP_1 = df.copy()
    T030W_DESCS_DC_TEMP_1['ZF_T030W_LTEXT_T030_CL'] = T030W_DESCS_DC_TEMP_1.apply(lambda row: process_row_T030W_DESCS_DC_TEMP_1(row), axis=1)
    selected_columns = ['T030_KTOPL', 'ZF_T030_KONTSH', 'T030_KTOSL', 'ZF_T030W_LTEXT_T030_CL']
    T030W_DESCS_DC_TEMP_1 = T030W_DESCS_DC_TEMP_1[selected_columns]
    
    return T030W_DESCS_DC_TEMP_1


def process_row_T030W_DESCS_DC_TEMP_1(row):
    KTOSL = row['T030_KTOSL']
    KOMOK = row['T030_KOMOK']
    T030W_LTEXT_T030 = row['T030W_LTEXT_T030']
    if pd.isna(KTOSL):
        KTOSL = ''
    if pd.isna(KOMOK):
        KOMOK = ''
    if pd.isna(T030W_LTEXT_T030):
        T030W_LTEXT_T030 = ''
    if KTOSL =='' and KOMOK ==''  and T030W_LTEXT_T030=='':
        return ''
    
    if KTOSL == 'PRE' and KOMOK == '':
        return np.nan
    elif KTOSL == 'ZUZ' and KOMOK == 'A0':
        return np.nan
  
    if KTOSL == 'GBB':
        if KOMOK == 'AUA':
            return 'GBB - AUA (Inventory offset: settling of production order to financial accounts)'
        elif KOMOK == 'AUF':
            return 'GBB - AUF (Inventory offset: Posting goods receipt from a production order)'
        elif KOMOK == 'AUI':
            return 'GBB - AUI (Inventory offset: Adjustment of price from cost center directly to material)'
        elif KOMOK == 'BSA':
            return 'GBB - BSA (Inventory offset: Initial entry stock balances)'
        elif KOMOK == 'INV':
            return 'GBB - INV (Inventory offset: Expense/ revenue from inventory differences)'
        elif KOMOK == 'VAX':
            return 'GBB - VAX (Inventory offset: Goods issue from a valuated sales stock order)'
        elif KOMOK == 'VAY':
            return 'GBB - VAY (Inventory offset: Goods issue from a non-valuated sales stock order)'
        elif KOMOK == 'VBO':
            return 'GBB - VBO (Inventory offset: Stock consumption in sub-contracting)'
        elif KOMOK == 'VBR':
            return 'GBB - VBR (Inventory offset: Internal goods issue)'
        elif KOMOK == 'VKA':
            return 'GBB - VKA (Inventory offset: Goods issue to sale instead of BSX)'
        elif KOMOK == 'VKP':
            return 'GBB - VKP (Inventory offset: Goods issue to project instead of BSX)'
        elif KOMOK == 'VNG':
            return 'GBB - VNG (Inventory offset: Scrapping of inventory stock or materials)'
        elif KOMOK == 'VQP':
            return 'GBB - VQP (Inventory offset: Sample goods issue to quality management without account assignment)'
        elif KOMOK == 'VQY':
            return 'GBB - VQY (Inventory offset: Same goods issue to quality management with account assignment)'
        elif KOMOK == 'ZOB':
            return 'GBB - ZOB (Inventory offset: Goods receipt without referece to a purchase order)'
        elif KOMOK == 'ZOF':
            return 'GBB - ZOF (Inventory offset: Goods receipt without reference to a production order)'
        elif KOMOK == 'VBO':
            return 'GBB - VBO (Inventory offset: Goods consumption for subcontracting)'
        elif KOMOK == '':
            return 'KON (Consignment liabilities)'
        elif KOMOK == 'PIP':
            return 'KON - PIP (Pipeline liabilities)'
        elif KOMOK == 'PRD':
            return 'PRD (Goods and invoice receitps against purchase orders)'
        elif KOMOK == 'PRA':
            return 'PRD - PRA (Price differences: goods issues and other goods movements)'
        elif KOMOK == 'PRF':
            return 'PRD - PRF (Price differences: Goods receipts for production orders)'
        elif KOMOK == 'PRU':
            return 'PRD - PRU (Price differences: Price variation - transfer posting)'
        else:
            return f"{KTOSL}-{KOMOK}({T030W_LTEXT_T030})"

    else:
        return f"{KTOSL}-{KOMOK}({T030W_LTEXT_T030})"

T030W_DESCS_DC_TEMP_1 = create_T030W_DESCS_DC_TEMP_1(B_SS00_SS04_05_TT_T030W_DESCS_D)


def create_T030W_DESCS_DC_TEMP_2(df):
    T030W_DESCS_DC_TEMP_2 = df.copy()
    T030W_DESCS_DC_TEMP_2['ZF_T030W_LTEXT_T030_CL'] = T030W_DESCS_DC_TEMP_2.apply(lambda row: process_row_T030W_DESCS_DC_TEMP_2(row), axis=1)
    selected_columns = ['T030_KTOPL', 'ZF_T030_KONTSH', 'T030_KTOSL', 'ZF_T030W_LTEXT_T030_CL']
    T030W_DESCS_DC_TEMP_2 = T030W_DESCS_DC_TEMP_2[selected_columns]
    
    return T030W_DESCS_DC_TEMP_2

def process_row_T030W_DESCS_DC_TEMP_2(row):
    KTOSL = row['T030_KTOSL']
    KOMOK = row['T030_KOMOK']
    T030W_LTEXT_T030 = row['T030W_LTEXT_T030']
    if pd.isna(KTOSL):
        KTOSL = ''
    if pd.isna(KOMOK):
        KOMOK = ''
    if pd.isna(T030W_LTEXT_T030):
        T030W_LTEXT_T030 = ''
    if KTOSL =='' and KOMOK ==''  and T030W_LTEXT_T030=='':
        return ''
    
    if KTOSL == 'PRE' and KOMOK == '':
        return np.nan
    elif KTOSL == 'ZUZ' and KOMOK == 'A0':
        return np.nan
    if KTOSL == 'GBB':
        if KOMOK == 'AUA':
            return 'GBB - AUA (Inventory offset: settling of production order to financial accounts)'
        elif KOMOK == 'AUF':
            return 'GBB - AUF (Inventory offset: Posting goods receipt from a production order)'
        elif KOMOK == 'AUI':
            return 'GBB - AUI (Inventory offset: Adjustment of price from cost center directly to material)'
        elif KOMOK == 'BSA':
            return 'GBB - BSA (Inventory offset: Initial entry stock balances)'
        elif KOMOK == 'INV':
            return 'GBB - INV (Inventory offset: Expense/ revenue from inventory differences)'
        elif KOMOK == 'VAX':
            return 'GBB - VAX (Inventory offset: Goods issue from a valuated sales stock order)'
        elif KOMOK == 'VAY':
            return 'GBB - VAY (Inventory offset: Goods issue from a non-valuated sales stock order)'
        elif KOMOK == 'VBO':
            return 'GBB - VBO (Inventory offset: Stock consumption in sub-contracting)'
        elif KOMOK == 'VBR':
            return 'GBB - VBR (Inventory offset: Internal goods issue)'
        elif KOMOK == 'VKA':
            return 'GBB - VKA (Inventory offset: Goods issue to sale instead of BSX)'
        elif KOMOK == 'VKP':
            return 'GBB - VKP (Inventory offset: Goods issue to project instead of BSX)'
        elif KOMOK == 'VNG':
            return 'GBB - VNG (Inventory offset: Scrapping of inventory stock or materials)'
        elif KOMOK == 'VQP':
            return 'GBB - VQP (Inventory offset: Sample goods issue to quality management without account assignment)'
        elif KOMOK == 'VQY':
            return 'GBB - VQY (Inventory offset: Same goods issue to quality management with account assignment)'
        elif KOMOK == 'ZOB':
            return 'GBB - ZOB (Inventory offset: Goods receipt without reference to a purchase order)'
        elif KOMOK == 'ZOF':
            return 'GBB - ZOF (Inventory offset: Goods receipt without reference to a production order)'
        elif KOMOK == 'VBO':
            return 'GBB - VBO (Inventory offset: Goods consumption for subcontracting)'
        else:
            return f"{KTOSL}-{KOMOK}({T030W_LTEXT_T030})"
    elif KTOSL == 'KON' and KOMOK == '':
        return 'KON (Consignment liabilities)'
    elif KTOSL == 'KON' and KOMOK == 'PIP':
        return 'KON - PIP (Pipeline liabilities)'
    elif KTOSL == 'PRD' and KOMOK == '':
        return 'PRD (Goods and invoice receipts against purchase orders)'
    elif KTOSL == 'PRD' and KOMOK == 'PRA':
        return 'PRD - PRA (Price differences: goods issues and other goods movements)'
    elif KTOSL == 'PRD' and KOMOK == 'PRF':
        return 'PRD - PRF (Price differences: Goods receipts for production orders)'
    elif KTOSL == 'PRD' and KOMOK == 'PRU':
        return 'PRD - PRU (Price differences: Price variation - transfer posting)'
    else:
        return f"{KTOSL}-{KOMOK}({T030W_LTEXT_T030})"
      

T030W_DESCS_DC_TEMP_2 = create_T030W_DESCS_DC_TEMP_2(B_SS00_SS04_06_TT_T030W_DESCS_C)

# T030W_DESCS_DC_TEMP_1.to_csv('T030W_DESCS_DC_TEMP_1.csv', sep='\t', index=False)
# T030W_DESCS_DC_TEMP_2.to_csv('T030W_DESCS_DC_TEMP_2.csv', sep='\t', index=False)

def create_B_SS00_SS04_07_TT_T030W_DESCS_DC(T030W_DESCS_DC_TEMP_1, T030W_DESCS_DC_TEMP_2):
    B_SS00_SS04_07_TT_T030W_DESCS_DC = pd.concat([T030W_DESCS_DC_TEMP_1, T030W_DESCS_DC_TEMP_2], ignore_index=True)
    return B_SS00_SS04_07_TT_T030W_DESCS_DC

B_SS00_SS04_07_TT_T030W_DESCS_DC = create_B_SS00_SS04_07_TT_T030W_DESCS_DC(T030W_DESCS_DC_TEMP_1,T030W_DESCS_DC_TEMP_2)

# B_SS00_SS04_07_TT_T030W_DESCS_DC.to_csv('B_SS00_SS04_07_TT_T030W_DESCS_DC.csv', sep='\t', index=False)

# Step 6.4/ Remove duplication on the account description

def create_B_SS00_SS04_08_TT_T030W_REM_DUP(B_SS00_SS04_07_TT_T030W_DESCS_DC):
    B_SS00_SS04_07_TT_T030W_DESCS_DC['T030_KTOPL'].fillna('', inplace=True)
    B_SS00_SS04_07_TT_T030W_DESCS_DC['ZF_T030_KONTSH'].fillna('', inplace=True)
    B_SS00_SS04_07_TT_T030W_DESCS_DC['ZF_T030W_LTEXT_T030_CL'].fillna('', inplace=True)

    df_B_SS00_SS04_08_TT_T030W_REM_DUP = (
        B_SS00_SS04_07_TT_T030W_DESCS_DC
        .groupby(['T030_KTOPL', 'ZF_T030_KONTSH'])
        .agg(ZF_SAKNR_CONFIG_DESC=('ZF_T030W_LTEXT_T030_CL', 'first'))
        .reset_index()
    )
    
    return df_B_SS00_SS04_08_TT_T030W_REM_DUP

B_SS00_SS04_08_TT_T030W_REM_DUP = create_B_SS00_SS04_08_TT_T030W_REM_DUP(B_SS00_SS04_07_TT_T030W_DESCS_DC)

# B_SS00_SS04_08_TT_T030W_REM_DUP.to_csv('B_SS00_SS04_08_TT_T030W_REM_DUP.csv', sep='\t', index=False)

# Step 7/ Make a list unique on KTOPL, BUKRS, SAKNR
#         Concatenate the descriptions

# 7.1/ Concatenate table descriptions with those from T030

def create_B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS(B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC, B_SS00_SS04_08_TT_T030W_REM_DUP ): # 7.1/ Done testing
    df1_unique = B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC[['ZF_ACCDESC_KTOPL', 'ZF_ACCDESC_SAKNR', 'ZF_SAKNR_CONFIG_DESC']].drop_duplicates()
    df2_selected = B_SS00_SS04_08_TT_T030W_REM_DUP[['T030_KTOPL', 'ZF_T030_KONTSH', 'ZF_SAKNR_CONFIG_DESC']]
    df2_selected.columns = ['ZF_ACCDESC_KTOPL', 'ZF_ACCDESC_SAKNR', 'ZF_SAKNR_CONFIG_DESC']
    B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS = pd.concat([df1_unique, df2_selected]).drop_duplicates()
    B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS_sorted = B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS.sort_values(by=['ZF_ACCDESC_KTOPL', 'ZF_ACCDESC_SAKNR', 'ZF_SAKNR_CONFIG_DESC'],na_position='first')

    return B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS_sorted

B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS = create_B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS(B_SS00_SS04_04_TT_CONC_ADD_KEY_DESC, B_SS00_SS04_08_TT_T030W_REM_DUP)
# B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS.to_csv('B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS.csv',sep='\t', index=False)


# 7.2/ List descriptions together per chart of accounts and account 

def create_B_SS00_SS04_10_TT_LIST_ACCT_DESCS (B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS): # 7.2/ Differents data in final column
    df_filtered = B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS[
    (B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS['ZF_ACCDESC_KTOPL'].str.len() > 0) &
    (B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS['ZF_ACCDESC_SAKNR'].str.len() > 0)
    ]
    mask = df_filtered['ZF_SAKNR_CONFIG_DESC'].apply(lambda x: pd.isna(x) or x == '')
    df_filtered.loc[mask, 'ZF_SAKNR_CONFIG_DESC'] = np.nan
    df_filtered.loc[~mask, 'ZF_SAKNR_CONFIG_DESC'] += ';'
    df_result = df_filtered.groupby(['ZF_ACCDESC_KTOPL', 'ZF_ACCDESC_SAKNR'])[['ZF_SAKNR_CONFIG_DESC']].first().reset_index()
    df_result.sort_values(by=['ZF_ACCDESC_KTOPL', 'ZF_ACCDESC_SAKNR'], na_position='first', inplace=True)
    condition = (
    (
        (df_result['ZF_ACCDESC_KTOPL'].isin(['CACA', 'CAIN', 'CAUS', 'GKR', 'INT', 'INTA'])) &
        (df_result['ZF_ACCDESC_SAKNR'] == '0000175000')
    ) |
    (
        (df_result['ZF_ACCDESC_KTOPL'] == 'CAJP') &
        (df_result['ZF_ACCDESC_SAKNR'] == '0000422100')
    ) |
    (
        (df_result['ZF_ACCDESC_KTOPL'] == 'IKR') &
        (df_result['ZF_ACCDESC_SAKNR'] == '0000480000')
    )
    )
    df_result.loc[condition, 'ZF_SAKNR_CONFIG_DESC'] = np.nan
     
    return df_result

B_SS00_SS04_10_TT_LIST_ACCT_DESCS = create_B_SS00_SS04_10_TT_LIST_ACCT_DESCS(B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS)

# B_SS00_SS04_10_TT_LIST_ACCT_DESCS.to_csv('B_SS00_SS04_10_TT_LIST_ACCT_DESCS.csv',sep='\t', index=False)


# Step 8/ Add the list back to the chart of accounts
# Add P&L/BS description
# Add Rec accnts/ bank description
# Add account group description
# Create the table and load data



def create_IT_SKB1_SKA1_ACC_DESCS_TEMP_1(B_SS00_SS04_02_TT_SKB1_ADD_SKA1):  # Done testing
    IT_SKB1_SKA1_ACC_DESCS_TEMP_1 = B_SS00_SS04_02_TT_SKB1_ADD_SKA1.copy()
    columns_to_check = ['SKA1_GVTYP', 'SKA1_XBILK', 'SKB1_MITKZ', 'SKB1_HBKID', 'SKB1_XGKON', 'T012K_HBKID_SKB1', 'FEBKO_HKONT_SKB1', 'REGUH_HKONT_SKB1']
    for col in columns_to_check:
        if IT_SKB1_SKA1_ACC_DESCS_TEMP_1[col].dtype == float:
            IT_SKB1_SKA1_ACC_DESCS_TEMP_1[col] = IT_SKB1_SKA1_ACC_DESCS_TEMP_1[col].astype(int)
        if IT_SKB1_SKA1_ACC_DESCS_TEMP_1[col].dtype == int:
            IT_SKB1_SKA1_ACC_DESCS_TEMP_1[col] = IT_SKB1_SKA1_ACC_DESCS_TEMP_1[col].astype(str)

    def process_ska1_gvtyp(value):
        if pd.isna(value):
            value=''
        elif isinstance(value, float):  
            value = int(value)  
        value_str = str(value)  
        return len(value_str)  
    IT_SKB1_SKA1_ACC_DESCS_TEMP_1['ZF_SAKNR_PL_OR_BS'] = IT_SKB1_SKA1_ACC_DESCS_TEMP_1.apply(lambda row: 'Profit & Loss' if process_ska1_gvtyp(row['SKA1_GVTYP']) > 0 else
                                    ('Balance Sheet' if process_ska1_gvtyp(row['SKA1_XBILK']) > 0 else ''), axis=1)

    IT_SKB1_SKA1_ACC_DESCS_TEMP_1['ZF_SAKNR_PL_OR_BS_W_DESC'] = IT_SKB1_SKA1_ACC_DESCS_TEMP_1.apply(lambda row: 'S: P&L' if process_ska1_gvtyp(row['SKA1_GVTYP']) > 0 else
                                            ('A: Asset' if row['SKB1_MITKZ'] == 'A' else
                                            ('D: AR reconciliation account' if row['SKB1_MITKZ'] == 'D' else
                                                ('K: AP reconciliation account' if row['SKB1_MITKZ'] == 'K' else
                                                ('V: Contract Accounts Receivable' if row['SKB1_MITKZ'] == 'V' else
                                                ('S: Bank' if (process_ska1_gvtyp(row['SKB1_HBKID']) > 0 or
                                                                process_ska1_gvtyp(row['SKB1_XGKON']) > 0 or
                                                                process_ska1_gvtyp(row['T012K_HBKID_SKB1']) > 0 or
                                                                process_ska1_gvtyp(row['FEBKO_HKONT_SKB1']) > 0 or
                                                                process_ska1_gvtyp(row['REGUH_HKONT_SKB1']) > 0)
                                                else 'S: Other'))))), axis=1)
    IT_SKB1_SKA1_ACC_DESCS_TEMP_1['T077Z_TXT30_SKA1'] = IT_SKB1_SKA1_ACC_DESCS_TEMP_1['T077Z_TXT30_SKA1'].apply(lambda x: '' if pd.isna(x) else x)
 
    for index, row in IT_SKB1_SKA1_ACC_DESCS_TEMP_1.iterrows():
        if  pd.isna(row['T077Z_TXT30_SKA1']) :
            IT_SKB1_SKA1_ACC_DESCS_TEMP_1.at[index, 'T077Z_TXT30_SKA1'] = ''
        
    IT_SKB1_SKA1_ACC_DESCS_TEMP_1['ZF_KTOKS_T077Z_TXT30'] = IT_SKB1_SKA1_ACC_DESCS_TEMP_1['SKA1_KTOKS'].str.rstrip() + '-' + IT_SKB1_SKA1_ACC_DESCS_TEMP_1['T077Z_TXT30_SKA1'].str.lstrip()
    IT_SKB1_SKA1_ACC_DESCS_TEMP_1['ZF_KEY_JN_ACCDESC_SKA1'] = \
    IT_SKB1_SKA1_ACC_DESCS_TEMP_1['T001_KTOPL_SKB1'] + IT_SKB1_SKA1_ACC_DESCS_TEMP_1['SKB1_SAKNR']
    return IT_SKB1_SKA1_ACC_DESCS_TEMP_1

IT_SKB1_SKA1_ACC_DESCS_TEMP_1 = create_IT_SKB1_SKA1_ACC_DESCS_TEMP_1(B_SS00_SS04_02_TT_SKB1_ADD_SKA1)

# IT_SKB1_SKA1_ACC_DESCS_TEMP_1.to_csv('IT_SKB1_SKA1_ACC_DESCS_TEMP_1.csv',sep='\t', index=False)

def create_IT_SKB1_SKA1_ACC_DESCS_TEMP_2(B_SS00_SS04_10_TT_LIST_ACCT_DESCS):  # Done testing
    IT_SKB1_SKA1_ACC_DESCS_TEMP_2 = B_SS00_SS04_10_TT_LIST_ACCT_DESCS[['ZF_SAKNR_CONFIG_DESC', 'ZF_ACCDESC_KTOPL', 'ZF_ACCDESC_SAKNR']].copy()
    IT_SKB1_SKA1_ACC_DESCS_TEMP_2['ZF_KEY_JN_ACCDESC_SKA1'] = IT_SKB1_SKA1_ACC_DESCS_TEMP_2['ZF_ACCDESC_KTOPL'] + IT_SKB1_SKA1_ACC_DESCS_TEMP_2['ZF_ACCDESC_SAKNR']

    return IT_SKB1_SKA1_ACC_DESCS_TEMP_2

IT_SKB1_SKA1_ACC_DESCS_TEMP_2 = create_IT_SKB1_SKA1_ACC_DESCS_TEMP_2(B_SS00_SS04_10_TT_LIST_ACCT_DESCS)
# IT_SKB1_SKA1_ACC_DESCS_TEMP_2.to_csv('IT_SKB1_SKA1_ACC_DESCS_TEMP_2.csv',sep='\t', index=False)

def create_B_SS00_SS04_11_IT_SKB1_SKA1_ACC_DESCS(IT_SKB1_SKA1_ACC_DESCS_TEMP_1,IT_SKB1_SKA1_ACC_DESCS_TEMP_2):   # Done testing
    IT_SKB1_SKA1_ACC_DESCS = pd.merge(IT_SKB1_SKA1_ACC_DESCS_TEMP_1, IT_SKB1_SKA1_ACC_DESCS_TEMP_2[['ZF_KEY_JN_ACCDESC_SKA1', 'ZF_SAKNR_CONFIG_DESC']], 
                                  on='ZF_KEY_JN_ACCDESC_SKA1', how='left')
    IT_SKB1_SKA1_ACC_DESCS['SKB1_BUKRS'] = IT_SKB1_SKA1_ACC_DESCS['SKB1_BUKRS'].astype(str).str.zfill(4)

    return IT_SKB1_SKA1_ACC_DESCS

IT_SKB1_SKA1_ACC_DESCS = create_B_SS00_SS04_11_IT_SKB1_SKA1_ACC_DESCS(IT_SKB1_SKA1_ACC_DESCS_TEMP_1,IT_SKB1_SKA1_ACC_DESCS_TEMP_2)
IT_SKB1_SKA1_ACC_DESCS.to_csv('B_SS00_SS04_11_IT_SKB1_SKA1_ACC_DESCS.csv',sep='\t', index=False)

