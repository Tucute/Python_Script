import pandas as pd
def create_SKB1_TEMP(A_SKB1):
    SKB1_TEMP = pd.DataFrame()
    SKB1_TEMP['SKB1_BUKRS'] = A_SKB1['BUKRS']
    SKB1_TEMP['SKB1_ERDAT'] = A_SKB1['ERDAT']
    SKB1_TEMP['SKB1_ERNAM'] = A_SKB1['ERNAM']
    SKB1_TEMP['SKB1_HBKID'] = A_SKB1['HBKID']
    SKB1_TEMP['SKB1_HKTID'] = A_SKB1['HKTID']
    SKB1_TEMP['SKB1_SAKNR'] = A_SKB1['SAKNR']
    SKB1_TEMP['SKB1_WAERS'] = A_SKB1['WAERS']
    SKB1_TEMP['SKB1_XGKON'] = A_SKB1['XGKON']
    SKB1_TEMP['SKB1_STEXT'] = A_SKB1['STEXT']
    SKB1_TEMP['SKB1_FIPLS'] = A_SKB1['FIPLS']
    SKB1_TEMP['SKB1_XINTB'] = A_SKB1['XINTB']
    SKB1_TEMP['SKB1_XOPVW'] = A_SKB1['XOPVW']
    SKB1_TEMP['SKB1_BEWGP'] = A_SKB1['BEWGP']
    SKB1_TEMP['SKB1_XLGCLR'] = A_SKB1['XLGCLR']
    SKB1_TEMP['SKB1_MITKZ'] = A_SKB1['MITKZ']
    SKB1_TEMP['ZF_KEY_JN_USR21_SKB1'] = A_SKB1['ERNAM']
    SKB1_TEMP['ZF_KEY_JN_USER_ADDR_SKA1'] = A_SKB1['ERNAM']
    SKB1_TEMP['ZF_KEY_JN_T001_SKB1'] = A_SKB1['BUKRS']
    SKB1_TEMP['ZF_KEY_JN_T012K_SKB1'] = A_SKB1['BUKRS'].astype(str) + A_SKB1['SAKNR'].astype(str)
    SKB1_TEMP['ZF_KEY_JN_REGUH_SKB1'] = A_SKB1['BUKRS'].astype(str) + A_SKB1['SAKNR'].astype(str)
    SKB1_TEMP['ZF_KEY_JN_FEBKO_SKB1'] = A_SKB1['BUKRS'].astype(str) + A_SKB1['SAKNR'].astype(str)
    
    return SKB1_TEMP

def create_SKB1_TEMP_2(A_USR21):
    SKB1_TEMP_2 = pd.DataFrame()
    SKB1_TEMP_2['USR21_BNAME_SKB1'] = A_USR21['BNAME']
    SKB1_TEMP_2['USR21_KOSTL_SKB1'] = A_USR21['KOSTL']
    SKB1_TEMP_2['USR21_PERSNUMBER_SKB1'] = A_USR21['PERSNUMBER']
    SKB1_TEMP_2['ZF_KEY_JN_USR21_SKB1'] = A_USR21['BNAME']
    SKB1_TEMP_2 = SKB1_TEMP_2.groupby('USR21_BNAME_SKB1').agg({'USR21_BNAME_SKB1': 'first', 'USR21_KOSTL_SKB1': 'first', 'USR21_PERSNUMBER_SKB1': 'first', 'ZF_KEY_JN_USR21_SKB1': 'first'})
    return SKB1_TEMP_2

def create_SKB1_TEMP_3(A_USER_ADDR):
    SKB1_TEMP_3 = pd.DataFrame(A_USER_ADDR.groupby('BNAME').agg({'BNAME': 'first', 'NAME_TEXTC': 'first'}))
    SKB1_TEMP_3.rename(columns={'BNAME': 'USER_ADDR_BNAME_SKA1', 'NAME_TEXTC': 'USER_ADDR_NAME_TEXTC_SKA1'}, inplace=True)
    SKB1_TEMP_3['ZF_KEY_JN_USER_ADDR_SKA1'] = SKB1_TEMP_3['USER_ADDR_BNAME_SKA1']
    return SKB1_TEMP_3

def create_SKB1_TEMP_4(A_T012K):
    SKB1_TEMP_4 = pd.DataFrame()
    SKB1_TEMP_4['T012K_BUKRS_SKB1'] = A_T012K['BUKRS'].astype(str).str.zfill(3)
    SKB1_TEMP_4['T012K_HKONT_SKB1'] = A_T012K['HKONT'].astype(str).str.zfill(4)
    SKB1_TEMP_4['T012K_HBKID_SKB1'] = A_T012K['HBKID']
    SKB1_TEMP_4['ZF_KEY_JN_T012K_SKB1'] = SKB1_TEMP_4['T012K_BUKRS_SKB1'] + SKB1_TEMP_4['T012K_HKONT_SKB1']
    SKB1_TEMP_4 = SKB1_TEMP_4.groupby(['T012K_BUKRS_SKB1','T012K_HKONT_SKB1','T012K_HBKID_SKB1']).agg({'T012K_BUKRS_SKB1': 'first', 'T012K_HKONT_SKB1': 'first', 'T012K_HBKID_SKB1': 'first', 'ZF_KEY_JN_T012K_SKB1': 'first'})

    return SKB1_TEMP_4

path_to_A_SKB1 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_SKB1.csv"
path_to_A_USR21 = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_USR21.csv"
path_to_A_USER_ADDR = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_USER_ADDR.csv"
path_to_A_T012K = "C:\\HuongData\\Convert_SQLite_to_Python\\Python_Script\\RUN_D01\\A_PHASE_RUN_SQLITE\\A_T012K.csv"


A_SKB1 = pd.read_csv(path_to_A_SKB1,sep='\t')
A_USR21 = pd.read_csv(path_to_A_USR21,sep='\t')
A_USER_ADDR = pd.read_csv(path_to_A_USER_ADDR,sep='\t')
A_T012K = pd.read_csv(path_to_A_T012K,sep='\t')

SKB1_TEMP = create_SKB1_TEMP(A_SKB1)
print(SKB1_TEMP.head())
SKB1_TEMP_2 = create_SKB1_TEMP_2(A_USR21)
print(SKB1_TEMP_2.head())
SKB1_TEMP_3 = create_SKB1_TEMP_3(A_USER_ADDR)
print(SKB1_TEMP_3.head())
SKB1_TEMP_4 = create_SKB1_TEMP_4(A_T012K)
print(SKB1_TEMP_4.head())

# SKB1_TEMP.to_csv('SKB1_TEMP.csv', index=False)
# SKB1_TEMP_2.to_csv('SKB1_TEMP_2.csv', index=False)
# SKB1_TEMP_3.to_csv('SKB1_TEMP_3.csv', index=False)
SKB1_TEMP_4.to_csv('SKB1_TEMP_6.csv', index=False)
