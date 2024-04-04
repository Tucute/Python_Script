import pandas as pd

def create_table_B_SS00_SS02_01B_IT_TCURX (A_TCURX):
    B_SS00_SS02_01B_IT_TCURX = pd.DataFrame()
    B_SS00_SS02_01B_IT_TCURX['TCURX_CURRKEY'] = A_TCURX['CURRKEY']
    B_SS00_SS02_01B_IT_TCURX['ZF_TCURX_CURRDEC'] = 10 ** (2 - A_TCURX['CURRDEC'].astype(float))  
    return B_SS00_SS02_01B_IT_TCURX
