import sqlite3
import pandas as pd

def sqlite_to_csv(db_file, query, output_csv):
    conn = sqlite3.connect(db_file)
    
    df = pd.read_sql_query(query, conn)
    
    df.to_csv(output_csv, sep='\t', index=False)

    print(f"The data has been successfully exported to CSV file: {output_csv}")

db_file = '300App.db'
# query = 'SELECT * FROM SKB1_ADD_SKA1_TEMP'

# output_csv = 'SKB1_ADD_SKA1_TEMP_SQL.csv'
# sqlite_to_csv(db_file, query, output_csv)

# query = 'SELECT * FROM SKB1_ADD_SKA1_TEMP_1'

# output_csv = 'SKB1_ADD_SKA1_TEMP_1_SQL.csv'
# sqlite_to_csv(db_file, query, output_csv)
# query = 'SELECT * FROM SKB1_ADD_SKA1_TEMP_2'

# output_csv = 'SKB1_ADD_SKA1_TEMP_2_SQL.csv'
# sqlite_to_csv(db_file, query, output_csv)


# query = 'SELECT * FROM SKB1_ADD_SKA1_TEMP_3'

# output_csv = 'SKB1_ADD_SKA1_TEMP_3_SQL.csv'
# sqlite_to_csv(db_file, query, output_csv)

# query = 'SELECT * FROM SKB1_ADD_SKA1_TEMP_4'

# output_csv = 'SKB1_ADD_SKA1_TEMP_4_SQL.csv'
# sqlite_to_csv(db_file, query, output_csv)
# query = 'SELECT * FROM B_SS00_SS04_02_TT_SKB1_ADD_SKA1'

# output_csv = 'B_SS00_SS04_02_TT_SKB1_ADD_SKA1_SQL.csv'
# sqlite_to_csv(db_file, query, output_csv)


query = 'SELECT * FROM B_SS00_SS04_11_IT_SKB1_SKA1_ACC_DESCS_test'

output_csv = 'B_SS00_SS04_11_IT_SKB1_SKA1_ACC_DESCS_test_SQL.csv'
sqlite_to_csv(db_file, query, output_csv)

# query = 'SELECT * FROM DATA'

# output_csv = 'DATA.csv'
# sqlite_to_csv(db_file, query, output_csv)