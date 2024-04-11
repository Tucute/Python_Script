import pandas as pd
from sqlalchemy import create_engine

def csv_to_sqlite(csv_file, db_file, table_name):
    df = pd.read_csv(csv_file)
    
    engine = create_engine('sqlite:///' + db_file)
    
    df.to_sql(table_name, engine, sep='\t',index=False, if_exists='replace')
    
    print("CSV file has been successfully imported into SQLite database.")

csv_file_path = "B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS_SQL.csv"
table_name = "B_SS00_SS04_09_TT_CONCAT_ACCT_DESCS_SQL_CSV"

csv_to_sqlite(csv_file_path, "300App.db", table_name)
