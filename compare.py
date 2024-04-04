import pandas as pd

def compare_csv_files(file_path1, file_path2):
    try:
        df1 = pd.read_csv(file_path1)
        df2 = pd.read_csv(file_path2)
        
        if df1.equals(df2):
            print("Hai tệp CSV giống nhau.")
        else:
            print("Hai tệp CSV khác nhau.")
        
    except Exception as e:
        print("Error:", e)

file1_path = "file1.csv"
file2_path = "file2.csv"
compare_csv_files(file1_path, file2_path)