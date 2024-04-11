import pandas as pd

def compare_csv_files(file_path1, file_path2):
    try:
        # Đọc dữ liệu từ hai tệp CSV vào hai DataFrame
        df1 = pd.read_csv(file_path1,sep='\t')
        df2 = pd.read_csv(file_path2, sep='\t')
        
        # So sánh hai DataFrame
        if df1.equals(df2):
            print("Hai tệp CSV giống nhau.")
        else:
            print("Hai tệp CSV khác nhau.")
        
    except Exception as e:
        print("Error:", e)

# Sử dụng hàm để so sánh hai tệp CSV
file1_path = "file1.csv"
file2_path = "file2.csv"
compare_csv_files(file1_path, file2_path)