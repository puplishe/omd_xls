import pandas as pd

# Загрузка данных из Excel-файла
file_path = 'input_data/data.xlsx'
xl = pd.ExcelFile(file_path)

# Извлечение данных из листа "stores"
stores_df = xl.parse('​Stores', skiprows=1)

stores_df.columns = ['№ ТТ','БЛК', 'КДЦ_OLD','ГОРОД', 'РЕГИОН', 'ШИР.', 'ДОЛ.', 'ДАТА ОТКР.', 'ДАТА ЗАКР.']

# Сохранение данных о магазинах в CSV-файл
stores_df.to_csv('stores.csv', index=False, encoding='utf-8')

# Load data from CSV file skipping the first two rows
stores_df = pd.read_csv('stores.csv', encoding='utf-8')

# Remove unnamed columns
stores_df = stores_df.loc[:, ~stores_df.columns.str.contains('^Unnamed')]

# Find the index where the issue starts
shifted_index = stores_df[stores_df.iloc[:, 0].str.startswith('N150')].index[0]

# Shift columns to the right for rows after the issue
stores_df.iloc[shifted_index:, 2:] = stores_df.iloc[shifted_index:, 1:-1].values

# Replace incorrect values in the shifted column with "-"
stores_df.iloc[1:, 2] = '-'
stores_df.iloc[:, 0] = stores_df.iloc[:, 0].str.replace('N', '')

# Save the corrected data to a new CSV file
stores_df.to_csv('corrected_stores.csv', index=False, encoding='utf-8')