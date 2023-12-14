import pandas as pd
sales_xlsx_file = 'input_data/data.xlsx'
xl = pd.ExcelFile(sales_xlsx_file)

# Get all sheets that start with 'Sales -'
sales_sheets = [sheet_name for sheet_name in xl.sheet_names if sheet_name.startswith('Sales -')]
print("Sales Sheet Names:", sales_sheets)

# List to store individual cleaned DataFrames
cleaned_dfs = []

# Process each sales sheet
for sheet_name in sales_sheets:
    # Read the entire sheet without skipping rows
    df = xl.parse(sheet_name, header=None)

    # Remove rows that contain only empty values before reaching the data
    df = df[df.notna().any(axis=1)]

    # Skip the first row
    df = df.iloc[1:]

    # Set the column headers explicitly
    df.columns = ['№ TT', 'НЕДЕЛЯ', 'КОЛ-ВО']

    # Convert column names to strings
    df.columns = df.columns.astype(str)

    # Find the index of the first column that doesn't contain 'Unnamed'
    first_data_col_index = next((i for i, col in enumerate(df.columns) if 'Unnamed' not in col), None)

    if first_data_col_index is not None:
        # Shift data to the left by removing leading empty columns
        df_cleaned = df.iloc[:, first_data_col_index:]

        # Drop columns with 'Unnamed' in the name
        df_cleaned = df_cleaned.loc[:, ~df_cleaned.columns.str.contains('Unnamed')]

        # Keep only the first three columns
        df_cleaned = df_cleaned.iloc[:, :3]

        # Remove rows that contain only empty values before or after the data
        df_cleaned = df_cleaned[df_cleaned.notna().any(axis=1)]

        # Append the cleaned DataFrame to the list
        cleaned_dfs.append(df_cleaned)

# Concatenate all cleaned DataFrames into one DataFrame
combined_sales_df = pd.concat(cleaned_dfs, ignore_index=True)

# Save the combined DataFrame to a single CSV file
combined_sales_df.to_csv('combined_sales.csv', index=False)




class SalesDataProcessing:
    def __init__(self, sales_xlsx_file, store_csv_file, output_csv_file='combined_sales.csv', change_log_file='changelog.txt') -> None:
        self.sales_xlsx_file = sales_xlsx_file
        self.store_csv_file = store_csv_file
        self.output_csv_file = output_csv_file
        self.change_log = change_log_file
    def _load_store_cities(self):
        store_df = pd.read_csv(self.store_csv_file, encoding='utf-8')
        return set(store_df['ГОРОД'].str.lower())

    def process_sales_data(self):
        xl = pd.ExcelFile(self.sales_xlsx_file)
        sales_sheets = [sheet_name for sheet_name in xl.sheet_names if sheet_name.startswith('Sales -')]
        cleaned_dfs = []
        valid_cities = self._load_store_cities()
        for sheet_name in sales_sheets:
            city_name = sheet_name.split('Sales - ')[1].strip()
            if city_name.lower() not in valid_cities:
                with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                    changelog_file.write(f"Mistakes incorrect city {city_name}\n")
                continue
            df = xl.parse(sheet_name, header=None)
            df = df[df.notna().any(axis=1)]
            df = df.iloc[1:]
            df.columns = ['№ TT', 'НЕДЕЛЯ', 'КОЛ-ВО']
            df.columns = df.columns.astype(str)
            first_data_col_index = next((i for i, col in enumerate(df.columns) if 'Unnamed' not in col), None)
            if first_data_col_index is not None:
                df_cleaned = df.iloc[:, first_data_col_index:]
                df_cleaned = df_cleaned.loc[:, ~df_cleaned.columns.str.contains('Unnamed')]
                df_cleaned = df_cleaned.iloc[:, :3]
                df_cleaned = df_cleaned[df_cleaned.notna().any(axis=1)]
                cleaned_dfs.append(df_cleaned)
        combined_sales_df = pd.concat(cleaned_dfs, ignore_index=True)
        combined_sales_df.to_csv(self.output_csv_file, index=False)
        print(f"Combined sales data saved to {self.output_csv_file}")



        