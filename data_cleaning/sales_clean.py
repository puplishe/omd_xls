import pandas as pd

class SalesDataProcessing:
    def __init__(self, sales_xlsx_file, store_csv_file, output_csv_file='combined_sales.csv', change_log_file='changelog.txt') -> None:
        self.sales_xlsx_file = sales_xlsx_file
        self.store_csv_file = store_csv_file
        self.output_csv_file = output_csv_file
        self.change_log = change_log_file

    def _load_store_tt_numbers(self):
        store_df = pd.read_csv(self.store_csv_file, encoding='utf-8')
        return set(store_df['№ ТТ'])

    def _validate_tt_numbers(self, sales_df, store_df, sheet_name):
        for tt_number in sales_df['№ TT'].unique():
            # Check if the TT number is present in the store data
            if tt_number in store_df['№ ТТ'].values:
                # Find the city associated with the TT number in the store data
                store_city = store_df.loc[store_df['№ ТТ'] == tt_number, 'ГОРОД'].iloc[0]
                
                # Extract the city from the sheet name
                sheet_city = sheet_name

                # Compare the cities
                if store_city.lower() != sheet_city.lower():
                    with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                        changelog_file.write(f"Mistakes: Invalid TT number {tt_number} in sales data. "
                                            f"Associated with {store_city}, expected {sheet_city}\n")
            else:
                # Log that the TT number is not present in the store data
                with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                    changelog_file.write(f"Mistakes: TT number {tt_number} in sales data is not present in the store data\n")

    def _load_store_cities(self):
        store_df = pd.read_csv(self.store_csv_file, encoding='utf-8')
        return set(store_df['ГОРОД'].str.lower())

    def process_sales_data(self):
        xl = pd.ExcelFile(self.sales_xlsx_file)
        sales_sheets = [sheet_name for sheet_name in xl.sheet_names if sheet_name.startswith('Sales -')]
        cleaned_dfs = []
        valid_cities = self._load_store_cities()
        store_df = pd.read_csv(self.store_csv_file, encoding='utf-8')  # Load store data once

        for sheet_name in sales_sheets:
            city_name = sheet_name.split('Sales - ')[1].strip()
            if city_name.lower() not in valid_cities:
                with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                    changelog_file.write(f"Mistakes: Incorrect city {city_name}\n")
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
            self._validate_tt_numbers(df_cleaned, store_df, city_name)
        combined_sales_df = pd.concat(cleaned_dfs, ignore_index=True)
        combined_sales_df.to_csv(self.output_csv_file, index=False)
        print(f"Combined sales data saved to {self.output_csv_file}")

        # Validate TT numbers after processing all sheets
