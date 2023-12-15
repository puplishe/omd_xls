from datetime import datetime

import pandas as pd


class SalesDataProcessing:
    def __init__(self, store_csv_file: str, sales_xlsx_file: str = 'input_data/data.xlsx',
                 output_csv_file: str = 'result_data/combined_sales.csv', change_log_file: str = 'logs/changelog.txt') -> None:
        self.sales_xlsx_file: str = sales_xlsx_file
        self.store_csv_file: str = store_csv_file
        self.output_csv_file: str = output_csv_file
        self.change_log: str = change_log_file
        self.logged_tt_numbers: set = set()

    def _load_store_tt_numbers(self) -> set:
        store_df = pd.read_csv(self.store_csv_file, encoding='utf-8')
        return set(store_df['№ ТТ'])

    def _validate_tt_numbers(self, sales_df: pd.DataFrame, store_df: pd.DataFrame, sheet_name: str) -> None:
        for tt_number in sales_df['№ TT'].unique():
            if tt_number in store_df['№ ТТ'].values:
                store_city = store_df.loc[store_df['№ ТТ'] == tt_number, 'ГОРОД'].iloc[0]
                sheet_city = sheet_name
                if store_city.lower() != sheet_city.lower():
                    with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                        changelog_file.write(f'Mistakes: Invalid TT number {tt_number} in sales data. '
                                             f'Associated with {store_city}, expected {sheet_city}\n')
            else:
                with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                    changelog_file.write(
                        f'Mistakes: TT number {tt_number} in sales data is not present in the store data\n')

    def _load_store_cities(self) -> set:
        store_df = pd.read_csv(self.store_csv_file, encoding='utf-8')
        return set(store_df['ГОРОД'].str.lower())

    def _validate_store_dates(self, sales_df: pd.DataFrame, store_df: pd.DataFrame) -> None:
        store_df['ДАТА ЗАКР.'] = pd.to_datetime(store_df['ДАТА ЗАКР.'], errors='coerce')
        store_df['ДАТА ОТКР.'] = pd.to_datetime(store_df['ДАТА ОТКР.'], errors='coerce')
        min_sale_date = sales_df['НЕДЕЛЯ'].min()
        if min_sale_date != datetime(2018, 1, 1):
            with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                changelog_file.write(f'Mistakes: TT NUMBER Sales data should start from January 1, 2018. '
                                     f'Found start date: {min_sale_date}.\n')
        max_sale_dates = sales_df.groupby('№ TT')['НЕДЕЛЯ'].max()
        closed_stores = store_df.dropna(subset=['ДАТА ЗАКР.'])
        for index, row in closed_stores.iterrows():
            store_tt = row['№ ТТ']
            close_date = row['ДАТА ЗАКР.']
            if store_tt in max_sale_dates and not pd.isnull(close_date) and close_date < max_sale_dates[store_tt]:
                with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                    changelog_file.write(f'Mistakes: TT NUMBER Store {store_tt} closed on {close_date}, '
                                         f'but sales data available until {max_sale_dates[store_tt]}.\n')

        opened_stores = store_df.dropna(subset=['ДАТА ОТКР.'])
        for index, row in opened_stores.iterrows():
            store_tt = row['№ ТТ']
            open_date = row['ДАТА ОТКР.']
            if store_tt in max_sale_dates and not pd.isnull(open_date) and open_date > max_sale_dates[store_tt]:
                with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                    changelog_file.write(f'Mistakes: TT NUMBER Store {store_tt} opened on {open_date}, '
                                         f'but sales data available from {max_sale_dates[store_tt]}.\n')

    def process_sales_data(self) -> None:
        """
        Processing from xlxs into validated csv sales file
        """
        xl = pd.ExcelFile(self.sales_xlsx_file)
        sales_sheets = [sheet_name for sheet_name in xl.sheet_names if sheet_name.startswith('Sales -')]
        cleaned_dfs = []
        valid_cities = self._load_store_cities()
        store_df = pd.read_csv(self.store_csv_file, encoding='utf-8')

        for sheet_name in sales_sheets:
            city_name = sheet_name.split('Sales - ')[1].strip()
            if city_name.lower() not in valid_cities:
                with open(self.change_log, 'a', encoding='utf-8') as changelog_file:
                    changelog_file.write(f'Mistakes: Incorrect city {city_name}\n')
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
                self._validate_store_dates(df_cleaned, store_df)

        combined_sales_df = pd.concat(cleaned_dfs, ignore_index=True)
        combined_sales_df.to_csv(self.output_csv_file, index=False)
        print(f'Combined sales data saved to {self.output_csv_file}')
