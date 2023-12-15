from Sales_data_clean.sales_clean import SalesDataProcessing
from Store_data_clean.stores_validation import StoresValidation, StoreXlsxProcessing

if __name__ == '__main__':
    a = StoreXlsxProcessing()
    path_csv_store = a.store_xls_processing()
    store_valid = StoresValidation(path_csv_store)
    store_valid.validate_and_remove_incorrect_regions()
    store_valid.validate_and_correct_coordinates()
    sales_procesing = SalesDataProcessing(path_csv_store)
    sales_procesing.process_sales_data()
