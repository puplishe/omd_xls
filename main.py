import pandas as pd
from data_validation.stores_validation import StoresValidation
from geopy.geocoders import Nominatim
from data_cleaning.sales_clean import SalesDataProcessing

#a = StoresValidation('corrected_stores.csv')
#a.validate_and_remove_incorrect_regions()
#a.validate_and_correct_coordinates()

a=SalesDataProcessing('input_data/data.xlsx','corrected_stores.csv')
a.process_sales_data()