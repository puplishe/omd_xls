import pandas as pd
from geopy.geocoders import Nominatim
import os

class StoresValidation:
    def __init__(self, file_path, log_file_path='modification_log.txt', changelog_file_path='changelog.txt') -> None:
        self.df = pd.read_csv(file_path, encoding='utf-8')
        self.valid_regions = ['Урал', 'Сибирь']
        self.log_file_path = log_file_path
        self.changelog_file_path = changelog_file_path
        self.valid_county = ['Уфимский район', 'Челябинский городской округ', 'городской округ Томск', 'Кемеровский муниципальный округ', 'Омский район', 'Иркутский район', 'Новосибирский район', 'Оренбургский район', 'Емельяновский район', 'городской округ Барнаул']
        self.mistakes_counts = {'coordinates': 0, 'dates_incorrect': 0, 'dates_missing': 0}
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, 'w'):
                pass

    def _save_to_csv(self, file_path):
        self.df.to_csv(file_path, index=False, encoding='utf-8')

    def _validate_dates(self, tt_number, date_opened, date_closed):
        if pd.isnull(date_opened):
            self._log_modification(tt_number, "Open Date is missing")
            self.mistakes_counts['dates_missing'] += 1
        if pd.to_datetime(date_opened) > pd.to_datetime(date_closed):
            self._log_modification(tt_number, f"Open date is greater than Closed Date: {date_opened} > {date_closed}")
            self.mistakes_counts['dates_incorrect'] += 1
    
    def _validate_and_correct_coordinates(self, tt_number, latitude, longitude, city, region):
        if not (-90 <= latitude <= 90):
            self._log_modification(tt_number, f"Invalid latitude, coordinates swapped: {latitude}, {longitude} -> {longitude}, {latitude}")
            self.mistakes_counts['coordinates'] += 1
            return (longitude, latitude)
        if not (-180 <= longitude <= 180):
            self._log_modification(tt_number, f"Invalid longitude, no change made")
            return (latitude, longitude)
        geolocator = Nominatim(user_agent="your_app_name") 
        try:
            location = geolocator.reverse((latitude, longitude), language='ru')
            if location is None:
                self._log_modification(tt_number, f"Coordinates swapped: {latitude}, {longitude} -> {longitude}, {latitude}")
                self.mistakes_counts['coordinates'] += 1
                return (longitude, latitude)
            location_data = location.raw.get('address', {})
            if 'city' in location_data and city.lower() in location_data['city'].lower():
                self._log_modification(tt_number, f"Valid coordinates: {latitude}, {longitude}")
            elif 'county' in location_data and location_data['county'].lower() in map(str.lower, self.valid_county):
                self._log_modification(tt_number, f"Valid coordinates: {latitude}, {longitude}")
            else:
                self._log_modification(tt_number, f"Coordinates swapped: {latitude}, {longitude} -> {longitude}, {latitude}")
                self.mistakes_counts['coordinates'] += 1
                return (longitude, latitude)
        except Exception as e:
            self._log_modification(tt_number, f"Error during geocoding: {str(e)}, no change made")
            return (latitude, longitude)
        return (latitude, longitude)

    def _log_modification(self, tt_number, modification):
        with open(self.log_file_path, 'a') as log_file:
            log_file.write(f"TT number: {tt_number}, Log: {modification}\n")

    def _generate_changelog(self):
        with open(self.changelog_file_path, 'w') as changelog_file:
            changelog_file.write(f"Mistakes incorrect coordinates {self.mistakes_counts['coordinates']} times\n")
            changelog_file.write(f"Mistakes incorrect dates {self.mistakes_counts['dates_incorrect']} times\n")
            changelog_file.write(f"Mistake missing dates {self.mistakes_counts['dates_missing']} times\n")

    def validate_and_correct_coordinates(self):
        for index, row in self.df.iterrows():
            tt_number = row['№ ТТ']
            latitude = row['ШИР.']
            longitude = row['ДОЛ.']
            city = row['ГОРОД']
            region = row['РЕГИОН']
            date_opend = row['ДАТА ОТКР.']
            date_closed = row['ДАТА ЗАКР.']
            new_coordinates = self._validate_and_correct_coordinates(tt_number, latitude, longitude, city, region)
            self._validate_dates(tt_number, date_opend, date_closed)
            self.df.at[index, 'ШИР.'] = new_coordinates[0]
            self.df.at[index, 'ДОЛ.'] = new_coordinates[1]

        self._save_to_csv('corrected_stores.csv')
        self._generate_changelog()

    def validate_and_remove_incorrect_regions(self):
        self.df = self.df[self.df['РЕГИОН'].isin(self.valid_regions)]
        self._save_to_csv('corrected_stores.csv')