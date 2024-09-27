import pandas as pd
from datetime import datetime
from datetime import time
import requests
import json
import xmltodict


class SecureConnectionToApi():
    """
    https://data.rte-france.com/documents/20182/22648/FR_GuideOauth2_v5.1.pdf/b02d3246-98bc-404c-81c8-dffaad2f1836
    """

    def __init__(self):
        url_oauth = "https://digital.iservices.rte-france.com/token/oauth/"
        #False key -> to be replace by the real one
        header_oauth_connexion = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic NjdlNWNkOTctYjg0OC00MjgzLWE5MDQtNjMzMjE2YmVjOTM0OmZlNTQ1YjM4LTM4MWYtNGMwNC04NzcxLWY5NDE1ODY2Nzk1Nw=="
        }
        response_from_server = requests.post(url_oauth, headers=header_oauth_connexion)
        self.access_token = response_from_server.json()["access_token"]


class EdfTempoApi(SecureConnectionToApi):
    """
    https://data.rte-france.com/catalog/-/api/doc/user-guide/Tempo+Like+Supply+Contract/1.1
    """

    def __init__(self):
        super().__init__()
        self.api_url = "https://digital.iservices.rte-france.com/open_api/tempo_like_supply_contract/v1/tempo_like_calendars"
        self.message_header = {
            "Authorization": f"Bearer {self.access_token}"
        }

    def api_request_between_two_dates(self, start_date, end_date, fallback_status="false"):
        """
        :param start_date:
        :param end_date:
        :param fallback_status:
        :return:
        """
        try:
            start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_datetime = datetime.strptime(end_date, "%Y-%m-%d").date()
            number_of_days = end_date_datetime - start_date_datetime
            if number_of_days.days > 366:
                raise ValueError("The number of  day send to the api must be < 366")
            if end_date_datetime > datetime.today().date():
                raise ValueError("Wrong end date (we are in future)")
        except ValueError as e:
            print(f"Erreur : {e}")
            exit(1)
        start_date = start_date + "T00:00:00%2B02:00"
        end_date = end_date + "T00:00:00%2B02:00"
        full_url = self.api_url + "?start_date=" + start_date + "&end_date=" + end_date + "&fallback_status=" + fallback_status
        # Example url :  'https://digital.iservices.rte-france.com/open_api/tempo_like_supply_contract/v1/tempo_like_calendars?start_date=2022-09-17T00:00:00%2B02:00&end_date=2023-09-17T00:00:00%2B02:00&fallback_status=false'
        response = requests.get(full_url, headers=self.message_header)
        return response

    def create_color_dict_from_json(self, data_string_to_transform):
        """
        :param data_string_to_transform: JSON (from the API) in string format
        :return: dictionary with the date as key and the color as value
        """
        json_data = json.loads(data_string_to_transform)
        date_color_dict = {}
        for color in json_data['tempo_like_calendars']['values']:
            date = str(color['start_date'])
            date_color_dict[datetime.strptime(date[0:10], "%Y-%m-%d").date()] = color['value']
        return date_color_dict

    def create_color_dict_from_xml(self, data_string_to_transform):
        """
        :param data_string_to_transform: XML (from the API) in string format
        :return: dictionary with the date as key and the color as value
        """
        data_dict = xmltodict.parse(data_string_to_transform)
        date_color_dict = {}
        for color in data_dict['Tempos']['Tempo']:
            date = str(color['DateApplication'])
            date_color_dict[datetime.strptime(date[0:10], "%Y-%m-%d").date()] = color['Couleur']
        return date_color_dict

    def dict_from_two_dates(self, start_date, end_date):
        response = self.api_request_between_two_dates(start_date=start_date, end_date=end_date)
        response_type = response.headers.get("Content-Type")
        date_color_dict = {}
        if "json" in response_type:
            date_color_dict = self.create_color_dict_from_json(response.text)
        if "xml" in response_type:
            date_color_dict = self.create_color_dict_from_xml(response.text)
        return date_color_dict


class EdfVariables():
    def __init__(self):
        self.HP_START_TIME = time(6, 0, 0)
        self.HP_END_TIME = time(22, 0, 0)

        # Connection to the EDF API to get the price
        tempo = EdfTempoApi()
        dict_2021 = tempo.dict_from_two_dates(start_date="2021-01-01", end_date="2021-12-31")
        dict_2022 = tempo.dict_from_two_dates(start_date="2022-01-01", end_date="2022-12-31")
        dict_2023 = tempo.dict_from_two_dates(start_date="2023-01-01", end_date="2023-12-31")
        dict_2024 = tempo.dict_from_two_dates(start_date="2024-01-01", end_date="2024-09-26")
        self.dict_calendar_colors = {**dict_2021, **dict_2022, **dict_2023, **dict_2024}


class EdfClientFileGenerator():
    def __init__(self,
                 csv_file_downloaded='/Users/tsvk/Documents/Projets/edf_tempo/mes-puissances-atteintes-30min-004037583323-75010.csv'):
        self.df = pd.read_csv(csv_file_downloaded, sep=';', encoding='latin-1')
        self.edf_datas = EdfVariables()

        self.client_df = self.correct_bug_in_file(self.df)
        self.add_data_to_df()

    def correct_bug_in_file(self, df):
        """Correction of a bug -> some values are duplicated"""
        rows_to_keep = []
        previous_row = None
        for row in df.itertuples(index=False):
            if previous_row is None or row != previous_row:
                rows_to_keep.append(row)
            previous_row = row
        df_corrected = pd.DataFrame(rows_to_keep, columns=df.columns)
        return df_corrected

    def add_data_to_df (self):
        date_read = None
        rows_to_keep = []
        new_column_with_date_str = []
        new_column_with_date_datetime = []
        new_column_with_hour_price = []
        new_column_with_color = []

        for row in self.client_df.itertuples(index=False):
            if "/" in row[0]:
                date_read = row[0]
            elif ":" in row[0]:
                rows_to_keep.append(row)
                new_column_with_date_str.append(date_read)
                date_time_str = date_read + ' ' + row[0]
                date_time = datetime.strptime(date_time_str, "%d/%m/%Y %H:%M:%S")
                new_column_with_date_datetime.append(date_time)

                try:
                    new_column_with_color.append(self.edf_datas.dict_calendar_colors[date_time.date()])
                except KeyError as e:
                    new_column_with_color.append("NA")

                if self.edf_datas.HP_START_TIME <= date_time.time() <= self.edf_datas.HP_END_TIME:
                    new_column_with_hour_price.append("HP")
                else:
                    new_column_with_hour_price.append("HC")

        self.client_df = pd.DataFrame(rows_to_keep, columns=self.client_df.columns)
        self.client_df['date'] = new_column_with_date_str
        self.client_df['datetime'] = new_column_with_date_datetime
        self.client_df['tarif_horraire'] = new_column_with_hour_price
        self.client_df['code_couleur'] = new_column_with_color


if __name__ == "__main__":
    fichier = EdfClientFileGenerator()
    print(fichier.client_df)
