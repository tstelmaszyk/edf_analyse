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
        header_oauth_connexion = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic "
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
        try :
            start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_datetime = datetime.strptime(end_date, "%Y-%m-%d").date()
            number_of_days = end_date_datetime - start_date_datetime
            if number_of_days.days > 366 :
                raise ValueError("The number of  day send to the api must be < 366")
            if end_date_datetime > datetime.today().date() :
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

    def create_color_dict_from_xml(self,data_string_to_transform):
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
        response = self.api_request_between_two_dates(start_date=start_date,end_date=end_date)
        response_type = response.headers.get("Content-Type")
        date_color_dict = {}
        if "json" in response_type:
            date_color_dict = tempo.create_color_dict_from_json(response.text)
        if "xml" in response_type:
            date_color_dict = tempo.create_color_dict_from_xml(response.text)
        return date_color_dict



tempo = EdfTempoApi()
dict_2021 = tempo.dict_from_two_dates(start_date="2021-01-01",end_date="2021-12-31")
dict_2022 = tempo.dict_from_two_dates(start_date="2022-01-01",end_date="2022-12-31")
dict_2023 = tempo.dict_from_two_dates(start_date="2023-01-01",end_date="2023-12-31")
dict_2024 = tempo.dict_from_two_dates(start_date="2024-01-01",end_date="2024-09-26")

dico = {**dict_2021, **dict_2022, **dict_2023, **dict_2024}



# Créer des objets time
HP_START = time(6, 0, 0)
HP_END = time(22, 0, 0)

# Lire le fichier CSV
# todo supprimer la première ligne du fichier
df = pd.read_csv('/Users/tsvk/Documents/Projets/edf_tempo/mes-puissances-atteintes-30min-004037583323-75010.csv',
                 sep=';', encoding='latin-1')

######
# correction d'un bug dans le fichier
# -> Certaines valeurs sont en doubles
rows_to_keep = []
ligne_precedente = None
# Parcourir chaque ligne avec itertuples
for row in df.itertuples(index=False):
    if ligne_precedente is None or row != ligne_precedente:
        rows_to_keep.append(row)
    ligne_precedente = row
# Transformer la liste de tuples en DataFrame
df_modifie = pd.DataFrame(rows_to_keep, columns=df.columns)

######
# Transformation des données : sur chaque ligne la date au lieu d'être en début de bloc
# ajout dune colonne datetimeformat
date = None
rows_to_keep = []
nouvelle_colonne_date = []
nouvelle_colonne_datetime = []
nouvelle_colone_tarif_heure = []
nouvelle_colone_coleur = []
for row in df_modifie.itertuples(index=False):
    if "/" in row[0]:
        date = row[0]
    elif ":" in row[0]:
        rows_to_keep.append(row)
        nouvelle_colonne_date.append(date)
        date_time_str = date + ' ' + row[0]
        date_time = datetime.strptime(date_time_str, "%d/%m/%Y %H:%M:%S")
        nouvelle_colonne_datetime.append(date_time)

        try:
            nouvelle_colone_coleur.append(dico[date_time.date()])
        except KeyError as e:
            nouvelle_colone_coleur.append("NA")

        if HP_START <= date_time.time() <= HP_END:
            nouvelle_colone_tarif_heure.append("HP")
        else:
            nouvelle_colone_tarif_heure.append("HC")

df_modifie = pd.DataFrame(rows_to_keep, columns=df_modifie.columns)
df_modifie['date'] = nouvelle_colonne_date
df_modifie['datetime'] = nouvelle_colonne_datetime
df_modifie['tarif_horraire'] = nouvelle_colone_tarif_heure
df_modifie['code_couleur'] = nouvelle_colone_coleur
print(df_modifie)
