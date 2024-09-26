import pandas as pd
from datetime import datetime
from datetime import time
import requests
import json
import xmltodict


url ="https://digital.iservices.rte-france.com/token/oauth/"
api_url = 'https://digital.iservices.rte-france.com/open_api/tempo_like_supply_contract/v1/tempo_like_calendars?start_date=2022-09-17T00:00:00%2B02:00&end_date=2023-09-17T00:00:00%2B02:00&fallback_status=false'  # URL de l'API à laquelle vous voulez accéder

header_secu = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "Basic "
}
response = requests.post(url,headers=header_secu)
access_token = response.json()["access_token"]


header = {
    "Authorization": f"Bearer {access_token}"
}
response = requests.get(api_url,headers=header)
response_type = response.headers.get("Content-Type")
dico = {}

if "json" in response_type :
    data = json.loads(response.text)
    print(data['tempo_like_calendars']['values'][0]['value'])
    for couleur in data['tempo_like_calendars']['values'] :
        date = str(couleur['start_date'])
        dico[datetime.strptime(date[0:10], "%Y-%m-%d").date()]=couleur['value']
if "xml" in response_type :
    print("xml")
    data_dict = xmltodict.parse(response.text)
    json_data = json.dumps(data_dict)
    jso = json.loads(json_data)
    for couleur in data_dict['Tempos']['Tempo']:
        date = str(couleur['DateApplication'])
        dico[datetime.strptime(date[0:10], "%Y-%m-%d").date()] = couleur['Couleur']



# Créer des objets time
HP_START = time(6,0,0)
HP_END = time(22,0,0)

# Lire le fichier CSV
#todo supprimer la première ligne du fichier
df = pd.read_csv('/Users/tsvk/Documents/Projets/edf_tempo/mes-puissances-atteintes-30min-004037583323-75010.csv', sep=';', encoding='latin-1')

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
    if "/"in row[0] :
        date = row[0]
    elif ":" in row[0] :
        rows_to_keep.append(row)
        nouvelle_colonne_date.append(date)
        date_time_str = date + ' ' + row[0]
        date_time = datetime.strptime(date_time_str, "%d/%m/%Y %H:%M:%S")
        nouvelle_colonne_datetime.append(date_time)

        try:
            nouvelle_colone_coleur.append(dico[date_time.date()])
        except KeyError as e :
            nouvelle_colone_coleur.append("NA")


        if HP_START <= date_time.time() <= HP_END:
            nouvelle_colone_tarif_heure.append("HP")
        else :
            nouvelle_colone_tarif_heure.append("HC")

df_modifie = pd.DataFrame(rows_to_keep, columns=df_modifie.columns)
df_modifie['date'] = nouvelle_colonne_date
df_modifie['datetime'] = nouvelle_colonne_datetime
df_modifie['tarif_horraire'] = nouvelle_colone_tarif_heure
df_modifie['code_couleur'] = nouvelle_colone_coleur
print(df_modifie)








