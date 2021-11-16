import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import ast

url_params = {
    'location_ids' : ['474368'], # Kolla söksträngen på hemnet
    'item_types' : ['villa'], # 'bostadsratt', 'villa', 'radhus' osv
    'rooms_min' : '', # Lägst antal rum
    'rooms_max' : '', # Högst antal rum
    'living_area_min' : '', # Lägst antal kvm
    'living_area_max' : '', # Högst antal kvm
    'fee_min' : '', # Lägsta månadsavgiften
    'fee_max' : '', # Högsta månadsavgiften
    'sold_age' : '', # Tom för 12 månaders historik. Sätt denna till 'all' för att ta med historik from 2013. 
}

# Skapa url från dict ovan
url_string = 'https://www.hemnet.se/salda/bostader?'

locations_ids_list = url_params.get('location_ids')
i = 0
for location_id in locations_ids_list:
    i = i + 1
    if i > 1:
        url_string = url_string + '&'
    url_string = url_string + 'location_ids%5B%5D=' + location_id

item_types_list = url_params.get('item_types')
for item_type in item_types_list:
    url_string = url_string + '&item_types%5B%5D=' + item_type

if url_params.get('rooms_min'):
    url_string = url_string + '&rooms_min=' + url_params.get('rooms_min')

if url_params.get('rooms_max'):
    url_string = url_string + '&rooms_max=' + url_params.get('rooms_max')

if url_params.get('living_area_min'):
    url_string = url_string + '&living_area_min=' + url_params.get('living_area_min')

if url_params.get('living_area_max'):
    url_string = url_string + '&living_area_max=' + url_params.get('living_area_max')
    
if url_params.get('fee_min'):
    url_string = url_string + '&fee_min=' + url_params.get('fee_min')
    
if url_params.get('fee_max'):
    url_string = url_string + '&fee_max=' + url_params.get('fee_max')

if url_params.get('sold_age'):
    url_string = url_string + '&sold_age=' + url_params.get('sold_age')

# Scrapea med BeautifulSoup
page = 0
all_lgh_link_list = []
while True:
    page = page + 1
    sida = requests.get(url_string+'&page='+str(page), headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(sida.content,features="html.parser")
    
    if soup.find_all('div', class_='sold-zero-hits'):
        break
    else:
        omraden_soup = soup.find('div', class_='location-search-post textinput')
        input_string = omraden_soup.find('input').get('value')
        input_list = ast.literal_eval(input_string)
        omrade_string =''
        for omrade_info in input_list:
            omrade_string = omrade_string + omrade_info['name'] + ', ' + omrade_info['parent_name'] +'\n'
        
        lgh_link_list = soup.find_all('a',class_='item-link-container')
        all_lgh_link_list.extend(lgh_link_list)

data_dict_list = []
for lgh_link in all_lgh_link_list:
    lgh_sida = requests.get(lgh_link['href'], headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(lgh_sida.content, from_encoding='utf-8', features="html.parser")
    
    body = soup.find('body')
    script = body.find_all('script')[1]
    text = script.getText()
    
    start_index = text.find('{\"sold_property')
    end_index = text.find('}}]')
    
    data = text[start_index+17:end_index]
    #avgift = body.find_all('dd', class_='sold-property__attribute-value')[5].getText()
    if soup.find('dt', text="Avgift/månad"):
        avgift = soup.find('dt', text="Avgift/månad").next_sibling.next_sibling.getText()
        avgift = int(avgift[0:len(avgift)-7].encode('ascii','ignore'))
    else:
        avgift = -1
        
    if soup.find('dt', text="Byggår"):
        byggar = soup.find('dt', text="Byggår").next_sibling.next_sibling.getText()
    else:
        byggar = -1
    try:
        byggar = int(byggar)
    except ValueError:
        byggar = -1
    if byggar == 0:
        byggar = -1
    
    typ_text_soup = body.find('p', class_='sold-property__metadata')
    typ = typ_text_soup.find('title').getText()  

    data = data + ",\"avgift\": " + str(avgift) + ",\"byggar\" : " + str(byggar) + " ,\"typ\" : \"" + typ + "\"} "
    data_dict = ast.literal_eval(data)
    data_dict_list.append(data_dict)

lgh_df = pd.DataFrame(data_dict_list)
lgh_df = lgh_df.astype({'living_area':'float64','rooms':'float64','sold_at_date':'datetime64'})
lgh_df['utgang_kvm_pris'] = round(lgh_df['price']/lgh_df['living_area'],0)
lgh_df['slut_kvm_pris'] = round(lgh_df['selling_price']/lgh_df['living_area'],0)
lgh_df = lgh_df.replace(-1,np.NaN)
lgh_df = lgh_df.astype({'byggar':'Int64'})

col_list=['broker_agency','location','locations','street_address','typ']
for name in col_list:
    lgh_df[name] = lgh_df[name].astype(str)

# Tanka in i db. Obs att den gör ny db-fil om ingen existerar
import sqlite3
conn = sqlite3.connect('HN.db')

c = conn.cursor()

# Skapa tabell
sql_string = """ CREATE TABLE IF NOT EXISTS hemnet (
id INTEGER unique,
  broker_agency TEXT,
  broker_agency_id INTEGER,
  location TEXT,
  locations TEXT,
  street_address TEXT,
  price INTEGER,
  selling_price INTEGER,
  rooms REAL,
  living_area REAL,
  sold_at_date TIMESTAMP,
  avgift REAL,
  byggar INTEGER,
  typ TEXT,
  utgang_kvm_pris REAL,
  slut_kvm_pris REAL
); """
c.execute(sql_string)

lgh_df.to_sql('hemnet', conn, if_exists='append', index = False)

