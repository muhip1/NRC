# File to push NDX countries list into Kobo but not including pcodes (yet)
#!/usr/bin/env python3

# Requirements are pandas, hdx-python-api-, openpyxl

import warnings
warnings.filterwarnings('ignore')

import csv
import json
import requests
import base64
import os
from time import sleep
from datetime import datetime
from glob import glob
from io import StringIO

import pandas as pd

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset


def get_ndx_countries(ndx_api_token):
    # NDX
    url = "https://exchange.nrc.no/dataset/8f884a61-506f-4d0a-9347-2d069378c574/resource/d03c944d-d2b3-4679-a9d7-3efce6715271/download/countries-and-territories.csv"
    headers = {'Authorization': ndx_api_token}

    response = requests.request("GET", url, headers=headers)

    raw_ndx_data = pd.read_csv(StringIO(response.text), sep=",")
    COUNTRY_CODE_DATA  = raw_ndx_data[['Countries and Territories', 'ISO3']]
    COUNTRY_CODE_DATA.rename(columns = {'Countries and Territories': 'country', 'ISO3': 'code'}, inplace = True)


def get_country_code_map(ndx_api_token):
    COUNTRY_CODE_DATA = get_ndx_countries(ndx_api_token)
    with open(COUNTRY_CODE_DATA) as csvfile:
        data = csv.DictReader(csvfile)
        country_code_map = {row['code']:row['country'] for row in data}
    return country_code_map


def get_config(config=None):
    if config is None:
        with open('config.json', 'r') as f:
            config = json.loads(f.read())

    kobo_config = []
    for kc in config['kobo_config']:
        kobo_config.append({
            **kc,
            "asset_url": f'{kc["kf_url"]}/api/v2/assets/',
            "asset_bulk_url": f'{kc["kf_url"]}/api/v2/assets/bulk/',
            "parent_url": f'{kc["kf_url"]}/api/v2/assets/{kc["parent_uid"]}/',
            "import_url": f'{kc["kf_url"]}/api/v2/imports/',
            "headers": {'Authorization': f'Token {kc["token"]}'}
        })

    config.update({
        "pcodes_url": 'https://data.humdata.org/dataset/global-pcodes',
        "pcodes_file": 'global_pcodes.csv',
        "pcodes_path": '/tmp/global_pcodes.csv',
        "dataset_name": 'global pcodes',
        "xlsform_path": '/tmp/xlsforms',
        "kobo_config": kobo_config
    })

    config['kobo_config'] = kobo_config

    return config


### ------------------------------------- ###
### ----- DOWNLOAD PCODES FROM HDX ------ ###
### ------------------------------------- ###

def download_global_pcodes(pcodes_file, pcodes_path):
    datasets = Dataset.search_in_hdx(pcodes_file)

    resources = Dataset.get_all_resources(datasets)
    for resource in resources:
        if pcodes_file in resource['name']:
            url = resource['url']

    file_response = requests.get(url)
    file_response.raise_for_status()

    with open(pcodes_path, 'wb') as f:
        f.write(file_response.content)


### ------------------------------------- ###
### --- GENERATE XLSFORMS FROM PCODES --- ###
### ------------------------------------- ###

def get_questions(max_level):
    questions = []
    for i in range(1, max_level + 1):
        questions.append(
            {
                "type": f"select_one level_{i}",
                "name": f"level_{i}",
                "label": f"Level {i}",
                "choice_filter": "starts-with(name, ${" + f'level_{i - 1}' + "})" if i > 1 else "",
                "default": "\"-\"",
                "appearance": "minimal",
                "hxl": "#adm+code"
            }
        )
    return pd.DataFrame(questions)


def get_choices(df):
    choices = []
    for i, row in df.iterrows():
        choices.append(
            {
                'list_name': f'level_{row["Admin Level"]}',
                'name': row['P-Code'],
                'label': row['Name']
            }
        )
    return pd.DataFrame(choices)


def get_settings(form_title):
    settings = [{
        'form_title': form_title,
        'version': str(datetime.now()),
        'allow_choice_duplicates': 'yes'
    }]
    return pd.DataFrame(settings)


def generate_xlsforms(xlsform_path, pcodes_path, country_code_map):

    df = pd.read_csv(pcodes_path, low_memory=False)
    df.drop(0, inplace=True)

    if not os.path.isdir(xlsform_path):
        os.makedirs(xlsform_path)

    countries = df['Location'].unique()
    for country_code in countries:
        df_loc = df[df['Location'] == country_code]
        filename_base = f'{country_code} ({country_code_map[country_code]})'
        with pd.ExcelWriter(f'{xlsform_path}/{filename_base}.xlsx') as writer:
            get_questions(df_loc['Admin Level'].astype(int).max()).to_excel(writer, sheet_name="survey", index=False)
            get_choices(df_loc).to_excel(writer, sheet_name="choices", index=False)
            get_settings(filename_base).to_excel(writer, sheet_name="settings", index=False)


### ------------------------------------- ###
### ---- HANDLE KOBO SIDE OF THINGS ----- ###
### ------------------------------------- ###

def get_params(parent_uid=None):
    q = f'(asset_type:template OR asset_type:block OR asset_type:question) AND '
    q = q + f'parent__uid:{parent_uid}' if parent_uid else q + 'parent:null'
    return {
        'q': q,
        'limit': '1000',
        'metadata': 'on',
        'collections_first': 'true',
        'format': 'json'
    }

def delete_assets_in_collection(config):
    res = requests.get(
        url=config['asset_url'],
        params=get_params(config['parent_uid']),
        headers=config['headers']
    )
    res.raise_for_status()
    assets = res.json()['results']

    asset_uids = [asset['uid'] for asset in assets]
    payload = {
        'payload': {
            'asset_uids': asset_uids,
            'action': 'delete'
        }
    }
    if asset_uids:
        res = requests.post(config['asset_bulk_url'], headers=config['headers'], json=payload)


def move_assets_to_collection(config, country_code_map):
    res = requests.get(url=config['asset_url'], params=get_params(), headers=config['headers'])
    res.raise_for_status()
    assets = res.json()['results']

    for asset in assets:
        if asset['name'][:3] in country_code_map:
            res = requests.patch(asset['url'], headers=config['headers'], data={'parent': config['parent_url']})
            res.raise_for_status()

def upload_xlsforms(config, xlsform_path):
    for path in glob(f'{xlsform_path}/*'):
        filename = os.path.basename(path)
        with open(path, 'rb') as file:
            file_content = file.read()

        encoded_content = base64.b64encode(file_content)

        files = {
            'library': (None, 'true'),
            'name': (None, filename),
            'base64Encoded': (
                None,
                'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,' + encoded_content.decode()
            ),
            'desired_type': (None, 'block'),
        }

        response = requests.post(url=config['import_url'], files=files, headers=config['headers'])
        response.raise_for_status()


def connect_to_hdx():
    Configuration.create(
        hdx_site="prod",
        user_agent="Kobo_pcodes",
        hdx_read_only=True,
    )


def main():
    config = get_config()
    ndx_api_token = config['ndx_api_token']
    country_code_map = get_country_code_map(ndx_api_token)

    # configire HDX connector
    connect_to_hdx()

    # download global pcodes
    print('Getting pcodes')
    download_global_pcodes(
        pcodes_file=config['pcodes_file'],
        pcodes_path=config['pcodes_path']
    )

    # create xlsforms from pcode file
    print('Generating xlsforms')
    generate_xlsforms(
        xlsform_path=config['xlsform_path'],
        pcodes_path=config['pcodes_path'],
        country_code_map=country_code_map
    )

    # handle Kobo operations to move to public collection
    print('Creating Kobo assets')
    for kc in config['kobo_config']:
        delete_assets_in_collection(kc)
        upload_xlsforms(kc, xlsform_path=config['xlsform_path'])
        # Wait for all assets to be uploaded before moving
        sleep(30)
        move_assets_to_collection(kc, country_code_map)


if __name__ == '__main__':
    main()
