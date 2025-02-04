import wikipedia
import json
import time
import os

import pandas as pd
import numpy as np
import boto3
import awswrangler as wr

from bs4 import BeautifulSoup
from difflib import SequenceMatcher
from dotenv import load_dotenv

load_dotenv()

def get_unique_ship_id_list():
    DATABASE = os.environ['DATABASE']
    TABLE = os.environ['TABLE']
    OUTPUT_LOCATION = os.environ['QUERY_LOCATION']
    
    my_session = boto3.session.Session(
        region_name=os.environ['REGION'], 
        aws_access_key_id=os.environ['ACCESS_KEY'], 
        aws_secret_access_key=os.environ['SECRET']
    )
    
    query = f"""
        WITH latest_versions AS (
            SELECT CAST(year AS INTEGER) AS year, MAX(CAST(version AS INTEGER)) AS latest_version
            FROM "{DATABASE}"."{TABLE}"
            GROUP BY CAST(year AS INTEGER)
        ),

        latest_data AS (
            SELECT *
            FROM "{DATABASE}"."{TABLE}" se
            JOIN latest_versions lv
            ON CAST(se.year AS INT) = lv.year
            AND CAST(se.version AS INT) = lv.latest_version
        )
        
        SELECT DISTINCT imo_number, name FROM latest_data;
    """
    
    distinc_imo_numbers = wr.athena.read_sql_query(query, database=DATABASE, boto3_session=my_session)
    print(distinc_imo_numbers.shape)
    
    return distinc_imo_numbers

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def get_wikipedia_page(page_title: str):
    print('Page title: ', page_title)
    page_object = wikipedia.page(page_title, auto_suggest=False)
    html_page = page_object.html()
    
    return html_page

def get_info_box_from_article(html_page):
    soup = BeautifulSoup(html_page, "html.parser")
    table = soup.find('table', attrs={'class':'infobox'})
    table_body = table.find('tbody')
    
    ship_info_dict = {}
    rows = table_body.find_all('tr', attrs={'style': 'vertical-align:top;'})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        
        if len(cols) == 2:
            ship_info_dict[cols[0]] = cols[1]
    
    return ship_info_dict


def main():
    ships_not_found = []
    ship_info_json = {}
    distinct_imo_numbers = get_unique_ship_id_list()
    
    for index, row in distinct_imo_numbers[['imo_number', 'name']].iterrows():
        print(index, ' ', row['imo_number'], ' ',row['name'])
        result = wikipedia.search(f"IMO number: {row['imo_number']}", results = 1)
        
        if not result:
            ships_not_found.append(row['imo_number'])
            time.sleep(10)
            continue
        
        if similar(row['name'].lower(), result[0].lower()) > 0.6:
            print(result)
            time.sleep(5)
            article_html_page = get_wikipedia_page(result[0])
            ship_info = get_info_box_from_article(html_page=article_html_page)
            ship_info_json[row['imo_number']] = ship_info
        print()
        time.sleep(10)
        
    with open('../data/raw/wikipedia_ship_data.json', 'w', encoding='utf-8') as f:
        json.dump(ship_info_json, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()