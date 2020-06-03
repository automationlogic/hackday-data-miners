import requests as rq
import logging
import os
import time
import datetime
import csv
import json
from random import randint

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict


PRECIPITATION_TYPE = "pr"
TEMPERATURE_TYPE = "tas"
MONTHLY_MOVING_AVERAGE = "mavg"


print("Preparing..")
project_id = "data-miners-279116"



dataset_id = "wb_climate_data"
precipitation_table_id = "wb_climate_precipitations"
temperature_table_id = "wb_climate_temperatures"

bq_client = bigquery.Client()
precipitation_table_ref = bq_client.dataset(dataset_id).table(precipitation_table_id)
temperature_table_ref = bq_client.dataset(dataset_id).table(temperature_table_id)


def ingest(event, context):
    create_table(precipitation_table_ref)
    create_table(temperature_table_ref)

    for start_year in [1980, 2020]:
        end_year = start_year + 19
        extract(precipitation_table_ref, MONTHLY_MOVING_AVERAGE, PRECIPITATION_TYPE, start_year, end_year, "GBR")
        extract(temperature_table_ref, MONTHLY_MOVING_AVERAGE, TEMPERATURE_TYPE, start_year, end_year, "GBR")

def extract(table_ref, api_type, var, start_year, end_year, country_code):
    url = f"http://climatedataapi.worldbank.org/climateweb/rest/v1/country/{api_type}/{var}/{start_year}/{end_year}/{country_code}.json"
    print(f"URL: {url}")
    response = rq.get(url)
    response.raise_for_status() # raise Exception if not a 200 OK

    raw_json_rows = response.json()
    print("Raw rows in API response:")
    print(raw_json_rows)

    json_rows = []
    for raw_row in raw_json_rows:
        json_rows.append({
            "GCM": raw_row["gcm"],
            "var": raw_row["variable"],
            "from_year": raw_row["fromYear"],
            "to_year": raw_row["toYear"],
            "Jan": raw_row["monthVals"][0],
            "Feb": raw_row["monthVals"][1],
            "Mar": raw_row["monthVals"][2],
            "Apr": raw_row["monthVals"][3],
            "May": raw_row["monthVals"][4],
            "Jun": raw_row["monthVals"][5],
            "Jul": raw_row["monthVals"][6],
            "Aug": raw_row["monthVals"][7],
            "Sep": raw_row["monthVals"][8],
            "Oct": raw_row["monthVals"][9],
            "Nov": raw_row["monthVals"][10],
            "Dec": raw_row["monthVals"][11],
        })

    print("JSON rows (cleaned):")
    print(json_rows)

    print("About to insert rows in BigQuery...")
    insert_response = bq_client.insert_rows_json(table_ref, json_rows)

    print("Insert response:")
    print(insert_response)

def create_table(table_ref):
    schema = [
        bigquery.SchemaField("GCM", "STRING", "NULLABLE"),
        bigquery.SchemaField("var", "STRING", "NULLABLE"),
        bigquery.SchemaField("from_year", "INTEGER", "NULLABLE"),
        bigquery.SchemaField("to_year", "INTEGER", "NULLABLE"),
        bigquery.SchemaField("Jan", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Feb", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Mar", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Apr", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("May", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Jun", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Jul", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Aug", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Sep", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Oct", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Nov", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("Dec", "FLOAT", "NULLABLE")
    ]

    table = bigquery.Table(table_ref, schema=schema)
    try:
        bq_client.get_table(table)
    except NotFound:
        try:
            table = bq_client.create_table(table)
            print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            # print("Going to sleep for 90 seconds to ensure data availability in newly created table")
            # time.sleep(90)
        except Conflict:
            pass

    return
