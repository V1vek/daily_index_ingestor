from airflow.models import Variable
from datetime import timedelta
import datetime
import requests
import shutil
import time
import csv
import re
import os

def ingest_raw(ds, **kwargs):
    """
    Returns a dict

    Gets file from url using current_date - 7
    reads all lines after ---
    parses each row and downloads the file from url 
    and saves it in path created from form_type, date, CKID
    """
    INDEX_BASE_URL = Variable.get("index_base_url")
    ARCHIVES_BASE_URL = Variable.get("archives_base_url")
    RAW_DIR_PATH = Variable.get("raw_dir_path")

    date = (datetime.datetime.today() - timedelta(days = 7)).strftime('%Y%m%d')
    print(date)
    url = INDEX_BASE_URL + "master." + date + ".idx"
    response_file = requests.get(url)

    if response_file.status_code != 200:
        print("Request failed to: ", url)
        print("status: ", response_file.status_code, response_file.reason)
        return {'data_retrieved': False}

    # find the first line of the table
    lines = [line for line in response_file.text.splitlines()]
    indices = [i for i, s in enumerate(lines) if '-----' in s]
    first_row_line = indices[0] + 1
    files_completed = 0

    # get index of columns
    headers =  lines[first_row_line - 2].split('|')
    date_filed_index = headers.index("Date Filed")
    form_type_index = headers.index("Form Type")
    CIK_index = headers.index("CIK")

    for line_no in range(first_row_line, len(lines)):
        try:
            rows = lines[line_no].split('|')
            url = ARCHIVES_BASE_URL + rows[len(rows) - 1]
            print(url)
            date_filed = rows[date_filed_index]
            CIK = rows[CIK_index]
            form_type = rows[form_type_index]
            dir_path = RAW_DIR_PATH + form_type.replace('/', '') + '/' + date_filed
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            file_path = os.path.join(dir_path, CIK)
            req = requests.get(url)
            files_completed += 1
            open(file_path, 'wb').write(req.content)
            print("({}/{}) write completed to: {}".format(files_completed, (len(lines) - first_row_line), file_path))
            time.sleep(60)
        except Exception as e:
            files_completed += 1
            print("({}/{}) write failed. line no: {}, row: {}".format(files_completed, (len(lines) - first_row_line), line_no, lines[line_no]))
            print(e)

    print("Parse completed. Total files: ", files_completed)
    return {'data_retrieved': True, 'date': date}


def aggregate_data(ds, **kwargs):
    """
    Parses csv file to move raw files to processed folder with new name
    retrieved from csv
    """
    RAW_DIR_PATH = Variable.get("raw_dir_path")
    PROCESSED_DIR_PATH = Variable.get("processed_dir_path")
    MAPPING_CSV = Variable.get("mapping_csv")
    MISSING_CSV = Variable.get("missing_csv")

    is_data_retrieved = kwargs['task_instance'].xcom_pull(task_ids='ingest_raw_data')['data_retrieved']
    if not is_data_retrieved:
        print('Data not retrieved. Check previous task logs')
        return

    cik_mapping = {}
    with open(MAPPING_CSV, mode='r') as mapping_file:
        reader = csv.reader(mapping_file)
        next(reader)
        cik_mapping = {rows[0]:rows[1] for rows in reader}

    date = kwargs['task_instance'].xcom_pull(task_ids='ingest_raw_data')['date']
    form_type_dirs = os.listdir(RAW_DIR_PATH)

    for form_type in form_type_dirs:
        date_path = os.path.join(RAW_DIR_PATH, form_type, date)

        # check if date exists
        if not os.path.exists(date_path):
            continue

        files = os.listdir(date_path)
        for file in files:
            if file in cik_mapping:
                new_path = os.path.join(PROCESSED_DIR_PATH, form_type, date)
                if not os.path.exists(new_path):
                    os.makedirs(new_path)

                # copy file to processed folder with entity id as name
                shutil.copy(os.path.join(date_path, file), os.path.join(new_path, cik_mapping[file]))
                print("file {} copied to processed folder as: {}".format(file, cik_mapping[file]))
            else:
                if not os.path.exists(PROCESSED_DIR_PATH):
                    os.makedirs(PROCESSED_DIR_PATH)
                print("missing cid: {}".format(file))
                with open(MISSING_CSV, mode='a+') as outfile:
                    outfile.write("{}".format(file))
                    outfile.write("\n")

