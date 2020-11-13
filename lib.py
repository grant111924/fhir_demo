import pandas as pd
import os
import api
import time
from logger import create_logger
from create_patient import Patient
from create_diagnostic import Observation, DiagnosticReport

folderPath = "/home/c95hgr/下載/CDSData/saved_train"
pd.set_option('max_columns', None)
err_logger = create_logger()


def read_one_csv(file):
    df = pd.read_csv(file)
    df = df.fillna(value={'Units': 0})
    return df


def read_all_csv(file_list):
    print("csv_file counts: " + str(len(file_list)))
    for file_name in file_list:
        print("CSV :", file_name)
        read_csv(folderPath + os.sep + file_name)
        # break


def insert_patient(df):
    new_patient = Patient(df.loc[df['Code'] == "Gender"].values[0, 0],
                          df.loc[df['Code'] == "Gender"].values[0, 4],
                          df.loc[df['Code'] == "Birthday"].values[0, 4])
    resp = api.add_patient(new_patient.get_info())
    if resp.status_code != 201:
        raise Exception('Cannot create task: {}'.format(resp.status_code))
    return resp.json()


def insert_observation(df, patient_id):
    print("patient_id", patient_id)
    print("length", len(df.index))
    observation_list = []
    for i in range(len(df.index)):
        if i > 2:
            new_observation = Observation(df.iloc[i], patient_id).get_info()
            resp = api.add_observation(new_observation)
            print('insert_observation resp', resp.json())
            if resp.status_code != 201:
                raise Exception('Cannot create task: {}'.format(resp.status_code))
            observation_list.append({
                "reference": "Observation/" + resp.json().get('id'),
                "display": resp.json().get('code').get('coding')[0].get('display')
            })
            time.sleep(0.1)

    return observation_list


def insert_diagnostic_report(df, observation_list, patient_id):
    new_diagnostic_report = DiagnosticReport(df.iloc[0], observation_list, patient_id)
    print(new_diagnostic_report.get_info())

    resp = api.add_diagnostic_report(new_diagnostic_report.get_info())
    if resp.status_code != 201:
        raise Exception('Cannot create task: {}'.format(resp.status_code))
    print('insert_diagnostic_report resp', resp.json())


def read_csv(file):
    df = pd.read_csv(file)
    df = df.fillna(value={'Units': 0})
    patient_data = ""

    for index, key in enumerate(df.groupby("EpisodeDate").groups.keys()):
        try:
            if index == 0:
                patient_data = insert_patient(df.groupby("EpisodeDate").get_group(key))
            else:
                observation_result = insert_observation(df.groupby("EpisodeDate").get_group(key),
                                                        patient_data.get('id'))
                print("observation_result", observation_result)
                insert_diagnostic_report(df.groupby("EpisodeDate").get_group(key), observation_result,
                                         patient_data.get('id'))

        except Exception as e:
            err_logger.warning("Connection refused by the server..  Caused by: " + str(e) + " csv :" + file)
            print("Connection refused by the server..  Caused by", e)
        #     print("Let me sleep for 5 seconds")
        #     time.sleep(5)
        #     print("Was a nice sleep, now let me continue...")
        #     continue
