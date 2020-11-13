import pandas as pd
import os
import api
import time
import asyncio
from tqdm import tqdm
from logger import create_logger
from create_patient import Patient, Patient_bundle
from create_diagnostic import Observation, DiagnosticReport, Observation_bundle, DiagnosticReport_bundle

folderPath = "/home/c95hgr/下載/CDSData/saved_train"
pd.set_option('max_columns', None)
err_logger = create_logger()


def read_csv(file):
    # print("CSV path :", file)
    df = pd.read_csv(file)
    df = df.fillna(value={'Units': 0})
    return df


def generate_bundle(entry_data):
    return {
        "resourceType": "Bundle",
        "id": "bundle-transaction",
        "type": "transaction",
        "entry": entry_data
    }


def get_patient_id_list(resp_data):
    id_list = []
    for i, obj in enumerate(resp_data.get('entry')):
        id_list.append(obj.get('response').get("location").split("/")[1])
    return id_list


def get_observation_str_list(resp_data, d_list):
    str_list = []
    for i, obj in enumerate(resp_data.get('entry')):
        str_list.append({
            "reference": "Observation/" + obj.get('response').get("location").split("/")[1],
            "display": d_list[i]
        })
    return str_list


class csv_to_mysql:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.file_list = []
        self.patient_list = []
        self.send_data = ""

    def main(self):
        if os.path.isdir(self.folder_path):
            self.file_list = os.listdir(self.folder_path)
            self.merge_multi_patient()
            resp_patient = self.send_bundle()
            patient_id_list = get_patient_id_list(resp_patient)
            print("patient_id_list", patient_id_list)

            self.merge_multi_diagnostic_report(patient_id_list)
        else:
            print("Failed: folder path isn't exist")

    def merge_multi_patient(self):
        for file_index, file_name in enumerate(tqdm(self.file_list)):
            df = read_csv(self.folder_path + os.sep + file_name)
            new_patient = Patient(df.loc[df['Code'] == "Gender"].values[0, 0],
                                  df.loc[df['Code'] == "Gender"].values[0, 4],
                                  df.loc[df['Code'] == "Birthday"].values[0, 4])
            self.patient_list.append(Patient_bundle(new_patient.get_info(), "POST", "Patient").get_info())

        self.send_data = generate_bundle(self.patient_list)

    def merge_multi_diagnostic_report(self, patient_id_list):
        diagnostic_report_list = []
        for file_index, file_name in enumerate(tqdm(self.file_list)):
            df = read_csv(self.folder_path + os.sep + file_name)
            patient_id = patient_id_list[file_index]
            for index, key in enumerate(df.groupby("EpisodeDate").groups.keys()):
                display_list = self.merge_multi_observation(df.groupby("EpisodeDate").get_group(key), patient_id)
                resp_observation = self.send_bundle()
                observation_result = get_observation_str_list(resp_observation, display_list)

                new_diagnostic_report = DiagnosticReport(df.groupby("EpisodeDate").get_group(key).iloc[0],
                                                         observation_result, patient_id).get_info()
                diagnostic_report_list.append(DiagnosticReport_bundle(new_diagnostic_report, "POST", "DiagnosticReport")
                                              .get_info())
            #     break
            # break

        self.send_data = generate_bundle(diagnostic_report_list)
        resp_diagnostic_report = self.send_bundle()
        print("resp_diagnostic_report", resp_diagnostic_report)

    def merge_multi_observation(self, df, patient_id):
        observation_list = []
        display_list = []
        for i in range(len(df.index)):
            if i > 2:
                try:
                    new_observation = Observation(df.iloc[i], patient_id).get_info()
                    display_list.append(new_observation.get('code').get('coding')[0].get('display'))
                    observation_list.append(Observation_bundle(new_observation, "POST", "Observation").get_info())
                except Exception as e :
                    err_logger("Error : {} {}".format(e,patient_id))

        self.send_data = generate_bundle(observation_list)
        return display_list

    def send_bundle(self):
        if self.send_data != "":
            resp = api.add_bundle(self.send_data)
            if resp.status_code != 200:
                raise Exception('Cannot create task: {} {}'.format(resp.status_code, resp.json()))
            self.send_data = ""
            return resp.json()
        else:
            return "send_data is empty."


if __name__ == "__main__":
    runner = csv_to_mysql(folderPath)
    runner.main()
    # main()
