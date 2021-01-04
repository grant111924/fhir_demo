import pandas as pd
import os
import api
import uuid
import threading
from lib import read_one_csv
from tqdm import tqdm
import queue
from logger import create_logger
from create_patient import Patient, Patient_bundle
from create_diagnostic import Observation, DiagnosticReport, Observation_bundle, DiagnosticReport_bundle

folderPath = "/home/c95hgr/下載/CDSData/saved_train"
folderPath = "/home/c95hgr/2020data"
pd.set_option('max_columns', None)


# err_logger = create_logger()
def get_patient_id_list(resp_data):
    id_list = []
    for i, obj in enumerate(resp_data.get('entry')):
        id_list.append(obj.get('response').get("location").split("/")[1])
    return id_list


def get_observation_id_data(resp_data):
    id_list = []
    for i, obj in enumerate(resp_data.get('entry')):
        id_list.append(obj.get('response').get("location").split("/")[1])
    return id_list


def generate_bundle(entry_data, send_type):
    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": entry_data
    }


def send_bundle(send_data):
    if send_data != "":
        resp = api.add_bundle(send_data)
        if resp.status_code != 200:
            raise Exception('Cannot create task: {} {}'.format(resp.status_code, resp.json()))
        return resp.json()
    else:
        return "send_data is empty."


class batch_csv_work(threading.Thread):
    def __init__(self, csv_batch_queue, folder_path):
        threading.Thread.__init__(self)
        self.csv_batch_queue = csv_batch_queue
        self.folder_path = folder_path

    def run(self):
        self.batch_csv_file(os.listdir(self.folder_path))

    def batch_csv_file(self, file_list, size=100):
        batch_list = []
        for file_index, file_name in enumerate(file_list):
            batch_list.append(file_name)
            if (file_index + 1) % size == 0 or (file_index + 1) == len(file_list):
                self.csv_batch_queue.put(batch_list)
                batch_list = []
            if file_index > 99:
                break


class read_csv_work(threading.Thread):
    def __init__(self, csv_batch_queue, folder_path):
        threading.Thread.__init__(self)
        self.csv_batch_queue = csv_batch_queue
        self.folder_path = folder_path
        self.csv_list = []
        self.patient_list = []

    def run(self):
        self.read_csv_queue()

    def read_csv_queue(self):
        self.csv_list = []
        while len(self.csv_list) == 0 or self.csv_batch_queue.qsize() > 0:
            self.csv_list = self.csv_batch_queue.get()
            # create Observation and Diagnostic report
            for csv_index, csv_name in tqdm(enumerate(self.csv_list)):
                self.merge_bundle(csv_name)
                if (csv_index + 1) == len(self.csv_list):
                    self.csv_list = []

    def merge_bundle(self, csv_name):
        print("merge_bundle", csv_name)

        bundle_data = []
        df = read_one_csv(self.folder_path + os.sep + csv_name)
        new_patient_uuid = uuid.uuid1()
        new_patient = Patient(df.loc[df['Code'] == "Gender"].values[0, 0],
                              df.loc[df['Code'] == "Gender"].values[0, 4],
                              df.loc[df['Code'] == "Birthday"].values[0, 4]).get_info()
        bundle_data.append(Patient_bundle(new_patient, "POST", "Patient", new_patient_uuid).get_info())

        for index, key in enumerate(df.groupby("EpisodeDate").groups.keys()):
            report_df = df.groupby("EpisodeDate").get_group(key)
            report_data = []
            for i in range(len(report_df.index)):
                if i > 2:
                    new_observation_uuid = uuid.uuid1()
                    new_observation = Observation(report_df.iloc[i], new_patient_uuid).get_info()
                    report_data.append({
                        "reference": "urn:uuid:" + str(new_observation_uuid),
                        "display": new_observation.get('code').get('coding')[0].get('display')
                    })
                    bundle_data.append(Observation_bundle(new_observation, "POST", "Observation", new_observation_uuid).
                                       get_info())
            new_diagnostic_report = DiagnosticReport(report_df.iloc[i], report_data, new_patient_uuid).get_info()
            bundle_data.append(DiagnosticReport_bundle(new_diagnostic_report, "POST", 'DiagnosticReport').get_info())

        # print(generate_bundle(bundle_data, " transaction"))
        resp = send_bundle(generate_bundle(bundle_data, " transaction"))
        # print("resp_observation", resp)


if __name__ == "__main__":
    batch_csv_queue = queue.Queue()
    batch_csv_worker = batch_csv_work(batch_csv_queue, folderPath)
    read_csv_worker = read_csv_work(batch_csv_queue, folderPath)

    batch_csv_worker.start()
    read_csv_worker.start()

    batch_csv_worker.join()
    read_csv_worker.join()
