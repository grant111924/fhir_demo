import pandas as pd
import os
# import api
import uuid
import threading
from lib import read_one_csv
from tqdm import tqdm
import queue
from logger import create_logger
from create_patient import Patient, Patient_bundle
from create_diagnostic import Observation, DiagnosticReport, Observation_bundle, DiagnosticReport_bundle

folderPath = "/home/c95hgr/下載/CDSData/saved_train"
pd.set_option('max_columns', None)
err_logger = create_logger()


def generate_bundle(entry_data):
    return {
        "resourceType": "Bundle",
        # "id": "bundle-transaction",
        "type": "transaction",
        "entry": entry_data
    }


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
            if (file_index + 1) % 100 == 0 or (file_index + 1) == len(file_list):
                self.csv_batch_queue.put(batch_list)
                batch_list = []


class read_csv_work(threading.Thread):
    def __init__(self, csv_batch_queue, folder_path):
        threading.Thread.__init__(self)
        self.csv_batch_queue = csv_batch_queue
        self.folder_path = folder_path
        self.csv_list = []

    def run(self):
        self.read_csv_queue()

    def read_csv_queue(self):
        self.csv_list = []
        while len(self.csv_list) == 0:
            self.csv_list = self.csv_batch_queue.get()
            for csv_index, csv_name in enumerate(self.csv_list):
                if (csv_index + 1) == len(self.csv_list):
                    self.csv_list = []
                print(self.merge_bundle(csv_name))
                break

    def merge_bundle(self, csv_name):
        print("merge_bundle")
        bundle_data = []
        df = read_one_csv(self.folder_path + os.sep + csv_name)
        new_patient_uuid = uuid.uuid1()
        new_patient = Patient(df.loc[df['Code'] == "Gender"].values[0, 0],
                              df.loc[df['Code'] == "Gender"].values[0, 4],
                              df.loc[df['Code'] == "Birthday"].values[0, 4])
        bundle_data.append(Patient_bundle(new_patient.get_info(), "POST", "Patient", new_patient_uuid).get_info())

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

                    bundle_data.append(Observation_bundle(new_observation, "POST", "Observation", new_observation_uuid)
                                       .get_info())
            break
            # new_diagnostic_report = DiagnosticReport(report_df.iloc[i], report_data, new_patient_uuid).get_info()
            # bundle_data.append(DiagnosticReport_bundle(new_diagnostic_report, "POST", "DiagnosticReport").get_info())

        return generate_bundle(bundle_data)


if __name__ == "__main__":
    batch_csv_queue = queue.Queue()
    batch_csv_worker = batch_csv_work(batch_csv_queue, folderPath)
    read_csv_worker = read_csv_work(batch_csv_queue, folderPath)

    batch_csv_worker.start()
    read_csv_worker.start()

    batch_csv_worker.join()
    read_csv_worker.join()
