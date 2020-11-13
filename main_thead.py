import pandas as pd
import os
import api
import threading
import queue
from tqdm import tqdm
from logger import create_logger
from create_patient import Patient, Patient_bundle
from create_diagnostic import Observation, DiagnosticReport, Observation_bundle

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
    # print("d_list", len(d_list))
    for i, obj in enumerate(resp_data.get('entry')):
        str_list.append({
            "reference": "Observation/" + obj.get('response').get("location").split("/")[1],
            "display": d_list[i]
        })
    return str_list


def send_bundle(send_data):
    if send_data != "":
        resp = api.add_bundle(send_data)
        if resp.status_code != 200:
            raise Exception('Cannot create task: {} {}'.format(resp.status_code, resp.json()))
        return resp.json()
    else:
        return "send_data is empty."


def insert_diagnostic_report(df, observation_list, patient_id):
    print((observation_list, patient_id))
    new_diagnostic_report = DiagnosticReport(df, observation_list, patient_id)

    resp = api.add_diagnostic_report(new_diagnostic_report.get_info())
    if resp.status_code != 201:
        raise Exception('Cannot create task: {} {}'.format(resp.status_code, resp.json()))
    print('insert_diagnostic_report resp success')


class report_To_mysql_Worker(threading.Thread):
    def __init__(self, display_queue, data_queue, df_queue, patient_queue):
        threading.Thread.__init__(self)
        self.display_queue = display_queue
        self.data_queue = data_queue
        self.df_queue = df_queue
        self.patient_queue = patient_queue

    def run(self):
        while True:
            int(self.data_queue.size)
            pair_send_data = self.data_queue.get()
            pair_display_list = self.display_queue.get()
            pair_df = self.df_queue.get()
            pair_patient_id = self.patient_queue.get()

            pair_resp_observation = send_bundle(pair_send_data)  # create all observation
            pair_observation_result = get_observation_str_list(pair_resp_observation, pair_display_list)

            insert_diagnostic_report(pair_df, pair_observation_result, pair_patient_id)  # create one report
            # time.sleep(0.5)
            self.display_queue.task_done()
            self.data_queue.task_done()
            self.df_queue.task_done()
            self.patient_queue.task_done()


def merge_multi_observation(df, patient_id):
    observation_list = []
    display_list = []
    for i in range(len(df.index)):
        if i > 2:
            new_observation = Observation(df.iloc[i], patient_id).get_info()
            display_list.append(new_observation.get('code').get('coding')[0].get('display'))
            observation_list.append(Observation_bundle(new_observation, "POST", "Observation").get_info())

    return display_list, generate_bundle(observation_list)


class csv_to_mysql(threading.Thread):
    def __init__(self, display_queue, observation_queue, df_queue, patient_queue, folder_path):
        threading.Thread.__init__(self)
        self.display_queue = display_queue
        self.observation_queue = observation_queue
        self.patient_queue = patient_queue
        self.df_queue = df_queue

        self.folder_path = folder_path
        self.file_list = []
        self.patient_list = []

    def run(self):
        if os.path.isdir(self.folder_path):
            self.file_list = os.listdir(self.folder_path)
            resp_patient = send_bundle(self.merge_multi_patient())
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

        return generate_bundle(self.patient_list)

    def merge_multi_diagnostic_report(self, patient_id_list):

        for file_index, file_name in enumerate(tqdm(self.file_list)):
            df = read_csv(self.folder_path + os.sep + file_name)
            patient_id = patient_id_list[file_index]
            for index, key in enumerate(df.groupby("EpisodeDate").groups.keys()):
                display_list, send_data = merge_multi_observation(df.groupby("EpisodeDate").get_group(key),
                                                                  patient_id)
                self.display_queue.put(display_list)
                self.observation_queue.put(send_data)
                self.df_queue.put(df.groupby("EpisodeDate").get_group(key).iloc[0])
                self.patient_queue.put(patient_id)

        print("set queue finished ...")


if __name__ == "__main__":
    # resp = api.drop_all_data()
    # resp = api.delete_all_diagnostic_report()
    # print(resp.json())

    display_queue = queue.Queue()
    observation_queue = queue.Queue()
    df_queue = queue.Queue()
    patient_queue = queue.Queue()

    # 建立4個 Worker
    runner = csv_to_mysql(display_queue, observation_queue, df_queue, patient_queue, folderPath)

    worker1 = report_To_mysql_Worker(display_queue, observation_queue, df_queue, patient_queue)
    worker2 = report_To_mysql_Worker(display_queue, observation_queue, df_queue, patient_queue)
    worker3 = report_To_mysql_Worker(display_queue, observation_queue, df_queue, patient_queue)
    worker4 = report_To_mysql_Worker(display_queue, observation_queue, df_queue, patient_queue)

    # 讓ker 開始處理資料
    runner.start()
    worker1.start()
    worker2.start()
    worker3.start()
    worker4.start()

    # 等待所有 Worker 結束
    runner.join()
    worker1.join()
    worker2.join()
    worker3.join()
    worker4.join()

    print("Done.")
