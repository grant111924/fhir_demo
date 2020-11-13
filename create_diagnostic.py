import datetime
import re


class DiagnosticReport:
    def __init__(self, data, result, patient_id):
        self.data = data
        date_list = self.data.get('EpisodeDate')[:-5].split("/")
        date = datetime.datetime(int(date_list[2]), int(date_list[1]), int(date_list[0]))
        self.date = date.strftime("%Y-%m-%dT%H:%M:%S-%M:%S")
        self.result = result
        self.patient_id = patient_id

    def __str__(self):
        return 'DiagnosticReport({0}, {1}, {2}, {3})'.format(
            self.data.get('Code'), self.data.get('Name'), self.data.get('Value'), self.data.get('EpisodeDate'))

    def get_info(self):
        return {
            "resourceType": "DiagnosticReport",
            "status": "final",
            "subject": {
                # "reference": "Patient/" + str(self.patient_id)
                "reference": "urn:uuid:" + str(self.patient_id)
            },
            "performer": [
                {
                    "reference": "urn:uuid:" + str(self.patient_id),
                    "display": "NCSIST Patient",
                }
            ],
            "effectiveDateTime": self.date,
            "issued": self.date,
            "result": self.result,
        }


class Observation:
    def __init__(self, data, patient_id):
        self.data = data
        date_list = self.data.get('EpisodeDate')[:-5].split("/")
        date = datetime.datetime(int(date_list[2]), int(date_list[1]), int(date_list[0]))
        self.date = date.strftime("%Y-%m-%dT%H:%M:%S-%M:%S")
        self.patient_id = patient_id

    def __str__(self):
        return 'Observation({0}, {1}, {2}, {3})'.format(
            self.data.get('Code'), self.data.get('Name'), self.data.get('Value'), self.data.get('EpisodeDate'))

    def get_info(self):
        res = {
            "resourceType": "Observation",
            "status": "final",
            "code": {
                "coding": [{
                    "display": self.data.get('Name')
                }]
            },
            "text": {
                "status": "generated",
                "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">131329</div>"
            },
            # "performer": [
            #     {
            #         "reference": "urn:uuid:" + str(self.patient_id),
            #         "display": "NCSIST Patient",
            #     }
            # ],
            "subject": {
                # "reference": "Patient/" + str(self.patient_id)
                "reference": "Patient/urn:uuid:" + str(self.patient_id)
            },
            "effectivePeriod": {
                "start": self.date
            },
            "issued": self.date,
        }

        if self.data.get('Value') != "":
            res["valueString"] = self.data.get('Value')
        else:
            res["valueString"] = "3"

        # unit = self.data.get('Units')
        # if unit == 0 or re.search("[\",',-,+,(,),a-z,A-Z]", self.data.get('Value')):
        #     unit = ""
        #     res["valueString"] = self.data.get('Value')
        # else:
        #     if re.search("[\",',-,+,(,),a-z,A-Z]", self.data.get('Value')):
        #         res["valueQuantity"] = {
        #             "value": self.data.get('Value').replace("＊", ""),  #
        #             "unit": unit,
        #             "system": "http://unitsofmeasure.org"
        #         }
        #     else:
        #         res["valueString"] = self.data.get('Value')

        return res


class Observation_bundle:
    def __init__(self, observation, method, url, uuid):
        self.observation = observation
        self.method = method
        self.url = url
        self.uuid = uuid

    def __str__(self):
        return 'mutli_Observation: {}'.format(self.observation)

    def get_info(self):
        return {
            "fullUrl": "urn:uuid:" + str(self.uuid),
            "resource": self.observation,
            "request": {
                "method": self.method,
                "url": self.url
            }
        }


class DiagnosticReport_bundle:
    def __init__(self, diagnostic_report, method, url):
        self.diagnostic_report = diagnostic_report
        self.method = method
        self.url = url

    def __str__(self):
        return 'mutli_DiagnosticReport: {}'.format(self.diagnostic_report)

    def get_info(self):
        return {
            "resource": self.diagnostic_report,
            "request": {
                "method": self.method,
                "url": self.url
            }
        }
