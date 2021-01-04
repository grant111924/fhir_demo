import requests


def _url(path):
    return 'http://localhost:8080/fhir' + path


def add_patient(data):
    print("pre-send add patient :", data)
    return requests.post(_url('/Patient?_format=json&_pretty=true'), json=data)


def add_observation(data):
    print("pre-send add observation :", data)
    return requests.post(_url('/Observation?_format=json&_pretty=true'), json=data)


def add_diagnostic_report(data):
    # print("pre-send add diagnostic :", data)
    return requests.post(_url('/DiagnosticReport?_format=json&_pretty=true'), json=data)


def add_bundle(data):
    # print("pre-send add bundle :", data)
    return requests.post(_url("/?_format=json&_pretty=true"), json=data)


def add_condtional_bundle():
    pass


def drop_all_data():
    return requests.post(_url("/$expunge"), json={
        "resourceType": "Parameters",
        "parameter": [
            {
                "name": "limit",
                "valueInteger": 100000
            },
            {
                "name": "expungeEverything",
                "valueBoolean": True
            },
            # {
            #     "name": "expungePreviousVersions",
            #     "valueBoolean": True
            # }
        ]
    }
                         )


def delete_all_diagnostic_report():
    return requests.delete(_url("/DiagnosticReport?status=cancelled&_expunge=true"))
