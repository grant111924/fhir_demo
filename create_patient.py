import names


class Patient:
    def __init__(self, case, gender, birthdate):
        birthdate_list = birthdate[:-5].split("/")
        self.id = case
        self.name = {
            "text": names.get_full_name(gender=gender)
        }
        self.gender = gender.lower()
        self.birthDate = birthdate_list[2] + "-" + birthdate_list[1] + "-" + birthdate_list[0]

    def __str__(self):
        return 'Patient({0}, {1}, {2}, {3})'.format(
            self.id, self.name, self.gender, self.birthDate)

    def get_info(self):
        return {
            "resourceType": "Patient",
            'id': self.id,
            'name': self.name,
            'gender': self.gender,
            'birthDate': self.birthDate,
        }


class Patient_bundle:
    def __init__(self, patient, method, url, uuid):
        self.patient = patient
        self.method = method
        self.url = url
        self.uuid = uuid

    def __str__(self):
        return 'mutli_Patient: {}'.format(self.patient)

    def get_info(self):
        return {
            "fullUrl": "urn:uuid:"+str(self.uuid),
            "resource": self.patient,
            "request": {
                "method": self.method,
                "url": self.url,
                # "ifNoneExist": "identifier=http://localhost:8080/fhir|12345"
            }
        }
