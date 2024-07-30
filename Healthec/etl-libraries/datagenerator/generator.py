import click
import uuid
import random
import csv
from datetime import datetime, timedelta, timezone
from collections import namedtuple
from string import Template

# define namedtuple for patient details
fields = (
    "org_name",
    "org_street_line_1",
    "org_city",
    "org_state",
    "org_zip",
    "firstname",
    "lastname",
    "street_line_1",
    "city",
    "state",
    "zip",
    "birthdate",
    "gender",
)
PatientDetails = namedtuple("PatientDetails", fields)

# list of options
available_options = [
    "high_blood_pressure_positive",
    "high_blood_pressure_negative",
    "cervical_cancer_positive",
    "cervical_cancer_negative",
    "diabetes_eye_exam_positive",
    "diabetes_eye_exam_negative",
]


def _extract_placeholders(template):
    return list(
        set(
            [
                patrn.group("named") or patrn.group("braced")
                for patrn in template.pattern.finditer(template.template)
                if patrn.group("named") or patrn.group("braced")
            ]
        )
    )


def _get_uuid():
    return str(uuid.uuid4())


def _random_integer_by_size(x):
    return "{0:0{x}d}".format(random.randint(0, 10**x - 1), x=x)


def _random_integer_by_range(x, y):
    return "{}".format(random.randint(x, y))


def _random_datetime(years=1):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=years * 365.25)
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return (start + timedelta(seconds=random_second)).strftime("%Y%m%d%H%M%S")


def _convert_to_datetime(date):
    return datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d%H%M%S")


@click.command()
@click.option(
    "--option", help="available options", type=int, default=None, required=True
)
@click.option(
    "--patient-filepath", help="list of patient details", default=None, required=True
)
def generate_ccda_samples(option, patient_filepath):
    patients = []
    # read option and load template
    if option > len(available_options):
        raise ValueError("invalid option provided!")

    template_name = available_options[option - 1]
    template_filepath = f"templates/ccda/{template_name}.xml"
    print("template_filepath: ", template_filepath)
    with open(template_filepath, "r") as f:
        template = Template(f.read())

    # extract placeholders from template
    placeholders = _extract_placeholders(template)

    # read patient filepath
    with open(patient_filepath, "r") as f:
        rows = csv.reader(f, delimiter=",")
        for row in rows:
            patients.append(
                PatientDetails(
                    org_name=row[0].strip(),
                    org_street_line_1=row[1].strip(),
                    org_city=row[2].strip(),
                    org_state=row[3].strip(),
                    org_zip=row[4].strip(),
                    firstname=row[5].strip(),
                    lastname=row[6].strip(),
                    street_line_1=row[7].strip(),
                    city=row[8].strip(),
                    state=row[9].strip(),
                    zip=row[10].strip(),
                    birthdate=row[11].strip(),
                    gender=row[12].strip(),
                )
            )

    # generate CCDA samples for each patient
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    for patient in patients:
        patient_dict = patient._asdict()
        for placeholder in placeholders:
            if patient_dict.get(placeholder, None):
                continue
            if placeholder.startswith("uuid_"):
                patient_dict[placeholder] = _get_uuid()
            elif placeholder == "activity_datetime":
                patient_dict[placeholder] = _random_datetime(1)
            elif placeholder == "ssn":
                patient_dict[placeholder] = _random_integer_by_size(9)
            elif placeholder == "org_ein":
                patient_dict[placeholder] = _random_integer_by_size(9)
            elif placeholder == "birth_datetime":
                patient_dict[placeholder] = _convert_to_datetime(
                    patient_dict.get("birthdate")
                )
            elif placeholder == "diastolic":
                if template_name == "high_blood_pressure_positive":
                    patient_dict[placeholder] = _random_integer_by_range(90, 100)
                elif template_name == "high_blood_pressure_negative":
                    patient_dict[placeholder] = _random_integer_by_range(80, 89)
            elif placeholder == "systolic":
                if template_name == "high_blood_pressure_positive":
                    patient_dict[placeholder] = _random_integer_by_range(140, 150)
                elif template_name == "high_blood_pressure_negative":
                    patient_dict[placeholder] = _random_integer_by_range(130, 139)
            else:
                raise ValueError(f"could not find value for placeholder {placeholder}!")
            
        print("patient_dict: ", patient_dict)

        ccda_content = template.safe_substitute(patient_dict)
        with open(
            f'output/{patient_dict["firstname"]}_{patient_dict["lastname"]}_{template_name}_{timestamp}_ccda.xml',
            "w",
        ) as f:
            f.write(ccda_content)


if __name__ == "__main__":
    generate_ccda_samples()
