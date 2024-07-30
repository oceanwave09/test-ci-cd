## Data Generator

A python script to generate CCDA samples based on templates and input patient details.


### Run Generator

#### Usage

```sh
# usage:
    python generator.py --option 1 --patient-filepath input/patients_option1.csv
```

#### Options
```
    1 - high_blood_pressure_positive
    2 - high_blood_pressure_negative
    3 - cervical_cancer_positive
    4 - cervical_cancer_negative
    5 - diabetes_eye_exam_positive
    6 - diabetes_eye_exam_negative
```

#### Patient Details Schema (comma separated fields)

> "org_name","org_street_line_1","org_city","org_state","org_zip","firstname","lastname","street_line_1","city","state","zip","birthdate","gender"
