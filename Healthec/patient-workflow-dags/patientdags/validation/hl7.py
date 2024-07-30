# import json
# import logging

# from airflow.exceptions import AirflowFailException

# from patientdags.hl7parser.hl7 import HL7Message
# from patientdags.utils.error_codes import PatientDagsErrorCodes
# from patientdags.utils.utils import publish_error_code


# def validate(data: str):
#     pass


# def validate_required_fields(data: str):
#     try:
#         msg = HL7Message(data)
#         if msg.missing_required_fields:
#             error_message = f"The required fields are missing in respective segments \
#                     {json.dumps(msg.missing_required_fields, indent=4, sort_keys=False)}"
#             raise ValueError(error_message)
#     except ValueError as e:
#         logging.error(e)
#         publish_error_code(PatientDagsErrorCodes.HL7_FILE_MANDATORY_FIELD_VALIDATION_ERROR.value)
#         raise AirflowFailException(e)
