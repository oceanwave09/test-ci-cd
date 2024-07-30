# import json
# import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable

# from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from patientdags.fhirbundle.bundle_processor import BundleProcessor

# from patientdags.fhirbundle.processor import (
#     process_allergy_intolerance,
#     process_claim,
#     process_claim_response,
#     process_condition,
#     process_service_request,
#     process_coverage,
#     process_encounter,
#     process_immunization,
#     process_insurance_plan,
#     process_location,
#     process_medication,
#     process_medication_request,
#     process_medication_statement,
#     process_observation,
#     process_organization,
#     process_patient,
#     process_practitioner,
#     process_practitioner_role,
#     process_procedure,
# )
from patientdags.utils.aws_s3 import read_file_from_s3

# from patientdags.utils.constants import DATA_SEARCH_PATH
from patientdags.utils.utils import (  # initialize_target_files,
    publish_start_status,
    read_conf,
    update_dag_status,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=2, year=2022, month=8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}


def process_fhir_bundle(
    file_key: str,
    file_tenant: str,
    file_source: str,
    file_resource_type: str,
    file_batch_id: str,
    src_file_name: str,
    src_organization_id: str,
):
    bucket, key = Variable.get("DATA_INGESTION_BUCKET"), file_key
    data, metadata = read_file_from_s3(bucket, key)
    if metadata.get("src_organization_id"):
        src_organization_id = metadata.get("src_organization_id")
    obj = BundleProcessor(
        data=data,
        file_tenant=file_tenant,
        file_source=file_source,
        file_resource_type=file_resource_type,
        file_batch_id=file_batch_id,
        src_file_name=src_file_name,
        src_organization_id=src_organization_id,
    )
    bundle_status = obj.process_bundle()
    return bundle_status


# def _read_bundle(ti, bucket: str, key: str):
#     # local_file_path, metadata = download_file_from_s3(bucket, key, DATA_SEARCH_PATH)
#     # bundle_stats = {}
#     # with open(local_file_path, "rb") as f:
#     #     for entry in ijson.items(f, "entry.item"):
#     #         resource_type = entry["resource"].get("resourceType")
#     #         bundle_stats[resource_type] = bundle_stats[resource_type] + 1 if resource_type in bundle_stats else 1
#     bundle_data, metadata = read_file_from_s3(bucket, key)
#     bundle_json = json.loads(bundle_data)
#     bundle_stats = {}
#     for entry in bundle_json.get("entry", []):
#         resource_type = entry["resource"].get("resourceType")
#         bundle_stats[resource_type] = bundle_stats[resource_type] + 1 if resource_type in bundle_stats else 1
#     ti.xcom_push(key="bundle_stats", value=json.dumps(bundle_stats))
#     # parse source organization id from DAG config and fhirbundle s3 file metadata
#     src_organization_id = ti.xcom_pull(task_ids="parse_config", key="src_organization_id")
#     src_organization_id = str(src_organization_id).replace("None", "") if src_organization_id else ""
#     if metadata.get("src_organization_id"):
#         src_organization_id = metadata.get("src_organization_id")
#     ti.xcom_push(key="src_organization_id", value=src_organization_id)
#     s3_file_path = f"s3://{bucket}/{key}"

#     return s3_file_path


# def _clean_up(file_path: str):
#     # remove local file
#     if os.path.exists(file_path):
#         os.remove(file_path)


with DAG(
    dag_id="fhirbundle_processing_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=10,
    concurrency=10,
    description="Parse FHIR bundles and post resources with platform services",
    tags=["fhirbundle"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    process_fhirbundle = PythonOperator(
        task_id="process_fhirbundle",
        python_callable=process_fhir_bundle,
        op_kwargs={
            "file_key": '{{ ti.xcom_pull(task_ids="parse_config", key="file_key") }}',
            "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
            "file_source": '{{ ti.xcom_pull(task_ids="parse_config", key="file_source") }}',
            "file_resource_type": '{{ ti.xcom_pull(task_ids="parse_config", key="resource_type") }}',
            "file_batch_id": '{{ ti.xcom_pull(task_ids="parse_config", key="file_batch_id") }}',
            "src_file_name": '{{ ti.xcom_pull(task_ids="parse_config", key="src_file_name") }}',
            "src_organization_id": '{{ ti.xcom_pull(task_ids="parse_config", key="src_organization_id") }}',
        },
    )

    # read_bundle = PythonOperator(
    #     task_id="read_bundle",
    #     python_callable=_read_bundle,
    #     op_kwargs={
    #         "bucket": Variable.get("DATA_INGESTION_BUCKET"),
    #         "key": '{{ ti.xcom_pull(task_ids="parse_config", key="file_key") }}',
    #     },
    # )

    # # Organization processing
    # organization_processing = PythonOperator(
    #     task_id="organization_processing",
    #     python_callable=process_organization,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #     },
    # )

    # wait_for_level_0_finish = DummyOperator(
    #     task_id="wait_for_level_0_finish",
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    # # Location processing
    # location_processing = PythonOperator(
    #     task_id="location_processing",
    #     python_callable=process_location,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #         "src_organization_id": '{{ ti.xcom_pull(task_ids="read_bundle", key="src_organization_id") }}',
    #     },
    # )

    # # Prcatitioner processing
    # practitioner_processing = PythonOperator(
    #     task_id="practitioner_processing",
    #     python_callable=process_practitioner,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #     },
    # )

    # # InsurancePlan processing
    # insurance_plan_processing = PythonOperator(
    #     task_id="insurance_plan_processing",
    #     python_callable=process_insurance_plan,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #     },
    # )

    # # Medication processing
    # medication_processing = PythonOperator(
    #     task_id="medication_processing",
    #     python_callable=process_medication,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #     },
    # )

    # wait_for_level_1_finish = DummyOperator(
    #     task_id="wait_for_level_1_finish",
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    # # Patient processing
    # patient_processing = PythonOperator(
    #     task_id="patient_processing",
    #     python_callable=process_patient,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "src_organization_id": '{{ ti.xcom_pull(task_ids="read_bundle", key="src_organization_id") }}',
    #         "file_resource_type": '{{ ti.xcom_pull(task_ids="parse_config", key="resource_type") }}',
    #     },
    # )

    # # PractitionerRole processing
    # practitioner_role_processing = PythonOperator(
    #     task_id="practitioner_role_processing",
    #     python_callable=process_practitioner_role,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "location_ids": '{{ ti.xcom_pull(task_ids="location_processing") }}',
    #         "src_organization_id": '{{ ti.xcom_pull(task_ids="read_bundle", key="src_organization_id") }}',
    #     },
    # )

    # wait_for_level_2_finish = DummyOperator(
    #     task_id="wait_for_level_2_finish",
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    # # Coverage processing
    # coverage_processing = PythonOperator(
    #     task_id="coverage_processing",
    #     python_callable=process_coverage,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "insurance_plan_ids": '{{ ti.xcom_pull(task_ids="insurance_plan_processing") }}',
    #     },
    # )

    # # Encounter processing
    # encounter_processing = PythonOperator(
    #     task_id="encounter_processing",
    #     python_callable=process_encounter,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #         "location_ids": '{{ ti.xcom_pull(task_ids="location_processing") }}',
    #         "src_organization_id": '{{ ti.xcom_pull(task_ids="read_bundle", key="src_organization_id") }}',
    #     },
    # )

    # wait_for_level_3_finish = DummyOperator(
    #     task_id="wait_for_level_3_finish",
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    # # Condition processing
    # condition_processing = PythonOperator(
    #     task_id="condition_processing",
    #     python_callable=process_condition,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #     },
    # )

    # # SericeRequest processing
    # service_request_processing = PythonOperator(
    #     task_id="service_request_processing",
    #     python_callable=process_service_request,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #     },
    # )

    # # Observation processing
    # observation_processing = PythonOperator(
    #     task_id="observation_processing",
    #     python_callable=process_observation,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #     },
    # )

    # # AllergyIntolerance processing
    # allergy_intolerance_processing = PythonOperator(
    #     task_id="allergy_intolerance_processing",
    #     python_callable=process_allergy_intolerance,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #     },
    # )

    # # # QuestionnaireResponse processing
    # # questionnaire_response_processing = PythonOperator(
    # #     task_id="questionnaire_response_processing",
    # #     python_callable=process_questionnaire_response,
    # #     op_kwargs={
    # #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    # #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    # #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    # #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    # #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    # #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    # #     },
    # # )

    # # MedicationRequest processing
    # medication_request_processing = PythonOperator(
    #     task_id="medication_request_processing",
    #     python_callable=process_medication_request,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "medication_ids": '{{ ti.xcom_pull(task_ids="medication_processing") }}',
    #     },
    # )

    # wait_for_level_4_finish = DummyOperator(
    #     task_id="wait_for_level_4_finish",
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    # # Procedure processing
    # procedure_processing = PythonOperator(
    #     task_id="procedure_processing",
    #     python_callable=process_procedure,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "location_ids": '{{ ti.xcom_pull(task_ids="location_processing") }}',
    #         "condition_ids": '{{ ti.xcom_pull(task_ids="condition_processing") }}',
    #     },
    # )

    # # MedicationStatement processing
    # medication_statement_processing = PythonOperator(
    #     task_id="medication_statement_processing",
    #     python_callable=process_medication_statement,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "medication_ids": '{{ ti.xcom_pull(task_ids="medication_processing") }}',
    #         "condition_ids": '{{ ti.xcom_pull(task_ids="condition_processing") }}',
    #     },
    # )

    # # Immunization processing
    # immunization_processing = PythonOperator(
    #     task_id="immunization_processing",
    #     python_callable=process_immunization,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "location_ids": '{{ ti.xcom_pull(task_ids="location_processing") }}',
    #         "condition_ids": '{{ ti.xcom_pull(task_ids="condition_processing") }}',
    #     },
    # )

    # wait_for_level_5_finish = DummyOperator(
    #     task_id="wait_for_level_5_finish",
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    # # Claim processing
    # claim_processing = PythonOperator(
    #     task_id="claim_processing",
    #     python_callable=process_claim,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "encounter_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "location_ids": '{{ ti.xcom_pull(task_ids="location_processing") }}',
    #         "condition_ids": '{{ ti.xcom_pull(task_ids="condition_processing") }}',
    #         "procedure_ids": '{{ ti.xcom_pull(task_ids="procedure_processing") }}',
    #         "medication_request_ids": '{{ ti.xcom_pull(task_ids="medication_request_processing") }}',
    #         "coverage_ids": '{{ ti.xcom_pull(task_ids="coverage_processing") }}',
    #     },
    # )

    # wait_for_level_6_finish = DummyOperator(
    #     task_id="wait_for_level_6_finish",
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    # # ClaimResponse processing
    # claim_response_processing = PythonOperator(
    #     task_id="claim_response_processing",
    #     python_callable=process_claim_response,
    #     op_kwargs={
    #         "bundle_stats": '{{ ti.xcom_pull(task_ids="read_bundle", key="bundle_stats") }}',
    #         "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
    #         "file_path": '{{ ti.xcom_pull(task_ids="read_bundle") }}',
    #         "organization_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
    #         "patient_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
    #         "practitioner_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
    #         "claim_ids": '{{ ti.xcom_pull(task_ids="claim_processing") }}',
    #     },
    # )

    # mark_processed_file = DummyOperator(
    #     task_id="mark_processed_file",
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # # Level 0
    # (parse_config >> init_dag >> read_bundle >> organization_processing >> wait_for_level_0_finish)

    # # Level 1
    # (
    #     wait_for_level_0_finish
    #     >> [location_processing, practitioner_processing, insurance_plan_processing, medication_processing]
    #     >> wait_for_level_1_finish
    # )

    # # Level 2
    # (wait_for_level_1_finish >> [patient_processing, practitioner_role_processing] >> wait_for_level_2_finish)

    # # Level 3
    # (wait_for_level_2_finish >> [coverage_processing, encounter_processing] >> wait_for_level_3_finish)

    # # Level 4
    # (
    #     wait_for_level_3_finish
    #     >> [
    #         condition_processing,
    #         service_request_processing,
    #         observation_processing,
    #         allergy_intolerance_processing,
    #         medication_request_processing,
    #     ]
    #     >> wait_for_level_4_finish
    # )

    # # Level 5
    # (
    #     wait_for_level_4_finish
    #     >> [procedure_processing, medication_statement_processing, immunization_processing]
    #     >> wait_for_level_5_finish
    # )

    # # Level 6
    # (wait_for_level_5_finish >> [claim_processing] >> wait_for_level_6_finish)

    # # Level 7
    # (wait_for_level_6_finish >> [claim_response_processing] >> mark_processed_file)

    # # Level Final
    # mark_processed_file >> post_dag_status

    (parse_config >> process_fhirbundle >> post_dag_status)
