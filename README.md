## Airflow scripts

A script to process MIMIC and Tuva data (MVP v1) using Airflow. This was mainly done to quickly iterate over our data to get to Gold, rather than the longer NiFi process.

### Prerequisites

The script operates on the output of pre-processing service. It requires standardization-service (Pivot), profile-matching-service and Smile CDR (or HAPI) for FHIR validation.

### Usage

Run using Docker:

```
docker run -v $PWD/workspace:/opt/airflow/workspace ameya/trigger-ingestion run --input-dir=/opt/airflow/workspace/input_tuva_labs_all --run-id-prefix=tuva_labs_patient --workspace-dir=/opt/airflow/workspace
```

Here `input_tuva_labs_all` is a dir containing all patients from the preprocessing service.

You can also process data for certain patients only, by using a `--patients` flag

### Analysis

After running the ingestion script above, a directory containing validations is created in the run dir of the workspace. You can run an analysis on these validated resources to find e.g how many gold instances are found:

```
python scripts/trigger_ingestion.py analyze $PWD/workspace/tuva_labs_patient-2023-03-24T18:03:36/validated
```
