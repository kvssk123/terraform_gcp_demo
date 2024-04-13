from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import zipfile
import os
from google.cloud import storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def download_zip_files(**kwargs):
    years = range(2016, 2021)
    for year in years:
        #url = f"https://static.nhtsa.gov/nhtsa/downloads/CRSS/{year}/CRSS{year}CSV.zip"
        url = f"https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip"
        local_zip_path = f"/home/airflow/gcs/data/FARS{year}.zip"
        response = requests.get(url)
        with open(local_zip_path, 'wb') as file:
            file.write(response.content)
    # Push the years list to XCom for other tasks to use
    kwargs['ti'].xcom_push(key='years', value=list(years))

def extract_zip_files(**kwargs):
    years = kwargs['ti'].xcom_pull(key='years', task_ids='download_zip_files')
    for year in years:
        local_zip_path = f"/home/airflow/gcs/data/FARS{year}.zip"
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            extract_path = f"/home/airflow/gcs/data/FARS{year}/"
            zip_ref.extractall(extract_path)

# def process_and_combine_data(**kwargs):
#     years = kwargs['ti'].xcom_pull(key='years', task_ids='download_zip_files')
#     combined_data = []
#     for year in years:
#         extract_path = f"/data/CRSS{year}/"
#         all_files = [os.path.join(extract_path, file) for file in os.listdir(extract_path) if file.endswith('.csv')]
#         for file in all_files:
#             df = pd.read_csv(file)
#             combined_data.append(df)
#     combined_csv = pd.concat(combined_data)
#     combined_csv.to_csv("/data/combined_CRSS_all_years.csv", index=False)

# def process_and_combine_data(**kwargs):
#     years = kwargs['ti'].xcom_pull(key='years', task_ids='download_zip_files')
#     accident_data = []
#     vehicle_data = []
#     for year in years:
#         extract_path = f"/home/airflow/gcs/data/CRSS{year}/"
#         # Only read accident.csv and vehicle.csv
#         accident_file = os.path.join(extract_path, 'accident.csv')
#         vehicle_file = os.path.join(extract_path, 'vehicle.csv')
        
#         if os.path.exists(accident_file):
#             df_accident = pd.read_csv(accident_file)
#             accident_data.append(df_accident)
        
#         if os.path.exists(vehicle_file):
#             df_vehicle = pd.read_csv(vehicle_file)
#             vehicle_data.append(df_vehicle)
    
#     # Combine accident data and vehicle data separately
#     combined_accident_csv = pd.concat(accident_data)
#     combined_vehicle_csv = pd.concat(vehicle_data)
    
#     # Save combined CSVs
#     combined_accident_csv.to_csv("/home/airflow/gcs/data/combined_CRSS_accidents.csv", index=False)
#     combined_vehicle_csv.to_csv("/home/airflow/gcs/data/combined_CRSS_vehicles.csv", index=False)
    
def process_and_combine_data(**kwargs):
    years = kwargs['ti'].xcom_pull(key='years', task_ids='download_zip_files')
    accident_data = []
    vehicle_data = []
    for year in years:
        extract_path = f"/home/airflow/gcs/data/FARS{year}/"
        all_files = os.listdir(extract_path)
        for file_name in all_files:
            full_file_path = os.path.join(extract_path, file_name)
            if file_name.lower().endswith('.csv'):
                if 'accident' in file_name.lower():
                    df = pd.read_csv(full_file_path,encoding='latin1')
                    accident_data.append(df)
                elif 'vehicle' in file_name.lower():
                    df = pd.read_csv(full_file_path,encoding='latin1')
                    vehicle_data.append(df)
    combined_accident_csv = pd.concat(accident_data)
    combined_vehicle_csv = pd.concat(vehicle_data)
    combined_accident_csv.to_csv("/home/airflow/gcs/data/combined_FARS_accidents.csv", index=False)
    combined_vehicle_csv.to_csv("/home/airflow/gcs/data/combined_FARS_vehicles.csv", index=False)



def upload_to_gcs(**kwargs):
    gcs_bucket_name = 'nhtsa_crs'
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    local_combined_path = "gs://nhtsa_crs/tmp/combined_CRSS_all_years.csv"
    gcs_file_path = 'combined_CRSS_all_years.csv'
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(local_combined_path)
    # Optionally, clean up the local combined file
    os.remove(local_combined_path)

with DAG('crss_data_processing_and_uploading_daily',
         default_args=default_args,
         description='Daily CRSS data processing and uploading',
         schedule_interval='@daily',
         catchup=False) as dag:

    download = PythonOperator(
        task_id='download_zip_files',
        python_callable=download_zip_files
    )

    extract = PythonOperator(
        task_id='extract_zip_files',
        python_callable=extract_zip_files,
        provide_context=True
    )

    process_combine = PythonOperator(
        task_id='process_and_combine_data',
        python_callable=process_and_combine_data,
        provide_context=True
    )

    upload = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs
    )

    download >> extract >> process_combine
    #>> upload
