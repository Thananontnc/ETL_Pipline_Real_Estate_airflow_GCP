from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
from io import StringIO
from datetime import datetime,timedelta

# Parameters
GCS_BUCKET = 'YOUR_BUCKET_NAME'
GCS_SOURCE_FILE = 'FILE_NAME'
GCS_TRANSFORMED_FILE = 'TRANSFORM_FILE_NAME.csv'
BQ_PROJECT_ID = 'PROJECT_ID_NAME'
BQ_DATASET = 'YOUR_DATASET_NAME' 
BQ_TABLE = 'TABE_NAME'

# Define Functions
def extract_from_gcs(**kwargs):
    """Extract data from GCS."""
    gcs_hook = GCSHook()
    client = gcs_hook.get_conn()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_SOURCE_FILE)
    data = blob.download_as_text()
    kwargs['ti'].xcom_push(key='raw_data', value=data)

def transform_data(**kwargs):
    """Transform data with Pandas."""
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract_data')
    df = pd.read_csv(StringIO(raw_data))
    
    # Transformations
    broker_unique = df['brokered_by'].unique()
    broker_mapping = {broker_id: f"Broker {chr(65 + i % 26)}-{i // 26 + 1}" for i, broker_id in enumerate(broker_unique)}
    df['brokered_by'] = df['brokered_by'].map(broker_mapping)
    df.drop(columns=['prev_sold_date'], inplace=True)
    df.fillna({
        "brokered_by": 'Unknown',
        "price": df["price"].mean(),
        'bed': df['bed'].median(),
        'bath': df['bath'].median(),
        'acre_lot': df['acre_lot'].mean(),
        'street': df['street'].mode()[0],
        'city': "Unknown",
        'state': "Unknown",
        'zip_code': "Unknown",
        'house_size': df['house_size'].mean(),
    }, inplace=True)
    df['price_per_sqft'] = df['price'] / df['house_size']
    df['lot_size_in_sqft'] = df['acre_lot'] * 43560
    
    transformed_data = df.to_csv(index=False)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_to_gcs(**kwargs):
    """Load transformed data to GCS."""
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    gcs_hook = GCSHook()
    client = gcs_hook.get_conn()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_TRANSFORMED_FILE)
    blob.upload_from_string(transformed_data, content_type='text/csv')

# Define DAG
default_args = {
    'owner': 'OWNER_NAME',
    'start_date': "YOUR_DATE_TIME",
    'depends_on_past': False,
    'email':['YOUR_EMAIL'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='real_estate_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for real estate data',
    schedule_interval=None,
    start_date="YOUR_DATE_TIME",
    catchup=False,
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_from_gcs,
        provide_context=True,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_to_gcs = PythonOperator(
        task_id='load_to_gcs',
        python_callable=load_to_gcs,
        provide_context=True,
    )

    load_to_bigquery = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        configuration={
            "load": {
                "sourceUris": [f"gs://{GCS_BUCKET}/{GCS_TRANSFORMED_FILE}"],
                "destinationTable": {
                    "projectId": BQ_PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE",
                "skipLeadingRows": 1,
                "autodetect": True,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Set task dependencies
    extract_data >> transform_data >> load_to_gcs >> load_to_bigquery
