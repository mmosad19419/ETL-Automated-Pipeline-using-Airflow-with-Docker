# import etl script functions
from etl_script import *


# start bilding the dag
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(0),
    "retries": 1,
}

dag = DAG(
    'accidents_data_etl_pipeline',
    default_args=default_args,
    description='accidents_data_etl_pipeline',
)
with DAG(
    dag_id = 'accidents_data_etl_pipeline',
    schedule_interval = '@once',
    default_args = default_args,
    tags = ['accidents_data_etl_pipeline'],
)as dag:
    extract_integrate_task= PythonOperator(
        task_id = 'extract_integrate_task',
        python_callable = extract_integrate,
        op_kwargs={
            "filepath": '/opt/airflow/data/'
        },
    )
    cleanning_task= PythonOperator(
        task_id = 'cleanning_task',
        python_callable = data_clean,
        op_kwargs={
            "filename": "/opt/airflow/data/accidents_integrated.csv"
        },
    )
    transform_encoding_load_task= PythonOperator(
        task_id = 'transform_encoding_load_task',
        python_callable = transform_encode_load,
        op_kwargs={
            "filename": "/opt/airflow/data/accidents_cleaned.csv"
        },
    )
    

# pipeline stream dag
extract_integrate_task >> cleanning_task >> transform_encoding_load_task
