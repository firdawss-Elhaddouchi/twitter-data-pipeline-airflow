from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from twitter_etl import extract_data, transform_data, load_data
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # DAG commencera hier → il sera visible
    'email': ['firdawss.elhaddouchi@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='twitter_etl_pipeline',
    default_args=default_args,
    description='ETL pour nettoyer les tweets Kaggle',
    schedule_interval='@daily',  #  None si je veux lancer manuellement
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    # Définir l'ordre d'exécution :
    extract_task >> transform_task >> load_task