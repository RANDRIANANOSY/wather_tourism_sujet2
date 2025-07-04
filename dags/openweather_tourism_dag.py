from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Configuration par défaut du DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Définition du DAG
dag = DAG(
    'openweather_tourism_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour données météo OpenWeather et recommandations tourisme',
    schedule=timedelta(hours=6),  # Exécution toutes les 6 heures
    catchup=False,
    tags=['weather', 'tourism', 'etl']
)

# Définition des tâches
extract_task = BashOperator(
    task_id='extract_data',
    bash_command='python3 /opt/airflow/dags/weather_tourism_project/scripts/extract.py',
    dag=dag
)

transform_task = BashOperator(
    task_id='transform_data',
    bash_command='python3 /opt/airflow/dags/weather_tourism_project/scripts/transform.py',
    dag=dag
)

load_task = BashOperator(
    task_id='load_data',
    bash_command='python3 /opt/airflow/dags/weather_tourism_project/scripts/load.py',
    dag=dag
)

# Définition des dépendances
extract_task >> transform_task >> load_task


