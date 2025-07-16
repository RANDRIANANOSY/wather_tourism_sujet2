#!/usr/bin/env python3
"""
DAG Airflow pour le pipeline météo touristique
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Import de la classe des tâches
from weather_dag_tasks import WeatherDAGTasks

# Configuration du DAG
default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Définition du DAG
dag = DAG(
    'weather_tourism_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour recommandations météo touristiques',
    schedule_interval='0 6 * * *',  # Quotidien à 6h
    max_active_runs=1,
    tags=['weather', 'tourism', 'etl']
)

# Instance des tâches
weather_tasks = WeatherDAGTasks()

# Définition des tâches
task_extract = PythonOperator(
    task_id='extract_weather_data',
    python_callable=weather_tasks.task_extract_weather_data,
    dag=dag
)

task_validate = PythonOperator(
    task_id='validate_data_quality',
    python_callable=weather_tasks.task_validate_data_quality,
    dag=dag
)

task_transform = PythonOperator(
    task_id='transform_weather_data',
    python_callable=weather_tasks.task_transform_weather_data,
    dag=dag
)

task_create_schema = PythonOperator(
    task_id='create_star_schema',
    python_callable=weather_tasks.task_create_star_schema,
    dag=dag
)

task_load = PythonOperator(
    task_id='load_to_database',
    python_callable=weather_tasks.task_load_to_database,
    dag=dag
)

task_recommendations = PythonOperator(
    task_id='generate_recommendations',
    python_callable=weather_tasks.task_generate_recommendations,
    dag=dag
)

task_reports = PythonOperator(
    task_id='generate_reports',
    python_callable=weather_tasks.task_generate_reports,
    dag=dag
)

task_cleanup = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=weather_tasks.task_cleanup_old_data,
    dag=dag
)

# Définition des dépendances
task_extract >> task_validate >> task_transform >> task_create_schema >> task_load
task_load >> [task_recommendations, task_reports]
task_reports >> task_cleanup
