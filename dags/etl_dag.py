from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    # 'end_date': datetime(2026, 1, 1),
}
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='A data preprocessing DAG',
    schedule_interval=timedelta(days=1),
)

def etl():
    from os import system

    system('python spark_script.py')


t1 = PythonOperator(
    task_id='etl',
    python_callable=etl,
    dag=dag,
)
