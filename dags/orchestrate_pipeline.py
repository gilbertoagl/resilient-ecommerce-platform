from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'gilberto',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'ecommerce_resilient_pipeline',
    default_args=default_args,
    description='Pipeline completo: Chaos Monkey -> dbt (SCD2 & Quality)',
    schedule_interval=None, # Manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['advanced-project'],
) as dag:

    # generar el caos
    t1 = BashOperator(
        task_id='inject_chaos_events',
        bash_command='python /opt/airflow/dags/scripts/chaos_generator.py',
    )

    # correr dbt
    t2 = BashOperator(
        task_id='dbt_transformation_suite',
        bash_command='cd /opt/airflow/dags/ecommerce_dbt && dbt run --profiles-dir . && dbt snapshot --profiles-dir . && dbt test --profiles-dir .',
    )

    t1 >> t2