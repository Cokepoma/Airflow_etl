from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

def test_sql():
    hook = MsSqlHook(mssql_conn_id="mi_sqlserver")
    result = hook.get_records("SELECT TOP 5 * FROM AdventureWorks2022.HumanResources.Department")
    return result

with DAG(
    dag_id="prueba_sqlserver",
    start_date=datetime(2025, 6, 29),
    schedule="@once",
    catchup=False,
) as dag:
    prueba = PythonOperator(
        task_id="sacadatos",
        python_callable=test_sql
    )
