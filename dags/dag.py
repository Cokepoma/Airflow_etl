#Airfow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

#funciones
from scripts.extract import copiar_tabla

from datetime import datetime

tablas = [
    'Sales.SalesOrderHeader',
    'Sales.SalesOrderDetail'
]

with DAG(
    dag_id="prueba_sqlserver",
    start_date=datetime(2025, 6, 29),
    schedule="@once",
    catchup=False,
) as dag:
    for tabla in tablas:
            PythonOperator(
                task_id=f"copiar_{tabla.replace('.', '_')}",
                python_callable=copiar_tabla,
                op_kwargs={
                    "tabla": tabla,
                    "origen_conn_id": "sqlserver_origen",
                    "destino_conn_id": "sqlserver_destino"
                }
            )