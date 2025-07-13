#Airfow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#funciones
from scripts.extract import compare_table_data_with_hash
from scripts.extract import usar_ids_de_snapshot
#libreras básicass
from datetime import datetime

with DAG(
    dag_id="prueba_sqlserver",
    start_date=datetime(2025, 6, 29),
    schedule="@once",
    catchup=False,
) as dag:

    snapshot_Sales = PythonOperator(
        task_id="snapshot_Sales_SalesOrderDetail",
        python_callable=compare_table_data_with_hash,
        op_kwargs={
            "tabla": "Sales.SalesOrderDetail",
            "origen_conn_id": "sqlserver_origen",
            "destino_conn_id": "sqlserver_destino"
        }
    )
    procesar_ids = PythonOperator(
        task_id="procesar_ids",
        python_callable=usar_ids_de_snapshot,
        op_kwargs={
            "origen_conn_id": "sqlserver_origen"
        }
    )
    snapshot_Sales >> procesar_ids