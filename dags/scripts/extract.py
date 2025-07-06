import pandas as pd
from sqlalchemy import create_engine
import urllib
from airflow.hooks.base import BaseHook

def get_sqlalchemy_engine(conn_id):
    conn = BaseHook.get_connection(conn_id)
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={conn.host};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}") 

def copiar_tabla(tabla, origen_conn_id, destino_conn_id, **kwargs):
    origen_engine = get_sqlalchemy_engine(origen_conn_id)
    destino_engine = get_sqlalchemy_engine(destino_conn_id)

    df = pd.read_sql(f"SELECT top 1 * FROM {tabla}", origen_engine)
    print(f"Extraídas {len(df)} filas desde {tabla}")

    tabla_destino = tabla.split('.')[-1].lower()
    df.to_sql(tabla_destino, destino_engine, if_exists='replace', index=False)
    print(f"Tabla {tabla_destino} cargada en destino.")
