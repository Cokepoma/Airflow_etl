import pandas as pd
from sqlalchemy import create_engine
import urllib
import sqlalchemy
from airflow.hooks.base import BaseHook
import logging


#obtenemos las conexiones de origen y de salida para la bbdd

def get_sqlalchemy_engine(conn_id):
    conn = BaseHook.get_connection(conn_id)
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={conn.host};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
    )
    logging.info("---Obtenemos la conexión")
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")



def compare_table_data_with_hash(tabla, origen_conn_id, destino_conn_id, **kwargs):
    origen_engine = get_sqlalchemy_engine(origen_conn_id)
    destino_engine = get_sqlalchemy_engine(destino_conn_id)

    print(f"Extrayendo datos modificados desde la tabla: {tabla}")

    query = f"""
        WITH cte AS (
            SELECT 
                SalesOrderDetailID AS id,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CAST(SalesOrderID AS NVARCHAR(100)), ''),
                    ISNULL(CarrierTrackingNumber, ''),
                    ISNULL(CAST(OrderQty AS NVARCHAR(100)), ''),
                    ISNULL(CAST(ProductID AS NVARCHAR(100)), ''),
                    ISNULL(CAST(SpecialOfferID AS NVARCHAR(100)), ''),
                    ISNULL(CAST(UnitPrice AS NVARCHAR(100)), ''),
                    ISNULL(CAST(UnitPriceDiscount AS NVARCHAR(100)), ''),
                    ISNULL(CAST(LineTotal AS NVARCHAR(100)), ''),
                    ISNULL(CAST(rowguid AS NVARCHAR(100)), ''),
                    ISNULL(CAST(ModifiedDate AS NVARCHAR(100)), '')
                )) AS HashFila,
                GETDATE() AS fecha,
                'SalesOrderDetail' AS tabla
            FROM {tabla}
        )
        SELECT c.*
        FROM cte c
        JOIN pruebas_load.dbo.Snapshot_SalesOrderDetail t 
          ON c.id = t.id AND t.HashFila <> c.HashFila
    """
    logging.info("---datos comparados")

    df = pd.read_sql(query, origen_engine)
    logging.info(f"--Columnas devueltas: {df.columns.tolist()}")
    logging.info(f"--Filas extraídas: {len(df)}")

    if not df.empty and "id" in df.columns:
        tabla_destino = "temporal"
        df.to_sql(
            tabla_destino,
            destino_engine,
            if_exists='append',
            index=False,
            dtype={
                "id": sqlalchemy.types.Integer(),
                "HashFila": sqlalchemy.types.BINARY(32),
                "fecha": sqlalchemy.types.DateTime(),
                "tabla": sqlalchemy.types.NVARCHAR(50),
            }
        )
        ids_list = df["id"].tolist()
        kwargs['ti'].xcom_push(key='ids', value=ids_list)
        return 
    else:
        logging.info("---No hay cambios o columna 'id' no encontrada.")
        return []

def usar_ids_de_snapshot(ti, origen_conn_id, **kwargs):
    ids = ti.xcom_pull(task_ids="snapshot_Sales_SalesOrderDetail", key='ids')
    logging.info(f"---IDs modificados recibidos por XCom: {ids}")

    if not ids:
        logging.info("---No hay IDs para procesar.")
        return []

    ids_str = ','.join(str(i) for i in ids)
    logging.info(f"---IDs modificados recibidos por XCom: {ids_str}")

    query = f"""
        INSERT INTO pruebas_load.dbo.SalesOrderDetail 
        SELECT     
            ss.SalesOrderID,
            ss.salesorderdetailid,
            ss.carriertrackingnumber COLLATE Modern_Spanish_CI_AS,
            ss.orderqty,
            ss.productid,
            ss.specialofferid,
            ss.unitprice,
            ss.unitpricediscount,
            ss.linetotal,
            ss.rowguid,
            ss.modifieddate,
            COALESCE(oa.estado, 'inicial' COLLATE Modern_Spanish_CI_AS) AS estado,
            GETDATE() AS auditoria 
        FROM AdventureWorks2022.sales.salesorderdetail ss
        OUTER APPLY (
            SELECT 'actualización' AS estado
            FROM pruebas_load.dbo.SalesOrderDetail ds 
            WHERE ds.SalesOrderDetailID = ss.SalesOrderDetailID
        ) oa
        WHERE ss.SalesOrderDetailID IN ({ids_str})
    """

    origen_engine = get_sqlalchemy_engine(origen_conn_id)
    with origen_engine.begin() as connection:
        connection.execute(query)

    logging.info("-----Inserción ejecutada correctamente.")
    return []
    

def update_snapshot(ti, tabla, origen_conn_id, **kwargs):
    logging.info(f"---------------------Entramos en update_snapshot")
    origen_engine = get_sqlalchemy_engine(origen_conn_id)

    logging.info(f"---Extrayendo datos modificados desde la tabla: {tabla}")
    ids = ti.xcom_pull(task_ids="snapshot_Sales_SalesOrderDetail", key='ids')
    logging.info(f"---IDs modificados recibidos por XCom: {ids}")

    if not ids:
        logging.info("---No hay IDs para actualizar en el snapshot.")
        return []

    ids_str = ','.join(str(i) for i in ids)
    query = f"""
        WITH cte AS (   
            SELECT 
                SalesOrderDetailID AS id,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CAST(SalesOrderID AS NVARCHAR(100)), ''),
                    ISNULL(CarrierTrackingNumber, ''),
                    ISNULL(CAST(OrderQty AS NVARCHAR(100)), ''),
                    ISNULL(CAST(ProductID AS NVARCHAR(100)), ''),
                    ISNULL(CAST(SpecialOfferID AS NVARCHAR(100)), ''),
                    ISNULL(CAST(UnitPrice AS NVARCHAR(100)), ''),
                    ISNULL(CAST(UnitPriceDiscount AS NVARCHAR(100)), ''),
                    ISNULL(CAST(LineTotal AS NVARCHAR(100)), ''),
                    ISNULL(CAST(rowguid AS NVARCHAR(100)), ''),
                    ISNULL(CAST(ModifiedDate AS NVARCHAR(100)), '')
                )) AS HashFila,
                GETDATE() AS fecha,
                'SalesOrderDetail' AS tabla
            FROM {tabla}
        )
        UPDATE t
        SET HashFila = c.HashFila 
        FROM cte c
        JOIN pruebas_load.dbo.Snapshot_SalesOrderDetail t 
            ON c.id = t.id 
            AND t.HashFila <> c.HashFila 
            AND t.id IN ({ids_str})
    """

    logging.info("---Actualizando snapshot...")
    with origen_engine.begin() as connection:
        connection.execute(query)

    logging.info("---Snapshot actualizado.")
    return []
    # Aquí puedes usarlos como desees (guardar, enviar, registrar, etc.)    
