import argparse
import duckdb
from datetime import datetime, timedelta
import uuid

from etl_utils import get_etl_parameters, calculate_execution_horizons

def main(params=None):  
    if params is None:
        params = get_etl_parameters()
    params['etl_name'] = 'events'

    print(f"Iniciando configuración ETL para: {params['etl_name']}")

    # Conexión a DuckDB
    from pathlib import Path
    db_path = Path("db/database.db")
    if not db_path.exists():
        raise FileNotFoundError(f"La Base de Datos no existe en {db_path}. Ejecuta etls/init_db.py primero.")
        
    conn = duckdb.connect(str(db_path))
    
    try:
        ejecutar, p_start_date, p_end_date, p_catchup, p_backfill, p_etl_name = calculate_execution_horizons(conn, params)
        
        if ejecutar == 'false':
            print(f"No hay cargas de datos pendientes. \nSi desea volver a ejecutar para fechas previas utilice Backfill=true.")
            return
            
        print(f"Ejecutando ETL:\n* Desde: {p_start_date}, Hasta: {p_end_date} \n* Backfill: {p_backfill}, Catchup: {p_catchup})")
        
        # Insertar registro con estado "running"
        run_uuid = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO metadata.etl_audit 
            (uuid, etl_name, status, start_date, end_date, catchup, backfill, updated_at)
            VALUES (?, ?, 'running', ?, ?, ?, ?, current_timestamp)
        """, [run_uuid, p_etl_name, p_start_date, p_end_date, p_catchup, p_backfill])
        
        try:
            conn.execute("BEGIN TRANSACTION")
            
            # Borramos datos de silver.events comprendidos entre fechas
            conn.execute("""
                DELETE FROM silver.events 
                WHERE event_date >= ? AND event_date <= ?
            """, [p_start_date, p_end_date])
            
            # Obtenemos datos de silver.events y realizamos una pequeña transformación (poner en mayúsculas el campo 'event')
            conn.execute("""
                INSERT INTO silver.events (uuid, event_date, user_mail, event, system)
                SELECT 
                    uuid() as uuid, 
                    br.event_date,
                    split(br.user_mail, '@')[1] as user_mail,
                    UPPER(br.event) as event,
                    UPPER(LEFT(br.system, 1)) as system
                FROM bronze.events br
                WHERE br.event_date >= ? AND br.event_date <= ?
            """, [p_start_date, p_end_date])
            
            conn.execute("COMMIT")
            
            # Auditoría Exitosa
            conn.execute("""
                UPDATE metadata.etl_audit 
                SET status = 'success', updated_at = current_timestamp
                WHERE uuid = ?
            """, [run_uuid])
            print(f"--- Finalizado correctamente. ---")
            
        except Exception as e:
            conn.execute("ROLLBACK")
            # Auditoría Fallida
            conn.execute("""
                UPDATE metadata.etl_audit 
                SET status = 'failed', updated_at = current_timestamp
                WHERE uuid = ?
            """, [run_uuid])
            print(f"ERROR DURANTE ETL. Ejecución abortada y registro actualizado a failed. Excepción: {e}")
            raise e
    finally:
        conn.close()

if __name__ == "__main__":
    main()