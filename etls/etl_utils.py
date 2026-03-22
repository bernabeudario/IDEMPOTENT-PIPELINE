import argparse
from datetime import datetime, timedelta

def get_etl_parameters() -> dict:
    """
    Parsea y devuelve los parámetros de línea de comandos
    """
    parser = argparse.ArgumentParser(description="ETL Process Configuration")
    parser.add_argument("--start_date", type=str, default=None, help="Fecha inicio para backfill y catchup(YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, default=None, help="Fecha fin para backfill (YYYY-MM-DD)")
    parser.add_argument("--catchup", type=str, default="false", choices=["true", "false"])
    parser.add_argument("--backfill", type=str, default="false", choices=["true", "false"])
    
    args = parser.parse_args()
    
    return {
        'start_date': args.start_date,
        'end_date': args.end_date,
        'catchup': args.catchup,
        'backfill': args.backfill
    }

def calculate_execution_horizons(conn, params):
    """
    Valida auditorías previas y calcula los horizontes de fechas para la ejecución del ETL.
    Retorna una tupla con: (ejecutar, p_start_date, p_end_date, p_catchup, p_backfill, p_etl_name)
    """
    p_etl_name = params['etl_name']
    p_backfill = params['backfill']
    p_catchup = params['catchup'] 

    if p_backfill == 'true' and p_catchup == 'true':
        raise ValueError("Los parámetros 'catchup' y 'backfill' son mutuamente excluyentes.")

    # Verifica si el ETL ya se está ejecutando
    running_query = """
        SELECT COUNT(*) 
        FROM metadata.etl_audit 
        WHERE 1 = 1
        AND etl_name = ? 
        AND status = 'running'
    """
    is_running = conn.execute(running_query, [p_etl_name]).fetchone()[0]
    
    if is_running > 0:
        raise RuntimeError(f"El ETL '{p_etl_name}' ya se está ejecutando actualmente. Proceso abortado.")
        
    # Busca el último registro con backfill=false y selecciona MAX(end_date)
    max_date_query = """
        SELECT MAX(end_date) 
        FROM metadata.etl_audit 
        WHERE 1 = 1
        AND etl_name = ? 
        AND backfill = false
        AND status = 'success'
    """
    last_execution_date = conn.execute(max_date_query, [p_etl_name]).fetchone()[0]
    
    yesterday = (datetime.now() - timedelta(days=1)).date()    
        
    # Establecer p_start_date
    if p_backfill == 'false' and p_catchup == 'false':
        p_start_date = yesterday
        p_end_date = yesterday

    if p_catchup == 'true':
        user_start_date = datetime.strptime(params['start_date'], '%Y-%m-%d').date()
        if last_execution_date is None:
            p_start_date = user_start_date
        else:
            # Selecciona el mayor entre la fecha enviada y el último día cargado + 1
            p_start_date = max(user_start_date, last_execution_date + timedelta(days=1))
        p_end_date = yesterday

    if p_backfill == 'true':
        p_start_date = datetime.strptime(params['start_date'], '%Y-%m-%d').date()
        p_end_date = datetime.strptime(params['end_date'], '%Y-%m-%d').date()

    # Lógica 'ejecutar'
    ejecutar = 'true'
    if p_start_date > p_end_date:
        ejecutar = 'false'
        
    return ejecutar, p_start_date, p_end_date, p_catchup, p_backfill, p_etl_name
