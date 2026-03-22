import os
import duckdb
import random
import string
from datetime import datetime, timedelta
import uuid

def main():
    # Crear conexión con DuckDB basado en archivo
    os.makedirs('db', exist_ok=True)
    db_path = os.path.join('db', 'database.db')
    
    # Nos conectamos a DuckDB. Si el archivo no existe, lo crea automáticamente.
    conn = duckdb.connect(db_path)
    
    try:
        # Crear las bases de datos (esquemas lógicos en DuckDB)
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS metadata;")
        
        # Crear tabla bronze.events
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze.events (
                event_date DATE,
                user_mail TEXT,
                event TEXT,
                system TEXT
            );
        """)
        
        # Crear tabla metadata.etl_audit
        conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.etl_audit (
                uuid UUID PRIMARY KEY,
                etl_name TEXT,
                status TEXT,
                start_date DATE,
                end_date DATE,
                catchup BOOLEAN,
                backfill BOOLEAN,
                updated_at TIMESTAMP
            );
        """)
        
        # Crear tabla silver.events
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver.events (
                uuid UUID PRIMARY KEY,
                event_date DATE,
                user_mail TEXT,
                event TEXT,
                system TEXT
            );
        """)

        # Limpiar tablas
        conn.execute("TRUNCATE TABLE bronze.events;")
        conn.execute("TRUNCATE TABLE silver.events;")
        conn.execute("TRUNCATE TABLE metadata.etl_audit;")
        
        # Cargar 30 días a partir de ayer
        yesterday = datetime.now() - timedelta(days=1)
        
        actions = ['comment', 'post', 'view', 'search', 'profile']
        systems = ['alpha', 'beta', 'gamma', 'delta']
        
        records = []
        
        for i in range(30):
            current_date = (yesterday - timedelta(days=i)).strftime('%Y-%m-%d')
            
            for _ in range(3):
                random_letters = ''.join(random.choices(string.ascii_lowercase, k=3))
                mail_usuario = f"{random_letters}@mail.com"
                accion = random.choice(actions)
                sistema = random.choice(systems)
                
                records.append((current_date, mail_usuario, accion, sistema))
                
        # Insertar los registros en bronze.events
        conn.executemany("""
            INSERT INTO bronze.events (event_date, user_mail, event, system)
            VALUES (?, ?, ?, ?)
        """, records)

        # Insertar registros de auditoría de ejemplo para pruebas
        now = datetime.now()
        audit_records = [
            (str(uuid.uuid4()), 'events', 'success', (now - timedelta(days=7)).date(), (now - timedelta(days=7)).date(), True, False, (now - timedelta(days=7))),
            (str(uuid.uuid4()), 'events', 'failed', (now - timedelta(days=6)).date(), (now - timedelta(days=6)).date(), True, False, (now - timedelta(days=6))),
            (str(uuid.uuid4()), 'events', 'failed', (now - timedelta(days=5)).date(), (now - timedelta(days=5)).date(), True, False, (now - timedelta(days=5)))
        ]

        conn.executemany("""
            INSERT INTO metadata.etl_audit (uuid, etl_name, status, start_date, end_date, catchup, backfill, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, audit_records)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()