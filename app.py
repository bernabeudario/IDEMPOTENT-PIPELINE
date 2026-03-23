import streamlit as st
import duckdb
import pandas as pd
import os
import sys
import subprocess
import uuid
from datetime import datetime, timedelta

DB_PATH = "db/database.db"

def db_exists():
    return os.path.exists(DB_PATH)

def fetch_table_data(table_name, order_by=None):
    if not db_exists():
        return pd.DataFrame()
    try:
        # Usamos read_only para evitar bloqueos con los procesos de ingesta/ETL
        with duckdb.connect(DB_PATH, read_only=True) as conn:
            query = f"SELECT * FROM {table_name}"
            if order_by:
                query += f" ORDER BY {order_by}"
            df = conn.execute(query).df()
            
            # PyArrow no reconoce objetos UUID de Python, los convertimos a string
            for col in df.columns:
                if df[col].dtype == 'object' and not df[col].empty:
                    if isinstance(df[col].iloc[0], uuid.UUID):
                        df[col] = df[col].astype(str)
                
            return df
    except Exception as e:
        # Silencia errores de tabla no existente en la primera corrida antes de init_db
        return pd.DataFrame()

st.set_page_config(layout="wide", page_title="ETL Idempotente")

# Ocultar elementos por defecto de Streamlit
st.markdown("""
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .stDeployButton {display:none;}
        [data-testid="stHeader"] {display: none;}
        .block-container {
            padding-top: 2rem;
            padding-bottom: 0rem;
        }
    </style>
""", unsafe_allow_html=True)

if "init_running" not in st.session_state:
    st.session_state.init_running = False
if "etl_running" not in st.session_state:
    st.session_state.etl_running = False

# Función para resetear las fechas a ayer cuando se desactivan los flags
def reset_dates_callback():
    yesterday_val = (datetime.now() - timedelta(days=1)).date()
    # Si ambos flags están en false, forzamos el estado de los componentes de fecha a ayer
    if st.session_state.get("catchup_sel") == "false" and st.session_state.get("backfill_sel") == "false":
        st.session_state.sd_val = yesterday_val
        st.session_state.ed_val = yesterday_val
    # Caso especial Catchup=true: habilitamos start_date pero reseteamos end_date a ayer
    elif st.session_state.get("catchup_sel") == "true":
        st.session_state.ed_val = yesterday_val

def clear_etl_results():
    if "etl_toast" in st.session_state: del st.session_state.etl_toast
    if "etl_code" in st.session_state: del st.session_state.etl_code
    if "etl_error" in st.session_state: del st.session_state.etl_error

def start_init():
    clear_etl_results()
    st.session_state.init_running = True

def start_etl():
    clear_etl_results()
    st.session_state.etl_running = True

# -- Flujo de Datos --
col_title, col_status, col_init = st.columns([6, 2, 2])

with col_title:
    st.header("Flujo de Datos")

with col_status:
    st.write("")
    if not db_exists():
        st.error("❌ Base de Datos no encontrada")

with col_init:
    st.write("")
    if st.button("🦆 Inicializar Base de Datos", width='stretch', type="secondary", help="Ejecuta etls/init_db.py.",disabled=st.session_state.init_running, on_click=start_init):
        with st.spinner("Inicializando Base de Datos y cargando datos de ejemplo..."):
            result = subprocess.run([sys.executable, "etls/init_db.py"], capture_output=True, text=True)
            
            st.session_state.init_running = False
            if result.returncode == 0:
                st.session_state.init_toast = "Base de Datos inicializada."
            else:
                st.session_state.init_error = f"Error al inicializar:\n{result.stderr}"
            st.rerun()

if "init_toast" in st.session_state:
    st.toast(st.session_state.init_toast, icon="✅")
    del st.session_state.init_toast
if "init_error" in st.session_state:
    st.error(st.session_state.init_error, icon="❌")
    del st.session_state.init_error

col_bronze, col_etl, col_silver = st.columns(3)

with col_bronze:
    st.subheader("🥉 Bronze Events")
    df_bronze = fetch_table_data("bronze.events", "event_date DESC")
    st.dataframe(df_bronze, width='stretch', hide_index=True)

with col_etl:
    st.subheader("⚙️ Ejecutar ETL")
    st.text_input("ETL Name", value="events", disabled=True, help="El nombre está hardcodeado pero podría usarse un selector.", key="etl_name")
    etl_name = st.session_state.etl_name

    if "catchup_sel" not in st.session_state: st.session_state.catchup_sel = "false"
    if "backfill_sel" not in st.session_state: st.session_state.backfill_sel = "false"
    
    c_val = st.session_state.catchup_sel
    b_val = st.session_state.backfill_sel
    
    col_flags1, col_flags2 = st.columns(2)
    with col_flags1:
        catchup = st.selectbox("Catchup", ["false", "true"], disabled=(b_val == "true"), key="catchup_sel", on_change=reset_dates_callback)
    with col_flags2:
        backfill = st.selectbox("Backfill", ["false", "true"], disabled=(c_val == "true"), key="backfill_sel", on_change=reset_dates_callback)

        
    # Si backfill es true, se habilitan ambas fechas. Si catchup es true, se habilita solo Start Date.
    start_date_disabled = not (backfill == "true" or catchup == "true")
    end_date_disabled = (backfill != "true")
    yesterday = (datetime.now() - timedelta(days=1)).date()
        
    # Inicializar las keys de fecha en el primer render para que st.date_input las use
    if "sd_val" not in st.session_state: st.session_state.sd_val = yesterday
    if "ed_val" not in st.session_state: st.session_state.ed_val = yesterday

    start_date = st.date_input("Start Date", disabled=start_date_disabled, key="sd_val")
    end_date = st.date_input("End Date", disabled=end_date_disabled, key="ed_val")
    
    # Validación para inhabilitar botón si start_date > end_date
    invalid_dates = (start_date > end_date) if backfill == "true" else False
    if invalid_dates:
        st.warning("⚠️ La Fecha de Inicio (Start Date) no puede ser mayor a la Fecha de Fin (End Date).")
    
    if st.button("Ejecutar ETL", type="primary", disabled=(invalid_dates or st.session_state.etl_running), on_click=start_etl, help=f"Ejecuta etls/{etl_name}.py."):
        cmd = [
            sys.executable, f"etls/{etl_name}.py",
            "--start_date", start_date.strftime("%Y-%m-%d"),
            "--end_date", end_date.strftime("%Y-%m-%d"),
            "--catchup", catchup,
            "--backfill", backfill
        ]
        with st.spinner("Ejecutando proceso ETL..."):
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            st.session_state.etl_running = False
            if result.returncode == 0:
                st.session_state.etl_toast = "ETL finalizado"
                st.session_state.etl_code = result.stdout
                st.session_state.etl_error = False
            else:
                st.session_state.etl_toast = "Error en ETL"
                st.session_state.etl_code = result.stderr or result.stdout
                st.session_state.etl_error = True
            st.rerun()

    if "etl_toast" in st.session_state:
        # Hack CSS para que st.code haga wrap del texto (evita scroll horizontal)
        st.markdown("""
            <style>
                code {
                    white-space: pre-wrap !important;
                }
            </style>
        """, unsafe_allow_html=True)
        
        if st.session_state.etl_error:
            st.error(st.session_state.etl_toast, icon="❌")
            st.code(st.session_state.etl_code, language="text")
        else:
            st.success(st.session_state.etl_toast, icon="✅")
            st.code(st.session_state.etl_code, language="text")
        
        st.button("🧹 Limpiar resultados", on_click=clear_etl_results, width='stretch')

with col_silver:
    st.subheader("🥈 Silver Events")
    df_silver = fetch_table_data("silver.events", "event_date DESC")
    st.dataframe(df_silver, width='stretch', hide_index=True, column_config={"uuid": st.column_config.Column(width="small")})

st.divider()

# -- Auditoría de Metadata --
col_audit_title, col_audit_btn = st.columns([8, 2])
with col_audit_title:
    st.header("Auditoría de Metadata")
with col_audit_btn:
    st.write("")
    if st.button("🗑️ Borrar datos", width='stretch', help="Borra todos los registros."):
        if db_exists():
            try:
                with duckdb.connect(DB_PATH) as conn:
                    conn.execute("TRUNCATE TABLE metadata.etl_audit;")
                st.session_state.audit_toast = "Datos borrados exitósamente."
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

if "audit_toast" in st.session_state:
    st.toast(st.session_state.audit_toast, icon="✅")
    del st.session_state.audit_toast

df_audit = fetch_table_data("metadata.etl_audit", "updated_at DESC")
st.dataframe(df_audit, width='stretch', hide_index=True)
