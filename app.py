import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import numpy as np

# ==============================================================================
# 1. CONFIGURACIÓN DEL SISTEMA Y ESTILO (PALANTIR DARK MODE)
# ==============================================================================
st.set_page_config(page_title="Brasas Capitales | Command Center", layout="wide", page_icon="🔥")

# Inyección de CSS para forzar estilo Dashboard Ejecutivo
st.markdown("""
<style>
    /* Fondo Principal y Sidebar */
    [data-testid="stAppViewContainer"] {background-color: #0e1117;}
    [data-testid="stSidebar"] {background-color: #1a1c24;}
    
    /* Tarjetas de Métricas (KPIs) - Diseño "Bloque de Comando" */
    div[data-testid="metric-container"] {
        background-color: #262730;
        border-left: 5px solid #FF4B4B; /* Acento Rojo de Alerta */
        padding: 15px;
        border-radius: 6px;
        box-shadow: 0px 4px 6px rgba(0,0,0,0.4);
    }
    div[data-testid="metric-container"] label {
        color: #b0b3b8 !important; 
        font-size: 0.85rem; 
        text-transform: uppercase;
        font-family: 'Source Sans Pro', sans-serif;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #ffffff !important; 
        font-weight: 700; 
        font-size: 1.8rem;
    }
    
    /* Tablas de Datos */
    [data-testid="stDataFrame"] {border: 1px solid #41424C; border-radius: 5px;}
    
    /* Títulos y Textos */
    h1, h2, h3 {color: white !important; font-family: 'Source Sans Pro', sans-serif;}
    p, span, div, li {color: #e0e0e0;}
    
    /* Alertas Personalizadas */
    .critical-box {
        padding: 1rem; 
        background-color: #3d0000; 
        border: 1px solid #ff4b4b; 
        border-radius: 5px; 
        color: white; 
        font-weight: bold;
        text-align: center;
    }
    .success-box {
        padding: 1rem; 
        background-color: #002b20; 
        border: 1px solid #00cc96; 
        border-radius: 5px; 
        color: white; 
        font-weight: bold;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. GESTIÓN DE CREDENCIALES Y CONEXIÓN
# ==============================================================================
@st.cache_resource
def connect_google_sheets():
    """Conecta con la API de Google usando los Secretos de Streamlit"""
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error Crítico de Autenticación: {e}")
        st.stop()

# --- MAPA DE ARQUITECTURA DE ARCHIVOS (IDs REALES) ---
# ⚠️ IMPORTANTE: Asegúrate de que estos IDs sean los correctos en GitHub
IDS = {
    "REGISTROS":  "https://docs.google.com/spreadsheets/d/1pbpbkZWH6RHpUwdjrTFGtkNAi4ameR2PJZVbR5OPeZQ/edit?gid=1445845805#gid=1445845805",
    "LIBROS":     "https://docs.google.com/spreadsheets/d/1-juSBgRcNdKWNjDL6ZuKcBIVVQXtnpL3qCR9Z1AWQyU/edit?gid=988070039#gid=988070039",
    "COSTOS":     "https://docs.google.com/spreadsheets/d/1JNKE-5nfOsJ7U9k0bCLAW-xjOzSGRG15rdGdWrC3h8U/edit?gid=1976317468#gid=1976317468",
    "INVENTARIO": "https://docs.google.com/spreadsheets/d/1vDI6y_xN-abIFkv9z63rc94PTnCtJURC4r7vN3RCeLo/edit?gid=10562125#gid=10562125",
    "CAJA":       "https://docs.google.com/spreadsheets/d/1Ck6Um7PG8uZ626x9kMvf1tMlBckBUHBjy6dTYRxUIZY/edit?gid=0#gid=0",
    "FORECAST":   "https://docs.google.com/spreadsheets/d/1rmb0tvFhNQgiVOvUC3u5IxeBSA1w4HiY5lr13sD1VU0/edit?gid=1023849055#gid=1023849055"
}

# ==============================================================================
# 3. MOTOR DE EXTRACCIÓN Y LIMPIEZA DE DATOS (ETL DEFENSIVO)
# ==============================================================================
@st.cache_data(ttl=600) # Caché de 10 minutos para velocidad
def load_all_data():
    client = connect_google_sheets()
    DB = {}
    
    # Función auxiliar robusta para leer hojas
    def safe_read(file_key, sheet_name):
        try:
            if IDS[file_key].startswith("PON_AQUI"): return pd.DataFrame() # Evita error si no hay ID
            
            sh = client.open_by_key(IDS[file_key])
            ws = sh.worksheet(sheet_name)
            raw_data = ws.get_all_records()
            df = pd.DataFrame(raw_data)
            return df
        except Exception:
            # Retorna DF vacío si falla para no romper la app entera
            return pd.DataFrame()

    # --- CARGA DE DATOS ---
    DB['ventas'] = safe_read("REGISTROS", "BD_Ventas")
    DB['feriados'] = safe_read("REGISTROS", "MASTER_FERIADOS")
    DB['partidos'] = safe_read("REGISTROS", "MASTER_PARTIDOS")
    DB['costos'] = safe_read("COSTOS", "OUT_Costos_Productos")
    DB['qc'] = safe_read("COSTOS", "OUT_QC_Compras_NoConvertibles")
    DB['merma'] = safe_read("INVENTARIO", "OUT_Merma_Valorizada")
    DB['caja'] = safe_read("CAJA", "BD_Caja_Diaria")
    DB['capex'] = safe_read("CAJA", "PARAM_PROYECTOS_CAPEX")
    DB['forecast'] = safe_read("FORECAST", "OUT_Pronostico_Ventas")
    DB['soberania'] = safe_read("FORECAST", "OUT_Soberania_Financiera") 
    DB['deuda'] = safe_read("LIBROS", "Libro_Cuentas_Pagar")

    # --- LIMPIEZA DE FECHAS (GLOBAL) ---
    # Intentamos convertir cualquier columna que parezca fecha en datetime
    for key in DB:
        if not DB[key].empty:
            for col_name in ['Fecha', 'Fecha_dt', 'ds', 'Marca temporal', 'Fecha_Vencimiento']:
                if col_name in DB[key].columns:
                    # dayfirst=True es critico para fechas latinas (DD/MM/YYYY)
                    DB[key]['Fecha_dt'] = pd.to_datetime(DB[key][col_name], dayfirst=True, errors='coerce')
                    break # Solo convertimos la primera columna de fecha que encontremos

    # --- LIMPIEZA DE NÚMEROS (GLOBAL) ---
    # Limpia simbolos de moneda "S/" y comas ","
    def clean_currency(x):
        if isinstance(x, str):
            return x.replace('S/', '').replace(',', '').replace('%', '').strip()
        return x

    # Ventas
    if not DB['ventas'].empty and 'Total_Venta' in DB['ventas'].columns:
        DB['ventas']['Monto'] = pd.to_numeric(DB['ventas']['Total_Venta'].apply(clean_currency), errors='coerce').fillna(0)
    
    # Merma
    if not DB['merma'].empty and 'Merma_Soles' in DB['merma'].columns:
        DB['merma']['Monto_Merma'] = pd.to_numeric(DB['merma']['Merma_Soles'].apply(clean_currency), errors='coerce').fillna(0)
    
    # Costos (Margen)
    if not DB['costos'].empty and 'Margen_%' in DB['costos'].columns:
        DB['costos']['Margen_Pct'] = pd.to_numeric(DB['costos']['Margen_%'].apply(clean_currency), errors='coerce').fillna(0)
    
    # Soberania (Runway y Liquidez)
    if not DB['soberania'].empty:
        cols_fin = ['Runway_Dias', 'Liquidez_Neta', 'Burn_Rate_Diario', 'Deuda_TC_Auditada']
        for col in cols_fin:
            if col in DB['soberania'].columns:
                DB['soberania'][col] = pd.to_numeric(DB['soberania'][col].apply(clean_currency), errors='coerce').fillna(0)

    # Deuda
    if not DB['deuda'].empty and 'Saldo_Pendiente' in DB['deuda'].columns:
        DB['deuda']['Saldo'] = pd.to_numeric(DB['deuda']['Saldo_Pendiente'].apply(clean_currency), errors='coerce').fillna(0)

    return DB

# EJECUCIÓN DEL ETL
try:
    DATA = load_all_data()
    STATUS_CONN = "🟢 ONLINE | DATA SYNCED"
except Exception as e:
    STATUS_CONN = f"🔴 ERROR CRÍTICO: {e}"
    st.stop()

# ==============================================================================
# 4. LÓGICA DE INTERFAZ Y NAVEGACIÓN
# ==============================================================================
hoy = datetime.now()

with st.sidebar:
    st.title("🔥 BRASAS CAPITALES")
    st.caption(f"CEO Dashboard | {hoy.strftime('%d-%b-%Y')}")
    st.markdown("---")
    
    # Menú de Navegación
    menu = st.radio("MENÚ ESTRATÉGICO", 
        ["1. CORPORATE OVERVIEW", "2. EFICIENCIA & COSTOS", "3. FINANZAS & RUNWAY"])
    
    st.markdown("---")
    
    # Filtro Global de Tiempo
    st.markdown("### 📅 Filtro de Tiempo")
    filtro_tiempo = st.selectbox("Ventana de Análisis", ["Mes en Curso (MTD)", "Últimos 30 Días"])
    
    # Lógica de Fechas
    if filtro_tiempo == "Mes en Curso (MTD)":
        start_date = hoy.replace(day=1)
        periodo_label = "Acumulado Mes"
    else:
        start_date = hoy - timedelta(days=30)
        periodo_label = "Últimos 30 días"
        
    st.markdown("---")
    st.caption(STATUS_CONN)

# ==============================================================================
# PESTAÑA 1: CORPORATE OVERVIEW (SALUD DEL NEGOCIO)
# ==============================================================================
if menu == "1. CORPORATE OVERVIEW":
    st.header(f"🏥 Signos Vitales ({periodo_label})")
    
    # --- CÁLCULO DE KPIs PRINCIPALES ---
    kpi_venta = 0.0
    kpi_margen_avg = 0.0
    kpi_merma_total = 0.0
    kpi_runway = 0.0
    
    # 1. Ventas (MTD o Rolling)
    df_v = DATA['ventas']
    if not df_v.empty:
        mask_v = (df_v['Fecha_dt'].dt.date >= start_date.date()) & (df_v['Fecha_dt'].dt.date <= hoy.date())
        kpi_venta = df_v[mask_v]['Monto'].sum()
    
    # 2. Margen (Teórico Promedio)
    df_c = DATA['costos']
    if not df_c.empty:
        kpi_margen_avg = df_c['Margen_Pct'].mean() * 100

    # 3. Merma (Dinero Perdido)
    df_m = DATA['merma']
    if not df_m.empty:
        mask_m = (df_m['Fecha_dt'].dt.date >= start_date.date()) & (df_m['Fecha_dt'].dt.date <= hoy.date())
        kpi_merma_total = df_m[mask_m]['Monto_Merma'].sum()

    # 4. Runway (Ultimo dato calculado por Colab)
    df_sob = DATA['soberania']
    if not df_sob.empty:
        kpi_runway = df_sob.iloc[-1].get('Runway_Dias', 0)

    # --- RENDERIZADO DE TARJETAS ---
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("VENTAS TOTALES", f"S/ {kpi_venta:,.0f}", periodo_label)
    
    with col2:
        st.metric("MARGEN BRUTO (TEÓRICO)", f"{kpi_margen_avg:.1f}%", "Meta: >65%")
        
    with col3:
        # Gestión por Excepción: Alerta si Merma > 300
        delta_m = "-ALERTA" if kpi_merma_total > 300 else "Controlado"
        st.metric("PÉRDIDA X MERMA", f"S/ {kpi_merma_total:,.0f}", delta_m, delta_color="inverse")
        
    with col4:
        # Semáforo de Runway
        color_rw = "normal" if kpi_runway > 45 else "inverse"
        st.metric("CASH RUNWAY", f"{kpi_runway:.1f} Días", "Vida Financiera", delta_color=color_rw)

    st.markdown("---")

    # --- GRÁFICOS CENTRALES ---
    c_left, c_right = st.columns([2, 1])
    
    # GRÁFICO 1: VENTAS REALES vs FORECAST vs EVENTOS
    with c_left:
        st.subheader("📈 Rendimiento Comercial vs Pronóstico")
        
        fig_main = go.Figure()
        
        # A. Historia (Ventas Reales)
        if not df_v.empty:
            hist_start = hoy - timedelta(days=30)
            mask_hist = df_v['Fecha_dt'].dt.date >= hist_start.date()
            df_hist = df_v[mask_hist].groupby('Fecha_dt')['Monto'].sum().reset_index()
            fig_main.add_trace(go.Bar(
                x=df_hist['Fecha_dt'], y=df_hist['Monto'], 
                name='Venta Real', marker_color='#00A3E0'
            ))

        # B. Futuro (Forecast 006)
        df_f = DATA['forecast']
        if not df_f.empty:
            # Colab genera 'Venta_P50_Probable'
            if 'Venta_P50_Probable' in df_f.columns:
                df_f['yhat'] = pd.to_numeric(df_f['Venta_P50_Probable'], errors='coerce')
                mask_fore = (df_f['Fecha_dt'].dt.date >= hoy.date()) & (df_f['Fecha_dt'].dt.date <= (hoy + timedelta(days=7)).date())
                df_fore_plot = df_f[mask_fore]
                fig_main.add_trace(go.Scatter(
                    x=df_fore_plot['Fecha_dt'], y=df_fore_plot['yhat'], 
                    name='IA Forecast', line=dict(color='#FFA500', width=3, dash='dash')
                ))
            
        # C. Contexto (Partidos y Feriados)
        # Combinamos Feriados y Partidos en un solo set de eventos
        eventos_list = []
        if not DATA['feriados'].empty:
            df_fer = DATA['feriados'].copy()
            df_fer['Evento'] = df_fer['Nombre_Evento'] # Estandarizar nombre
            eventos_list.append(df_fer)
        if not DATA['partidos'].empty:
            eventos_list.append(DATA['partidos'])
            
        if eventos_list:
            df_evt = pd.concat(eventos_list, ignore_index=True)
            if 'Fecha_dt' in df_evt.columns
