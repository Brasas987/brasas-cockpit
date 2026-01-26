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
    "REGISTROS":  "PON_AQUI_ID_REGISTROS_VENTAS_COMPRAS",
    "LIBROS":     "PON_AQUI_ID_001_LIBROS_CONTABLES",
    "COSTOS":     "PON_AQUI_ID_003_ANALISIS_COSTOS",
    "INVENTARIO": "PON_AQUI_ID_004_INVENTARIO",
    "CAJA":       "PON_AQUI_ID_005_CAJA",
    "FORECAST":   "PON_AQUI_ID_006_FORECAST"
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
        # Tomamos el último disponible
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
            if 'Venta_P50_Probable' in df_f.columns:
                df_f['yhat'] = pd.to_numeric(df_f['Venta_P50_Probable'], errors='coerce')
                mask_fore = (df_f['Fecha_dt'].dt.date >= hoy.date()) & (df_f['Fecha_dt'].dt.date <= (hoy + timedelta(days=7)).date())
                df_fore_plot = df_f[mask_fore]
                fig_main.add_trace(go.Scatter(
                    x=df_fore_plot['Fecha_dt'], y=df_fore_plot['yhat'], 
                    name='IA Forecast', line=dict(color='#FFA500', width=3, dash='dash')
                ))
            
        # C. Contexto (Partidos y Feriados)
        eventos_list = []
        if not DATA['feriados'].empty:
            df_fer = DATA['feriados'].copy()
            df_fer['Evento'] = df_fer['Nombre_Evento'] # Estandarizar nombre
            eventos_list.append(df_fer)
        if not DATA['partidos'].empty:
            eventos_list.append(DATA['partidos'])
            
        if eventos_list:
            df_evt = pd.concat(eventos_list, ignore_index=True)
            # CORRECCIÓN AQUÍ: Se agregaron los dos puntos ':'
            if 'Fecha_dt' in df_evt.columns:
                mask_evt = (df_evt['Fecha_dt'].dt.date >= (hoy - timedelta(days=30)).date()) & (df_evt['Fecha_dt'].dt.date <= (hoy + timedelta(days=7)).date())
                df_evt_plot = df_evt[mask_evt]
                if not df_evt_plot.empty:
                    fig_main.add_trace(go.Scatter(
                        x=df_evt_plot['Fecha_dt'], y=[0]*len(df_evt_plot), # Marcadores en el eje X
                        mode='markers', marker=dict(symbol='star', size=12, color='white'),
                        name='Evento', hovertext=df_evt_plot['Evento']
                    ))

        fig_main.update_layout(template="plotly_dark", height=400, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_main, use_container_width=True)

    # GRÁFICO 2: MATRIZ BCG (Volumen Ventas vs Margen Costos)
    with c_right:
        st.subheader("🧩 Matriz BCG (Productos)")
        if not df_v.empty and not df_c.empty:
            # 1. Calcular Volumen por Producto desde Ventas
            df_vol = df_v.groupby('Producto_ID')['Cantidad'].sum().reset_index()
            
            # 2. Unir con Costos para obtener Margen
            df_bcg = pd.merge(df_vol, df_c, on='Producto_ID', how='inner')
            
            if not df_bcg.empty:
                fig_bcg = px.scatter(
                    df_bcg, x="Cantidad", y="Margen_Pct", size="Precio_num", 
                    color="Menu", hover_name="Menu",
                    labels={"Cantidad": "Volumen Ventas", "Margen_Pct": "Margen Unitario %"}
                )
                fig_bcg.add_hline(y=60, line_dash="dot", annotation_text="Meta Margen")
                fig_bcg.update_layout(template="plotly_dark", height=400, paper_bgcolor='rgba(0,0,0,0)', showlegend=False)
                st.plotly_chart(fig_bcg, use_container_width=True)
            else:
                st.info("No hay coincidencia de IDs entre Ventas y Costos.")
        else:
            st.warning("Faltan datos de Ventas o Costos para la Matriz.")

# ==============================================================================
# PESTAÑA 2: EFICIENCIA & COSTOS
# ==============================================================================
elif menu == "2. EFICIENCIA & COSTOS":
    st.header("⚙️ Análisis de Desperdicios y Compras")

    col_ef1, col_ef2 = st.columns(2)
    
    # 1. RANKING DE MERMA (004)
    with col_ef1:
        st.subheader("🗑️ Ranking de Pérdidas (Merma)")
        df_m = DATA['merma']
        if not df_m.empty:
            df_tree = df_m.groupby('Insumo')['Monto_Merma'].sum().reset_index()
            df_tree['Merma_Abs'] = df_tree['Monto_Merma'].abs()
            
            fig_tree = px.treemap(
                df_tree, path=['Insumo'], values='Merma_Abs',
                color='Merma_Abs', color_continuous_scale='Reds',
                title="Merma Valorizada por Insumo"
            )
            fig_tree.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_tree, use_container_width=True)
        else:
            st.info("No hay registros de Merma en el archivo 004.")

    # 2. CONTROL DE CALIDAD COMPRAS (003)
    with col_ef2:
        st.subheader("🛡️ Compras No Convertibles (Mala Calidad)")
        df_qc = DATA['qc']
        if not df_qc.empty:
            st.dataframe(df_qc, use_container_width=True)
            if 'Total_Pagado' in df_qc.columns:
                 total_bad = pd.to_numeric(df_qc['Total_Pagado'].astype(str).str.replace(r'[S/,]', '', regex=True), errors='coerce').sum()
                 st.metric("TOTAL PERDIDO EN COMPRAS MALAS", f"S/ {total_bad:,.2f}", delta="-QC FAIL", delta_color="inverse")
        else:
            st.success("✅ Excelente. No hay reportes de compras rechazadas.")

    st.markdown("---")
    
    # 3. GAP FOOD COST (Inventario Teorico vs Real)
    st.subheader("⚖️ Discrepancia de Inventario (Gap Analysis)")
    if not df_m.empty and 'Stock_teorico_gr' in df_m.columns:
        df_gap = df_m.copy()
        df_gap['Gap'] = df_gap['Stock_teorico_gr'] - df_gap['Stock_real_gr']
        df_gap = df_gap.sort_values('Gap', ascending=False).head(10)
        
        fig_gap = px.bar(df_gap, x='Insumo', y='Gap', color='Gap', title="Diferencia en Gramos (Teórico - Real)")
        fig_gap.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_gap, use_container_width=True)
    else:
        st.info("Cargando datos de comparación de stocks...")

# ==============================================================================
# PESTAÑA 3: FINANZAS & RUNWAY (INTEGRACION CON COLAB)
# ==============================================================================
elif menu == "3. FINANZAS & RUNWAY":
    st.header("🔮 Soberanía Financiera")

    df_sob = DATA['soberania']
    
    if not df_sob.empty:
        ultimo = df_sob.iloc[-1]
        
        orden = ultimo.get('ORDEN_TESORERIA', 'SIN DATOS')
        runway_val = ultimo.get('Runway_Dias', 0)
        burn_rate = ultimo.get('Burn_Rate_Diario', 0)
        deuda_tc = ultimo.get('Deuda_TC_Auditada', 0)
        
        # --- BLOQUE DE ORDEN EJECUTIVA ---
        st.markdown(f"### 📢 ORDEN DEL DÍA")
        if "ALERTA" in str(orden):
            st.markdown(f'<div class="critical-box">🚨 {orden}</div>', unsafe_allow_html=True)
        elif "CRECIMIENTO" in str(ultimo.get('STATUS','')):
            st.markdown(f'<div class="success-box">🚀 {orden}</div>', unsafe_allow_html=True)
        else:
            st.info(f"🛡️ {orden}")
            
        st.markdown("---")

        # 1. GRÁFICO DE RUNWAY
        st.subheader("✈️ Evolución de tu Pista de Aterrizaje")
        fig_run = px.line(df_sob, x='Fecha_dt', y='Runway_Dias', markers=True)
        fig_run.add_hline(y=45, line_dash="dot", line_color="green", annotation_text="Objetivo (45)")
        fig_run.add_hline(y=30, line_dash="dot", line_color="red", annotation_text="Peligro (30)")
        fig_run.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_run, use_container_width=True)

        # 2. DETALLE DE MÉTRICAS
        c_fin1, c_fin2 = st.columns(2)
        with c_fin1:
            st.metric("BURN RATE DIARIO", f"S/ {burn_rate:,.2f}", "Costo de operar 1 día")
        with c_fin2:
            st.metric("DEUDA PASIVA (TC)", f"S/ {deuda_tc:,.2f}", "Deuda Corriente")

    else:
        st.warning("⚠️ El módulo de Forecast no ha generado datos de Soberanía Financiera. Ejecuta el Colab.")

    # 3. DEUDAS CON PROVEEDORES
    st.markdown("---")
    st.subheader("📉 Deudas con Proveedores (Cuentas por Pagar)")
    df_d = DATA['deuda']
    if not df_d.empty:
        total_deuda = df_d['Saldo'].sum()
        st.metric("TOTAL PENDIENTE PROVEEDORES", f"S/ {total_deuda:,.2f}")
        st.dataframe(df_d[['Fecha_Vencimiento', 'Concepto', 'Saldo']], use_container_width=True)
    else:
        st.success("✅ Sin deudas registradas en Libros Contables.")

    # 4. CAPEX
    st.subheader("🏗️ Proyectos de Inversión (CAPEX)")
    df_cap = DATA['capex']
    if not df_cap.empty:
        df_cap['Avance'] = (df_cap['Monto_Acumulado_Actual'] / df_cap['Monto_Total'])
        st.dataframe(
            df_cap, 
            column_config={
                "Avance": st.column_config.ProgressColumn("Progreso", min_value=0, max_value=1, format="%.0f%%")
            },
            use_container_width=True
        )
    else:
        st.info("No hay proyectos activos.")
