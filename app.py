import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import numpy as np
from streamlit_gsheets import GSheetsConnection

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
    "LIBROS":     "https://docs.google.com/spreadsheets/d/1-juSBgRcNdKWNjDL6ZuKcBIVVQXtnpL3qCR9Z1AWQyU/edit?gid=0#gid=0",
    "COSTOS":     "https://docs.google.com/spreadsheets/d/1JNKE-5nfOsJ7U9k0bCLAW-xjOzSGRG15rdGdWrC3h8U/edit?gid=1976317468#gid=1976317468",
    "INVENTARIO": "https://docs.google.com/spreadsheets/d/1vDI6y_xN-abIFkv9z63rc94PTnCtJURC4r7vN3RCeLo/edit?gid=10562125#gid=10562125",
    "CAJA":       "https://docs.google.com/spreadsheets/d/1Ck6Um7PG8uZ626x9kMvf1tMlBckBUHBjy6dTYRxUIZY/edit?gid=1914701014#gid=1914701014",
    "FORECAST":   "https://docs.google.com/spreadsheets/d/1rmb0tvFhNQgiVOvUC3u5IxeBSA1w4HiY5lr13sD1VU0/edit?gid=0#gid=0"
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
hoy = datetime.now() - timedelta(hours=5)

with st.sidebar:
    st.title("🔥 BRASAS CAPITALES")
    st.caption(f"CEO Dashboard | {hoy.strftime('%d-%b-%Y')}")
    st.markdown("---")
    
    # Menú de Navegación
    menu = st.radio("MENÚ ESTRATÉGICO", 
        ["1. CORPORATE OVERVIEW", "2. EFICIENCIA & COSTOS", "3. FINANZAS & RUNWAY", "4. MENU ENGINEERING", "5. CX & TIEMPOS", "6. GROWTH & LEALTAD", "7. GESTION DE MARCA"])
    
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
    
    # --- PROCESAMIENTO DE DATOS ---
    kpi_venta = 0.0
    kpi_margen_avg = 0.0
    kpi_merma_total = 0.0
    kpi_runway = 0.0
    
    # Variables Comerciales
    ticket_promedio = 0.0
    num_transacciones = 0
    pe_diario = 0.0
    venta_hoy = 0.0
    
    # 1. Ventas & Ticket Promedio
    df_v = DATA['ventas']
    if not df_v.empty:
        # Filtro de fecha seleccionado
        mask_v = (df_v['Fecha_dt'].dt.date >= start_date.date()) & (df_v['Fecha_dt'].dt.date <= hoy.date())
        df_filtrada = df_v[mask_v]
        kpi_venta = df_filtrada['Monto'].sum()
        
        # Venta solo de HOY (para la meta diaria)
        venta_hoy = df_v[df_v['Fecha_dt'].dt.date == hoy.date()]['Monto'].sum()
        
        # Ticket Promedio
        if 'ID_Ticket' in df_filtrada.columns:
            df_tickets = df_filtrada.groupby('ID_Ticket')['Monto'].sum()
            num_transacciones = df_tickets.count()
            ticket_promedio = df_tickets.mean() if num_transacciones > 0 else 0

    # 2. Margen & Merma
    df_c = DATA['costos']
    if not df_c.empty: kpi_margen_avg = df_c['Margen_Pct'].mean() * 100

    df_m = DATA['merma']
    if not df_m.empty:
        mask_m = (df_m['Fecha_dt'].dt.date >= start_date.date()) & (df_m['Fecha_dt'].dt.date <= hoy.date())
        kpi_merma_total = df_m[mask_m]['Monto_Merma'].sum()

    # 3. Datos Financieros (Para Meta Diaria)
    df_sob = DATA['soberania']
    if not df_sob.empty:
        ultimo = df_sob.iloc[-1]
        kpi_runway = ultimo.get('Runway_Dias', 0)
        
        # Cálculo Rápido PE (Burn Rate / Margen)
        burn = ultimo.get('Burn_Rate_Diario', 0)
        try:
            ratio = float(str(ultimo.get('Ratio_Costo_Real', '0.6')).replace('%',''))
            if ratio > 1: ratio /= 100
        except: ratio = 0.60
        
        margen_contrib = 1 - ratio
        pe_diario = burn / margen_contrib if margen_contrib > 0 else 9999

    # --- VISUALIZACIÓN ---
    
    # 1. BLOQUE DE META DIARIA (BARRA DE PROGRESO TÁCTICA)
    st.markdown("##### 🏁 Meta del Día (Break-even Operativo)")
    pct_meta = min(venta_hoy / pe_diario, 1.0) if pe_diario > 0 else 0
    cols_meta = st.columns([3, 1])
    with cols_meta[0]:
        st.progress(pct_meta)
    with cols_meta[1]:
        st.caption(f"**{pct_meta*100:.0f}%** (S/ {venta_hoy:,.0f} / {pe_diario:,.0f})")
    
    st.markdown("---")

    # 2. TARJETAS PRINCIPALES
    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("VENTAS ACUMULADAS", f"S/ {kpi_venta:,.0f}", periodo_label)
    with col2: st.metric("TICKET PROMEDIO", f"S/ {ticket_promedio:,.1f}", f"{num_transacciones} Mesas")
    with col3: 
        delta_m = "-ALERTA" if kpi_merma_total > 300 else "Controlado"
        st.metric("MERMA VALORIZADA", f"S/ {kpi_merma_total:,.0f}", delta_m, delta_color="inverse")
    with col4:
        color_rw = "normal" if kpi_runway > 45 else "inverse"
        st.metric("CASH RUNWAY", f"{kpi_runway:.1f} Días", "Vida Financiera", delta_color=color_rw)

    st.markdown("---")

    # 3. GRÁFICOS (Ventas y BCG)
    c_left, c_right = st.columns([2, 1])
    
    # Gráfico Ventas
    with c_left:
        st.subheader("📈 Rendimiento Comercial")
        fig_main = go.Figure()
        if not df_v.empty:
            hist_start = hoy - timedelta(days=30)
            mask_hist = df_v['Fecha_dt'].dt.date >= hist_start.date()
            df_hist = df_v[mask_hist].groupby('Fecha_dt')['Monto'].sum().reset_index()
            fig_main.add_trace(go.Bar(x=df_hist['Fecha_dt'], y=df_hist['Monto'], name='Venta Real', marker_color='#00A3E0'))
            
            # Línea de Meta en el Gráfico
            fig_main.add_hline(y=pe_diario, line_dash="dot", line_color="green", annotation_text="Meta PE")

        # Forecast (2 días)
        df_f = DATA['forecast']
        if not df_f.empty and 'Venta_P50_Probable' in df_f.columns:
            df_f['yhat'] = pd.to_numeric(df_f['Venta_P50_Probable'], errors='coerce')
            mask_fore = (df_f['Fecha_dt'].dt.date >= hoy.date()) & (df_f['Fecha_dt'].dt.date <= (hoy + timedelta(days=2)).date())
            df_fore_plot = df_f[mask_fore]
            fig_main.add_trace(go.Scatter(x=df_fore_plot['Fecha_dt'], y=df_fore_plot['yhat'], name='IA Forecast', line=dict(color='#FFA500', width=3, dash='dash')))

        fig_main.update_layout(template="plotly_dark", height=400, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_main, use_container_width=True)

    # Gráfico BCG
    with c_right:
        st.subheader("🧩 Matriz BCG")
        if not df_v.empty and not df_c.empty:
            df_vol = df_v.groupby('Producto_ID')['Cantidad'].sum().reset_index()
            df_bcg = pd.merge(df_vol, df_c, on='Producto_ID', how='inner')
            if not df_bcg.empty:
                fig_bcg = px.scatter(df_bcg, x="Cantidad", y="Margen_Pct", size="Precio_num", color="Menu", hover_name="Menu")
                fig_bcg.add_hline(y=60, line_dash="dot", annotation_text="Meta Margen")
                fig_bcg.update_layout(template="plotly_dark", height=400, paper_bgcolor='rgba(0,0,0,0)', showlegend=False)
                st.plotly_chart(fig_bcg, use_container_width=True)
            else: st.info("Sin coincidencias ID.")
        else: st.warning("Faltan datos.")
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
            st.info("No hay registros de Merma en el Inventario")

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
    st.header("🔮 Soberanía Financiera & Estructura")

    # --- 1. INICIALIZACIÓN DE VARIABLES (Valores por defecto 0.00) ---
    orden = "ESPERANDO DATOS... (Ejecuta Colab)"
    runway_val = 0.0
    burn_rate = 0.0
    deuda_tc = 0.0
    pe_diario = 0.0
    pe_mensual = 0.0
    margen_contrib = 0.0
    
    df_sob = DATA['soberania']
    
    # --- 2. SI HAY DATOS, SOBRESCRIBIMOS LOS CEROS ---
    if not df_sob.empty:
        ultimo = df_sob.iloc[-1]
        
        orden = ultimo.get('ORDEN_TESORERIA', 'SIN DATOS')
        runway_val = ultimo.get('Runway_Dias', 0)
        burn_rate = ultimo.get('Burn_Rate_Diario', 0)
        deuda_tc = ultimo.get('Deuda_TC_Auditada', 0)
        
        # Cálculo de Equilibrio
        try:
            ratio = float(str(ultimo.get('Ratio_Costo_Real', '0.6')).replace('%',''))
            if ratio > 1: ratio /= 100
        except: ratio = 0.60
        margen_contrib = 1 - ratio
        
        pe_diario = burn_rate / margen_contrib if margen_contrib > 0 else 0
        pe_mensual = pe_diario * 30
    else:
        # Aviso pequeño, no intrusivo
        st.caption("⚠️ Modo Visualización: Ejecuta el script de Colab para poblar estos datos.")

    # --- 3. VISUALIZACIÓN (SE MUESTRA SIEMPRE) ---
    
    # A. BLOQUE DE ORDEN
    st.markdown(f"### 📢 ORDEN DEL DÍA")
    if "ALERTA" in str(orden):
        st.markdown(f'<div class="critical-box">🚨 {orden}</div>', unsafe_allow_html=True)
    elif "CRECIMIENTO" in str(orden):
        st.markdown(f'<div class="success-box">🚀 {orden}</div>', unsafe_allow_html=True)
    elif "ESPERANDO" in str(orden):
         st.info(f"⏳ {orden}")
    else:
        st.info(f"🛡️ {orden}")
        
    st.markdown("---")

    # B. BLOQUE DE ESTRUCTURA DE COSTOS
    st.subheader("⚖️ Estructura de Costos (Break-even Analysis)")
    c_pe1, c_pe2, c_pe3 = st.columns(3)
    
    with c_pe1:
        st.metric("COSTO FIJO DIARIO", f"S/ {burn_rate:,.2f}", "Burn Rate Operativo")
    with c_pe2:
        val_margen = margen_contrib * 100 if margen_contrib > 0 else 0
        st.metric("PE DIARIO (META)", f"S/ {pe_diario:,.2f}", f"Margen Real: {val_margen:.1f}%")
    with c_pe3:
        st.metric("PE MENSUAL (META)", f"S/ {pe_mensual:,.0f}", "Para no perder dinero")
        
    st.markdown("---")

    # C. BLOQUE DE RUNWAY & DEUDA
    st.subheader("✈️ Evolución de Supervivencia")
    
    # Lógica del Gráfico (Si está vacío, crea uno ficticio plano)
    if not df_sob.empty:
        fig_run = px.line(df_sob, x='Fecha_dt', y='Runway_Dias', markers=True)
    else:
        # Gráfico vacío placeholder
        dummy_data = pd.DataFrame({'Fecha_dt': [hoy], 'Runway_Dias': [0]})
        fig_run = px.line(dummy_data, x='Fecha_dt', y='Runway_Dias')
        fig_run.update_layout(yaxis_range=[0, 60]) # Rango fijo para que se vea bien

    fig_run.add_hline(y=45, line_dash="dot", line_color="green", annotation_text="Objetivo (45)")
    fig_run.add_hline(y=30, line_dash="dot", line_color="red", annotation_text="Peligro (30)")
    fig_run.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_run, use_container_width=True)

    st.metric("DEUDA PASIVA (TC)", f"S/ {deuda_tc:,.2f}", "Deuda Corriente a Pagar")

    # D. DEUDAS & CAPEX (Se mantiene igual)
    st.markdown("---")
    c_prov, c_cap = st.columns(2)
    with c_prov:
        st.subheader("📉 Deudas Proveedores")
        df_d = DATA['deuda']
        if not df_d.empty:
            st.dataframe(df_d[['Fecha_Vencimiento', 'Concepto', 'Saldo']], use_container_width=True)
        else: st.info("✅ Sin deudas registradas.")
        
    with c_cap:
        st.subheader("🏗️ CAPEX")
        df_cap = DATA['capex']
        if not df_cap.empty:
            df_cap['Avance'] = (df_cap['Monto_Acumulado_Actual'] / df_cap['Monto_Total'])
            st.dataframe(df_cap, column_config={"Avance": st.column_config.ProgressColumn("Progreso", format="%.0f%%")}, use_container_width=True)
        else: st.info("🔨 Sin proyectos activos.")

# ==============================================================================
# PESTAÑA 4: MENU ENGINEERING                      
# ==============================================================================
elif menu == "4. MENU ENGINEERING":
    st.header("🚀 Marketing Science (En Vivo)")

    try:
        # --- MÉTODO "BYPASS": CONEXIÓN DIRECTA CSV ---
        # Pega aquí el enlace que copiaste de "Publicar en la web"
        url_csv = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRIJmWfryiBKTZYd3_mkOCr3Nm4AEMSMu2gD77ro_R9bnyMpL_7c-iRsogkMuCBXQ_ImIE8u1Nja2PN/pub?gid=0&single=true&output=csv"
        
        # Leemos directo con Pandas (Sin pedir permiso al robot)
        df_menu_eng = pd.read_csv(url_csv)
        
        # --- PROCESAMIENTO ---
        if df_menu_eng.empty:
            st.warning("⚠️ La hoja está vacía.")
            st.stop()
            
        # Limpieza de datos
        cols_num = ['Margen', 'Mix_Percent', 'Total_Venta', 'Precio_num', 'Foto_Calidad']
        for col in cols_num:
            if col in df_menu_eng.columns:
                df_menu_eng[col] = pd.to_numeric(df_menu_eng[col], errors='coerce').fillna(0)

        # --- GRÁFICOS (Igual que antes) ---
        st.subheader("🎯 Matriz de Ingeniería de Menú")
        
        fig_matrix = px.scatter(
            df_menu_eng,
            x="Mix_Percent",
            y="Margen",
            color="Clasificacion",
            size="Total_Venta", 
            hover_name="Menu",
            color_discrete_map={
                "⭐ ESTRELLA": "#00FF00", "🐎 CABALLO BATALLA": "#FFFF00", 
                "🧩 PUZZLE": "#00FFFF", "🐶 PERRO": "#FF0000"   
            },
            title="Mapa de Rentabilidad vs Popularidad"
        )
        
        avg_mix = df_menu_eng['Mix_Percent'].mean()
        avg_margen = df_menu_eng['Margen'].mean()
        fig_matrix.add_hline(y=avg_margen, line_dash="dot", line_color="white", annotation_text="Margen Promedio")
        fig_matrix.add_vline(x=avg_mix, line_dash="dot", line_color="white", annotation_text="Popularidad Promedio")
        
        fig_matrix.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', height=550)
        st.plotly_chart(fig_matrix, use_container_width=True)

        st.markdown("### ⚡ Plan de Acción")
        st.dataframe(df_menu_eng[['Menu', 'Clasificacion', 'Accion_Sugerida', 'Precio_num']], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"❌ Error leyendo CSV: {e}")
# ==============================================================================
# PESTAÑA 5: CX & TIEMPOS
# ==============================================================================
elif menu == "5. CX & TIEMPOS":
    st.header("⏱️ Speed of Service (SOS) & Calidad")
    st.info("Objetivo: Entregar en menos de 15 minutos. (Muestreo Aleatorio)")

    # 1. ENLACE CONVERTIDO A CSV (El truco automático)
    # Tu enlace original era HTML, aquí lo forzamos a CSV
    url_cx = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRIJmWfryiBKTZYd3_mkOCr3Nm4AEMSMu2gD77ro_R9bnyMpL_7c-iRsogkMuCBXQ_ImIE8u1Nja2PN/pub?gid=1382289241&single=true&output=csv"

    try:
        # Cargar datos
        df_cx = pd.read_csv(url_cx)

        # Validación de Seguridad: Si está vacío
        if df_cx.empty:
            st.warning("⚠️ La base de datos de CX está vacía. Registra algunos tickets primero.")
            st.stop()

        # 2. PROCESAMIENTO DE TIEMPOS (La Matemática)
        # Convertimos las columnas de texto a Objetos de Tiempo reales
        # Asumimos formato dd/mm/yyyy para la fecha
        df_cx['Fecha_dt'] = pd.to_datetime(df_cx['Fecha'], dayfirst=True, errors='coerce')
        
        # Función auxiliar para combinar Fecha + Hora y crear un Timestamp completo
        def combinar_fecha_hora(row, col_hora):
            try:
                # Une "26/01/2026" con "13:00" y lo convierte a fecha-hora
                return pd.to_datetime(f"{row['Fecha']} {row[col_hora]}", dayfirst=True)
            except:
                return pd.NaT

        df_cx['Inicio_Real'] = df_cx.apply(lambda x: combinar_fecha_hora(x, 'Hora_Pedido'), axis=1)
        df_cx['Fin_Real'] = df_cx.apply(lambda x: combinar_fecha_hora(x, 'Hora_Entrega'), axis=1)

        # Cálculo de Minutos (La Resta)
        df_cx['Minutos_Espera'] = (df_cx['Fin_Real'] - df_cx['Inicio_Real']).dt.total_seconds() / 60

        # Eliminar errores (filas donde no se pudo calcular)
        df_cx = df_cx.dropna(subset=['Minutos_Espera'])

        # 3. SEMÁFORO DE VELOCIDAD (KPIs)
        # Definimos tus estándares: <15 min (Rápido), 15-25 (Normal), >25 (Lento)
        def clasificar_velocidad(minutos):
            if minutos <= 15: return "🟢 RÁPIDO"
            elif minutos <= 25: return "🟡 NORMAL"
            else: return "🔴 LENTO"

        df_cx['Status'] = df_cx['Minutos_Espera'].apply(clasificar_velocidad)

        # --- DASHBOARD VISUAL ---

        # FILA 1: KPIs MACRO
        promedio_min = df_cx['Minutos_Espera'].mean()
        pct_lentos = (len(df_cx[df_cx['Status'] == "🔴 LENTO"]) / len(df_cx)) * 100
        total_muestras = len(df_cx)

        kpi1, kpi2, kpi3 = st.columns(3)
        
        kpi1.metric(
            "Tiempo Promedio", 
            f"{promedio_min:.1f} min", 
            delta="-2 min vs Objetivo" if promedio_min < 17 else f"+{promedio_min-15:.1f} min demora",
            delta_color="inverse" # Si es bajo es verde (bueno)
        )
        
        kpi2.metric(
            "% Pedidos Lentos (>25m)", 
            f"{pct_lentos:.1f}%",
            "Meta: < 5%",
            delta_color="inverse"
        )
        
        kpi3.metric("Muestras Auditadas", f"{total_muestras} Tickets")

        st.markdown("---")

        # FILA 2: GRÁFICOS
        col_graf1, col_graf2 = st.columns(2)

        with col_graf1:
            st.subheader("📊 Distribución de Tiempos")
            # Histograma: ¿Dónde se concentra la mayoría de tus pedidos?
            fig_hist = px.histogram(
                df_cx, 
                x="Minutos_Espera", 
                nbins=10, 
                color="Status",
                color_discrete_map={"🟢 RÁPIDO": "green", "🟡 NORMAL": "yellow", "🔴 LENTO": "red"},
                title="Curva de Velocidad en Cocina"
            )
            # Línea de meta (15 min)
            fig_hist.add_vline(x=15, line_dash="dot", line_color="white", annotation_text="Meta (15m)")
            st.plotly_chart(fig_hist, use_container_width=True)

        with col_graf2:
            st.subheader("🚨 Incidencias Reportadas")
            # Contar incidencias que NO sean "Ninguna" o "Todo OK"
            df_incidencias = df_cx[~df_cx['Incidencia'].isin(['Ninguna', 'Todo OK', 'OK', '-'])]
            
            if not df_incidencias.empty:
                # Gráfico de Pastel de problemas
                fig_pie = px.pie(
                    df_incidencias, 
                    names='Incidencia', 
                    title='Causas de Quejas / Demoras',
                    hole=0.4
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.success("🎉 ¡Increíble! No hay incidencias negativas registradas en la muestra.")

        # FILA 3: TABLA DETALLADA (Para ver quién falló)
        st.subheader("🕵️ Auditoría de Tickets (Últimos 10)")
        st.dataframe(
            df_cx[['ID_Ticket', 'Fecha', 'Hora_Pedido', 'Hora_Entrega', 'Minutos_Espera', 'Status', 'Incidencia']]
            .sort_values(by='Fecha_dt', ascending=False)
            .head(10),
            use_container_width=True,
            hide_index=True
        )

    except Exception as e:
        st.error("❌ Error procesando datos de CX.")
        st.write(f"Detalle técnico: {e}")
        st.info("Consejo: Revisa que en el Excel las horas estén formato '13:00' (24h) y las fechas 'dd/mm/yyyy'.")

# ==============================================================================
# PESTAÑA 6: GROWTH & LEALTAD
# ==============================================================================
elif menu == "6. GROWTH & LEALTAD":
    st.header("💎 CRM & Lealtad (Yape Mining)")
    st.info("Estrategia: Análisis financiero de flujos digitales (Yape/Plin).")

    # ---------------------------------------------------------
    # 1. CONFIGURACIÓN Y CONEXIÓN
    # ---------------------------------------------------------
    # 👇 PEGA AQUÍ EL ENLACE CSV DE TU PESTAÑA 'Data_Clientes_Yape'
    url_yape = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRIJmWfryiBKTZYd3_mkOCr3Nm4AEMSMu2gD77ro_R9bnyMpL_7c-iRsogkMuCBXQ_ImIE8u1Nja2PN/pubhtml?gid=1959458691&single=true" 
    
    # Intentamos calcular el Ticket Promedio Global usando tu data de Ventas (si existe)
    # Esto define la "Vara de Medir" para saber quién es VIP
    try:
        # Buscamos df_ventas en el entorno (debe cargarse al inicio de la app)
        if 'df_ventas' in locals() and not df_ventas.empty and 'ID_Ticket' in df_ventas.columns:
            # Agrupamos por Ticket para obtener el valor real por mesa
            df_tickets_ref = df_ventas.groupby('ID_Ticket')['Monto'].sum()
            ticket_promedio_global = df_tickets_ref.mean()
        else:
            ticket_promedio_global = 20.0 # Valor por defecto de seguridad
    except:
        ticket_promedio_global = 20.0

    c_kpi1, c_kpi2 = st.columns(2)
    c_kpi1.metric("Ticket Promedio Global (Base)", f"S/ {ticket_promedio_global:.2f}", help="Se usa para definir los umbrales VIP")

    # ---------------------------------------------------------
    # 2. CARGA Y LIMPIEZA DE DATOS (MODO MANUAL)
    # ---------------------------------------------------------
    try:
        df_yape = pd.read_csv(url_yape)
        
        # Copia de seguridad
        df_ingresos = df_yape.copy()

        # Estandarización de nombres de columnas (Por si en Sheets usas espacios o guiones bajos)
        # El código busca las versiones con guión bajo, si no las halla, intenta renombrar
        mapa_cols = {
            'Fecha de operación': 'Fecha_Operacion',
            'Fecha de Operación': 'Fecha_Operacion',
            'fecha': 'Fecha_Operacion',
            'monto': 'Monto',
            'origen': 'Origen'
        }
        df_ingresos.rename(columns=mapa_cols, inplace=True)

        # Validación de Seguridad
        cols_requeridas = ['Origen', 'Monto', 'Fecha_Operacion']
        if not all(col in df_ingresos.columns for col in cols_requeridas):
            st.error(f"❌ Error de Formato: Faltan columnas. Tu archivo debe tener: {cols_requeridas}")
            st.write("Columnas detectadas:", df_ingresos.columns.tolist())
            st.stop()

        # Conversión de Tipos
        # "coerce" transformará errores en NaT (Not a Time) para no romper el código
        df_ingresos['Fecha_dt'] = pd.to_datetime(df_ingresos['Fecha_Operacion'], dayfirst=True, errors='coerce')
        
        # Eliminar filas con fechas inválidas (basura)
        df_ingresos = df_ingresos.dropna(subset=['Fecha_dt'])
        
        # Crear columna Mes-Año para la tabla visual (Ej: 2026-01)
        df_ingresos['Mes_Año'] = df_ingresos['Fecha_dt'].dt.strftime('%Y-%m') 

        # Limpieza de Nombres de Clientes
        def limpiar_nombre(nombre):
            if not isinstance(nombre, str): return "Desconocido"
            nombre = nombre.upper().strip()
            # Lista negra de prefijos bancarios
            prefijos = ["PLIN - ", "YAPE - ", "TRANSFERENCIA - ", "IZIPAY - ", "INTERBANK - ", "BCP - "]
            for p in prefijos:
                nombre = nombre.replace(p, "")
            return nombre

        df_ingresos['Cliente_Limpio'] = df_ingresos['Origen'].apply(limpiar_nombre)

        # ---------------------------------------------------------
        # 3. LÓGICA DE NEGOCIO: SEGMENTACIÓN
        # ---------------------------------------------------------
        fecha_hoy = pd.to_datetime("today")
        
        # Agrupamos por Cliente (Histórico Total)
        df_clientes = df_ingresos.groupby('Cliente_Limpio').agg(
            Total_Historico=('Monto', 'sum'),
            Visitas_Totales=('Fecha_dt', 'count'),
            Ultima_Visita=('Fecha_dt', 'max'),
            Ticket_Maximo=('Monto', 'max') # Clave para detectar Ballenas de 1 noche
        ).reset_index()

        df_clientes['Dias_Ausente'] = (fecha_hoy - df_clientes['Ultima_Visita']).dt.days

        # Definición de Umbrales Dinámicos
        umbral_vip = ticket_promedio_global * 4        # 4x (Ej: S/ 80)
        umbral_recurrente = ticket_promedio_global * 1.5 # 1.5x (Ej: S/ 30)

        def segmentar_cliente(row):
            total = row['Total_Historico']
            visitas = row['Visitas_Totales']
            dias_off = row['Dias_Ausente']
            tk_max = row['Ticket_Maximo']
            
            estado = ""
            
            # --- CLASIFICACIÓN FINANCIERA ---
            # 1. BALLENA (Gastó mucho en 1 sola visita)
            if tk_max >= umbral_vip and visitas == 1:
                estado = "🐋 BALLENA (1 Visita)"
            
            # 2. VIP SOCIO (Gastó mucho acumulado y vino varias veces)
            elif total >= umbral_vip:
                estado = "💎 VIP (Socio)"
            
            # 3. RECURRENTE (Gasto medio)
            elif total >= umbral_recurrente:
                estado = "🔥 RECURRENTE"
            
            # 4. CASUAL (Gasto bajo)
            else:
                estado = "🌱 CASUAL"
            
            # --- ESTADO DE RETENCIÓN (CHURN) ---
            if dias_off > 30:
                if "CASUAL" in estado:
                    estado = "💤 PERDIDO"
                else:
                    # Mantiene su rango anterior pero con etiqueta de Dormido
                    estado = f"💤 DORMIDO ({estado.split('(')[0].strip()})" 
            
            return estado

        df_clientes['Segmento'] = df_clientes.apply(segmentar_cliente, axis=1)

        # KPI de Cartera
        total_vip = len(df_clientes[df_clientes['Segmento'].str.contains("VIP")])
        c_kpi2.metric("Socios VIP Activos", total_vip, "Clientes fidelizados de alto valor")

        # ---------------------------------------------------------
        # 4. TABLERO DE COHORTES (Vista Mensual)
        # ---------------------------------------------------------
        # Filtramos los últimos 6 meses con datos para la tabla
        meses_disponibles = sorted(df_ingresos['Mes_Año'].unique())[-6:] 
        
        if not meses_disponibles:
            st.warning("No hay suficientes meses de datos para generar el reporte histórico.")
        else:
            # Pivot Table: Clientes en filas, Meses en columnas
            df_recent = df_ingresos[df_ingresos['Mes_Año'].isin(meses_disponibles)]
            pivot_meses = df_recent.pivot_table(
                index='Cliente_Limpio', columns='Mes_Año', values='Monto', aggfunc='sum', fill_value=0
            ).reset_index()

            # Unir métricas con tabla mensual
            df_final = pd.merge(df_clientes, pivot_meses, on='Cliente_Limpio', how='left').fillna(0)
            
            # Ordenar: Los que más han gastado en la historia van primero
            df_final = df_final.sort_values(by='Total_Historico', ascending=False)

            # --- VISUALIZACIÓN ---
            st.divider()
            
            # Filtros Interactivos
            col_search, col_filtro = st.columns([2, 1])
            with col_search:
                busqueda = st.text_input("🔍 Buscar Cliente:", placeholder="Nombre...")
            with col_filtro:
                opciones_seg = ["TODOS"] + sorted(df_final['Segmento'].unique().tolist())
                filtro_seg = st.selectbox("Filtrar Segmento:", opciones_seg)

            # Aplicar Lógica de Filtrado
            df_display = df_final.copy()
            
            if busqueda:
                df_display = df_display[df_display['Cliente_Limpio'].str.contains(busqueda.upper())]
            
            if filtro_seg != "TODOS":
                df_display = df_display[df_display['Segmento'] == filtro_seg]

            # Límite de filas (Top 50) para no saturar, salvo que se use búsqueda
            if not busqueda and filtro_seg == "TODOS":
                st.caption(f"Mostrando Top 50 de {len(df_final)} clientes. Usa el buscador para ver más.")
                df_display = df_display.head(50)

            # Definir columnas a mostrar
            cols_fijas = ['Cliente_Limpio', 'Segmento', 'Total_Historico', 'Visitas_Totales', 'Dias_Ausente']
            # Solo mostramos los meses que existen en la data
            cols_meses_reales = [c for c in df_display.columns if c in meses_disponibles]
            cols_totales = cols_fijas + cols_meses_reales

            # Renderizar Tabla
            st.dataframe(
                df_display[cols_totales],
                column_config={
                    "Total_Historico": st.column_config.NumberColumn("Total Hist.", format="S/ %.2f"),
                    "Cliente_Limpio": "Cliente",
                    "Dias_Ausente": st.column_config.NumberColumn("Ausencia", format="%d días"),
                    "Visitas_Totales": st.column_config.NumberColumn("Visitas", format="%d"),
                },
                use_container_width=True,
                hide_index=True
            )

            # ---------------------------------------------------------
            # 5. ALERTAS DE ACCIÓN (La parte más importante)
            # ---------------------------------------------------------
            st.divider()
            c_alert1, c_alert2 = st.columns(2)
            
            with c_alert1:
                # Alerta 1: VIPs que se están yendo
                vips_riesgo = df_final[df_final['Segmento'].str.contains("DORMIDO") & df_final['Segmento'].str.contains("VIP")]
                if not vips_riesgo.empty:
                    st.error(f"🚨 **ALERTA ROJA: {len(vips_riesgo)} VIPs en Riesgo**")
                    st.caption("Eran tus mejores clientes y hace >30 días no vienen.")
                    st.dataframe(vips_riesgo[['Cliente_Limpio', 'Total_Historico', 'Ultima_Visita']], hide_index=True)
                else:
                    st.success("✅ Tus VIPs están sanos y activos.")

            with c_alert2:
                # Alerta 2: Ballenas de 1 noche (Retargeting)
                ballenas = df_final[df_final['Segmento'].str.contains("BALLENA")]
                if not ballenas.empty:
                    st.info(f"🎣 **OPORTUNIDAD: {len(ballenas)} Ballenas Detectadas**")
                    st.caption("Gastaron mucho en 1 sola visita. ¡Invítales algo para que vuelvan!")
                    st.dataframe(ballenas[['Cliente_Limpio', 'Total_Historico', 'Fecha_Operacion' if 'Fecha_Operacion' in ballenas.columns else 'Ultima_Visita']], hide_index=True)
                else:
                    st.info("No hay 'Ballenas de una noche' recientes.")

    except Exception as e:
        st.error(f"❌ Error leyendo datos: {e}")
        st.warning("Verifica que tu pestaña tenga exactamente: 'Origen', 'Monto', 'Fecha_Operacion'")

# ==============================================================================
# PESTAÑA 7: GESTIÓN DE MARCA
# ==============================================================================
elif menu == "4. GESTIÓN DE MARCA":
    st.header("📢 Gestión de Marca & Eficiencia (MER)")
    st.info("Objetivo: Abrir la 'Mandíbula de Cocodrilo'. Gasto estable, Ventas crecientes.")

    # 1. CARGA DE DATOS DE MARKETING
    url_marketing = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvbSRzYorHvUzcXl_GutWeXA6KI6XH8et1qPK6Z8TQhQiTQbgvubOmqZO3bEbWMifqdP7xcUoWwhjr/pubhtml?gid=1643539601&single=true" 
    
    # Intentamos cargar Ventas (Necesario para el cruce)
    try:
        # Buscamos df_ventas (Debe estar cargado globalmente)
        if 'df_ventas' not in locals() or df_ventas.empty:
            st.warning("⚠️ No se detectaron Ventas cargadas. El módulo necesita datos de Ventas para calcular el MER.")
            st.stop()
            
        df_mkt = pd.read_csv(url_marketing)
        
        # Limpieza básica
        df_mkt['Fecha_Cierre'] = pd.to_datetime(df_mkt['Fecha_Cierre'], dayfirst=True, errors='coerce')
        df_mkt = df_mkt.sort_values(by='Fecha_Cierre') # Ordenar cronológicamente

        # 2. PROCESAMIENTO: CRUCE DE VENTAS SEMANAL
        # Para cada fila de marketing (Domingo), sumamos las ventas de esa semana (Lun-Dom)
        
        reporte_final = []
        
        for index, row in df_mkt.iterrows():
            fecha_fin = row['Fecha_Cierre']
            fecha_ini = fecha_fin - pd.Timedelta(days=6) # Lunes previo
            
            # Filtramos ventas en ese rango
            mask_ventas = (df_ventas['Fecha_dt'] >= fecha_ini) & (df_ventas['Fecha_dt'] <= fecha_fin)
            venta_semanal = df_ventas.loc[mask_ventas, 'Total_Venta'].sum() # Asegúrate que tu col ventas se llame 'Total_Venta' o 'Monto'
            
            # Si usas 'Monto' en ventas, cambia arriba. Aquí asumo 'Monto' por consistencia con fases previas
            if 'Total_Venta' not in df_ventas.columns and 'Monto' in df_ventas.columns:
                 venta_semanal = df_ventas.loc[mask_ventas, 'Monto'].sum()

            # Cálculo de MER (Marketing Efficiency Ratio)
            gasto = row['Gasto_Ads']
            mer = venta_semanal / gasto if gasto > 0 else 0
            
            # Guardamos todo
            fila_procesada = {
                'Semana': fecha_fin.strftime("%d-%b"),
                'Fecha_Full': fecha_fin,
                'Gasto_Ads': gasto,
                'Ventas_Reales': venta_semanal,
                'MER': mer,
                'Reviews': row['Google_Reviews'],
                'Stars': row['Google_Stars']
            }
            reporte_final.append(fila_procesada)
            
        df_final = pd.DataFrame(reporte_final)

        # 3. CÁLCULOS DE TENDENCIA (Deltas)
        df_final['Nuevas_Reviews'] = df_final['Reviews'].diff().fillna(0)
        df_final['Costo_Por_Review'] = df_final['Gasto_Ads'] / df_final['Nuevas_Reviews']
        # Limpiamos infinitos si reviews es 0
        df_final['Costo_Por_Review'] = df_final['Costo_Por_Review'].replace([np.inf, -np.inf], 0).fillna(0)

        # 4. DASHBOARD VISUAL
        
        # --- KPIs SEMANA ACTUAL (Última fila) ---
        if not df_final.empty:
                actual = df_final.iloc[-1]
                anterior = df_final.iloc[-2] if len(df_final) > 1 else actual
                
                k1, k2, k3 = st.columns(3)
                
                # MER (Eficiencia)
                delta_mer = actual['MER'] - anterior['MER']
                k1.metric("MER Semanal (Eficiencia)", f"{actual['MER']:.1f}x", f"{delta_mer:.1f} vs sem ant")
                
                # Gasto vs Ventas
                k2.metric("Gasto Ads", f"S/ {actual['Gasto_Ads']}", f"Gen: S/ {actual['Ventas_Reales']:.0f}")
                
                # CORRECCIÓN AQUÍ ABAJO (Quité el espacio)
                delta_rev = actual['Reviews'] - anterior['Reviews']
                k3.metric("Google Stars", f"{actual['Stars']} ⭐", f"+{int(delta_rev)} Reviews nuevas")
                
                st.markdown("---")
            
            # --- GRÁFICO: LA MANDÍBULA DE COCODRILO ---
            st.subheader("🐊 La Mandíbula de Cocodrilo (Ads vs Ventas)")
            
            # Usamos Plotly para doble eje interactivo
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            # Barras: Ventas
            fig.add_trace(
                go.Bar(x=df_final['Semana'], y=df_final['Ventas_Reales'], name="Ventas (S/)", marker_color='#00CC96', opacity=0.6),
                secondary_y=False
            )

            # Línea: Gasto Ads
            fig.add_trace(
                go.Scatter(x=df_final['Semana'], y=df_final['Gasto_Ads'], name="Gasto Ads (S/)", mode='lines+markers', line=dict(color='#EF553B', width=3)),
                secondary_y=True
            )

            # Configuración Ejes
            fig.update_layout(title_text="Correlación: ¿Tu Gasto impulsa las Ventas?", showlegend=True)
            fig.update_yaxes(title_text="Ventas Totales", secondary_y=False)
            fig.update_yaxes(title_text="Inversión Ads", secondary_y=True)

            st.plotly_chart(fig, use_container_width=True)
            
            # --- TABLA DE DETALLE ---
            with st.expander("Ver Bitácora Semanal Completa"):
                st.dataframe(df_final.sort_values(by='Fecha_Full', ascending=False), use_container_width=True)

        else:
            st.info("Registra tu primera semana en el Excel para ver la magia.")

    except Exception as e:
        st.error(f"❌ Error procesando Marca: {e}")
        st.write("Asegúrate de que 'BD_Marketing_Semanal' tenga fechas válidas y que 'BD_Ventas' esté cargada.")
