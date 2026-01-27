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
        ["1. CORPORATE OVERVIEW", "2. EFICIENCIA & COSTOS", "3. FINANZAS & RUNWAY", "4. MARKETING & GROWTH"])
    
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
# PESTAÑA 4: MARKETING & GROWTH (INGENIERÍA DE MENÚ)
# ==============================================================================
elif menu == "4. MARKETING & GROWTH":
    st.header("🚀 Marketing Science (En Vivo)")

    try:
        # --- MÉTODO "BYPASS": CONEXIÓN DIRECTA CSV ---
        # Pega aquí el enlace que copiaste de "Publicar en la web"
        url_csv = "PEGA_TU_ENLACE_LARGO_AQUI_DENTRO"
        
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
