import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import numpy as np
import time

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

try:
    IDS = st.secrets["sheets"]
except Exception as e:
    st.error("❌ Error Fatal: No se encontraron los IDs de las hojas en secrets.toml. Configura [sheets].")
    st.stop()

# ==============================================================================
# 3. MOTOR DE EXTRACCIÓN Y LIMPIEZA DE DATOS (ETL SEGURO)
# ==============================================================================
@st.cache_data(ttl=600)
def load_all_data():
    client = connect_google_sheets()
    DB = {}
    
    # 1. Función de Lectura Segura (Anti-Bloqueos de Google)
    def safe_read(file_key, sheet_name):
        time.sleep(1.5) # Pausa técnica para evitar error 429
        try:
            sheet_id = IDS.get(file_key)
            if not sheet_id: return pd.DataFrame()
            
            sh = client.open_by_key(sheet_id)
            ws = sh.worksheet(sheet_name)
            
            # Leemos TODO como texto para evitar errores de cabecera
            raw_data = ws.get_all_values()
            
            if not raw_data: return pd.DataFrame()

            # Convertimos a DataFrame usando la primera fila como cabecera
            headers = raw_data[0]
            rows = raw_data[1:]
            
            # Si hay filas vacías al final, las limpiamos
            if not rows: return pd.DataFrame(columns=headers)
            
            df = pd.DataFrame(rows, columns=headers)
            return df
            
        except Exception as e:
            print(f"⚠️ Error recuperable en '{sheet_name}': {e}")
            return pd.DataFrame()

    # 2. Carga Masiva (El Robot Trabajando)
    with st.spinner('Procesando datos del negocio...'):
        DB['ventas'] = safe_read("REGISTROS", "BD_Ventas")
        DB['feriados'] = safe_read("REGISTROS", "MASTER_FERIADOS")
        DB['partidos'] = safe_read("REGISTROS", "MASTER_PARTIDOS")
        DB['costos'] = safe_read("COSTOS", "OUT_Costos_Productos")
        DB['qc'] = safe_read("COSTOS", "OUT_QC_Compras_NoConvertibles")
        DB['merma'] = safe_read("INVENTARIO", "OUT_Merma_Valorizada")
        DB['caja'] = safe_read("CAJA", "BD_Caja_Diaria")
        DB['capex'] = safe_read("CAJA", "PARAM_PROYECTOS_CAPEX")
        
        # ### NUEVO: Agregamos la lectura de Costos Fijos Dinámicos ###
        DB['fijos'] = safe_read("CAJA", "PARAM_COSTOS_FIJOS") 
        
        DB['forecast'] = safe_read("FORECAST", "OUT_Pronostico_Ventas")
        DB['soberania'] = safe_read("FORECAST", "OUT_Soberania_Financiera") 
        DB['deuda'] = safe_read("LIBROS", "Libro_Cuentas_Pagar")
        DB['menu_eng'] = safe_read("MKT_RESULTADOS", "OUT_Menu_Engineering")
        DB['cx_tiempos'] = safe_read("MKT_RESULTADOS", "BD_CX_Tiempos") 
        DB['yape'] = safe_read("MKT_RESULTADOS", "Data_Clientes_Yape")
        DB['mkt_semanal'] = safe_read("MKT_REGISTROS", "BD_Marketing_Semanal")
        DB['diaria'] = safe_read("REGISTROS", "Data_Diaria")

    # 3. LIMPIEZA DE FECHAS (Universal)
    for key in DB:
        if not DB[key].empty:
            date_cols = ['Fecha', 'Fecha_dt', 'ds', 'Marca temporal', 'Fecha_Vencimiento', 
                         'Fecha_Operacion', 'Fecha_Cierre', 'fecha', 'Date']
            for col_name in date_cols:
                if col_name in DB[key].columns:
                    col_data = DB[key][col_name].astype(str)
                    DB[key]['Fecha_dt'] = pd.to_datetime(col_data, dayfirst=True, errors='coerce')
                    if DB[key]['Fecha_dt'].isna().mean() > 0.8:
                        DB[key]['Fecha_dt'] = pd.to_datetime(col_data, format='mixed', errors='coerce')
                    DB[key] = DB[key].dropna(subset=['Fecha_dt'])
                    break

    # 4. LIMPIEZA NUMÉRICA INTEGRAL
    def clean_currency(x):
        if not isinstance(x, str): return x
        clean_str = x.replace('S/', '').replace(',', '').replace('%', '').strip()
        try:
            return float(clean_str)
        except:
            return 0.0

    # A. Limpieza Ventas
    if not DB['ventas'].empty:
        cols_venta = ['Total_Venta', 'Total Venta', 'total_venta', 'Monto']
        col_found = next((c for c in cols_venta if c in DB['ventas'].columns), None)
        if col_found:
            DB['ventas']['Monto'] = DB['ventas'][col_found].apply(clean_currency)
        else:
            DB['ventas']['Monto'] = 0.0
        if 'Cantidad' in DB['ventas'].columns:
             DB['ventas']['Cantidad'] = pd.to_numeric(DB['ventas']['Cantidad'], errors='coerce').fillna(0)

    # B. Limpieza Costos
    if not DB['costos'].empty:
        cols_margen = ['Margen_%', 'Margen %', 'Margen_Pct', 'Margen', 'margen']
        col_found = next((c for c in cols_margen if c in DB['costos'].columns), None)
        if col_found:
            DB['costos']['Margen_Pct'] = DB['costos'][col_found].apply(clean_currency)
        else:
            DB['costos']['Margen_Pct'] = 0.0
        cols_precio = ['Precio_num', 'Precio', 'Precio_Venta', 'PVP', 'Precio Carta']
        col_p_found = next((c for c in cols_precio if c in DB['costos'].columns), None)
        if col_p_found:
            DB['costos']['Precio_num'] = DB['costos'][col_p_found].apply(clean_currency)
        else:
            DB['costos']['Precio_num'] = 10.0

    # C. Limpieza Merma
    if not DB['merma'].empty and 'Merma_Soles' in DB['merma'].columns:
        DB['merma']['Monto_Merma'] = DB['merma']['Merma_Soles'].apply(clean_currency)

    # D. Limpieza Menu Engineering
    if not DB['menu_eng'].empty:
        targets = ['Margen', 'Mix_Percent', 'Total_Venta', 'Precio_num']
        for t in targets:
            if t in DB['menu_eng'].columns:
                DB['menu_eng'][t] = DB['menu_eng'][t].apply(clean_currency)

    # E. Limpieza Yape
    if not DB['yape'].empty:
        mapa_cols = {'monto': 'Monto', 'origen': 'Origen', 'fecha': 'Fecha_Operacion'}
        DB['yape'].rename(columns=mapa_cols, inplace=True)
        if 'Monto' in DB['yape'].columns:
            DB['yape']['Monto'] = DB['yape']['Monto'].apply(clean_currency)

    # F. Limpieza Marketing
    if not DB['mkt_semanal'].empty and 'Gasto_Ads' in DB['mkt_semanal'].columns:
        DB['mkt_semanal']['Gasto_Ads'] = DB['mkt_semanal']['Gasto_Ads'].apply(clean_currency)

    # ### NUEVO: Limpieza de Costos Fijos para poder sumarlos ###
    if not DB['fijos'].empty and 'Monto_Mensual' in DB['fijos'].columns:
        DB['fijos']['Monto_Mensual'] = DB['fijos']['Monto_Mensual'].apply(clean_currency)

    return DB

# --- HELPER: Limpieza de Float Segura para uso en la lógica principal ---
def safe_float(val):
    if pd.isna(val) or val == "":
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    val_str = str(val).replace('S/', '').replace(',', '').replace('%', '').strip()
    try:
        return float(val_str)
    except:
        return 0.0

# ==============================================================================
# MOTOR ECONOMÉTRICO (ETL AVANZADO)
# ==============================================================================
@st.cache_data(ttl=600)
def build_econometric_master(db_data):
    """
    Construye la Tabla Maestra unificando Ventas, Contexto y Marketing.
    """
    df_v = db_data.get('ventas', pd.DataFrame()).copy()
    df_ctx = db_data.get('diaria', pd.DataFrame()).copy() 
    df_ads = db_data.get('mkt_semanal', pd.DataFrame()).copy()

    if df_v.empty: return pd.DataFrame()

    df_daily = df_v.groupby('Fecha_dt').agg({
        'Monto': 'sum',
        'Cantidad': 'sum'
    }).reset_index()
    
    df_daily['Precio_Promedio_Real'] = np.where(
        df_daily['Cantidad'] > 0, 
        df_daily['Monto'] / df_daily['Cantidad'], 
        0
    )

    ads_daily_list = []
    if not df_ads.empty:
        col_fecha_ads = 'Fecha_dt' if 'Fecha_dt' in df_ads.columns else 'Fecha_Cierre'
        
        if col_fecha_ads in df_ads.columns:
            df_ads[col_fecha_ads] = pd.to_datetime(df_ads[col_fecha_ads], dayfirst=True, errors='coerce')
            df_ads = df_ads.sort_values(col_fecha_ads)

            for idx, row in df_ads.iterrows():
                curr_date = row[col_fecha_ads]
                gasto = row.get('Gasto_Ads', 0)
                gasto_diario = gasto / 7
                for i in range(7):
                    day_date = curr_date - timedelta(days=i)
                    ads_daily_list.append({'Fecha_dt': day_date, 'Gasto_Ads_Soles': gasto_diario})
    
    if ads_daily_list:
        df_ads_daily = pd.DataFrame(ads_daily_list).groupby('Fecha_dt')['Gasto_Ads_Soles'].sum().reset_index()
    else:
        df_ads_daily = pd.DataFrame(columns=['Fecha_dt', 'Gasto_Ads_Soles'])

    df_master = pd.merge(df_daily, df_ads_daily, on='Fecha_dt', how='left')
    
    if not df_ctx.empty and 'Fecha_dt' in df_ctx.columns:
        cols_ctx = ['Fecha_dt', 'Lluvia_Intensa', 'Competencia_Agresiva', 'Dia_Huelga', 'Stockout_Cierre']
        cols_existing = [c for c in cols_ctx if c in df_ctx.columns]
        df_master = pd.merge(df_master, df_ctx[cols_existing], on='Fecha_dt', how='left')

    df_master['Gasto_Ads_Soles'] = df_master['Gasto_Ads_Soles'].fillna(0)
    df_master = df_master.sort_values('Fecha_dt')
    
    return df_master


# EJECUCIÓN DEL ETL
try:
    DATA = load_all_data()
    STATUS_CONN = "🟢 ONLINE | SECURE CONNECTION"
except Exception as e:
    STATUS_CONN = f"🔴 ERROR CRÍTICO: {e}"
    st.stop()

# ==============================================================================
# 4. LÓGICA DE INTERFAZ Y NAVEGACIÓN
# ==============================================================================
hoy = datetime.now() - timedelta(hours=5)

# --- DETECCIÓN DE DATOS FUTUROS (CORRECCIÓN CTO) ---
# Si tus datos están en 2026 pero el server está en 2025, esto lo arregla.
if not DATA['ventas'].empty:
    max_date_data = DATA['ventas']['Fecha_dt'].max()
    if max_date_data > hoy:
        hoy = max_date_data

with st.sidebar:
    st.title("🔥 BRASAS CAPITALES")
    st.caption(f"CEO Dashboard | {hoy.strftime('%d-%b-%Y')}")
    st.markdown("---")
    
    menu = st.radio("MENÚ ESTRATÉGICO", 
        ["1. CORPORATE OVERVIEW", "2. EFICIENCIA & COSTOS", "3. FINANZAS & RUNWAY", "4. MENU ENGINEERING", "5. CX & TIEMPOS", "6. GROWTH & LEALTAD", "7. GESTION DE MARCA", "8. MODELO ECONOMÉTRICO"])
    
    st.markdown("---")
    
    st.markdown("### 📅 Filtro de Tiempo")
    filtro_tiempo = st.selectbox("Ventana de Análisis", ["Mes en Curso (MTD)", "Últimos 30 Días"])
    
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
    
    kpi_venta = 0.0
    kpi_margen_avg = 0.0
    kpi_merma_total = 0.0
    kpi_runway = 0.0
    
    ticket_promedio = 0.0
    num_transacciones = 0
    pe_diario = 0.0
    venta_hoy = 0.0
    
    # 1. Ventas & Ticket Promedio
    df_v = DATA['ventas']
    if not df_v.empty:
        mask_v = (df_v['Fecha_dt'].dt.date >= start_date.date()) & (df_v['Fecha_dt'].dt.date <= hoy.date())
        df_filtrada = df_v[mask_v]
        kpi_venta = df_filtrada['Monto'].sum()
        
        venta_hoy = df_v[df_v['Fecha_dt'].dt.date == hoy.date()]['Monto'].sum()
        
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

    # 3. Datos Financieros
    df_sob = DATA['soberania']
    if not df_sob.empty:
        ultimo = df_sob.iloc[-1]
        kpi_runway = safe_float(ultimo.get('Runway_Dias', 0))
        burn = safe_float(ultimo.get('Burn_Rate_Diario', 0))
        
        # Limpieza robusta del Ratio
        raw_ratio = ultimo.get('Ratio_Costo_Real', '0.6')
        ratio_val = safe_float(raw_ratio)
        if ratio_val > 1: ratio_val /= 100 # Si viene como 60 en vez de 0.6
        if ratio_val == 0: ratio_val = 0.60 # Default de seguridad
        
        margen_contrib = 1 - ratio_val
        pe_diario = burn / margen_contrib if margen_contrib > 0 else 9999

    # --- VISUALIZACIÓN ---
    st.markdown("##### 🏁 Meta del Día (Break-even Operativo)")
    pct_meta = min(venta_hoy / pe_diario, 1.0) if pe_diario > 0 else 0
    cols_meta = st.columns([3, 1])
    with cols_meta[0]:
        st.progress(pct_meta)
    with cols_meta[1]:
        st.caption(f"**{pct_meta*100:.0f}%** (S/ {venta_hoy:,.0f} / {pe_diario:,.0f})")
    
    st.markdown("---")

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

    c_left, c_right = st.columns([2, 1])
    
    with c_left:
        st.subheader("📈 Rendimiento Comercial")
        fig_main = go.Figure()
        if not df_v.empty:
            hist_start = hoy - timedelta(days=30)
            mask_hist = df_v['Fecha_dt'].dt.date >= hist_start.date()
            df_hist = df_v[mask_hist].groupby('Fecha_dt')['Monto'].sum().reset_index()
            fig_main.add_trace(go.Bar(x=df_hist['Fecha_dt'], y=df_hist['Monto'], name='Venta Real', marker_color='#00A3E0'))
            
            fig_main.add_hline(y=pe_diario, line_dash="dot", line_color="green", annotation_text="Meta PE")

        df_f = DATA['forecast']
        if not df_f.empty and 'Venta_P50_Probable' in df_f.columns:
            df_f['yhat'] = pd.to_numeric(df_f['Venta_P50_Probable'], errors='coerce')
            mask_fore = (df_f['Fecha_dt'].dt.date >= hoy.date()) & (df_f['Fecha_dt'].dt.date <= (hoy + timedelta(days=2)).date())
            df_fore_plot = df_f[mask_fore]
            fig_main.add_trace(go.Scatter(x=df_fore_plot['Fecha_dt'], y=df_fore_plot['yhat'], name='IA Forecast', line=dict(color='#FFA500', width=3, dash='dash')))

        fig_main.update_layout(template="plotly_dark", height=400, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_main, use_container_width=True)

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

    with col_ef2:
        st.subheader("🛡️ Compras No Convertibles (Mala Calidad)")
        df_qc = DATA['qc']
        if not df_qc.empty:
            st.dataframe(df_qc, use_container_width=True)
            if 'Total_Pagado' in df_qc.columns:
                 total_bad = safe_float(df_qc['Total_Pagado'].sum())
                 st.metric("TOTAL PERDIDO EN COMPRAS MALAS", f"S/ {total_bad:,.2f}", delta="-QC FAIL", delta_color="inverse")
        else:
            st.success("✅ Excelente. No hay reportes de compras rechazadas.")

    st.markdown("---")
    
    st.subheader("⚖️ Discrepancia de Inventario (Gap Analysis)")
    if not df_m.empty and 'Stock_teorico_gr' in df_m.columns:
        df_gap = df_m.copy()
        # Aseguramos que sean números
        df_gap['Stock_teorico_gr'] = df_gap['Stock_teorico_gr'].apply(safe_float)
        df_gap['Stock_real_gr'] = df_gap['Stock_real_gr'].apply(safe_float)
        
        df_gap['Gap'] = df_gap['Stock_teorico_gr'] - df_gap['Stock_real_gr']
        df_gap = df_gap.sort_values('Gap', ascending=False).head(10)
        
        fig_gap = px.bar(df_gap, x='Insumo', y='Gap', color='Gap', title="Diferencia en Gramos (Teórico - Real)")
        fig_gap.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_gap, use_container_width=True)
    else:
        st.info("Cargando datos de comparación de stocks...")

# ==============================================================================
# PESTAÑA 3: FINANZAS & RUNWAY (ACTUALIZADO DINÁMICO)
# ==============================================================================
elif menu == "3. FINANZAS & RUNWAY":
    st.header("🔮 Soberanía Financiera & Estructura")

    # Inicialización
    orden = "ESPERANDO DATOS... (Ejecuta Colab)"
    runway_val = 0.0
    burn_operativo = 0.0
    deuda_tc = 0.0
    pe_diario = 0.0
    pe_mensual = 0.0
    margen_contrib = 0.0
    costo_fijo_mensual_real = 0.0
    
    df_sob = DATA['soberania']
    df_fijos = DATA['fijos'] # ### NUEVO: Usamos la tabla de fijos ###
    
    # 1. CÁLCULO DE COSTOS FIJOS REALES (DINÁMICO)
    # ### NUEVO: Sumamos directamente del Excel ###
    if not df_fijos.empty and 'Monto_Mensual' in df_fijos.columns:
        costo_fijo_mensual_real = df_fijos['Monto_Mensual'].sum()
    else:
        # Fallback de seguridad si falla la hoja
        costo_fijo_mensual_real = 3600.00 

    # 2. CÁLCULO DE MÉTRICAS FINANCIERAS
    if not df_sob.empty:
        ultimo = df_sob.iloc[-1]
        
        orden = ultimo.get('ORDEN_TESORERIA', 'SIN DATOS')
        kpi_runway = safe_float(ultimo.get('Runway_Dias', 0))
        burn_operativo = safe_float(ultimo.get('Burn_Rate_Diario', 0)) # Fijos + Variable (Caja)
        deuda_tc = safe_float(ultimo.get('Deuda_TC_Auditada', 0))
        
        raw_ratio = ultimo.get('Ratio_Costo_Real', '0.6')
        ratio_val = safe_float(raw_ratio)
        if ratio_val > 1: ratio_val /= 100
        if ratio_val == 0: ratio_val = 0.60
        
        margen_contrib = 1 - ratio_val
        
        # PE = Costo Fijo Puro / Margen
        if margen_contrib > 0.05:
            pe_mensual = costo_fijo_mensual_real / margen_contrib
            pe_diario = pe_mensual / 30
        else:
            pe_mensual = 0
            pe_diario = 0
    else:
        st.caption("⚠️ Modo Visualización: Ejecuta el script de Colab para poblar estos datos.")

    # VISUALIZACIÓN
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

    st.subheader("⚖️ Estructura de Costos (Break-even Analysis)")
    c_pe1, c_pe2, c_pe3 = st.columns(3)
    
    with c_pe1:
        st.metric("CASH BURN DIARIO", f"S/ {burn_operativo:,.2f}", "Necesidad de Caja (Operativo)")
    with c_pe2:
        val_margen = margen_contrib * 100
        st.metric("PE DIARIO (META)", f"S/ {pe_diario:,.2f}", f"Margen Real: {val_margen:.1f}%")
    with c_pe3:
        # ### NUEVO: Mostramos que el dato viene del cálculo real ###
        st.metric("PE MENSUAL (META)", f"S/ {pe_mensual:,.0f}", f"Base Fija Real: S/ {costo_fijo_mensual_real:,.0f}")
        
    st.markdown("---")

    st.subheader("✈️ Evolución de Supervivencia")
    
    if not df_sob.empty:
        df_sob['Runway_Dias'] = df_sob['Runway_Dias'].apply(safe_float)
        fig_run = px.line(df_sob, x='Fecha_dt', y='Runway_Dias', markers=True)
    else:
        dummy_data = pd.DataFrame({'Fecha_dt': [hoy], 'Runway_Dias': [0]})
        fig_run = px.line(dummy_data, x='Fecha_dt', y='Runway_Dias')
        fig_run.update_layout(yaxis_range=[0, 60])

    fig_run.add_hline(y=45, line_dash="dot", line_color="green", annotation_text="Objetivo (45)")
    fig_run.add_hline(y=30, line_dash="dot", line_color="red", annotation_text="Peligro (30)")
    fig_run.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_run, use_container_width=True)

    st.metric("DEUDA PASIVA (TC)", f"S/ {deuda_tc:,.2f}", "Deuda Corriente a Pagar")

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
            df_cap['Monto_Acumulado_Actual'] = df_cap['Monto_Acumulado_Actual'].apply(safe_float)
            df_cap['Monto_Total'] = df_cap['Monto_Total'].apply(safe_float)
            
            df_cap['Avance'] = (df_cap['Monto_Acumulado_Actual'] / df_cap['Monto_Total'])
            st.dataframe(df_cap, column_config={"Avance": st.column_config.ProgressColumn("Progreso", format="%.0f%%")}, use_container_width=True)
        else: st.info("🔨 Sin proyectos activos.")

# ==============================================================================
# PESTAÑA 4: MENU ENGINEERING                        
# ==============================================================================
elif menu == "4. MENU ENGINEERING":
    st.header("🚀 Marketing Science (En Vivo)")
    df_menu_eng = DATA['menu_eng']

    if df_menu_eng.empty:
        st.warning("⚠️ No hay datos de Ingeniería de Menú. Revisa la carga en 'MKT_RESULTADOS'.")
    
    else:
        st.subheader("🎯 Matriz de Ingeniería de Menú")
        
        required_cols = ['Mix_Percent', 'Margen', 'Clasificacion', 'Total_Venta', 'Menu', 'Precio_num']
        if all(col in df_menu_eng.columns for col in required_cols):
            
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
            cols_show = ['Menu', 'Clasificacion', 'Accion_Sugerida', 'Precio_num', 'Margen', 'Mix_Percent']
            cols_final = [c for c in cols_show if c in df_menu_eng.columns]
            
            st.dataframe(
                df_menu_eng[cols_final].sort_values('Margen', ascending=False), 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Precio_num": st.column_config.NumberColumn("Precio", format="S/ %.2f"),
                    "Margen": st.column_config.NumberColumn("Margen Unit.", format="S/ %.2f"),
                    "Mix_Percent": st.column_config.NumberColumn("Mix %", format="%.2f %%")
                }
            )
            
        else:
            st.error(f"❌ Error de Estructura: Faltan columnas en el reporte. Se requiere: {required_cols}")
            st.write("Columnas detectadas:", df_menu_eng.columns.tolist())

# ==============================================================================
# PESTAÑA 5: CX & TIEMPOS
# ==============================================================================
elif menu == "5. CX & TIEMPOS":
    st.header("⏱️ Speed of Service (SOS) & Calidad")
    st.info("Objetivo: Entregar en menos de 15 minutos. (Muestreo Aleatorio)")

    df_cx = DATA['cx_tiempos']

    if df_cx.empty:
        st.warning("⚠️ La base de datos de CX está vacía o no se pudo cargar.")
    else:
        try:
            # --- PROCESAMIENTO DE TIEMPOS (CORREGIDO) ---
            required_cols = ['Fecha', 'Hora_Pedido', 'Hora_Entrega', 'Incidencia', 'ID_Ticket']
            if not all(col in df_cx.columns for col in required_cols):
                st.error(f"❌ Faltan columnas en tu Excel. Necesitas: {required_cols}")
                st.stop()

            # --- CORRECCIÓN LÓGICA DE TIEMPOS ---
            # Aseguramos que Fecha sea string puro para concatenar (Formato YYYY-MM-DD)
            df_cx['Fecha_Str'] = df_cx['Fecha_dt'].dt.strftime('%Y-%m-%d')
            
            # Limpiamos las horas
            df_cx['Hora_Pedido'] = df_cx['Hora_Pedido'].astype(str).str.strip()
            df_cx['Hora_Entrega'] = df_cx['Hora_Entrega'].astype(str).str.strip()

            # Concatenación Vectorizada (Mucho más rápida y segura)
            df_cx['Inicio_Real'] = pd.to_datetime(
                df_cx['Fecha_Str'] + ' ' + df_cx['Hora_Pedido'], 
                errors='coerce'
            )
            df_cx['Fin_Real'] = pd.to_datetime(
                df_cx['Fecha_Str'] + ' ' + df_cx['Hora_Entrega'], 
                errors='coerce'
            )

            # Cálculo de Minutos
            df_cx['Minutos_Espera'] = (df_cx['Fin_Real'] - df_cx['Inicio_Real']).dt.total_seconds() / 60
            df_validos = df_cx.dropna(subset=['Minutos_Espera']).copy()
            
            if df_validos.empty:
                st.warning("⚠️ Hay datos, pero las horas no tienen el formato correcto (ej: '13:30').")
                st.stop()

            # 3. SEMÁFORO DE VELOCIDAD
            def clasificar_velocidad(minutos):
                if minutos <= 5: return "🟢 RÁPIDO"
                elif minutos <= 10: return "🟡 NORMAL"
                else: return "🔴 LENTO"

            df_validos['Status'] = df_validos['Minutos_Espera'].apply(clasificar_velocidad)

            # --- DASHBOARD VISUAL ---
            promedio_min = df_validos['Minutos_Espera'].mean()
            pct_lentos = (len(df_validos[df_validos['Status'] == "🔴 LENTO"]) / len(df_validos)) * 100
            total_muestras = len(df_validos)

            kpi1, kpi2, kpi3 = st.columns(3)
            
            kpi1.metric(
                "Tiempo Promedio", 
                f"{promedio_min:.1f} min", 
                delta="-2 min vs Objetivo" if promedio_min < 17 else f"+{promedio_min-15:.1f} min demora",
                delta_color="inverse"
            )
            
            kpi2.metric(
                "% Pedidos Lentos (>25m)", 
                f"{pct_lentos:.1f}%",
                "Meta: < 5%",
                delta_color="inverse"
            )
            
            kpi3.metric("Muestras Auditadas", f"{total_muestras} Tickets")

            st.markdown("---")

            col_graf1, col_graf2 = st.columns(2)

            with col_graf1:
                st.subheader("📊 Distribución de Tiempos")
                fig_hist = px.histogram(
                    df_validos, 
                    x="Minutos_Espera", 
                    nbins=15, 
                    color="Status",
                    color_discrete_map={"🟢 RÁPIDO": "green", "🟡 NORMAL": "yellow", "🔴 LENTO": "red"},
                    title="Curva de Velocidad en Cocina"
                )
                fig_hist.add_vline(x=15, line_dash="dot", line_color="white", annotation_text="Meta (15m)")
                st.plotly_chart(fig_hist, use_container_width=True)

            with col_graf2:
                st.subheader("🚨 Incidencias Reportadas")
                df_incidencias = df_validos[~df_validos['Incidencia'].isin(['Ninguna', 'Todo OK', 'OK', '-', 'ok', 'nan'])]
                
                if not df_incidencias.empty:
                    fig_pie = px.pie(
                        df_incidencias, 
                        names='Incidencia', 
                        title='Causas de Quejas / Demoras',
                        hole=0.4
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.success("🎉 ¡Increíble! No hay incidencias negativas registradas en la muestra.")

            st.subheader("🕵️ Auditoría de Tickets (Últimos 10)")
            
            # CORRECCIÓN DE LÓGICA: Ordenamos PRIMERO, seleccionamos columnas DESPUÉS
            df_display = df_validos.sort_values(by='Fecha_dt', ascending=False).head(10)
            
            st.dataframe(
                df_display[['ID_Ticket', 'Fecha', 'Hora_Pedido', 'Hora_Entrega', 'Minutos_Espera', 'Status', 'Incidencia']],
                use_container_width=True,
                hide_index=True
            )

        except Exception as e:
            st.error("❌ Error de lógica en CX.")
            st.write(f"Detalle: {e}")

# ==============================================================================
# PESTAÑA 6: GROWTH & LEALTAD
# ==============================================================================
elif menu == "6. GROWTH & LEALTAD":
    st.header("💎 CRM & Lealtad (Yape Mining)")
    st.info("Estrategia: Análisis financiero de flujos digitales (Yape/Plin).")

    df_yape = DATA['yape']

    ticket_promedio_global = 20.0 
    
    if not DATA['ventas'].empty:
        try:
            if 'ID_Ticket' in DATA['ventas'].columns:
                df_tickets_ref = DATA['ventas'].groupby('ID_Ticket')['Monto'].sum()
                ticket_promedio_global = df_tickets_ref.mean()
            else:
                ticket_promedio_global = DATA['ventas']['Monto'].mean()
        except:
            pass

    c_kpi1, c_kpi2 = st.columns(2)
    c_kpi1.metric("Ticket Promedio Global (Base)", f"S/ {ticket_promedio_global:.2f}", help="Se usa para definir los umbrales VIP")

    if df_yape.empty:
        st.warning("⚠️ No hay datos de Yape cargados.")
    else:
        try:
            df_ingresos = df_yape.copy()
            
            if 'Fecha_dt' not in df_ingresos.columns:
                 df_ingresos['Fecha_dt'] = pd.to_datetime(df_ingresos['Fecha_Operacion'], dayfirst=True, errors='coerce')

            df_ingresos = df_ingresos.dropna(subset=['Fecha_dt'])
            df_ingresos['Mes_Año'] = df_ingresos['Fecha_dt'].dt.strftime('%Y-%m') 

            def limpiar_nombre(nombre):
                if not isinstance(nombre, str): return "DESCONOCIDO"
                nombre = nombre.upper().strip()
                prefijos = ["PLIN - ", "YAPE - ", "TRANSFERENCIA - ", "IZIPAY - ", "INTERBANK - ", "BCP - ", "PLIN", "YAPE"]
                for p in prefijos:
                    nombre = nombre.replace(p, "").strip()
                return nombre if len(nombre) > 2 else "ANÓNIMO"

            df_ingresos['Cliente_Limpio'] = df_ingresos['Origen'].apply(limpiar_nombre)

            fecha_hoy = pd.to_datetime("today")
            
            df_clientes = df_ingresos.groupby('Cliente_Limpio').agg(
                Total_Historico=('Monto', 'sum'),
                Visitas_Totales=('Fecha_dt', 'count'),
                Ultima_Visita=('Fecha_dt', 'max'),
                Ticket_Maximo=('Monto', 'max')
            ).reset_index()

            df_clientes['Dias_Ausente'] = (fecha_hoy - df_clientes['Ultima_Visita']).dt.days

            umbral_vip = ticket_promedio_global * 4        
            umbral_recurrente = ticket_promedio_global * 1.5

            def segmentar_cliente(row):
                total = row['Total_Historico']
                visitas = row['Visitas_Totales']
                dias_off = row['Dias_Ausente']
                tk_max = row['Ticket_Maximo']
                
                estado = ""
                if tk_max >= umbral_vip and visitas == 1: estado = "🐋 BALLENA (1 Visita)"
                elif total >= umbral_vip: estado = "💎 VIP (Socio)"
                elif total >= umbral_recurrente: estado = "🔥 RECURRENTE"
                else: estado = "🌱 CASUAL"
                
                if dias_off > 45: 
                    if "CASUAL" in estado: estado = "💤 PERDIDO"
                    else: estado = f"💤 DORMIDO ({estado.split('(')[0].strip()})" 
                return estado

            df_clientes['Segmento'] = df_clientes.apply(segmentar_cliente, axis=1)

            total_vip = len(df_clientes[df_clientes['Segmento'].str.contains("VIP")])
            c_kpi2.metric("Socios VIP Activos", total_vip, "Clientes fidelizados de alto valor")

            meses_disponibles = sorted(df_ingresos['Mes_Año'].unique())[-6:] 
            
            if meses_disponibles:
                df_recent = df_ingresos[df_ingresos['Mes_Año'].isin(meses_disponibles)]
                pivot_meses = df_recent.pivot_table(
                    index='Cliente_Limpio', columns='Mes_Año', values='Monto', aggfunc='sum', fill_value=0
                ).reset_index()
                
                df_final = pd.merge(df_clientes, pivot_meses, on='Cliente_Limpio', how='left').fillna(0)
                df_final = df_final.sort_values(by='Total_Historico', ascending=False)
                
                st.divider()
                
                col_search, col_filtro = st.columns([2, 1])
                with col_search:
                    busqueda = st.text_input("🔍 Buscar Cliente:", placeholder="Escribe nombre...")
                with col_filtro:
                    opciones_seg = ["TODOS"] + sorted(df_final['Segmento'].unique().tolist())
                    filtro_seg = st.selectbox("Filtrar Segmento:", opciones_seg)

                df_display = df_final.copy()
                if busqueda:
                    df_display = df_display[df_display['Cliente_Limpio'].str.contains(busqueda.upper())]
                if filtro_seg != "TODOS":
                    df_display = df_display[df_display['Segmento'] == filtro_seg]

                if not busqueda and filtro_seg == "TODOS":
                      df_display = df_display.head(50) 
                
                cols_meses_reales = [c for c in df_display.columns if c in meses_disponibles]
                cols_totales = ['Cliente_Limpio', 'Segmento', 'Total_Historico', 'Dias_Ausente'] + cols_meses_reales

                st.dataframe(
                    df_display[cols_totales],
                    column_config={
                        "Total_Historico": st.column_config.NumberColumn("Total LTV", format="S/ %.2f"),
                        "Cliente_Limpio": "Cliente",
                        "Dias_Ausente": st.column_config.NumberColumn("Días Sin Venir", format="%d"),
                    },
                    use_container_width=True,
                    hide_index=True
                )

                st.divider()
                c_alert1, c_alert2 = st.columns(2)
                
                with c_alert1:
                    vips_riesgo = df_final[df_final['Segmento'].str.contains("DORMIDO") & df_final['Segmento'].str.contains("VIP")]
                    if not vips_riesgo.empty:
                        st.error(f"🚨 **ALERTA DE FUGA: {len(vips_riesgo)} VIPs**")
                        st.dataframe(vips_riesgo[['Cliente_Limpio', 'Total_Historico', 'Ultima_Visita']], hide_index=True)
                    else:
                        st.success("✅ VIPs retenidos correctamente.")

                with c_alert2:
                    ballenas = df_final[df_final['Segmento'].str.contains("BALLENA")]
                    if not ballenas.empty:
                        st.info(f"🎣 **BALLENAS DETECTADAS: {len(ballenas)}**")
                        st.dataframe(ballenas[['Cliente_Limpio', 'Total_Historico', 'Ultima_Visita']], hide_index=True)
                    else:
                        st.info("Sin ballenas recientes.")

        except Exception as e:
            st.error(f"❌ Error en Procesamiento Yape: {e}")

# ==============================================================================
# PESTAÑA 7: GESTIÓN DE MARCA
# ==============================================================================
elif menu == "7. GESTION DE MARCA":
    st.header("📢 Gestión de Marca (MER)")
    st.info("Objetivo: Abrir la 'Mandíbula de Cocodrilo'. Gasto estable, Ventas crecientes.")

    if 'df_ventas' not in locals() and DATA['ventas'].empty:
         st.warning("⚠️ No se detectaron Ventas cargadas. Ve a 'Finanzas' primero.")
         st.stop()
    
    df_mkt = DATA['mkt_semanal']

    if df_mkt.empty:
        st.error("❌ Error de Conexión: La tabla de Marketing está vacía.")
        st.info("""
        PASOS PARA ARREGLARLO:
        1. Ve a tus 'Secrets' y copia el 'client_email' del robot.
        2. Ve al archivo '000. Registros Marketing' en Google Drive.
        3. Dale al botón 'Compartir' y pega el correo del robot.
        4. Recarga esta página.
        """)
    else:
        try:
            df_proc = df_mkt.copy()
            
            if 'Fecha_Cierre' not in df_proc.columns:
                 st.error(f"❌ No encuentro la columna 'Fecha_Cierre'. Veo estas: {df_proc.columns.tolist()}")
                 st.stop()

            df_proc['Fecha_Cierre'] = pd.to_datetime(df_proc['Fecha_Cierre'], dayfirst=True, errors='coerce')
            df_proc = df_proc.dropna(subset=['Fecha_Cierre']).sort_values(by='Fecha_Cierre')

            reporte_final = []
            df_v = DATA['ventas']
            
            for index, row in df_proc.iterrows():
                fecha_fin = row['Fecha_Cierre']
                fecha_ini = fecha_fin - pd.Timedelta(days=6)
                
                mask_ventas = (df_v['Fecha_dt'] >= fecha_ini) & (df_v['Fecha_dt'] <= fecha_fin)
                venta_semanal = df_v.loc[mask_ventas, 'Monto'].sum()

                gasto = safe_float(row.get('Gasto_Ads', 0))
                mer = venta_semanal / gasto if gasto > 0 else 0
                
                reviews = safe_float(row.get('Google_Review') if 'Google_Review' in row else row.get('Google_Reviews', 0))
                
                fila_procesada = {
                    'Semana': fecha_fin.strftime("%d-%b"),
                    'Fecha_Full': fecha_fin,
                    'Gasto_Ads': gasto,
                    'Ventas_Reales': venta_semanal,
                    'MER': mer,
                    'Reviews': reviews,
                    'Stars': row.get('Google_Stars', 0)
                }
                reporte_final.append(fila_procesada)
                
            df_final = pd.DataFrame(reporte_final)
            
            if not df_final.empty:
                 df_final['Nuevas_Reviews'] = df_final['Reviews'].diff().fillna(0)

            if not df_final.empty:
                actual = df_final.iloc[-1]
                anterior = df_final.iloc[-2] if len(df_final) > 1 else actual
                
                k1, k2, k3 = st.columns(3)
                
                delta_mer = actual['MER'] - anterior['MER']
                k1.metric("MER (Retorno)", f"{actual['MER']:.1f}x", f"{delta_mer:.1f} vs anterior")
                
                k2.metric("Gasto Ads Semanal", f"S/ {actual['Gasto_Ads']:,.0f}", f"Generó: S/ {actual['Ventas_Reales']:,.0f}")
                
                delta_rev = actual['Reviews'] - anterior['Reviews']
                k3.metric("Google Stars", f"{actual['Stars']} ⭐", f"+{int(delta_rev)} Reviews nuevas")
                
                st.markdown("---")
                
                st.subheader("🐊 La Mandíbula de Cocodrilo (Inversión vs Ventas)")
                
                fig = make_subplots(specs=[[{"secondary_y": True}]])

                fig.add_trace(go.Bar(x=df_final['Semana'], y=df_final['Ventas_Reales'], name="Ventas (S/)", marker_color='#00CC96', opacity=0.6), secondary_y=False)
                fig.add_trace(go.Scatter(x=df_final['Semana'], y=df_final['Gasto_Ads'], name="Inversión Ads (S/)", mode='lines+markers', line=dict(color='#EF553B', width=3)), secondary_y=True)

                fig.update_layout(title_text="Correlación Publicitaria", height=450, showlegend=True, template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
                fig.update_yaxes(title_text="Ventas (S/)", secondary_y=False, showgrid=False)
                fig.update_yaxes(title_text="Gasto Ads (S/)", secondary_y=True, showgrid=False)

                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("🔎 Ver Bitácora Semanal"):
                    st.dataframe(df_final.sort_values(by='Fecha_Full', ascending=False), use_container_width=True)

            else:
                st.info("El archivo procesado está vacío.")

        except Exception as e:
            st.error(f"❌ Error de Lógica: {e}")
            st.dataframe(pd.DataFrame(reporte_final))

# ==============================================================================
# PESTAÑA 8: MODELO ECONOMÉTRICO (EL ORÁCULO)
# ==============================================================================
elif menu == "8. MODELO ECONOMÉTRICO":
    st.header("🧠 Intelligence Hub: Causa & Efecto")
    st.info("Este módulo analiza qué variables mueven realmente la aguja de tus ventas.")

    df_master = build_econometric_master(DATA)

    if df_master.empty:
        st.warning("⚠️ No hay suficientes datos para generar el modelo econométrico.")
    else:
        mask_time = (df_master['Fecha_dt'].dt.date >= start_date.date()) & (df_master['Fecha_dt'].dt.date <= hoy.date())
        df_plot = df_master[mask_time].copy()

        if df_plot.empty:
            st.warning(f"No hay datos en el periodo seleccionado ({periodo_label}).")
        else:
            st.subheader("1. La Ley de la Demanda (Precio vs Ventas)")
            
            fig_price = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig_price.add_trace(
                go.Bar(x=df_plot['Fecha_dt'], y=df_plot['Cantidad'], name="Platos Vendidos (Q)", marker_color='#00A3E0', opacity=0.7),
                secondary_y=False
            )
            fig_price.add_trace(
                go.Scatter(x=df_plot['Fecha_dt'], y=df_plot['Precio_Promedio_Real'], name="Precio Efectivo (S/)", 
                           mode='lines+markers', line=dict(color='#FF4B4B', width=3, dash='dot')),
                secondary_y=True
            )

            fig_price.update_layout(template="plotly_dark", height=450, title_text="¿Los descuentos realmente aumentan el volumen?", paper_bgcolor='rgba(0,0,0,0)')
            fig_price.update_yaxes(title_text="Cantidad (Unidades)", secondary_y=False)
            fig_price.update_yaxes(title_text="Precio Promedio (S/)", secondary_y=True)
            st.plotly_chart(fig_price, use_container_width=True)

            st.subheader("2. Impacto Publicitario (Ads vs Tráfico)")
            
            fig_ads = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig_ads.add_trace(
                go.Scatter(x=df_plot['Fecha_dt'], y=df_plot['Monto'], name="Venta Total (S/)", 
                           fill='tozeroy', mode='none', fillcolor='rgba(0, 204, 150, 0.2)'),
                secondary_y=False
            )
            fig_ads.add_trace(
                go.Bar(x=df_plot['Fecha_dt'], y=df_plot['Gasto_Ads_Soles'], name="Inversión Ads (S/)", marker_color='#FFA500'),
                secondary_y=True
            )

            fig_ads.update_layout(template="plotly_dark", height=400, title_text="Relación Inversión vs Retorno", paper_bgcolor='rgba(0,0,0,0)')
            fig_ads.update_yaxes(title_text="Venta Caja (S/)", secondary_y=False)
            fig_ads.update_yaxes(title_text="Gasto Diario (S/)", secondary_y=True)
            st.plotly_chart(fig_ads, use_container_width=True)

            st.subheader("3. El Entorno (Clima, Competencia, Stockouts)")
            
            fig_env = px.line(df_plot, x='Fecha_dt', y='Cantidad', title="Impacto de Variables Externas")
            fig_env.update_traces(line_color='gray', opacity=0.5)

            if 'Lluvia_Intensa' in df_plot.columns and df_plot['Lluvia_Intensa'].apply(safe_float).sum() > 0:
                df_rain = df_plot[df_plot['Lluvia_Intensa'].astype(str) == '1']
                fig_env.add_trace(go.Scatter(
                    x=df_rain['Fecha_dt'], y=df_rain['Cantidad'],
                    mode='markers', name='Lluvia Intensa 🌧️',
                    marker=dict(color='blue', size=12, symbol='triangle-down')
                ))

            if 'Competencia_Agresiva' in df_plot.columns and df_plot['Competencia_Agresiva'].apply(safe_float).sum() > 0:
                df_comp = df_plot[df_plot['Competencia_Agresiva'].astype(str) == '1']
                fig_env.add_trace(go.Scatter(
                    x=df_comp['Fecha_dt'], y=df_comp['Cantidad'],
                    mode='markers', name='Ataque Competencia ⚔️',
                    marker=dict(color='red', size=12, symbol='x')
                ))

            if 'Stockout_Cierre' in df_plot.columns and df_plot['Stockout_Cierre'].apply(safe_float).sum() > 0:
                df_stock = df_plot[df_plot['Stockout_Cierre'].astype(str) == '1']
                fig_env.add_trace(go.Scatter(
                    x=df_stock['Fecha_dt'], y=df_stock['Cantidad'],
                    mode='markers', name='Quiebre de Stock 🚫',
                    marker=dict(color='orange', size=12, symbol='circle-x')
                ))

            fig_env.update_layout(template="plotly_dark", height=450, paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_env, use_container_width=True)
            
            with st.expander("🔎 Ver Data Maestra (Auditable)"):
                st.dataframe(df_plot, use_container_width=True)
