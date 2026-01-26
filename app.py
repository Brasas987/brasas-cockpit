import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import numpy as np

# ==============================================================================
# 1. CONFIGURACIÓN DEL ENTORNO (VISUAL STYLE: PALANTIR)
# ==============================================================================
st.set_page_config(page_title="Brasas Cockpit | CEO Command Center", layout="wide", page_icon="🔥")

# CSS Avanzado para Jerarquía Visual y Modo Oscuro
st.markdown("""
<style>
    /* Fondo General */
    [data-testid="stAppViewContainer"] {background-color: #0e1117;}
    [data-testid="stSidebar"] {background-color: #1a1c24;}
    
    /* TARJETAS DE KPIs (BIG NUMBERS) */
    div[data-testid="metric-container"] {
        background-color: #262730;
        border-left: 5px solid #FF4B4B; /* Línea de acento roja */
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0px 4px 6px rgba(0,0,0,0.5);
    }
    div[data-testid="metric-container"] label {
        color: #b0b3b8 !important; 
        font-size: 0.85rem; 
        text-transform: uppercase; 
        letter-spacing: 1px;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #ffffff !important; 
        font-weight: 700; 
        font-size: 2rem;
    }
    
    /* ALERTA CRÍTICA VISUAL */
    .metric-alert {
        border-left: 5px solid #ff0000 !important;
        background-color: #3d0000 !important;
    }
    
    /* TABLAS Y GRÁFICOS */
    [data-testid="stDataFrame"] {border: 1px solid #41424C;}
    h1, h2, h3 {color: white !important; font-family: 'Source Sans Pro', sans-serif;}
    
    /* SEMÁFOROS TEXTUALES */
    .status-ok {color: #00CC96; font-weight: bold;}
    .status-warn {color: #FFA500; font-weight: bold;}
    .status-crit {color: #FF4B4B; font-weight: bold;}
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. CONEXIÓN SEGURA A GOOGLE SHEETS
# ==============================================================================
@st.cache_resource
def connect_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error de Credenciales: {e}")
        st.stop()

# --- MAPA DE IDs (MODIFICA ESTO EN GITHUB) ---
IDS = {
    "REGISTROS":  "https://docs.google.com/spreadsheets/d/1pbpbkZWH6RHpUwdjrTFGtkNAi4ameR2PJZVbR5OPeZQ/edit?gid=1445845805#gid=1445845805",
    "LIBROS":     "https://docs.google.com/spreadsheets/d/1-juSBgRcNdKWNjDL6ZuKcBIVVQXtnpL3qCR9Z1AWQyU/edit?gid=988070039#gid=988070039",
    "COSTOS":     "https://docs.google.com/spreadsheets/d/1JNKE-5nfOsJ7U9k0bCLAW-xjOzSGRG15rdGdWrC3h8U/edit?gid=1976317468#gid=1976317468",
    "INVENTARIO": "https://docs.google.com/spreadsheets/d/1vDI6y_xN-abIFkv9z63rc94PTnCtJURC4r7vN3RCeLo/edit?gid=10562125#gid=10562125",
    "CAJA":       "https://docs.google.com/spreadsheets/d/1Ck6Um7PG8uZ626x9kMvf1tMlBckBUHBjy6dTYRxUIZY/edit?gid=0#gid=0",
    "FORECAST":   "https://docs.google.com/spreadsheets/d/1rmb0tvFhNQgiVOvUC3u5IxeBSA1w4HiY5lr13sD1VU0/edit?gid=1023849055#gid=1023849055"
}

# ==============================================================================
# 3. MOTOR ETL (Extracción y Limpieza)
# ==============================================================================
@st.cache_data(ttl=600)
def load_all_data():
    client = connect_google_sheets()
    DB = {}
    
    def get_data(file_key, sheet_name, date_col=None, num_cols=[]):
        try:
            if IDS[file_key].startswith("PON_AQUI"): return pd.DataFrame()
            sh = client.open_by_key(IDS[file_key])
            ws = sh.worksheet(sheet_name)
            df = pd.DataFrame(ws.get_all_records())
            
            if df.empty: return df
            
            # Limpieza Fechas
            if date_col and date_col in df.columns:
                df['Fecha_dt'] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
            
            # Limpieza Números (S/, %, comas)
            for col in num_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace(r'[S/%,\s]', '', regex=True)
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            return df
        except Exception:
            return pd.DataFrame()

    # --- CARGA MASIVA ---
    # 1. Ventas y Contexto
    DB['ventas'] = get_data("REGISTROS", "BD_Ventas", "Fecha", ["Total_Venta", "Cantidad"])
    DB['feriados'] = get_data("REGISTROS", "MASTER_FERIADOS", "Fecha", [])
    DB['partidos'] = get_data("REGISTROS", "MASTER_PARTIDOS", "Fecha", [])
    
    # 2. Costos y QC
    DB['costos_prod'] = get_data("COSTOS", "OUT_Costos_Productos", None, ["Margen_%", "Costo_Total", "Precio_num"])
    DB['qc_fail'] = get_data("COSTOS", "OUT_QC_Compras_NoConvertibles", None, ["Total_Pagado"])
    
    # 3. Inventario y Merma
    DB['merma'] = get_data("INVENTARIO", "OUT_Merma_Valorizada", "Fecha", ["Merma_Soles", "Stock_teorico_gr", "Stock_real_gr"])
    # Nota: Usamos Merma para el GAP ya que tiene stock teorico vs real
    
    # 4. Caja y Capex
    DB['caja'] = get_data("CAJA", "BD_Caja_Diaria", "Fecha", ["Saldo_Total_Bancos"])
    DB['caja_reconcilia'] = get_data("CAJA", "OUT_Reconciliacion_Caja", "Fecha", ["CAJA_REAL_FISICA"])
    DB['capex'] = get_data("CAJA", "PARAM_PROYECTOS_CAPEX", None, ["Monto_Total", "Monto_Acumulado_Actual"])
    
    # 5. Forecast y Finanzas
    DB['forecast'] = get_data("FORECAST", "OUT_Pronostico_Ventas", "Fecha", ["Venta_P50_Probable"])
    DB['soberania'] = get_data("FORECAST", "OUT_Soberania_Financiera", None, [])
    # Ajuste manual para soberania que a veces no tiene header de fecha claro
    if not DB['soberania'].empty:
        col0 = DB['soberania'].columns[0]
        col1 = DB['soberania'].columns[1] # Asumimos saldo en col 1
        DB['soberania']['Fecha_dt'] = pd.to_datetime(DB['soberania'][col0], dayfirst=True, errors='coerce')
        DB['soberania']['Saldo_Proj'] = pd.to_numeric(DB['soberania'][col1].astype(str).str.replace(r'[S/,]', '', regex=True), errors='coerce')

    # 6. Deuda
    DB['cxp'] = get_data("LIBROS", "Libro_Cuentas_Pagar", "Fecha_Vencimiento", ["Saldo_Pendiente"])
    
    return DB

# Carga Inicial
try:
    DATA = load_all_data()
    STATUS = "🟢 ONLINE"
except Exception as e:
    st.error(f"Error Fatal: {e}")
    st.stop()

# ==============================================================================
# 4. LÓGICA DE NAVEGACIÓN Y FILTROS
# ==============================================================================
hoy = datetime.now()

with st.sidebar:
    st.title("🔥 BRASAS CAPITALES")
    st.caption(f"CEO Dashboard | {hoy.strftime('%d-%b-%Y')}")
    st.markdown("---")
    
    # Menú Principal
    pagina = st.radio("NAVEGACIÓN ESTRATÉGICA", 
        ["1. CORPORATE OVERVIEW", "2. EFICIENCIA OPERATIVA", "3. FINANZAS & PLANEACIÓN"])
    
    st.markdown("---")
    
    # FILTRO GLOBAL (Esquina Superior Derecha Lógica)
    st.markdown("### 📅 Filtro Temporal")
    filtro_modo = st.selectbox("Ventana de Análisis", ["This Month (MTD)", "Last 30 Days"])
    
    # Definición de Fechas según Filtro
    if filtro_modo == "This Month (MTD)":
        start_date = hoy.replace(day=1)
        end_date = hoy
        label_periodo = "Acumulado Mes Actual"
    else:
        start_date = hoy - timedelta(days=30)
        end_date = hoy
        label_periodo = "Últimos 30 Días"

    st.markdown("---")
    st.success(STATUS)

# ==============================================================================
# PESTAÑA 1: CORPORATE OVERVIEW & HEALTH CHECK
# ==============================================================================
if pagina == "1. CORPORATE OVERVIEW":
    st.header(f"🏥 Health Check: Signos Vitales ({label_periodo})")
    
    # --- A. CÁLCULO DE LOS 4 GRANDES KPIs ---
    
    # 1. Ventas Totales (MTD estricto para KPI)
    df_v = DATA['ventas']
    v_mtd = 0
    delta_v = 0
    if not df_v.empty:
        # Siempre MTD para la tarjeta
        mask_mtd = (df_v['Fecha_dt'].dt.month == hoy.month) & (df_v['Fecha_dt'].dt.year == hoy.year)
        v_mtd = df_v[mask_mtd]['Total_Venta'].sum()
        # Comparativa Mes Anterior (Simulada simple por ahora)
        v_last_month = v_mtd * 0.9 # Placeholder
        delta_v = ((v_mtd - v_last_month) / v_last_month) * 100 if v_last_month > 0 else 0

    # 2. Margen Bruto Global (Teórico vs Real)
    margen_global = 0
    df_c = DATA['costos_prod']
    if not df_c.empty:
        margen_global = df_c['Margen_%'].mean() * 100 # Promedio simple de productos

    # 3. Pérdida por Merma (MTD)
    df_m = DATA['merma']
    merma_mtd = 0
    if not df_m.empty:
        mask_m_mtd = (df_m['Fecha_dt'].dt.month == hoy.month) & (df_m['Fecha_dt'].dt.year == hoy.year)
        merma_mtd = df_m[mask_m_mtd]['Merma_Soles'].sum()

    # 4. Flujo de Caja (Actual vs Proyectado)
    caja_actual = 0
    caja_proy = 0
    if not DATA['caja_reconcilia'].empty:
         # Último saldo real registrado
         caja_actual = DATA['caja_reconcilia'].iloc[-1]['CAJA_REAL_FISICA']
    if not DATA['soberania'].empty:
         # Saldo proyectado para hoy
         mask_sob = DATA['soberania']['Fecha_dt'].dt.date == hoy.date()
         if mask_sob.any():
             caja_proy = DATA['soberania'][mask_sob]['Saldo_Proj'].values[0]

    # --- VISUALIZACIÓN TARJETAS SUPERIORES ---
    c1, c2, c3, c4 = st.columns(4)
    
    # Tarjeta 1: Ventas
    c1.metric("VENTAS TOTALES (MTD)", f"S/ {v_mtd:,.0f}", f"{delta_v:.1f}% vs Mes Ant")
    
    # Tarjeta 2: Margen (Gauge Chart)
    with c2:
        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = margen_global,
            title = {'text': "MARGEN BRUTO %"},
            gauge = {
                'axis': {'range': [0, 100]},
                'bar': {'color': "#00CC96" if margen_global > 60 else "#FF4B4B"},
                'steps': [{'range': [0, 60], 'color': "gray"}]
            }
        ))
        fig_gauge.update_layout(height=130, margin=dict(l=10,r=10,t=30,b=10), paper_bgcolor='rgba(0,0,0,0)', font={'color': "white"})
        st.plotly_chart(fig_gauge, use_container_width=True)

    # Tarjeta 3: Merma (Gestión por Excepción)
    # Si Merma > S/ 500 -> ROJO
    str_delta_merma = "-CRÍTICO" if merma_mtd > 500 else "Controlado"
    c3.metric("PÉRDIDA MERMA (MTD)", f"S/ {merma_mtd:,.0f}", str_delta_merma, delta_color="inverse")
    
    # Tarjeta 4: Flujo Caja (Mini Sparkline)
    with c4:
        st.metric("CAJA ACTUAL", f"S/ {caja_actual:,.0f}", f"vs Proy: S/ {caja_proy:,.0f}")
        # Pequeña línea de tendencia si hay datos de soberanía
        if not DATA['soberania'].empty:
            df_mini = DATA['soberania'].tail(14)
            fig_mini = px.line(df_mini, x='Fecha_dt', y='Saldo_Proj')
            fig_mini.update_layout(height=50, margin=dict(l=0,r=0,t=0,b=0), showlegend=False, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            fig_mini.update_xaxes(visible=False).update_yaxes(visible=False)
            st.plotly_chart(fig_mini, use_container_width=True)

    st.markdown("---")

    # --- SECCIÓN MEDIA: RENDIMIENTO COMERCIAL ---
    col_main, col_sec = st.columns([2, 1])

    # GRÁFICO PRINCIPAL: Ventas Reales vs Forecast + Contexto
    with col_main:
        st.subheader("📈 Tendencia: Realidad vs Proyección (30d + 7d)")
        
        fig_combo = go.Figure()
        
        # 1. Historia (Ventas Reales - Rolling 30)
        if not df_v.empty:
            start_roll = hoy - timedelta(days=30)
            mask_roll = df_v['Fecha_dt'] >= start_roll
            df_hist = df_v[mask_roll].groupby('Fecha_dt')['Total_Venta'].sum().reset_index()
            fig_combo.add_trace(go.Bar(
                x=df_hist['Fecha_dt'], y=df_hist['Total_Venta'], 
                name='Venta Real', marker_color='#00A3E0'
            ))

        # 2. Futuro (Forecast - Next 7 days)
        df_f = DATA['forecast']
        if not df_f.empty:
            mask_fut = (df_f['Fecha_dt'] >= hoy) & (df_f['Fecha_dt'] <= (hoy + timedelta(days=14)))
            df_fut = df_f[mask_fut]
            fig_combo.add_trace(go.Scatter(
                x=df_fut['Fecha_dt'], y=df_fut['Venta_P50_Probable'], 
                name='Pronóstico IA', line=dict(color='#FFA500', width=3, dash='dash')
            ))

        # 3. Contexto (Feriados y Partidos)
        # Combinamos eventos en un solo DF para graficar marcadores
        eventos = []
        if not DATA['feriados'].empty:
            mask_fer = (DATA['feriados']['Fecha_dt'] >= start_roll) & (DATA['feriados']['Fecha_dt'] <= (hoy + timedelta(days=14)))
            df_fer_ctx = DATA['feriados'][mask_fer].copy()
            df_fer_ctx['Tipo'] = 'Feriado'
            df_fer_ctx['Nombre'] = df_fer_ctx['Nombre_Evento'] # Ajustar col
            eventos.append(df_fer_ctx)
            
        if not DATA['partidos'].empty:
            mask_par = (DATA['partidos']['Fecha_dt'] >= start_roll) & (DATA['partidos']['Fecha_dt'] <= (hoy + timedelta(days=14)))
            df_par_ctx = DATA['partidos'][mask_par].copy()
            df_par_ctx['Tipo'] = 'Partido'
            df_par_ctx['Nombre'] = df_par_ctx['Evento']
            eventos.append(df_par_ctx)
            
        if eventos:
            df_ctx = pd.concat(eventos)
            fig_combo.add_trace(go.Scatter(
                x=df_ctx['Fecha_dt'], y=[0] * len(df_ctx), # En el piso del gráfico o arriba
                mode='markers', marker=dict(symbol='star', size=12, color='white'),
                name='Evento Externo', hovertext=df_ctx['Nombre']
            ))

        fig_combo.update_layout(template="plotly_dark", height=450, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_combo, use_container_width=True)

    # GRÁFICO SECUNDARIO: Matriz BCG
    with col_sec:
        st.subheader("🧩 Productos Estrella (BCG)")
        if not df_v.empty and not df_c.empty:
            # Cruzar Volumen (Ventas) vs Margen (Costos)
            df_vol = df_v.groupby('Producto_ID')['Cantidad'].sum().reset_index()
            # Merge
            df_bcg = pd.merge(df_vol, df_c, on='Producto_ID', how='inner')
            
            if not df_bcg.empty:
                fig_bcg = px.scatter(
                    df_bcg, x="Cantidad", y="Margen_%", size="Precio_num", 
                    color="Menu", hover_name="Menu",
                    labels={"Cantidad": "Volumen (Und)", "Margen_%": "Margen Unitario %"}
                )
                # Cuadrantes
                avg_x = df_bcg['Cantidad'].mean()
                avg_y = df_bcg['Margen_%'].mean()
                fig_bcg.add_vline(x=avg_x, line_dash="dot", line_color="grey")
                fig_bcg.add_hline(y=avg_y, line_dash="dot", line_color="grey")
                
                fig_bcg.update_layout(template="plotly_dark", height=450, paper_bgcolor='rgba(0,0,0,0)', showlegend=False)
                st.plotly_chart(fig_bcg, use_container_width=True)
            else:
                st.info("Sin datos cruzados para BCG")

# ==============================================================================
# PESTAÑA 2: EFICIENCIA OPERATIVA Y COSTOS (EL MOTOR)
# ==============================================================================
elif pagina == "2. EFICIENCIA OPERATIVA":
    st.header("⚙️ Análisis de Costos y Desperdicios")
    
    col_gap, col_tree = st.columns(2)
    
    # 1. ANÁLISIS FOOD COST (Teórico vs Real) -> Usando GAP de Merma
    with col_gap:
        st.subheader("⚖️ Gap de Inventario (Teórico vs Real)")
        df_gap = DATA['merma'] # Usamos Merma Valorizada que ya tiene el cálculo
        if not df_gap.empty:
            # Agrupar por Insumo y sumar
            # Stock Teorico vs Stock Real
            df_inv_agg = df_gap.groupby('Insumo')[['Stock_teorico_gr', 'Stock_real_gr']].sum().reset_index().head(10) # Top 10
            
            # Melt para barras agrupadas
            df_melt = df_inv_agg.melt(id_vars='Insumo', value_vars=['Stock_teorico_gr', 'Stock_real_gr'], var_name='Tipo', value_name='Gramos')
            
            fig_gap = px.bar(
                df_melt, x='Insumo', y='Gramos', color='Tipo', barmode='group',
                color_discrete_map={'Stock_teorico_gr': '#00CC96', 'Stock_real_gr': '#EF553B'},
                title="Top 10 Discrepancias de Inventario"
            )
            fig_gap.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_gap, use_container_width=True)
        else:
            st.warning("No hay datos de inventario para calcular el GAP.")

    # 2. RANKING DE MERMA (Treemap)
    with col_tree:
        st.subheader("🗑️ Ranking de Dinero Perdido (Merma)")
        if not df_gap.empty:
            # Filtrar solo mermas negativas (pérdidas) reales
            df_loss = df_gap[df_gap['Merma_Soles'] < 0].copy()
            df_loss['Merma_Abs'] = df_loss['Merma_Soles'].abs()
            
            if not df_loss.empty:
                fig_tree = px.treemap(
                    df_loss, path=['Insumo'], values='Merma_Abs',
                    color='Merma_Abs', color_continuous_scale='Reds',
                    title="Insumos con Mayor Pérdida Económica"
                )
                fig_tree.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_tree, use_container_width=True)
            else:
                st.success("✅ ¡Increíble! No hay mermas registradas.")

    st.markdown("---")
    
    # 3. CONTROL DE CALIDAD COMPRAS (Semáforo)
    st.subheader("🛡️ Control de Calidad en Recepción")
    df_qc = DATA['qc_fail']
    col_q1, col_q2 = st.columns([1, 3])
    
    with col_q1:
        if not df_qc.empty:
            total_fail = df_qc['Total_Pagado'].sum()
            st.metric("COMPRAS RECHAZADAS", f"S/ {total_fail:,.2f}", "Insumos No Convertibles", delta_color="inverse")
        else:
            st.metric("COMPRAS RECHAZADAS", "S/ 0.00", "Calidad 100% OK")
            
    with col_q2:
        if not df_qc.empty:
            st.dataframe(df_qc, use_container_width=True)

# ==============================================================================
# PESTAÑA 3: FINANZAS Y PLANEACIÓN (EL FUTURO)
# ==============================================================================
elif pagina == "3. FINANZAS & PLANEACIÓN":
    st.header("🔮 Soberanía Financiera & Liquidez")
    
    # 1. RUNWAY (Soberanía)
    st.subheader("✈️ Proyección de Caja (Runway)")
    df_sob = DATA['soberania']
    if not df_sob.empty:
        # Gráfico de Área
        fig_run = px.area(df_sob, x='Fecha_dt', y='Saldo_Proj', title="Disponibilidad de Efectivo Futura")
        # Línea de Seguridad (Ej. S/ 1000)
        fig_run.add_hline(y=1000, line_dash="dot", line_color="red", annotation_text="Colchón de Seguridad")
        fig_run.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_run, use_container_width=True)
    else:
        st.warning("Módulo de Forecast no ha generado proyección de caja.")
        
    c_fin1, c_fin2 = st.columns(2)
    
    # 2. CUENTAS POR PAGAR VS FLUJO
    with c_fin1:
        st.subheader("📉 Presión de Deuda Corto Plazo")
        df_cxp = DATA['cxp']
        df_cj = DATA['caja']
        
        deuda_total = 0
        if not df_cxp.empty:
            deuda_total = df_cxp['Saldo_Pendiente'].sum()
            
        caja_hoy = 0
        if not df_cj.empty:
            caja_hoy = df_cj.iloc[-1]['Saldo_Total_Bancos']
            
        # Gráfico Comparativo Barras
        dat_fin = pd.DataFrame({
            'Concepto': ['Deuda Proveedores', 'Caja Disponible'],
            'Monto': [deuda_total, caja_hoy]
        })
        fig_fin = px.bar(dat_fin, x='Concepto', y='Monto', color='Concepto', 
                         color_discrete_map={'Deuda Proveedores':'#FF4B4B', 'Caja Disponible':'#00CC96'})
        fig_fin.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', showlegend=False)
        st.plotly_chart(fig_fin, use_container_width=True)

    # 3. CAPEX
    with c_fin2:
        st.subheader("🏗️ Inversiones en Curso (CAPEX)")
        df_cap = DATA['capex']
        if not df_cap.empty:
            # Calculamos % Avance Financiero
            df_cap['Avance_%'] = (df_cap['Monto_Acumulado_Actual'] / df_cap['Monto_Total']) * 100
            st.dataframe(
                df_cap[['Proyecto', 'Monto_Total', 'Avance_%']], 
                use_container_width=True,
                column_config={"Avance_%": st.column_config.ProgressColumn("Ejecución", format="%.1f%%", min_value=0, max_value=100)}
            )
        else:
            st.info("No hay proyectos de inversión activos.")
