# app.py — Dashboard Territorio con Anexo Estadístico por Sector/Bloque
# Requisitos: streamlit, pandas, numpy, openpyxl, (opcional) pydeck
import os, json, glob, math, re
import streamlit as st
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional

st.set_page_config(page_title="Dashboard Territorio", layout="wide")

# ====== pydeck opcional (mapa y polígonos) ======
try:
    import pydeck as pdk
    _HAS_PYDECK = True
except Exception:
    _HAS_PYDECK = False

# ====== Estilo ======
st.markdown("""
<style>
.block-container {padding-top: .9rem; padding-bottom: 2rem; max-width: 1400px;}
.stMetric {background: rgba(255,255,255,0.035); border-radius: 12px; padding: .6rem .9rem;}
[data-testid="stSidebar"] {min-width: 360px;}
h2, h3 { margin-top: 0.8rem; }
</style>
""", unsafe_allow_html=True)

# ====== Utilidades ======
def to_lower(x):
    try: return str(x).strip().lower()
    except: return x

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    df = df.copy(); df.columns = [str(c).strip() for c in df.columns]; return df

@st.cache_data(show_spinner=False)
def load_excel_first_sheet(path: str) -> Tuple[pd.DataFrame, List[str]]:
    xls = pd.ExcelFile(path)
    first = xls.sheet_names[0]
    df = pd.read_excel(path, sheet_name=first)
    return df, xls.sheet_names

def auto_glob(patterns: List[str]) -> Optional[str]:
    for pat in patterns:
        hits = glob.glob(pat, recursive=True)
        if hits: return hits[0]
    return None

def low_card_cats(df: pd.DataFrame, max_unique=60) -> List[str]:
    out=[]
    for c in df.columns:
        nun = df[c].nunique(dropna=True)
        if nun<=max_unique and (df[c].dtype=='object' or pd.api.types.is_bool_dtype(df[c]) or nun<=20):
            out.append(c)
    return out

def guess_lat_lon(df: pd.DataFrame):
    lat_candidates = ["lat","latitude","y","p002__latitude","latitud","coord_y","y_wgs84"]
    lon_candidates = ["lon","lng","longitude","x","p002__longitude","longitud","coord_x","x_wgs84"]
    cols_lower = {str(c).lower(): c for c in df.columns}
    lat = next((cols_lower[c] for c in lat_candidates if c in cols_lower), None)
    lon = next((cols_lower[c] for c in lon_candidates if c in cols_lower), None)
    return lat, lon

def coerce_decimal(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False), errors="coerce")

def geojson_center(gj: dict) -> Tuple[float,float]:
    if isinstance(gj, dict) and "bbox" in gj and isinstance(gj["bbox"], (list, tuple)) and len(gj["bbox"])>=4:
        minx, miny, maxx, maxy = gj["bbox"][:4]
        return (miny+maxy)/2.0, (minx+maxx)/2.0
    def _walk(coords, acc):
        if isinstance(coords, (list, tuple)):
            if len(coords)>0 and isinstance(coords[0], (int,float)):
                lon,lat=coords[0],coords[1]
                acc[0]=min(acc[0], lon); acc[1]=min(acc[1], lat)
                acc[2]=max(acc[2], lon); acc[3]=max(acc[3], lat)
            else:
                for c in coords: _walk(c, acc)
    acc=[math.inf, math.inf, -math.inf, -math.inf]
    if isinstance(gj, dict):
        if gj.get("type")=="FeatureCollection":
            for f in gj.get("features", []): _walk(f.get("geometry",{}).get("coordinates", []), acc)
        elif gj.get("type") in ("Polygon","MultiPolygon","LineString","MultiLineString"):
            _walk(gj.get("coordinates", []), acc)
    if acc[0] < acc[2] and acc[1] < acc[3]:
        return (acc[1]+acc[3])/2.0, (acc[0]+acc[2])/2.0
    return 13.494, -89.322  # fallback

# ====== Detectores de columnas del codebook ======
def _find_col(df, *aliases):
    cols_lc = {str(c).strip().lower(): c for c in df.columns}
    for a in aliases:
        a_lc = str(a).strip().lower()
        if a_lc in cols_lc:
            return cols_lc[a_lc]
    for a in aliases:
        a_lc = str(a).strip().lower()
        for k, orig in cols_lc.items():
            if a_lc in k:
                return orig
    return None

# ====== Codebook (parser robusto + renombrado) ======
def parse_codebook_any(path: str) -> Tuple[pd.DataFrame, Dict[str, Dict], Dict[str, pd.DataFrame], Dict[str, str]]:
    xls = pd.ExcelFile(path)
    dfs = {s: normalize_cols(pd.read_excel(path, sheet_name=s)) for s in xls.sheet_names}

    df_vars = pd.DataFrame(columns=["variable","tipo","descripcion","nuevo_nombre"])
    meta: Dict[str, Dict] = {}
    maps_por_var: Dict[str, pd.DataFrame] = {}
    ren_map_raw: Dict[str, str] = {}

    for s, df in dfs.items():
        if df is None or df.empty:
            continue

        var_col  = _find_col(df, "variable","var","nombre","campo","name")
        tipo_col = _find_col(df, "tipo","type","data_type","clase","class","tipo de variable")
        desc_col = _find_col(df, "descripcion","descripción","description","detalle","definicion","definición")
        newn_col = _find_col(df, "nuevo_nombre","new_name","etiqueta_variable","etiqueta de variable",
                                  "label_variable","display_name","nombre_publico","nombre público","nombre mostrado")
        code_col = _find_col(df, "valor","value","code","código","codigo","option_value","código")
        lab_col  = _find_col(df, "etiqueta","label","meaning","categoria","categoría",
                                  "option_label","etiqueta del código","etiqueta del codigo")

        if var_col:
            cols_take = [var_col]
            if tipo_col: cols_take.append(tipo_col)
            if newn_col: cols_take.append(newn_col)
            tmp = df[df[var_col].notna()][cols_take].copy()
            rename_map = {var_col: "variable"}
            if tipo_col: rename_map[tipo_col] = "tipo"
            if newn_col: rename_map[newn_col] = "nuevo_nombre"
            tmp = tmp.rename(columns=rename_map)
            tmp["variable"] = tmp["variable"].astype(str).str.strip()
            if "tipo" in tmp.columns: tmp["tipo"] = tmp["tipo"].astype(str).str.strip()
            if "nuevo_nombre" in tmp.columns: tmp["nuevo_nombre"] = tmp["nuevo_nombre"].astype(str).str.strip()
            df_vars = (pd.concat([df_vars, tmp], ignore_index=True)
                         .drop_duplicates(subset=["variable"], keep="first"))

        if newn_col and var_col:
            for _, r in df.dropna(subset=[var_col, newn_col]).iterrows():
                v = str(r[var_col]).strip()
                nn = str(r[newn_col]).strip()
                if v and nn:
                    ren_map_raw[v] = nn

        if var_col and code_col and lab_col:
            t = df[[var_col, code_col, lab_col]].copy()
            t[var_col] = t[var_col].ffill()
            t = t.dropna(subset=[code_col, lab_col])
            for _, r in t.iterrows():
                v = str(r[var_col]).strip()
                k = str(r[code_col]).strip().rstrip(".0")
                lbl = None if pd.isna(r[lab_col]) else str(r[lab_col]).strip()
                meta.setdefault(v, {"type": None, "map": {}})
                meta[v]["map"][k] = lbl

        if var_col and tipo_col:
            for _, r in df.dropna(subset=[var_col, tipo_col]).iterrows():
                v = str(r[var_col]).strip()
                vt = str(r[tipo_col]).strip().lower()
                meta.setdefault(v, {"type": None, "map": {}})
                if meta[v]["type"] is None:
                    meta[v]["type"] = vt

    if not df_vars.empty:
        df_vars = df_vars.drop_duplicates(subset=["variable"])
        df_vars["tipo"] = df_vars.apply(lambda r: (meta.get(str(r["variable"]), {}).get("type") or r.get("tipo")), axis=1)
    else:
        df_vars = pd.DataFrame([{
            "variable": v, "tipo": meta.get(v,{}).get("type"), "descripcion": None, "nuevo_nombre": ren_map_raw.get(v)
        } for v in meta.keys()])

    for v, info in meta.items():
        mp = info.get("map", {}) or {}
        if mp:
            df_map = pd.DataFrame({"codigo": list(mp.keys()), "etiqueta": [mp[k] for k in mp.keys()]})
            maps_por_var[v] = df_map.sort_values("codigo", key=lambda s: s.astype(str))

    meta_lc = {to_lower(k): {"type": v.get("type"), "map": v.get("map", {})} for k, v in meta.items()}
    ren_lc  = {to_lower(k): ren_map_raw[k] for k in ren_map_raw.keys()}
    maps_lc = {to_lower(k): val for k, val in maps_por_var.items()}

    return df_vars, meta_lc, maps_lc, ren_lc

def apply_codebook(df: pd.DataFrame, df_vars: pd.DataFrame, meta_lc: Dict[str, Dict], ren_lc: Dict[str, str], apply_labels: bool=True) -> pd.DataFrame:
    if df is None or df.empty: return df
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    cols_lower = {to_lower(c): c for c in out.columns}
    new_names = {}
    taken = set(out.columns)
    for v_lower, col in cols_lower.items():
        if v_lower in ren_lc:
            cand = str(ren_lc[v_lower]).strip()
            if cand and cand not in taken:
                new_names[col] = cand
                taken.add(cand)
    if new_names:
        out = out.rename(columns=new_names)
        cols_lower = {to_lower(c): c for c in out.columns}

    tipos = {}
    if df_vars is not None and not df_vars.empty:
        for _, r in df_vars.iterrows():
            v = str(r.get("variable","")).strip()
            if not v: continue
            t = r.get("tipo")
            tipos[to_lower(v)] = (None if pd.isna(t) else str(t).strip().lower())

    for v_lower, col in cols_lower.items():
        vtype = tipos.get(v_lower) or (meta_lc.get(v_lower, {}) or {}).get("type")
        if vtype:
            if any(k in vtype for k in ["num","int","float","double","decimal"]):
                out[col] = pd.to_numeric(out[col], errors="ignore")
            elif any(k in vtype for k in ["date","fecha","time"]):
                try: out[col] = pd.to_datetime(out[col], errors="ignore", infer_datetime_format=True)
                except Exception: pass

    if apply_labels and meta_lc:
        cols_lower = {to_lower(c): c for c in out.columns}
        for v_lower, col in cols_lower.items():
            info = meta_lc.get(v_lower)
            if not info: continue
            mapping = info.get("map", {}) or {}
            if mapping:
                raw_col = f"{col}_raw"
                if raw_col not in out.columns: out[raw_col] = out[col]
                out[col] = out[col].apply(lambda x: mapping.get(str(x), mapping.get(x, x)))
    return out

def sector_column(df):
    for c in df.columns:
        cl = str(c).strip().lower()
        if "sector" in cl or "bloque" in cl:
            return c
    return None

# ====== Sidebar: rutas ======
codebook_default = "data/metadata/Codebook.xlsx"
estr_default     = "data/estructura_hogar_etiquetada.xlsx"
hog_default      = ""
lim_default      = ""

st.sidebar.header("Datos de entrada")
codebook_path   = st.sidebar.text_input("Ruta Codebook",     codebook_default)
estructuras_path= st.sidebar.text_input("Ruta Estructuras",  estr_default)
hogares_path    = st.sidebar.text_input("Ruta Hogares (opcional)",      hog_default)
limite_path     = st.sidebar.text_input("Ruta límites (GeoJSON, opcional)", lim_default)

apply_labels    = st.sidebar.checkbox("Aplicar etiquetas del codebook (si existen)", True)
dataset_choice  = st.sidebar.radio("Dataset a explorar", ["Solo Estructuras", "Solo Hogares"], index=0)

# ====== Filtro de variables específicas (nuevo) ======
st.sidebar.subheader("🎯 Filtro de variables específicas")
_vars_selected = st.sidebar.multiselect(
    "Elige variables a priorizar en los tabulados y cruces (opcional)",
    options=[],
    default=[]
)

# ====== Carga ======
with st.spinner("Leyendo archivos…"):
    def load_or_empty(p):
        try:
            if not p: return pd.DataFrame()
            df,_ = load_excel_first_sheet(p)
            return normalize_cols(df)
        except Exception as e:
            st.warning(f"Archivo no cargado ({p}): {e}")
            return pd.DataFrame()
    try:
        df_vars, meta_lc, maps_por_var, ren_lc = parse_codebook_any(codebook_path)
    except Exception as e:
        st.warning(f"No se pudo parsear el codebook: {e}.")
        df_vars, meta_lc, maps_por_var, ren_lc = pd.DataFrame(), {}, {}, {}

    df_estr = load_or_empty(estructuras_path)
    df_hog  = load_or_empty(hogares_path)

base_df = df_estr if dataset_choice=="Solo Estructuras" else df_hog
df_display = apply_codebook(base_df, df_vars, meta_lc, ren_lc, apply_labels=apply_labels)

# Actualiza opciones del filtro de variables específicas
if df_display is not None and not df_display.empty:
    all_cols = df_display.columns.tolist()
    st.session_state["all_cols"] = all_cols
    # repinta el multiselect con opciones reales
    _vars_selected = st.sidebar.multiselect(
        "Elige variables a priorizar en los tabulados y cruces (opcional)",
        options=all_cols,
        default=[c for c in ["p004","p005","p006","p007","p008","p009a","p009b","p010","p011","p012","p013","p014","p015","p016","p017","p018","p019","p020","p021","p022","p025","p026","p027","p028","p029","p030","p031","p032","p035","p036"] if c in all_cols]
    )

sector_col = sector_column(df_display)

# ====== Helpers de tabulado ======
def tab_simple(df, col, label=None):
    if (len(_vars_selected)>0) and (col not in _vars_selected):
        return  # respeta filtro
    if col not in df.columns:
        st.caption(f"• {label or col}: no disponible.")
        return
    s = df[col].dropna().astype(str)
    if s.empty:
        st.caption(f"• {label or col}: sin datos.")
        return
    freq = s.value_counts(dropna=False).rename("freq")
    pct = (freq / freq.sum() * 100).rename("pct")
    out = pd.concat([freq, pct.round(1)], axis=1)
    st.subheader(label or col)
    st.dataframe(out, use_container_width=True, height=300)
    st.download_button(f"⬇️ Descargar {label or col}", out.to_csv().encode("utf-8-sig"), f"tab_{col}.csv", "text/csv")

def crosstab(df, rows, cols, normalize='index', label=None):
    if (len(_vars_selected)>0) and ((rows not in _vars_selected) or (cols not in _vars_selected)):
        return  # respeta filtro
    if rows not in df.columns or cols not in df.columns:
        st.caption(f"• {label or (rows+' x '+cols)}: no disponible.")
        return
    x = pd.crosstab(df[rows].astype(str), df[cols].astype(str), normalize=normalize)*100
    x = x.round(1)
    st.subheader(label or f"{rows} × {cols} (%)")
    st.dataframe(x, use_container_width=True, height=360)
    st.download_button(f"⬇️ Descargar {label or (rows+'x'+cols)}", x.to_csv().encode("utf-8-sig"), f"cross_{rows}_{cols}.csv", "text/csv")

def sumstats(df, cols, label=None):
    cols = [c for c in cols if (c in df.columns) and ((len(_vars_selected)==0) or (c in _vars_selected))]
    if not cols:
        st.caption(f"• {label or 'Estadísticos'}: columnas no disponibles o filtradas.")
        return
    dd = df[cols].apply(pd.to_numeric, errors="coerce").describe(percentiles=[.25,.5,.75]).T
    dd["missing_%"] = (1 - df[cols].notna().mean()) * 100
    st.subheader(label or "Estadísticos")
    st.dataframe(dd.round(2), use_container_width=True, height=360)
    st.download_button(f"⬇️ Descargar {label or 'estadisticos'}", dd.to_csv().encode("utf-8-sig"), f"stats_{'_'.join(cols[:3])}.csv", "text/csv")

# ====== Tabs ======
tab1, tab2, tab3 = st.tabs(["📊 Análisis", "📖 Diccionario", "📑 Anexo Estadístico (Plan)"])

# ------------------------- TAB 1: Análisis general -------------------------
with tab1:
    st.title("Dashboard del Territorio")
    st.caption("Filtros, KPIs y mapa. Usa el filtro de variables específicas en la barra lateral para acotar tabulados.")

    # ---------- Filtros ----------
    st.sidebar.subheader("Filtros")
    cats = low_card_cats(df_display)

    # filtro específico de sector/bloque
    if sector_col:
        opt_sector = sorted(df_display[sector_col].dropna().astype(str).unique().tolist())
        pick_sector = st.sidebar.multiselect("Filtrar por Sector/Bloque", opt_sector, default=opt_sector)
    else:
        pick_sector = None

    options_cats = [c for c in cats if c!=sector_col]

    if options_cats:
        other_selected = st.sidebar.multiselect(
            "Otras columnas para filtrar (categóricas)",
            options=options_cats,
            default=[]
        )
    else:
        st.sidebar.caption("No se detectaron columnas categóricas de baja cardinalidad.")
        other_selected = []

    # Aplica filtros
    filtered = df_display.copy()
    if sector_col and pick_sector:
        filtered = filtered[filtered[sector_col].astype(str).isin(pick_sector)]
    for col in other_selected:
        vals = sorted([v for v in filtered[col].dropna().unique().tolist()], key=lambda x: str(x))
        picks = st.sidebar.multiselect(f"{col}", options=vals, default=vals)
        if picks: filtered = filtered[filtered[col].isin(picks)]

    # KPIs
    c1,c2,c3,c4 = st.columns(4)
    with c1: st.metric("Registros (vista)", len(filtered))
    with c2: st.metric("Variables", filtered.shape[1] if not filtered.empty else 0)
    with c3:
        nn = float(filtered.notna().mean().mean()) if not filtered.empty else 0.0
        st.metric("% celdas no nulas (prom.)", f"{nn*100:.1f}%")
    with c4:
        st.metric("Con coordenadas GPS", filtered[["p002__Latitude","p002__Longitude"]].dropna().shape[0] if all(c in filtered.columns for c in ["p002__Latitude","p002__Longitude"]) else 0)

    st.divider()

    # Georreferencia + mapa
    lat_guess, lon_guess = (guess_lat_lon(filtered) if not filtered.empty else (None,None))
    st.sidebar.subheader("Georreferencia")
    lat_col = st.sidebar.selectbox("Columna Latitud", ["(auto)"] + list(filtered.columns), index=(filtered.columns.get_loc(lat_guess)+1 if (not filtered.empty and lat_guess in filtered.columns) else 0))
    lon_col = st.sidebar.selectbox("Columna Longitud", ["(auto)"] + list(filtered.columns), index=(filtered.columns.get_loc(lon_guess)+1 if (not filtered.empty and lon_guess in filtered.columns) else 0))
    if lat_col=="(auto)": lat_col=lat_guess
    if lon_col=="(auto)": lon_col=lon_guess

    st.sidebar.subheader("Mapa")
    map_mode        = st.sidebar.selectbox("Modo", ["Puntos","Heatmap","Hexágonos","Grilla"], index=0)
    color_dim_hint  = st.sidebar.text_input("Color por (categoría, opcional)", sector_col or "")
    pt_size         = st.sidebar.slider("Tamaño de punto", 2, 80, 18, 1)
    pt_opacity      = st.sidebar.slider("Opacidad de punto", 10, 255, 220, 5)
    show_limits     = st.sidebar.checkbox("Mostrar límites", False)
    fill_limits     = st.sidebar.checkbox("Rellenar límites", False)
    limit_opacity   = st.sidebar.slider("Opacidad de límites", 10, 255, 80, 5)

    pts = pd.DataFrame()
    if lat_col and lon_col and not filtered.empty and lat_col in filtered.columns and lon_col in filtered.columns:
        pts = filtered[[lat_col, lon_col]].copy()
        pts[lat_col] = coerce_decimal(pts[lat_col])
        pts[lon_col] = coerce_decimal(pts[lon_col])
        pts = pts.rename(columns={lat_col:"lat", lon_col:"lon"}).dropna(subset=["lat","lon"])

    st.subheader("Mapa")
    if _HAS_PYDECK and not pts.empty:
        layers=[pdk.Layer(
            "ScatterplotLayer",
            data=pts,
            get_position="[lon, lat]",
            get_radius=int(pt_size),
            get_fill_color=[0,128,255,int(pt_opacity)],
            stroked=True,
            get_line_color=[0,0,0,200],
            line_width_min_pixels=0.5,
            pickable=False,
        )]
        st.pydeck_chart(pdk.Deck(
            initial_view_state=pdk.ViewState(latitude=float(pts['lat'].median()), longitude=float(pts['lon'].median()), zoom=12),
            map_style=None,
            layers=layers
        ))
    elif not pts.empty:
        st.map(pts, size=3, zoom=12)
    else:
        st.info("Sin puntos para mostrar. Revisa columnas Lat/Long o filtros.")

    # Tabla & descarga
    st.subheader("Tabla filtrada")
    st.dataframe(filtered, use_container_width=True, height=420)
    st.download_button("⬇️ Descargar CSV filtrado",
        data=filtered.to_csv(index=False).encode("utf-8-sig"),
        file_name="filtrado.csv", mime="text/csv")

# ------------------------- TAB 2: Diccionario -------------------------
with tab2:
    st.title("Diccionario (Codebook)")
    if (df_vars is None or df_vars.empty) and not meta_lc:
        st.info("No se pudo mostrar el codebook (vacío o no cargado).")
    else:
        if df_vars is not None and not df_vars.empty:
            st.subheader("Variables")
            st.dataframe(df_vars.sort_values("variable"), use_container_width=True, height=420)
        vars_disponibles = sorted(list({*list(meta_lc.keys()), *[to_lower(v) for v in (df_vars["variable"] if df_vars is not None and not df_vars.empty else [])]}))
        var_sel = st.selectbox("Elige una variable", options=vars_disponibles) if vars_disponibles else None
        if var_sel:
            row = None
            if df_vars is not None and not df_vars.empty:
                row = df_vars[df_vars["variable"].astype(str).str.strip().str.lower()==var_sel]
            tipo = None; desc = None
            if row is not None and not row.empty:
                tipo = row["tipo"].iloc[0] if "tipo" in row.columns else None
                desc = row["descripcion"].iloc[0] if "descripcion" in row.columns else None
            st.write(f"**Variable:** `{var_sel}`")
            st.write(f"**Tipo:** {tipo if pd.notna(tipo) and tipo not in [None,'nan','None'] else '—'}")
            st.write(f"**Descripción:** {desc if pd.notna(desc) and desc not in [None,'nan','None'] else '—'}")
            mp = meta_lc.get(var_sel, {}).get("map", {})
            if mp:
                df_map = pd.DataFrame({"codigo": list(mp.keys()), "etiqueta": [mp[k] for k in mp.keys()]})
                st.dataframe(df_map, use_container_width=True, height=320)
                st.download_button("⬇️ Descargar mapeo CSV", df_map.to_csv(index=False).encode("utf-8-sig"), f"mapeo_{var_sel}.csv", "text/csv")
            else:
                st.caption("Esta variable no tiene mapeos categóricos registrados en el codebook.")

# ------------------------- TAB 3: Anexo Estadístico (Plan) -------------------------
with tab3:
    st.title("📊 Plan de Tabulados y Cruces – Anexo Estadístico Final")

    if df_display.empty:
        st.info("No hay datos cargados.")
        st.stop()

    # Filtro por Sector/Bloque
    sector_col = sector_column(df_display)
    if sector_col:
        sectores = sorted(df_display[sector_col].dropna().astype(str).unique().tolist())
        sectors_pick = st.multiselect("Sector/Bloque", sectores, default=sectores)
    else:
        st.warning("No se detectó columna de Sector/Bloque. Renombra o indica la columna en el dataset.")
        sectors_pick = None

    df_anx = df_display.copy()
    if sector_col and sectors_pick:
        df_anx = df_anx[df_anx[sector_col].astype(str).isin(sectors_pick)]

    # Heurística de p004 (uso)
    uso_col = "p004" if "p004" in df_anx.columns else next((c for c in df_anx.columns if to_lower(c) in ["uso","uso_estructura","p004_uso"]), None)
    def uso_cat(val):
        s = to_lower(val)
        if s in ["1","vivienda","residencial","hogar"]: return "vivienda"
        if s in ["2","negocio","comercial","empresa"]: return "negocio"
        if s in ["3","mixto","mixta","vivienda/negocio","residencial/comercial"]: return "mixto"
        if "vivi" in s: return "vivienda"
        if "nego" in s or "comer" in s or "emp" in s: return "negocio"
        if "mixt" in s: return "mixto"
        return val

    if uso_col:
        df_anx["_uso_norm"] = df_anx[uso_col].apply(uso_cat)
    else:
        df_anx["_uso_norm"] = np.nan

    # ============ BLOQUE B ============
    st.header("BLOQUE B – Características físicas de la estructura (todos)")
    for v,label in [("p004","Uso de estructura (p004)"),
                    ("p005","Estado físico (p005)"),
                    ("p006","Material del techo (p006)"),
                    ("p007","Material de las paredes (p007)"),
                    ("p008","Material del piso (p008)")]: 
        if v in df_anx.columns: tab_simple(df_anx, v, label)

    # Cruces clave B
    if uso_col and "p005" in df_anx.columns: crosstab(df_anx, uso_col, "p005", label="p004 × p005 (Estado físico por uso)")
    for cc, name in [("p006","techo (p006)"),("p007","paredes (p007)"),("p008","piso (p008)")]:
        if "p005" in df_anx.columns and cc in df_anx.columns:
            crosstab(df_anx, "p005", cc, label=f"p005 × {name}")
        if uso_col and cc in df_anx.columns:
            crosstab(df_anx, uso_col, cc, label=f"p004 × {name}")

    # Subconjunto hogares (vivienda/mixto)
    df_hh = df_anx[df_anx["_uso_norm"].isin(["vivienda","mixto"])].copy()

    # ============ BLOQUE C ============
    st.header("BLOQUE C – Hogares dentro de la estructura (p004 = vivienda o mixto)")
    c_vars = {
        "nvivienda": next((c for c in df_hh.columns if to_lower(c) in ["nvivienda","n_hogares","num_hogares","nro_hogares"]), None),
        "p009a": "p009a" if "p009a" in df_hh.columns else next((c for c in df_hh.columns if "espacio" in to_lower(c) and "habita" in to_lower(c)), None),
        "p009b": "p009b" if "p009b" in df_hh.columns else next((c for c in df_hh.columns if "nivel" in to_lower(c)), None),
        "p010":  "p010"  if "p010"  in df_hh.columns else next((c for c in df_hh.columns if "tenencia" in to_lower(c) or "propiedad" in to_lower(c)), None),
        "p011":  "p011"  if "p011"  in df_hh.columns else next((c for c in df_hh.columns if "personas"==to_lower(c) or to_lower(c) in ["tam_hogar","tamano_hogar","tamaño_hogar"]), None),
    }
    col_sex_jef = next((c for c in df_hh.columns if "sexo" in to_lower(c) and "jef" in to_lower(c)), None)
    sex_m_ad = next((c for c in df_hh.columns if to_lower(c) in ["sexom","mujeres_adultas"]), None)
    sex_h_ad = next((c for c in df_hh.columns if to_lower(c) in ["sexoh","hombres_adultos"]), None)
    sex_nh   = next((c for c in df_hh.columns if to_lower(c) in ["sexonh","ninos","niños"]), None)
    sex_nm   = next((c for c in df_hh.columns if to_lower(c) in ["sexonm","ninas","niñas"]), None)

    # Tabulados simples C
    if c_vars["nvivienda"]: sumstats(df_hh, [c_vars["nvivienda"]], "Nº de hogares (nvivienda) – promedio/mediana/min/máx")
    sumstats(df_hh, [v for v in [c_vars["p009a"], c_vars["p009b"]] if v], "Nº de espacios habitables (p009a) y Nº de niveles (p009b)")
    if c_vars["p010"]: tab_simple(df_hh, c_vars["p010"], "Tenencia del inmueble (p010)")
    if col_sex_jef: tab_simple(df_hh, col_sex_jef, "Sexo de la jefatura (nueva variable)")
    if c_vars["p011"]: sumstats(df_hh, [c_vars["p011"]], "Nº de personas (p011)")
    if any([sex_m_ad, sex_h_ad, sex_nh, sex_nm]):
        sumstats(df_hh, [v for v in [sex_m_ad, sex_h_ad, sex_nh, sex_nm] if v], "Desagregados: mujeres/hombres adultos, niños/niñas")

    # Cruces C
    if col_sex_jef and c_vars["p010"]: crosstab(df_hh, col_sex_jef, c_vars["p010"], label="Sexo jefatura × p010 (tenencia)")
    if col_sex_jef and "p015" in df_hh.columns: crosstab(df_hh, col_sex_jef, "p015", label="Sexo jefatura × p015 (servicios básicos)")
    if col_sex_jef and "p005" in df_hh.columns: crosstab(df_hh, col_sex_jef, "p005", label="Sexo jefatura × p005 (estado físico)")
    if col_sex_jef and "p014" in df_hh.columns: crosstab(df_hh, col_sex_jef, "p014", label="Sexo jefatura × p014 (fuente de ingreso)")
    if col_sex_jef and c_vars["p011"]:
        tmp = df_hh.groupby(col_sex_jef)[c_vars["p011"]].apply(pd.to_numeric, errors="coerce").reset_index(name=c_vars["p011"])
        sumstats(tmp, [c_vars["p011"]], "Sexo jefatura × tamaño del hogar (p011)")
    if c_vars["p010"] and "p015" in df_hh.columns: crosstab(df_hh, c_vars["p010"], "p015", label="Tenencia × servicios básicos")
    if c_vars["p010"] and "p005" in df_hh.columns: crosstab(df_hh, c_vars["p010"], "p005", label="Tenencia × estado físico")

    # ============ BLOQUE D ============
    st.header("BLOQUE D – Situación socioeconómica del hogar (p004 = vivienda o mixto)")
    col_p012 = "p012" if "p012" in df_hh.columns else next((c for c in df_hh.columns if "residen" in to_lower(c) and ("ano" in to_lower(c) or "año" in to_lower(c))), None)
    col_p013 = "p013" if "p013" in df_hh.columns else next((c for c in df_hh.columns if "ingres" in to_lower(c) and "person" in to_lower(c)), None)
    col_p014 = "p014" if "p014" in df_hh.columns else next((c for c in df_hh.columns if "fuente" in to_lower(c) and "ingreso" in to_lower(c)), None)
    col_p022 = "p022" if "p022" in df_hh.columns else next((c for c in df_hh.columns if "activo" in to_lower(c) and "hogar" in to_lower(c)), None)

    if col_p012: sumstats(df_hh, [col_p012], "Año de residencia (p012) – media, mediana")
    if col_p013: sumstats(df_hh, [col_p013], "Nº de personas con ingresos (p013)")
    if col_p014: tab_simple(df_hh, col_p014, "Fuente principal de ingreso (p014)")
    if col_p022: tab_simple(df_hh, col_p022, "Activos del hogar (p022) – distribución")

    if col_p014 and col_sex_jef: crosstab(df_hh, col_p014, col_sex_jef, label="Fuente de ingreso × sexo jefatura")
    if col_p013 and c_vars["p011"]:
        tmp = df_hh[[col_p013, c_vars["p011"]]].apply(pd.to_numeric, errors="coerce").dropna()
        if not tmp.empty:
            tmp["q_tam"] = pd.qcut(tmp[c_vars["p011"]], q=min(4, tmp[c_vars["p011"]].nunique()), duplicates="drop")
            sumstats(tmp.groupby("q_tam")[col_p013].mean().reset_index(name=col_p013), [col_p013], "Nº con ingresos × tamaño de hogar (cuartiles)")
    if col_p022 and c_vars["p010"]: crosstab(df_hh, col_p022, c_vars["p010"], label="Activos × tenencia")
    if col_p022 and "p015" in df_hh.columns: crosstab(df_hh, col_p022, "p015", label="Activos × servicios")

    # ============ BLOQUE E ============
    st.header("BLOQUE E – Acceso a servicios y saneamiento (p004 = vivienda o mixto)")
    for c,name in [("p015","Servicios básicos (p015)"),("p016","Frecuencia acceso agua (p016)"),
                   ("p017","Fuente de agua (p017)"),("p018","Tipo de sanitario (p018)"),
                   ("p019","Uso sanitario (p019)"),("p020","Eliminación aguas grises (p020)"),
                   ("p021","Eliminación basura (p021)")]:
        if c in df_hh.columns: tab_simple(df_hh, c, name)

    if "p015" in df_hh.columns and c_vars["p010"]: crosstab(df_hh, "p015", c_vars["p010"], label="Servicios básicos × tenencia")
    if "p015" in df_hh.columns and col_sex_jef: crosstab(df_hh, "p015", col_sex_jef, label="Servicios básicos × sexo jefatura")
    if "p015" in df_hh.columns and "p005" in df_hh.columns: crosstab(df_hh, "p015", "p005", label="Servicios básicos × estado físico")
    if "p016" in df_hh.columns and "p017" in df_hh.columns: crosstab(df_hh, "p016", "p017", label="Frecuencia acceso agua × fuente de agua")
    if "p018" in df_hh.columns and "p019" in df_hh.columns: crosstab(df_hh, "p018", "p019", label="Tipo sanitario × uso sanitario")
    if "p020" in df_hh.columns and "p021" in df_hh.columns: crosstab(df_hh, "p020", "p021", label="Eliminación aguas grises × eliminación basura")

    # ============ BLOQUE F ============
    st.header("BLOQUE F – Negocios (p004 = negocio o mixto)")
    df_neg = df_anx[df_anx["_uso_norm"].isin(["negocio","mixto"])].copy()
    for c,name in [("p025","Actividad principal (p025)"),("p026","Tiempo de operación (p026)"),
                   ("p027","Permisos de operación (p027)"),("p028","Tenencia local (p028)"),
                   ("p029","Nº trabajadores (p029)"),("p030","Nº empleados formales (p030)"),
                   ("p031","Ingreso mensual empleados (p031)")]:
        if c in df_neg.columns:
            if c in ["p026","p029","p030","p031"]:
                sumstats(df_neg, [c], name)
            else:
                tab_simple(df_neg, c, name)

    if "p032" in df_neg.columns: tab_simple(df_neg, "p032", "Activos negocio (p032)")

    # Cruces F
    if "p025" in df_neg.columns and "p027" in df_neg.columns: crosstab(df_neg, "p025", "p027", label="Actividad × permisos")
    if "p027" in df_neg.columns and "p028" in df_neg.columns: crosstab(df_neg, "p027", "p028", label="Permisos × tenencia local")
    if "p030" in df_neg.columns and "p029" in df_neg.columns:
        tmp = df_neg[["p029","p030"]].apply(pd.to_numeric, errors="coerce").dropna()
        if not tmp.empty:
            tmp["formales_%"] = np.where(tmp["p029"]>0, tmp["p030"]/tmp["p029"]*100, np.nan)
            sumstats(tmp, ["formales_%"], "Nº empleados formales × total trabajadores")
    if "p026" in df_neg.columns and "p027" in df_neg.columns: crosstab(df_neg, "p026", "p027", label="Tiempo de operación × permisos")
    if "p031" in df_neg.columns and "p027" in df_neg.columns: crosstab(df_neg, "p031", "p027", label="Ingreso mensual × permisos")

    # ============ BLOQUE G ============
    st.header("BLOQUE G – Espacios públicos y percepción (todos)")
    for c,name in [("p036","Percepción de seguridad (p036)"),
                   ("p035","Condiciones del espacio (p035)"),
                   ("p035tx","Problemas identificados (p035tx)")]:  # p035tx se sugiere codificar fuera
        if c in df_anx.columns:
            if c.endswith("tx"):
                st.caption("Variables abiertas (muestra):")
                muestras = df_anx[c].dropna().astype(str).unique().tolist()[:50]
                if muestras: st.write(muestras)
            else:
                tab_simple(df_anx, c, name)

    if uso_col and "p036" in df_anx.columns: crosstab(df_anx, "p036", uso_col, label="Percepción seguridad × uso de estructura")
    if "p036" in df_hh.columns and col_sex_jef: crosstab(df_hh, "p036", col_sex_jef, label="Percepción seguridad × sexo jefatura")
    if "p035" in df_anx.columns and "p035tx" in df_anx.columns: crosstab(df_anx, "p035", "p035tx", label="Condiciones espacio × problemas identificados")

    # ============ BLOQUE I ============
    st.header("BLOQUE I – Indicadores clave para resumen ejecutivo")
    ind = {}
    # % estructuras en mal estado
    if "p005" in df_anx.columns:
        s = df_anx["p005"].astype(str).str.lower()
        ind["% estructuras en mal estado"] = (s.str.contains("mal") | s.str.contains("defici")).mean()*100
    # % hogares con jefatura femenina
    if col_sex_jef and col_sex_jef in df_hh.columns:
        s = df_hh[col_sex_jef].astype(str).str.lower()
        ind["% hogares con jefatura femenina"] = s.str.contains("fem").mean()*100
    # % hogares con tenencia precaria
    if c_vars["p010"] and c_vars["p010"] in df_hh.columns:
        s = df_hh[c_vars["p010"]].astype(str).str.lower()
        ind["% hogares con tenencia precaria"] = s.str.contains("precari|ocup|cedid|informal|invad").mean()*100
    # % hogares sin acceso a agua potable
    if "p015" in df_hh.columns:
        s = df_hh["p015"].astype(str).str.lower()
        ind["% hogares sin acceso a agua potable"] = (~s.str.contains("agua|acueduct|potab")).mean()*100
    # % hogares con saneamiento inadecuado
    if "p018" in df_hh.columns:
        s = df_hh["p018"].astype(str).str.lower()
        ind["% hogares con saneamiento inadecuado"] = s.str.contains("ningun|ningún|letrina|impro").mean()*100
    # % negocios sin permisos
    if "p027" in df_neg.columns:
        s = df_neg["p027"].astype(str).str.lower()
        ind["% negocios sin permisos"] = (~s.str.contains("si|sí|permiso")).mean()*100
    # Promedio activos por hogar y negocio
    if "p022" in df_hh.columns:
        ind["Promedio activos por hogar"] = pd.to_numeric(df_hh["p022"], errors="coerce").mean()
    if "p032" in df_neg.columns:
        ind["Promedio activos por negocio"] = pd.to_numeric(df_neg["p032"], errors="coerce").mean()
    # % negocios con personal formalizado
    if "p029" in df_neg.columns and "p030" in df_neg.columns:
        tmp = df_neg[["p029","p030"]].apply(pd.to_numeric, errors="coerce").dropna()
        if not tmp.empty:
            ind["% negocios con personal formalizado"] = (np.where(tmp["p029"]>0, tmp["p030"]/tmp["p029"], np.nan).mean())*100

    if ind:
        card_cols = st.columns(min(4, len(ind)))
        i=0
        for k,v in ind.items():
            with card_cols[i%len(card_cols)]:
                if isinstance(v, (int,float)) and not pd.isna(v):
                    if 'Promedio' in k:
                        st.metric(k, f"{v:.1f}")
                    else:
                        st.metric(k, f"{v:.1f}%")
                else:
                    st.metric(k, "—")
            i+=1
    else:
        st.caption("No fue posible calcular indicadores (faltan variables).")
