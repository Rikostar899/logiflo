import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import datetime
import re
import tempfile
import os
import math
import time
import requests
import concurrent.futures
import base64
import json
import io
from fpdf import FPDF
from openai import OpenAI
import gspread
try:
    from supabase import create_client as _supa_create
except Exception:
    _supa_create = None
from google.oauth2.service_account import Credentials

# ══ IMPORTS MODULES ══════════════════════════════════════════════
from config.plans import USERS_PLAN, PLAN_LIMITS, USERS_DB, get_user_plan, can_access, show_lock, audit_counter_sidebar
from config.sectoral_db import SECTORAL_DB, detect_sector, get_sector_benchmarks
from config.translations import T, _
from services.supabase_client import get_supabase, save_audit_to_sheets, load_archives_from_sheets, load_user_prefs, save_user_prefs, get_historique_audits
from services.news import render_news_widget, get_sector_news
from engine.ingester import SYNONYMES, nettoyer, smart_ingester_stock_ultime, auto_map_columns_with_ai, detect_transport_mode, super_clean, detect_periode
from engine.ai_analysis import generate_ai_analysis, format_historique_pour_prompt
from engine.scoring import compute_logiflo_score
from engine.pdf_gen import PDFReport, _s, _asc, predict_ruptures, format_predictions_pour_prompt, compute_alerte_bfr, render_prediction_rupture, tooltip_metric, generate_exemple_excel, generate_free_pdf, generate_expert_pdf
from engine.routing import calculate_haversine, fetch_geo, geocode_cities_mapbox, _ors_distance, fetch_route, smart_multimodal_router

st.set_page_config(page_title="LOGIFLO.IO | Control Tower", layout="wide", page_icon="🏢")

try:
    client = OpenAI(api_key=st.secrets.get("OPENAI_API_KEY", ""))
    ORS_API_KEY = st.secrets.get("ORS_API_KEY", "")
    SHEET_ID = st.secrets.get("GOOGLE_SHEET_ID", "")
except Exception:
    client = None
    ORS_API_KEY = ""
    SHEET_ID = ""

# ══ SESSION STATE ═════════════════════════════════════════════════
for k, v in {
    "page": "accueil", "module": "", "auth": False, "current_user": None,
    "language": "fr",
    "df_stock_manager": None, "df_stock_terrain": None, "df_trans": None,
    "history_stock": [], "stock_view": "MANAGER",
    "seuil_bas": 15, "seuil_rupture": 0, "seuil_km": 0,
    "geo_cache": {}, "route_cache": {}, "trans_mapping": None, "trans_filename": None,
    "analysis_stock_manager": None, "analysis_stock_terrain": None, "analysis_trans": None,
    "last_pdf": None, "last_kpis": [], "last_labels": [],
    "trans_mode_detected": None, "audit_gratuit_done": False,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ══ CSS ═══════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');
:root{--navy:#0B2545;--navy2:#162D52;--green:#00C896;--green2:#00A87A;--slate:#4A6080;--light:#F0F4F8;--red:#E8304A;--orange:#f39c12;--white:#FFFFFF;}
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;color:var(--navy);}
.block-container{padding-top:2rem!important;padding-bottom:2rem!important;max-width:95%!important;}
.kpi-card{background:var(--white);padding:24px;border-radius:12px;border:1px solid #e2e8f0;border-top:3px solid var(--green);box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);}
.kpi-card h4{color:var(--slate)!important;font-size:0.75rem!important;text-transform:uppercase;font-weight:600;letter-spacing:1.5px;margin-bottom:10px;}
.kpi-card h2{font-family:'Syne',sans-serif!important;font-size:2.2rem!important;font-weight:800!important;margin-top:0;line-height:1;}
div.stButton>button{border-radius:8px;font-family:'Syne',sans-serif;font-weight:700;background-color:var(--navy);color:#f8fafc;border:none;}
[data-testid="stSidebar"]{background-color:var(--navy)!important;}
[data-testid="stSidebar"] *{color:#ffffff!important;}
.sidebar-logo{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;color:white;}
.sidebar-logo span{color:#00C896;}
.import-card{background:var(--white);padding:25px;border-radius:12px;border-left:6px solid var(--green);margin-bottom:20px;}
.report-text{background:var(--light);padding:32px;border-radius:12px;border-left:6px solid var(--navy);line-height:1.8;}
.report-text h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:800;color:var(--navy);text-transform:uppercase;letter-spacing:1.5px;margin-top:28px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.report-terrain{background:#f8fff8;padding:28px;border-radius:12px;border-left:6px solid var(--green);line-height:1.9;}
.report-terrain h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:700;color:var(--green2);margin-top:24px;margin-bottom:8px;}
.archive-card{background:var(--white);border:1px solid #E2EAF4;border-radius:12px;padding:20px;margin-bottom:16px;border-left:4px solid var(--green);}
.legal-text{background:var(--white);padding:32px;border-radius:12px;border:1px solid #E2EAF4;line-height:1.9;}
.legal-text h2{font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:800;color:var(--navy);margin-top:28px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.sans-prix-badge{background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:#00A87A;font-size:12px;font-weight:600;padding:4px 12px;border-radius:20px;display:inline-block;margin-bottom:12px;margin-right:8px;}
.mode-badge{display:inline-flex;align-items:center;gap:8px;background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:#00A87A;font-size:13px;font-weight:600;padding:8px 16px;border-radius:8px;margin-bottom:16px;}
.big-emoji{font-size:70px;margin-bottom:10px;display:block;text-align:center;}
</style>
""", unsafe_allow_html=True)


# ══ HELPERS ══════════════════════════════════════════════════════
def render_report(texte, mode="manager"):
    css = "report-terrain" if mode == "terrain" else "report-text"
    lines = []
    for line in texte.split('\n'):
        line = line.strip()
        if not line: continue
        if line.startswith('### '):
            lines.append(f"<h3>{line[4:].strip()}</h3>")
        else:
            line = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', line)
            prefix = "• " if (line.startswith('- ') or line.startswith('* ')) else ""
            body = line[2:] if prefix else line
            lines.append(f"<p>{prefix}{body}</p>")
    return f'<div class="{css}">{"".join(lines)}</div>'


class StepProgress:
    def __init__(self, steps, text=None):
        self._ph = st.empty()
        self._n  = max(len(steps), 1)
        self._i  = 0
        lang = st.session_state.get("language", "fr")
        self._txt = text or ("Computing..." if lang == "en" else "Calcul en cours...")
        self._ph.progress(0, text=self._txt)
    def step(self, label=None):
        self._i += 1
        self._ph.progress(min(self._i / self._n, 1.0), text=self._txt)
    def done(self):
        self._ph.empty()


# ══ PAGES ════════════════════════════════════════════════════════
if st.session_state.page == "accueil":
    st.markdown(f"<h1 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;font-weight:800;'>{_('home_title')}</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center;font-size:1.1em;color:#4A6080;'>{_('home_sub')}</p><br>", unsafe_allow_html=True)
    _c1, lc, _c2 = st.columns([3, 1, 3])
    with lc:
        lang_choice = st.selectbox("", ["🇫🇷 Français", "🇬🇧 English"], key="lang_accueil", label_visibility="collapsed")
        st.session_state.language = "en" if "English" in lang_choice else "fr"
    st.markdown("<br>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("<span class='big-emoji'>📦</span>", unsafe_allow_html=True)
        if st.button(_("home_stock"), use_container_width=True):
            st.session_state.module = "stock"; st.session_state.page = "choix_profil_stock"; st.rerun()
    with c2:
        st.markdown("<span class='big-emoji'>🌍</span>", unsafe_allow_html=True)
        if st.button(_("home_transport"), use_container_width=True):
            st.session_state.module = "transport"; st.session_state.page = "login"; st.rerun()
    st.markdown("<br><br>", unsafe_allow_html=True)
    _c1, cm, _c2 = st.columns([1, 1, 1])
    if cm.button(_("home_access"), use_container_width=True):
        st.session_state.page = "contact"; st.rerun()
    _ca1, _cf, _ca2 = st.columns([1, 2, 1])
    _free_label = "→ Launch my free audit" if st.session_state.get("language") == "en" else "→ Lancer mon audit gratuit"
    if _cf.button(_free_label, use_container_width=True, key="btn_free_home"):
        st.session_state.page = "audit_gratuit"; st.rerun()

elif st.session_state.page == "contact":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>{_('contact_title')}</h2>", unsafe_allow_html=True)
    _c1, cc, _c2 = st.columns([1, 1.5, 1])
    with cc:
        with st.form("vip"):
            st.text_input(_("contact_name")); st.text_input(_("contact_email")); st.text_input(_("contact_company"))
            st.selectbox(_("contact_volume"), [_("vol1"), _("vol2"), _("vol3")])
            st.selectbox(_("contact_issue"), [_("iss1"), _("iss2"), _("iss3")])
            if st.form_submit_button(_("contact_btn"), use_container_width=True):
                st.success(_("contact_ok"))
        if st.button(_("login_back"), use_container_width=True):
            st.session_state.page = "accueil"; st.rerun()

elif st.session_state.page == "audit_gratuit":
    lang_ag = st.session_state.get("language", "fr")
    st.markdown("<h1 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;font-weight:800;'>" + ("Free Audit" if lang_ag == "en" else "Audit Gratuit") + "</h1>", unsafe_allow_html=True)
    if st.session_state.get("audit_gratuit_done"):
        st.warning("You have already used your free audit." if lang_ag == "en" else "Vous avez deja utilise votre audit gratuit.")
        if st.button("Back" if lang_ag == "en" else "Retour", use_container_width=True):
            st.session_state.page = "accueil"; st.rerun()
    else:
        _fmc = st.radio("", ["Stock", "Transport"], horizontal=True, label_visibility="collapsed")
        _fmod = "stock" if "Stock" in _fmc else "transport"
        _upf = st.file_uploader("Fichier Excel ou CSV" if lang_ag == "fr" else "Excel or CSV file", type=["csv", "xlsx"])
        if _upf:
            try:
                _dff = pd.read_excel(_upf) if _upf.name.endswith("xlsx") else pd.read_csv(_upf, encoding="utf-8")
            except Exception:
                _upf.seek(0); _dff = pd.read_csv(_upf, encoding="latin-1")
            if _fmod == "stock":
                _dfok, _st2 = smart_ingester_stock_ultime(_dff, client_ai=client)
                if _dfok is None:
                    st.error(_st2)
                else:
                    _sp = bool(_dfok.get("_sans_prix", pd.Series([True])).iloc[0]) if "_sans_prix" in _dfok.columns else True
                    _dfok["valeur_totale"] = _dfok["quantite"] * _dfok["prix_unitaire"]
                    _vt = _dfok["valeur_totale"].sum()
                    _rf = _dfok[_dfok["quantite"] <= 0]
                    _txf = (1 - len(_rf) / max(len(_dfok), 1)) * 100
                    _fkpis = [_vt if not _sp else float(len(_dfok)), _txf, float(len(_rf))]
                    _flbl = ["Capital EUR" if not _sp else "Articles", "Service %", "Ruptures"]
                    with st.spinner("Analyse IA..." if lang_ag == "fr" else "AI Analysis..."):
                        _fsum = generate_ai_analysis(f"Items:{len(_dfok)}. Service:{_txf:.1f}%. Stockouts:{len(_rf)}.")
                    a1, a2, a3 = st.columns(3)
                    a1.metric("Capital" if not _sp else "Articles", f"{_fkpis[0]:,.0f}")
                    a2.metric("Service", f"{_txf:.1f}%")
                    a3.metric("Ruptures", str(len(_rf)))
                    st.markdown(render_report(_fsum, "manager"), unsafe_allow_html=True)
                    _fpdf = generate_free_pdf("stock", _fsum, _fkpis, _flbl)
                    st.download_button("Telecharger (PDF)" if lang_ag == "fr" else "Download (PDF)", _fpdf, "Audit_Gratuit_Logiflo.pdf", use_container_width=True)
                    st.session_state.audit_gratuit_done = True
            else:
                _mapf = auto_map_columns_with_ai(_dff, client_ai=client)
                def _colf(k): return _mapf.get(k) if _mapf.get(k) in _dff.columns else None
                _caf = _colf("ca"); _cof = _colf("co")
                if not _cof:
                    for _cc in _dff.columns:
                        if any(k in str(_cc).lower() for k in ["cout", "cost", "achat"]): _cof = _cc; break
                if not _cof:
                    st.error("Colonne cout introuvable." if lang_ag == "fr" else "Cost column not found.")
                else:
                    _dff["_CO"] = _dff[_cof].apply(super_clean)
                    _dff["_CA"] = _dff[_caf].apply(super_clean) if _caf else _dff["_CO"] / 0.85
                    _dff["_MG"] = _dff["_CA"] - _dff["_CO"]
                    _mgt = _dff["_MG"].sum(); _cat = _dff["_CA"].sum()
                    _txt = (_mgt / _cat * 100) if _cat > 0 else 0
                    _toxt = len(_dff[_dff["_MG"] < 0])
                    _fkpis = [_mgt, _txt, float(_toxt)]
                    _flbl = ["Marge EUR", "Taux %", "Deficitaires"]
                    with st.spinner("Analyse IA..." if lang_ag == "fr" else "AI Analysis..."):
                        _fsum = generate_ai_analysis(f"Routes:{len(_dff)}. Margin:{_mgt:.0f} EUR. Rate:{_txt:.1f}%.")
                    a1, a2, a3 = st.columns(3)
                    a1.metric("Marge", f"{_mgt:,.0f} EUR"); a2.metric("Taux", f"{_txt:.1f}%"); a3.metric("Deficitaires", str(_toxt))
                    st.markdown(render_report(_fsum, "manager"), unsafe_allow_html=True)
                    _fpdf = generate_free_pdf("transport", _fsum, _fkpis, _flbl)
                    st.download_button("Telecharger (PDF)" if lang_ag == "fr" else "Download (PDF)", _fpdf, "Audit_Gratuit_Transport.pdf", use_container_width=True)
                    st.session_state.audit_gratuit_done = True
        if st.button("Retour" if lang_ag == "fr" else "Back", use_container_width=True, key="back_free"):
            st.session_state.page = "accueil"; st.rerun()

elif st.session_state.page == "choix_profil_stock":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>{_('profile_title')}</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center;color:#4A6080;'>{_('profile_sub')}</p><br><br>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("<span class='big-emoji'>📊</span>", unsafe_allow_html=True)
        if st.button(_("profile_mgr"), use_container_width=True):
            st.session_state.stock_view = "MANAGER"; st.session_state.page = "login"; st.rerun()
    with c2:
        st.markdown("<span class='big-emoji'>👷</span>", unsafe_allow_html=True)
        if st.button(_("profile_ops"), use_container_width=True):
            st.session_state.stock_view = "TERRAIN"; st.session_state.page = "login"; st.rerun()

elif st.session_state.page == "login":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>Acces Securise -- {st.session_state.module.upper()}</h2><br>", unsafe_allow_html=True)
    _c1, cl, _c2 = st.columns([1, 1.2, 1])
    with cl:
        with st.form("login_form"):
            u = st.text_input(_("login_id")); p = st.text_input(_("login_pw"), type="password")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.form_submit_button(_("login_btn"), use_container_width=True):
                if u in USERS_DB and USERS_DB[u] == p:
                    st.session_state.auth = True; st.session_state.current_user = u
                    try:
                        _prefs_l = load_user_prefs(u)
                        if _prefs_l:
                            if _prefs_l.get("company_name"): st.session_state["company_name"] = _prefs_l["company_name"]
                            if _prefs_l.get("language"):     st.session_state["language"] = _prefs_l["language"]
                            if _prefs_l.get("seuil_rupture") is not None: st.session_state["seuil_rupture"] = int(_prefs_l["seuil_rupture"])
                    except Exception:
                        pass
                    st.session_state.page = "app"; st.rerun()
                else:
                    st.error(_("login_err"))
        if st.button(_("login_back"), use_container_width=True):
            st.session_state.page = "accueil"; st.rerun()

elif st.session_state.auth and st.session_state.page == "app":
    with st.sidebar:
        _plan_sb   = get_user_plan(st.session_state.current_user)
        _pinfo_sb  = PLAN_LIMITS.get(_plan_sb, PLAN_LIMITS["starter"])
        audit_counter_sidebar(st.session_state.current_user, _plan_sb)
        st.markdown(f"""<div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;"><div class="sidebar-logo">LOGI<span>FLO</span>.IO</div></div>
        <div style="font-size:12px;color:#4A6080;margin-bottom:6px;">👤 {st.session_state.current_user}</div>
        <div style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:20px;margin-bottom:10px;background:{_pinfo_sb['bg']};color:{_pinfo_sb['color']};border:1px solid {_pinfo_sb['color']}40;font-size:10px;font-weight:700;">{_pinfo_sb['icon']} {_pinfo_sb['label']}</div>""", unsafe_allow_html=True)
        st.markdown("---")
        _is_terrain = (st.session_state.get("stock_view", "") == "TERRAIN")
        _nav_items = ([_("nav_workspace"), _("nav_archives"), _("nav_params"), _("nav_legal")]
                      if _is_terrain else
                      [_("nav_dashboard"), _("nav_workspace"), _("nav_archives"), _("nav_compte"), _("nav_params"), _("nav_legal")])
        nav = st.radio("", _nav_items, label_visibility="collapsed")
        st.markdown("---")
        if st.button(_("nav_logout"), use_container_width=True):
            st.session_state.clear(); st.rerun()
        st.markdown("<div style='margin-top:40px;border-top:1px solid #1e3a5f;padding-top:14px;font-size:11px;color:#4A6080;'>© 2026 Logiflo B2B Enterprise</div>", unsafe_allow_html=True)

    # ══ DASHBOARD ══════════════════════════════════════════════════
    if nav == _("nav_dashboard"):
        lang_d     = st.session_state.get("language", "fr")
        username_d = st.session_state.current_user
        _df_arch   = load_archives_from_sheets(username_d)
        if _df_arch is not None and not _df_arch.empty:
            if "created_at" in _df_arch.columns and "date" not in _df_arch.columns:
                _df_arch["date"]  = pd.to_datetime(_df_arch["created_at"], errors="coerce").dt.strftime("%d/%m/%Y")
                _df_arch["heure"] = pd.to_datetime(_df_arch["created_at"], errors="coerce").dt.strftime("%H:%M")
            for _c in ["module","date","heure","kpi_1","kpi_2","kpi_3","kpi_label_1","kpi_label_2","kpi_label_3","resume_ia"]:
                if _c not in _df_arch.columns: _df_arch[_c] = ""
            for _c in ["kpi_1","kpi_2","kpi_3"]:
                _df_arch[_c] = pd.to_numeric(_df_arch[_c], errors="coerce").fillna(0)
        import datetime as _dt_dash
        _hour_d = _dt_dash.datetime.now().hour
        _greet  = ("Good morning" if _hour_d < 12 else "Good afternoon" if _hour_d < 18 else "Good evening") if lang_d == "en" else ("Bonjour" if _hour_d < 18 else "Bonsoir")
        _tagline = "Your supply chain at a glance" if lang_d == "en" else "Votre supply chain en un coup d'oeil"
        st.markdown(f"""<div style="background:linear-gradient(135deg,#0B2545 0%,#0f2f5a 100%);border-radius:14px;padding:22px 28px;margin-bottom:20px;border-left:4px solid #00C896;">
        <div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;color:white;margin-bottom:3px;">{_greet}, <span style="color:#00C896;">{username_d}</span></div>
        <div style="font-size:12px;color:rgba(255,255,255,0.5);">{_tagline}</div></div>""", unsafe_allow_html=True)

        if _df_arch is None or _df_arch.empty:
            st.info("Aucun audit encore. Lancez votre premier audit." if lang_d == "fr" else "No audit yet. Launch your first audit.")
        else:
            for _mod_al in _df_arch["module"].unique():
                _df_al = _df_arch[_df_arch["module"] == _mod_al].tail(2)
                if len(_df_al) >= 2:
                    try:
                        _dv = float(_df_al.iloc[1]["kpi_2"]) - float(_df_al.iloc[0]["kpi_2"])
                        _lbl2_al = str(_df_al.iloc[1].get("kpi_label_2", ""))
                        if _dv < -3:
                            _ico_al = "📦" if _mod_al == "stock" else "🚚"
                            st.warning(f"⚠️ {_ico_al} {_lbl2_al} : baisse de **{abs(_dv):.1f} pts** depuis le dernier audit")
                    except Exception:
                        pass

            _col_l, _col_r = st.columns(2)
            _mod_actif = st.session_state.get("module", "stock")

            if _mod_actif == "stock":
                with _col_l:
                    try:
                        _dfs_pie = _df_arch[_df_arch["module"] == "stock"].copy()
                        if not _dfs_pie.empty:
                            _sort_c = "created_at" if "created_at" in _dfs_pie.columns else "date"
                            _dfs_pie = _dfs_pie.sort_values(_sort_c, ascending=True)
                            _lp = _dfs_pie.iloc[-1]
                            _k2p = float(_lp.get("kpi_2", 0)); _k3p = float(_lp.get("kpi_3", 0))
                            _l2p = str(_lp.get("kpi_label_2", "Service")); _l3p = str(_lp.get("kpi_label_3", "Ruptures"))
                            _date_pie = str(_lp.get("date", "") or str(_lp.get("created_at", ""))[:10])
                            _ok = max(0.1, 100 - _k2p - min(_k3p*5, 30))
                            _fig_p = go.Figure(go.Pie(
                                labels=[_l2p, _l3p, "Sain"],
                                values=[_k2p, max(_k3p, 0.1), max(_ok, 0.1)],
                                hole=0.45,
                                marker=dict(colors=["#00C896","#E8304A","#E2E8F0"], line=dict(color="white", width=2)),
                                hovertemplate="%{label}: <b>%{value:.1f}%</b><extra></extra>",
                            ))
                            _fig_p.update_layout(title=dict(text=f"📦 Stock -- {_date_pie}", font=dict(size=12,color="#0B2545"), x=0),
                                                  legend=dict(font=dict(size=9),orientation="h",yanchor="bottom",y=-0.25),
                                                  margin=dict(t=36,b=50,l=0,r=0), height=230, paper_bgcolor="white")
                            st.plotly_chart(_fig_p, use_container_width=True, config={"displayModeBar": False})
                    except Exception:
                        pass

            with _col_r:
                try:
                    for _mc in ["stock", "transport"]:
                        _dfc = _df_arch[_df_arch["module"] == _mc].copy()
                        if len(_dfc) < 2: continue
                        _sort_c2 = "created_at" if "created_at" in _dfc.columns else "date"
                        _dfc = _dfc.sort_values(_sort_c2, ascending=True).reset_index(drop=True)
                        for _c in ["kpi_1","kpi_2","kpi_3"]:
                            _dfc[_c] = pd.to_numeric(_dfc[_c], errors="coerce").fillna(0)
                        _l2c = str(_dfc["kpi_label_2"].iloc[-1])
                        _clr = "#00C896" if _mc == "stock" else "#F39C12"
                        _dates_c = [str(d)[:10] for d in _dfc["date"].tolist()] if "date" in _dfc.columns else [str(i) for i in range(len(_dfc))]
                        _fig_c = go.Figure()
                        _fig_c.add_trace(go.Scatter(
                            x=list(range(len(_dfc))), y=_dfc["kpi_2"].tolist(),
                            mode="lines+markers", line=dict(color=_clr, width=2.5),
                            marker=dict(size=9, color=_clr, line=dict(color="white", width=2)),
                            fill="tozeroy", fillcolor="rgba(0,200,150,0.08)" if _mc == "stock" else "rgba(243,156,18,0.08)",
                        ))
                        _fig_c.update_layout(
                            title=dict(text=f"📈 {_l2c} -- {len(_dfc)} audits", font=dict(size=12,color="#0B2545"), x=0),
                            xaxis=dict(tickmode="array",tickvals=list(range(len(_dfc))),ticktext=[d[:5] for d in _dates_c],tickfont=dict(size=8),showgrid=False),
                            yaxis=dict(tickfont=dict(size=8),gridcolor="rgba(0,0,0,0.04)"),
                            plot_bgcolor="white", paper_bgcolor="white",
                            margin=dict(t=36,b=20,l=30,r=10), height=230, showlegend=False,
                        )
                        st.plotly_chart(_fig_c, use_container_width=True, config={"displayModeBar": False})
                        break
                except Exception:
                    pass

            st.markdown("<br>", unsafe_allow_html=True)
            try:
                _sort_col = "created_at" if "created_at" in _df_arch.columns else "date"
                _lr = _df_arch.sort_values(_sort_col, ascending=False).iloc[0]
                _resume_lr = str(_lr.get("resume_ia", "")).strip()
                _date_lr   = str(_lr.get("date", "") or str(_lr.get("created_at", ""))[:10])
                _k2_lr     = float(_lr.get("kpi_2", 0))
                _l2_lr     = str(_lr.get("kpi_label_2", ""))
                _mod_lr    = str(_lr.get("module", ""))
                _ico_lr    = "📦" if _mod_lr == "stock" else "🚚"
                _clr_lr    = "#00C896" if _k2_lr >= 90 else ("#F39C12" if _k2_lr >= 75 else "#E8304A")
                import re as _re_t
                _tasks = []
                _in_action = False
                for _ln in _resume_lr.split("\n"):
                    _ln = _ln.strip()
                    if not _ln or _ln.lower() in ("none","nan","null","n/a","-","---"): continue
                    if any(k in _ln.upper() for k in ["A FAIRE","PRIORIT","ACTION","RECOMMAND","NEXT STEP","TO DO"]):
                        if _ln.startswith("#"): _in_action = True; continue
                    if _in_action and _ln.startswith("#"): _in_action = False
                    _ln_c = _ln.replace("**","").replace("*","").strip()
                    if len(_ln_c) < 15: continue
                    if _re_t.match(r"^[-•*→✅]\s+.{15,}", _ln_c):
                        _t = _ln_c.lstrip("-•*→✅ ").strip()
                        if _t: _tasks.append(_t)
                    elif _in_action and _re_t.match(r"^\d+\.\s+.{15,}", _ln_c):
                        _t = _re_t.sub(r"^\d+\.\s+", "", _ln_c).strip()
                        if _t: _tasks.append(_t)
                _tasks = [t[:130] for t in _tasks if t and len(t) > 15][:6]
                _lbl_tasks = f"{_ico_lr} A faire -- {_date_lr} · {_l2_lr} : {_k2_lr:.1f}%"
                st.markdown(f'<div style="font-size:11px;font-weight:700;color:#4A6080;letter-spacing:2px;text-transform:uppercase;margin-bottom:8px;">✅ {_lbl_tasks}</div>', unsafe_allow_html=True)
                if _tasks:
                    _tk = f"tasks_{username_d}_{_date_lr}"
                    if _tk not in st.session_state: st.session_state[_tk] = [False]*len(_tasks)
                    while len(st.session_state[_tk]) < len(_tasks): st.session_state[_tk].append(False)
                    _done = sum(st.session_state[_tk]); _pct = int((_done/len(_tasks))*100)
                    st.markdown(f'<div style="background:white;border:1px solid #E2E8F0;border-radius:10px;padding:12px 16px;margin-bottom:8px;"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:6px;"><span style="color:#4A6080;">{_done}/{len(_tasks)} {"done" if lang_d=="en" else "terminees"}</span><span style="color:{_clr_lr};font-weight:700;">{_pct}%</span></div><div style="height:4px;background:#F0F4F8;border-radius:99px;overflow:hidden;"><div style="height:100%;width:{_pct}%;background:{_clr_lr};border-radius:99px;"></div></div></div>', unsafe_allow_html=True)
                    for _ti, _tt in enumerate(_tasks):
                        _chk = st.session_state[_tk][_ti]
                        _new = st.checkbox(_tt, value=_chk, key=f"task_{_tk}_{_ti}")
                        if _new != _chk: st.session_state[_tk][_ti] = _new; st.rerun()
            except Exception:
                pass

            if can_access("news"):
                try:
                    _sec = "generique"
                    if "module" in _df_arch.columns:
                        _lm2 = _df_arch["module"].dropna().iloc[-1] if len(_df_arch) > 0 else ""
                        _sec = "transport_routier" if str(_lm2) == "transport" else "stock_distribution"
                    render_news_widget(_sec, lang=lang_d)
                except Exception:
                    pass
            else:
                show_lock("news")

    # ══ MON COMPTE ═════════════════════════════════════════════════
    elif nav == _("nav_compte"):
        lang_c = st.session_state.get("language", "fr")
        username_c = st.session_state.current_user
        plan_c = get_user_plan(username_c)
        plan_info_c = PLAN_LIMITS.get(plan_c, PLAN_LIMITS["starter"])
        st.markdown(f"""<div style="background:linear-gradient(135deg,#0B2545 0%,#0f2f5a 100%);border-radius:14px;padding:22px 24px;margin-bottom:20px;display:flex;align-items:center;gap:16px;">
        <div style="width:52px;height:52px;border-radius:50%;background:#00C896;display:flex;align-items:center;justify-content:center;font-family:Syne,sans-serif;font-size:18px;font-weight:800;color:#0B2545;">{username_c[:2].upper()}</div>
        <div><div style="font-family:Syne,sans-serif;font-size:18px;font-weight:800;color:white;">{username_c}</div></div>
        <div style="margin-left:auto;padding:4px 12px;border-radius:20px;background:{plan_info_c['bg']};color:{plan_info_c['color']};font-size:11px;font-weight:700;">{plan_info_c['icon']} {plan_info_c['label']}</div></div>""", unsafe_allow_html=True)
        with st.form("form_company"):
            _company_new = st.text_input("Nom d'entreprise" if lang_c=="fr" else "Company name", value=st.session_state.get("company_name",""))
            _sector_opt = ["Transport","Distribution","Industrie","Agroalimentaire","Pharma","Retail","BTP","Autre"]
            _saved_sector = st.session_state.get("company_sector","")
            _sector_new = st.selectbox("Secteur" if lang_c=="fr" else "Sector", _sector_opt, index=_sector_opt.index(_saved_sector) if _saved_sector in _sector_opt else 0)
            if st.form_submit_button("Sauvegarder" if lang_c=="fr" else "Save", use_container_width=True):
                st.session_state["company_name"] = _company_new; st.session_state["company_sector"] = _sector_new
                save_user_prefs(username_c, {"company_name": _company_new, "company_sector": _sector_new})
                st.success("Sauvegarde" if lang_c=="fr" else "Saved")
        _seuil_lbl = "Seuil alerte rupture (unites)" if lang_c=="fr" else "Stockout alert threshold"
        _seuil_new = st.slider(_seuil_lbl, 0, 20, int(st.session_state.get("seuil_rupture", 5)))
        if _seuil_new != st.session_state.get("seuil_rupture", 5):
            st.session_state["seuil_rupture"] = _seuil_new
            save_user_prefs(username_c, {"seuil_rupture": _seuil_new})

    # ══ PARAMS ═════════════════════════════════════════════════════
    elif nav == _("nav_params"):
        lang_p = st.session_state.get("language", "fr")
        st.markdown(f"<h2 style='font-family:Syne,sans-serif;color:#0B2545;'>{'Parametres' if lang_p=='fr' else 'Settings'}</h2>", unsafe_allow_html=True)
        _lang_opts = ["🇫🇷 Français","🇬🇧 English"]
        _lang_sel = st.radio("", _lang_opts, index=1 if lang_p=="en" else 0, horizontal=True, label_visibility="collapsed")
        _new_lang = "en" if "English" in _lang_sel else "fr"
        if _new_lang != lang_p:
            st.session_state["language"] = _new_lang
            try: save_user_prefs(st.session_state.current_user, {"language": _new_lang})
            except Exception: pass
            st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)
        try:
            _ex_bytes_p = generate_exemple_excel()
            if _ex_bytes_p:
                st.download_button("📥 Telecharger le fichier exemple" if lang_p=="fr" else "📥 Download sample file",
                                   _ex_bytes_p, "logiflo_modele.xlsx",
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)
        except Exception:
            pass

    # ══ ARCHIVES ═══════════════════════════════════════════════════
    elif nav == _("nav_archives"):
        lang_arc = st.session_state.get("language", "fr")
        _view_arc = st.session_state.get("stock_view", "MANAGER")
        st.title(_("arch_title"))
        with st.spinner("Chargement..." if lang_arc=="fr" else "Loading..."):
            _dfa = load_archives_from_sheets(st.session_state.current_user)
        if _dfa is None or _dfa.empty:
            st.info(_("arch_empty"))
        else:
            if "created_at" in _dfa.columns and "date" not in _dfa.columns:
                _dfa["date"]  = pd.to_datetime(_dfa["created_at"], errors="coerce").dt.strftime("%d/%m/%Y")
                _dfa["heure"] = pd.to_datetime(_dfa["created_at"], errors="coerce").dt.strftime("%H:%M")
            for _cn in ["module","date","heure","kpi_1","kpi_2","kpi_3","kpi_label_1","kpi_label_2","kpi_label_3","resume_ia","profil"]:
                if _cn not in _dfa.columns: _dfa[_cn] = ""
            for _cn in ["kpi_1","kpi_2","kpi_3"]:
                _dfa[_cn] = pd.to_numeric(_dfa[_cn], errors="coerce").fillna(0)
            if _view_arc == "TERRAIN":
                _dfa_view = _dfa[_dfa["profil"].astype(str).str.lower() == "terrain"].copy()
                if _dfa_view.empty: _dfa_view = _dfa.copy()
            else:
                _dfa_view = _dfa[_dfa["profil"].astype(str).str.lower() != "terrain"].copy()
                if _dfa_view.empty: _dfa_view = _dfa.copy()
            mf = st.selectbox(_("arch_filter"), [_("arch_filter_all"), "stock", "transport"])
            ds = _dfa_view.copy()
            if mf != _("arch_filter_all"): ds = ds[ds["module"].astype(str) == mf]
            ds = ds.iloc[::-1].head(50)
            st.markdown(f"**{len(ds)} {_('arch_show')}**")
            for _i_a, _row_a in ds.iterrows():
                _mod_a  = str(_row_a.get("module",""))
                _date_a = str(_row_a.get("date","") or str(_row_a.get("created_at",""))[:10])
                _h_a    = str(_row_a.get("heure","") or str(_row_a.get("created_at",""))[11:16])
                _ico_a  = "📦" if _mod_a=="stock" else "🚚"
                _k2_a   = float(_row_a.get("kpi_2",0) or 0)
                _l2_a   = str(_row_a.get("kpi_label_2",""))
                _clr_a  = "#00C896" if _k2_a>=90 else ("#F39C12" if _k2_a>=75 else "#E8304A")
                st.markdown(f"""<div class="archive-card"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                <h4 style="margin:0;">{_ico_a} {_mod_a.upper()}</h4><span style="font-size:13px;color:#0B2545;font-weight:700;">{_date_a} -- {_h_a}</span></div>
                <span style="display:inline-block;background:#F0F4F8;border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;margin-right:8px;">{_row_a.get("kpi_label_1","")}: {float(_row_a.get("kpi_1",0)):.0f}</span>
                <span style="display:inline-block;background:#F0F4F8;border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;color:{_clr_a};margin-right:8px;">{_l2_a}: {_k2_a:.1f}%</span>
                <span style="display:inline-block;background:#F0F4F8;border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;margin-right:8px;">{_row_a.get("kpi_label_3","")}: {float(_row_a.get("kpi_3",0)):.0f}</span></div>""", unsafe_allow_html=True)
                with st.expander(_("arch_resume")):
                    _res_a = _row_a.get("resume_ia","")
                    if _res_a:
                        _render_mode = "terrain" if _view_arc=="TERRAIN" else "manager"
                        st.markdown(render_report(str(_res_a), _render_mode), unsafe_allow_html=True)
                    else:
                        st.info("N/A")
                if _view_arc != "TERRAIN":
                    _pdf_b = _row_a.get("pdf_base64","")
                    if _pdf_b:
                        try:
                            st.download_button(_("arch_dl"), base64.b64decode(str(_pdf_b)),
                                               f"Logiflo_{_date_a.replace('/','_')}_{_mod_a}.pdf",
                                               key=f"dl_arch_{_i_a}", use_container_width=True)
                        except Exception:
                            pass

    # ══ LEGAL ══════════════════════════════════════════════════════
    elif nav == _("nav_legal"):
        st.title(_("nav_legal"))
        _tab1, _tab2, _tab3 = st.tabs(["📋 Mentions Legales","🔒 RGPD","📄 CGU"])
        with _tab1:
            st.markdown("""<div class="legal-text"><h2>Editeur</h2><p><strong>Logiflo B2B Enterprise</strong> -- SASU (en cours d immatriculation)<br>Marseille, France -- contact@logiflo.io</p>
            <h2>Hebergement</h2><p>Streamlit Cloud (Snowflake Inc.) | GitHub Pages (GitHub Inc.)</p>
            <h2>Propriete Intellectuelle</h2><p>Tous les elements de LOGIFLO.IO sont la propriete exclusive de Logiflo B2B Enterprise.</p>
            <p style="color:#4A6080;font-size:13px;"><em>Version 2.0 -- Avril 2026</em></p></div>""", unsafe_allow_html=True)
        with _tab2:
            st.markdown("""<div class="legal-text"><h2>Conformite RGPD</h2><p>DPO : contact@logiflo.io</p>
            <h2>Ce que nous stockons</h2><p>Fichiers traites en RAM uniquement. Historique dans Supabase : KPIs, resume IA, PDF. Chiffrement TLS + AES-256. Suppression sous 30j sur demande.</p>
            <h2>Vos droits</h2><p>contact@logiflo.io -- www.cnil.fr</p></div>""", unsafe_allow_html=True)
        with _tab3:
            st.markdown("""<div class="legal-text"><h2>Offres</h2><p><strong>Starter</strong> 290 EUR/mois | <strong>Business</strong> 490 EUR/mois | <strong>Expert</strong> Sur devis</p>
            <h2>Engagement</h2><p>Analyses fournies a titre d aide a la decision uniquement.</p>
            <h2>Droit applicable</h2><p>Droit francais -- Tribunal de commerce de Marseille.</p></div>""", unsafe_allow_html=True)

    # ══ WORKSPACE ══════════════════════════════════════════════════
    elif nav == _("nav_workspace"):
        if st.session_state.module == "stock":
            st.title(_("stock_title"))
            ci, cb = st.columns([4, 1])
            ci.markdown(f"**{_('active_profile')} : {st.session_state.stock_view}**")
            if cb.button(_("change_profile")):
                st.session_state.page = "choix_profil_stock"; st.rerun()
            st.markdown(f"""<div class='import-card'><h3>{_('stock_import')}</h3><p>{_('stock_import_sub')}</p></div>""", unsafe_allow_html=True)
            up = st.file_uploader("", type=["csv","xlsx"], key="stock_upload")
            st.markdown("---")
            if up:
                pg = StepProgress([_("step_read"),_("step_detect"),_("step_calc")])
                pg.step(_("step_read"))
                try:
                    df_brut = pd.read_excel(up) if up.name.endswith("xlsx") else pd.read_csv(up, encoding='utf-8')
                except UnicodeDecodeError:
                    up.seek(0); df_brut = pd.read_csv(up, encoding='latin-1')
                pg.step(_("step_detect"))
                df_propre, statut = smart_ingester_stock_ultime(df_brut, client_ai=client)
                pg.step(_("step_calc")); pg.done()
                if df_propre is None:
                    st.error(statut)
                else:
                    _df_key = "df_stock_manager" if st.session_state.stock_view=="MANAGER" else "df_stock_terrain"
                    st.session_state[_df_key] = df_propre

            _df_key = "df_stock_manager" if st.session_state.stock_view=="MANAGER" else "df_stock_terrain"
            if st.session_state.get(_df_key) is not None:
                df = st.session_state[_df_key].copy()
                sans_prix = bool(df.get("_sans_prix", pd.Series([True])).iloc[0]) if "_sans_prix" in df.columns else True
                has_conso = bool(df.get("_has_conso", pd.Series([False])).iloc[0]) if "_has_conso" in df.columns else False
                if sans_prix:  st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_no_price')}</span>", unsafe_allow_html=True)
                if has_conso:  st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_conso')}</span>", unsafe_allow_html=True)
                else:          st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_no_conso')}</span>", unsafe_allow_html=True)

                if has_conso:
                    df["_conso_moy"] = df["_conso_moy"].fillna(0)
                    df["Couverture_mois"] = np.where(df["_conso_moy"] > 0, df["quantite"]/df["_conso_moy"], 9999)
                    df["Statut"] = np.select(
                        [(df["quantite"] <= st.session_state.seuil_rupture),
                         (df["quantite"] > 0) & (df["_conso_moy"] == 0),
                         (df["quantite"] > 0) & (df["Couverture_mois"] > 6)],
                        ["🔴 Rupture","🔴 Dormant","🟠 Surstock"], default="🟢 OK")
                else:
                    df["Statut"] = np.where(df["quantite"] <= st.session_state.seuil_rupture, "🔴 Rupture", "🟢 OK")

                df["valeur_totale"] = df["quantite"] * df["prix_unitaire"]
                val_totale = df["valeur_totale"].sum()
                ruptures   = df[df["Statut"] == "🔴 Rupture"]
                tx_serv    = (1 - len(ruptures)/len(df)) * 100 if len(df) > 0 else 100

                if not st.session_state.history_stock or st.session_state.history_stock[-1].get("valeur") != val_totale:
                    st.session_state.history_stock.append({"date":datetime.datetime.now().strftime("%H:%M:%S"),"valeur":val_totale})

                if st.session_state.stock_view == "MANAGER":
                    c1, c2, c3 = st.columns(3)
                    kpi1_label = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                    kpi1_val   = f"{val_totale:,.0f} EUR" if not sans_prix else str(len(df))
                    c1.markdown(f"<div class='kpi-card'><h4>{kpi1_label}</h4><h2 style='color:#0B2545;'>{kpi1_val}</h2></div>", unsafe_allow_html=True)
                    c2.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_service')}</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>", unsafe_allow_html=True)
                    c3.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_rupture')}</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>", unsafe_allow_html=True)
                    st.markdown("<br>", unsafe_allow_html=True)

                    cp, cl2 = st.columns(2)
                    cmap = {"🔴 Rupture":"#E8304A","🟢 OK":"#00C896","🔴 Dormant":"#c0392b","🟠 Surstock":"#f39c12"}
                    with cp:
                        fig_pie = px.pie(df, names="Statut", hole=0.4, color="Statut", color_discrete_map=cmap)
                        fig_pie.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                        st.plotly_chart(fig_pie, use_container_width=True, key='pie_stock_main')
                    with cl2:
                        if has_conso:
                            top15 = df.nlargest(15,"_conso_moy")[["reference","_conso_moy","quantite"]].copy()
                            fig_conso = px.bar(top15, x="reference", y=["quantite","_conso_moy"], barmode="group",
                                               color_discrete_map={"quantite":"#0B2545","_conso_moy":"#00C896"})
                            fig_conso.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_conso, use_container_width=True, key='bar_conso_main')
                        else:
                            fig_line = px.line(pd.DataFrame(st.session_state.history_stock), x="date", y="valeur")
                            fig_line.update_traces(line_color="#00C896")
                            fig_line.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_line, use_container_width=True, key='line_hist_main')

                    col_audit, col_save = st.columns([3, 1])
                    with col_audit: run_ia = st.button(_("stock_btn_ia"), use_container_width=True)
                    with col_save:
                        if st.button(_("stock_btn_save"), use_container_width=True, key="save_stock_early"):
                            kpi1 = val_totale if not sans_prix else float(len(df))
                            label1 = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                            ok = save_audit_to_sheets(st.session_state.current_user,"stock",len(df),
                                [kpi1,tx_serv,len(ruptures)],[label1,_("stock_kpi_service"),_("stock_kpi_rupture")],
                                st.session_state.analysis_stock_manager or "", st.session_state.last_pdf or b"")
                            if ok:
                                st.success(_("stock_saved"))
                            else:
                                st.info("Sauvegarde en cours..." if st.session_state.get("language","fr")=="fr" else "Saving...")


                    if can_access("prediction"):
                        render_prediction_rupture(df, lang=st.session_state.get("language","fr"))
                    else:
                        show_lock("prediction")

                    if run_ia:
                        pg2 = StepProgress([_("step_read"),_("step_ia"),_("step_report")])
                        pg2.step(_("step_read"))
                        df_tox = df[df["Statut"].isin(["🔴 Dormant","🟠 Surstock"])]
                        pires  = df_tox.nlargest(3,"quantite") if not df_tox.empty else df.nlargest(3,"quantite")
                        top_str = ", ".join([f"{r['reference']} (qty:{r['quantite']:.0f})" for _,r in pires.iterrows()])
                        rupt_l  = ruptures.nlargest(3,"quantite")["reference"].astype(str).tolist() if not ruptures.empty else "None"
                        med_info = "" if not has_conso else f" Avg conso: {df['_conso_moy'].mean():.1f}/period."
                        prix_info = "" if sans_prix else f" Capital: {val_totale:.0f} EUR."
                        pg2.step(_("step_ia"))
                        kpi1 = val_totale if not sans_prix else float(len(df))
                        label1 = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                        _kpis_final = [kpi1, tx_serv, float(len(ruptures))]
                        _labels_final = [label1, _("stock_kpi_service"), _("stock_kpi_rupture")]
                        st.session_state.last_kpis = _kpis_final
                        st.session_state.last_labels = _labels_final
                        _hist_s = get_historique_audits(st.session_state.current_user,"stock",
                                                         current_kpis=_kpis_final, current_labels=_labels_final)
                        _hist_txt_s = format_historique_pour_prompt(_hist_s,"stock",st.session_state.get("language","fr"))
                        _sector_s = detect_sector(df=df, module="stock")
                        st.session_state.analysis_stock_manager = generate_ai_analysis(
                            f"Items: {len(df)}. Service: {tx_serv:.1f}%. Stockouts: {len(ruptures)}. Top: {top_str}.{prix_info}{med_info}",
                            historique_txt=_hist_txt_s, df_raw=df, sector_key=_sector_s)
                        figs_pdf = [fig_pie]
                        if has_conso: figs_pdf.append(fig_conso)
                        st.session_state.last_pdf = generate_expert_pdf(_("pdf_title_stock"),
                            st.session_state.analysis_stock_manager, figs_pdf,
                            kpis=_kpis_final, labels=_labels_final, module="stock")
                        pg2.done()

                    if st.session_state.analysis_stock_manager:
                        try:
                            _score_ui = compute_logiflo_score(module="stock", df=df,
                                kpis=st.session_state.last_kpis, labels=st.session_state.last_labels,
                                sector_key=detect_sector(df=df, module="stock"),
                                lang=st.session_state.get("language","fr"))
                            _global_ui = _score_ui.get("global", 0)
                            _details_ui = _score_ui.get("details", {})
                            _clr_ui = "#00C896" if _global_ui>=70 else ("#F39C12" if _global_ui>=40 else "#E8304A")
                            st.markdown(f"""<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;padding:18px 22px;margin:16px 0;">
                            <div style="font-size:13px;font-weight:700;color:#0B2545;margin-bottom:14px;text-transform:uppercase;letter-spacing:1px;">Scoring Logiflo</div>
                            <div style="font-family:Syne,sans-serif;font-size:48px;font-weight:800;color:{_clr_ui};line-height:1;">{_global_ui}<span style="font-size:18px;color:#4A6080;">/100</span></div>
                            <div style="height:8px;background:#F0F4F8;border-radius:99px;overflow:hidden;margin:12px 0;"><div style="height:100%;width:{_global_ui}%;background:{_clr_ui};border-radius:99px;"></div></div>""", unsafe_allow_html=True)
                            for _dlbl, _dval in _details_ui.items():
                                _dc = "#00C896" if _dval>=70 else ("#F39C12" if _dval>=40 else "#E8304A")
                                st.markdown(f"""<div style="margin-bottom:8px;"><div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:4px;"><span style="color:#4A6080;">{_dlbl}</span><span style="color:{_dc};font-weight:700;">{_dval}/100</span></div>
                                <div style="height:5px;background:#F0F4F8;border-radius:99px;overflow:hidden;"><div style="height:100%;width:{_dval}%;background:{_dc};border-radius:99px;"></div></div></div>""", unsafe_allow_html=True)
                            st.markdown("</div>", unsafe_allow_html=True)
                        except Exception:
                            pass
                        st.markdown(render_report(st.session_state.analysis_stock_manager, "manager"), unsafe_allow_html=True)
                        if st.session_state.last_pdf:
                            st.download_button(_("stock_btn_dl"), st.session_state.last_pdf, "Audit_Stock_Logiflo.pdf", use_container_width=True)

                elif st.session_state.stock_view == "TERRAIN":
                    c1, c2 = st.columns(2)
                    c1.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_rupture')}</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>", unsafe_allow_html=True)
                    c2.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_service')}</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>", unsafe_allow_html=True)
                    st.markdown(f"### {_('stock_urgent')}")
                    if len(ruptures) > 0:
                        cols_s = ["reference","quantite","Statut"]
                        if has_conso: cols_s.append("_conso_moy")
                        st.dataframe(ruptures[cols_s], use_container_width=True)
                    else:
                        st.success(_("stock_no_rupture"))
                    run_ops = st.button(_("stock_btn_ia_terrain"), use_container_width=True, key="terrain_ia")
                    if run_ops:
                        pg3 = StepProgress([_("step_read"),_("step_ia"),_("step_report")])
                        pg3.step(_("step_read"))
                        top_c = df.nsmallest(5,"quantite")
                        top_s = ", ".join([f"{r['reference']} ({r['quantite']:.0f})" for _,r in top_c.iterrows()])
                        pg3.step(_("step_ia"))
                        _kpis_curr_t = [float(len(df)), float(len(ruptures)), tx_serv]
                        _labels_curr_t = ["Articles","Ruptures","Service %"]
                        _hist_t = get_historique_audits(st.session_state.current_user,"stock",
                                                         current_kpis=_kpis_curr_t, current_labels=_labels_curr_t)
                        _hist_txt_t = format_historique_pour_prompt(_hist_t,"terrain",st.session_state.get("language","fr"))
                        st.session_state.analysis_stock_terrain = generate_ai_analysis(
                            f"Field stock: {len(df)} refs. Stockouts: {len(ruptures)}. Lowest: {top_s}.",
                            historique_txt=_hist_txt_t, df_raw=df,
                            sector_key=detect_sector(df=df, module="stock"))
                        pg3.done()
                    if st.session_state.analysis_stock_terrain:
                        st.markdown(render_report(st.session_state.analysis_stock_terrain,"terrain"), unsafe_allow_html=True)
                        st.markdown(f"### {_('stock_full')}")
                        cols_s = ["reference","quantite","Statut"]
                        if has_conso: cols_s.append("_conso_moy")
                        st.dataframe(df[cols_s], use_container_width=True, height=400)

        elif st.session_state.module == "transport":
            st.title(_("trans_title"))
            st.markdown(f"""<div class='import-card'><h3>{_('trans_import')}</h3><p>{_('trans_import_sub')}</p></div>""", unsafe_allow_html=True)
            up_t = st.file_uploader("", type=["csv","xlsx"], key="trans_upload")
            st.markdown("---")

            if up_t and st.session_state.trans_filename != up_t.name:
                with st.spinner("Calcul en cours..." if st.session_state.get("language","fr")=="fr" else "Computing..."):
                    try:
                        df_t = pd.read_excel(up_t) if up_t.name.endswith("xlsx") else pd.read_csv(up_t, encoding="utf-8")
                    except UnicodeDecodeError:
                        up_t.seek(0); df_t = pd.read_csv(up_t, encoding="latin-1")
                    df_t = df_t.dropna(how="all")
                    mapping = auto_map_columns_with_ai(df_t, client_ai=client)
                    dep_c_tmp  = mapping.get("dep") if mapping.get("dep") in df_t.columns else None
                    arr_c_tmp  = mapping.get("arr") if mapping.get("arr") in df_t.columns else None
                    mode_c_tmp = mapping.get("mode") if mapping.get("mode") in df_t.columns else None
                    mode_det, mode_label, mode_emoji = detect_transport_mode(df_t, dep_c_tmp, arr_c_tmp, mode_c_tmp)
                    st.session_state.trans_mapping = mapping
                    st.session_state.df_trans = df_t
                    st.session_state.trans_filename = up_t.name
                    st.session_state.trans_mode_detected = (mode_det, mode_label, mode_emoji)
                    if dep_c_tmp and arr_c_tmp:
                        df_t = smart_multimodal_router(df_t, dep_c_tmp, arr_c_tmp, mode_c_tmp)
                        st.session_state.df_trans = df_t

            if st.session_state.df_trans is not None:
                df_t = st.session_state.df_trans
                mapping = st.session_state.trans_mapping
                if st.session_state.trans_mode_detected:
                    mode_det, mode_label, mode_emoji = st.session_state.trans_mode_detected
                    st.markdown(f"<div class='mode-badge'>{mode_label} {_('mode_detected')}</div>", unsafe_allow_html=True)

                def col(k): return mapping.get(k) if mapping.get(k) in df_t.columns else None
                tour_c = col("client") or df_t.columns[0]
                dep_c = col("dep"); arr_c = col("arr"); dist_c = col("dist")
                mode_c = col("mode"); ca_c = col("ca"); co_c = col("co"); poids_c = col("poids")

                if not co_c:
                    for c in df_t.columns:
                        if any(k in str(c).lower() for k in ["cout","cost","achat","charge"]): co_c = c; break
                if not ca_c:
                    for c in df_t.columns:
                        if any(k in str(c).lower() for k in ["ca","revenue","revenu","facture"]): ca_c = c; break
                if not co_c:
                    st.error(_("trans_no_cost")); st.stop()

                df_t["_CO"] = df_t[co_c].apply(super_clean)
                if ca_c: df_t["_CA"] = df_t[ca_c].apply(super_clean)
                else:    df_t["_CA"] = df_t["_CO"] / 0.85; st.warning(_("trans_ca_miss"))
                df_t["Marge_Nette"]   = df_t["_CA"] - df_t["_CO"]
                df_t["Rentabilite_%"] = np.where(df_t["_CA"] > 0, df_t["Marge_Nette"]/df_t["_CA"]*100, 0)

                if "_DIST_CALCULEE" not in df_t.columns and dep_c and arr_c:
                    df_t = smart_multimodal_router(df_t, dep_c, arr_c, mode_c)
                    st.session_state.df_trans = df_t

                df_t["_DIST_FINALE"] = (df_t["_DIST_CALCULEE"] if "_DIST_CALCULEE" in df_t.columns and df_t["_DIST_CALCULEE"].sum() > 0
                                        else (df_t[dist_c].apply(super_clean) if dist_c else 0))
                df_t["_DS"] = df_t["_DIST_FINALE"].replace(0, 1)
                df_t["Cout_KM"] = np.where(df_t["_DIST_FINALE"] > 0, df_t["_CO"]/df_t["_DS"], 0)

                marge_tot = df_t["Marge_Nette"].sum(); ca_tot = df_t["_CA"].sum()
                taux = (marge_tot/ca_tot*100) if ca_tot > 0 else 0
                toxiques = df_t[df_t["Marge_Nette"] < (df_t["_CA"]*0.05)]
                fuite = toxiques["_CO"].sum() - toxiques["_CA"].sum()
                nb_tox = len(toxiques); cout_km = df_t["Cout_KM"].mean()

                c1, c2, c3 = st.columns(3)
                c1.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_marge')}</h4><h2 style='color:#0B2545;'>{marge_tot:,.0f} EUR</h2></div>", unsafe_allow_html=True)
                c2.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_taux')}</h4><h2 style='color:#00C896;'>{taux:.1f} %</h2></div>", unsafe_allow_html=True)
                if fuite > 0:
                    c3.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_fuite')}</h4><h2 style='color:#E8304A;'>-{fuite:,.0f} EUR</h2></div>", unsafe_allow_html=True)
                else:
                    c3.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_sain')}</h4><h2 style='color:#00C896;'>OK</h2></div>", unsafe_allow_html=True)

                col_audit2, col_save2 = st.columns([3, 1])
                with col_audit2: run_ia_t = st.button(_("trans_btn_ia"), use_container_width=True)
                with col_save2:
                    if st.button(_("trans_btn_save"), use_container_width=True, key="save_trans_early"):
                        ok = save_audit_to_sheets(st.session_state.current_user,"transport",len(df_t),
                            [marge_tot,taux,nb_tox],[_("trans_kpi_marge"),_("trans_kpi_taux"),"Toxic"],
                            st.session_state.analysis_trans or "", st.session_state.last_pdf or b"")
                        if ok:
                            st.success(_("stock_saved"))
                        else:
                            st.info("Sauvegarde en cours..." if st.session_state.get("language","fr")=="fr" else "Saving...")

                df_plot = df_t.copy()
                df_plot["Statut"] = np.where(df_plot["Rentabilite_%"]<0,"🔴 Loss",
                    np.where(df_plot["Rentabilite_%"]<10,"🟠 Alert","🟢 Healthy"))
                CMAP = {"🔴 Loss":"#E8304A","🟠 Alert":"#f39c12","🟢 Healthy":"#00C896"}

                tab_top, tab_global = st.tabs([_("trans_tab_top"), _("trans_tab_all")])
                with tab_top:
                    top_n = df_plot.nsmallest(15,"Marge_Nette").sort_values("Marge_Nette")
                    top_n["label"] = top_n[tour_c].astype(str).str[:35]
                    top_n["pct_label"] = top_n["Rentabilite_%"].apply(lambda x: f"{x:.1f}%")
                    fig_top = px.bar(top_n, x="Marge_Nette", y="label", orientation="h",
                        color="Statut", color_discrete_map=CMAP, text="pct_label",
                        title=_("trans_top15_title"),
                        labels={"Marge_Nette":"Margin EUR","label":""})
                    fig_top.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=520, showlegend=False)
                    st.plotly_chart(fig_top, use_container_width=True, key='bar_top_trans')

                    cols_show = [tour_c,"_CA","_CO","Marge_Nette","Rentabilite_%","Statut"]
                    cols_show = [c for c in cols_show if c in df_t.columns]
                    st.dataframe(top_n[cols_show], use_container_width=True, height=380)

                with tab_global:
                    fig_scatter = px.scatter(df_plot, x="_CA", y="Rentabilite_%",
                        color="Statut", color_discrete_map=CMAP,
                        size=df_plot["_CO"].clip(lower=1), size_max=40,
                        hover_name=tour_c, title=_("trans_scatter_title"),
                        labels={"_CA":"Revenue EUR","Rentabilite_%":"Margin %"})
                    fig_scatter.add_hline(y=0, line_dash="solid", line_color="#E8304A", line_width=2, annotation_text=_("trans_seuil_zero"))
                    fig_scatter.add_hline(y=10, line_dash="dot", line_color="#f39c12", line_width=1.5, annotation_text=_("trans_seuil_alert"))
                    fig_scatter.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#fafbfc", height=500)
                    st.plotly_chart(fig_scatter, use_container_width=True, key='scatter_trans')

                if run_ia_t:
                    with st.spinner("Analyse IA..." if st.session_state.get("language","fr")=="fr" else "AI Analysis..."):
                        top3 = df_t[df_t["Marge_Nette"].notna() & df_t[tour_c].notna()].nsmallest(3,"Marge_Nette")
                        pires_s = ", ".join([f"{str(r[tour_c]).strip()} ({r['Marge_Nette']:.0f} EUR)"
                                             for _,r in top3.iterrows()
                                             if str(r[tour_c]).strip() not in ("","nan","None")]) if not top3.empty else "None"
                        _kpis_tr = [marge_tot, taux, nb_tox]
                        _labels_tr = [_("trans_kpi_marge"), _("trans_kpi_taux"), "Toxic"]
                        _hist_tr = get_historique_audits(st.session_state.current_user,"transport",
                                                          current_kpis=_kpis_tr, current_labels=_labels_tr)
                        _hist_txt_tr = format_historique_pour_prompt(_hist_tr,"transport",st.session_state.get("language","fr"))
                        _mode_k = st.session_state.trans_mode_detected[0] if st.session_state.get("trans_mode_detected") else "routier"
                        _sector_tr = detect_sector(df=df_t, module="transport", mode_detected=_mode_k)
                        st.session_state.analysis_trans = generate_ai_analysis(
                            f"Routes: {len(df_t)}. Total margin: {marge_tot:.0f} EUR. Rate: {taux:.1f}%. Loss routes: {len(df_t[df_t['Marge_Nette']<0])}. Top 3 worst: {pires_s}. Avg cost/km: {cout_km:.2f} EUR.",
                            historique_txt=_hist_txt_tr, df_raw=df_t, sector_key=_sector_tr, mode_detected=_mode_k)
                        st.session_state.last_kpis = _kpis_tr
                        st.session_state.last_labels = _labels_tr
                        st.session_state.last_pdf = generate_expert_pdf(_("pdf_title_trans"),
                            st.session_state.analysis_trans, [fig_top],
                            kpis=_kpis_tr, labels=_labels_tr, module="transport")

                if st.session_state.analysis_trans:
                    st.markdown(render_report(st.session_state.analysis_trans,"manager"), unsafe_allow_html=True)
                    if st.session_state.last_pdf:
                        st.download_button(_("trans_btn_dl"), st.session_state.last_pdf, "Transport_Logiflo.pdf", use_container_width=True)
