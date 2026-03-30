# Public app - no Streamlit auth required

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import datetime
import re
import tempfile
import os
import difflib
import unicodedata
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
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="LOGIFLO.IO | Control Tower", layout="wide", page_icon="🏢")

client      = OpenAI(api_key=st.secrets.get("OPENAI_API_KEY", ""))
ORS_API_KEY = st.secrets.get("ORS_API_KEY", "")
SHEET_ID    = st.secrets.get("GOOGLE_SHEET_ID", "")

USERS_DB = {
    "eric": "logiflo2026", "admin": "admin123",
    "demo_client1": "audit2026", "demo_client2": "test2026",
    "jury": "pitch2026", "partenaire": "partner2026", "test": "test123",
}

# ══════════════════════════════════════════════════════════════════
# TRADUCTIONS
# ══════════════════════════════════════════════════════════════════
T = {
    "fr": {
        "nav_workspace": "Espace de Travail", "nav_archives": "Archives",
        "nav_params": "Paramètres", "nav_legal": "Informations Légales",
        "nav_logout": "Déconnexion",
        "home_sub": "Plateforme d'Intelligence Logistique et d'Optimisation Financière",
        "home_stock": "AUDIT STOCKS", "home_transport": "AUDIT TRANSPORT",
        "home_access": "DEMANDER UN ACCÈS PRIVÉ",
        "login_id": "Identifiant", "login_pw": "Mot de passe",
        "login_btn": "Connexion", "login_err": "Identifiants incorrects.",
        "login_back": "← Retour",
        "profile_title": "Sélectionnez votre Espace de Travail",
        "profile_sub": "L'interface s'adaptera à vos habilitations.",
        "profile_mgr": "PROFIL MANAGER (Stratégie & Finance)",
        "profile_ops": "PROFIL TERRAIN (Action Opérationnelle)",
        "stock_title": "📦 Audit Financier des Stocks",
        "stock_import": "📥 Importation Sécurisée",
        "stock_import_sub": "Déposez votre fichier d'inventaire (CSV ou Excel).<br>Le <b>Smart Ingester™ V5</b> détecte automatiquement vos colonnes, même avec des noms atypiques ou des fautes de frappe.<br><span style='color:#00A87A;font-weight:600;'>✓ Prix optionnel</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ Historique optionnel</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ Tous formats</span>",
        "stock_change_profile": "Changer de profil",
        "stock_kpi_capital": "Capital Immobilisé", "stock_kpi_articles": "Articles en Stock",
        "stock_kpi_service": "Taux de Service", "stock_kpi_rupture": "Articles en Rupture",
        "stock_btn_ia": "GÉNÉRER L'AUDIT FINANCIER (IA)",
        "stock_btn_ia_terrain": "GÉNÉRER L'ANALYSE TERRAIN (IA)",
        "stock_btn_save": "💾 Sauvegarder", "stock_btn_dl": "📥 Télécharger le Rapport (PDF)",
        "stock_badge_no_price": "📊 Mode opérationnel — analyse sans prix",
        "stock_badge_conso": "📈 Historique de consommation détecté",
        "stock_badge_no_conso": "⚠️ Pas d'historique — couverture non calculable",
        "stock_saved": "✅ Sauvegardé !", "stock_save_err": "⚠️ Connexion Google Sheets absente.",
        "stock_no_rupture": "✅ Aucun article en rupture.",
        "stock_prio": "### 🚨 Priorités immédiates", "stock_full": "### 📋 Stock complet",
        "stock_detail": "**Détail des trajets en alerte**",
        "trans_title": "🚚 Audit de Rentabilité Transport",
        "trans_import": "🌍 Importation des Flux de Transport",
        "trans_import_sub": "Déposez votre fichier TMS ou Excel.<br>Le moteur détecte automatiquement le mode de transport depuis vos données.<br><span style='color:#00A87A;font-weight:600;'>✓ Maritime</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ Aérien</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ Routier</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ Ferroviaire</span>",
        "trans_kpi_marge": "Marge Nette Globale", "trans_kpi_taux": "Taux de Rentabilité",
        "trans_kpi_fuite": "🚨 Fuite de Marge", "trans_kpi_sain": "✅ Réseau",
        "trans_btn_ia": "GÉNÉRER L'AUDIT DE RENTABILITÉ (IA)",
        "trans_btn_dl": "📥 Télécharger le Rapport (PDF)",
        "trans_tab_top": "🎯 Top 15 — Pires trajets", "trans_tab_all": "🗺️ Vue d'ensemble",
        "trans_ca_miss": "💡 CA manquant — estimé à marge 15%.",
        "trans_no_cost": "🚨 Colonne 'Coût' introuvable.",
        "trans_chart_title_top": "Top 15 trajets les plus déficitaires",
        "trans_chart_title_scatter": "Vue d'ensemble — Rentabilité vs CA par trajet",
        "trans_chart_ca": "Chiffre d'affaires (€)", "trans_chart_marge": "Marge nette (%)",
        "trans_chart_zero": "Seuil zéro", "trans_chart_alert": "Seuil alerte 10%",
        "trans_scatter_caption": "trajets analysés",
        "trans_loss": "en perte", "trans_alert": "en alerte", "trans_healthy": "sains",
        "trans_stat_loss": "🔴 Perte", "trans_stat_alert": "🟠 Alerte (< 10%)", "trans_stat_ok": "🟢 Sain (> 10%)",
        "arch_title": "🗄️ Archives & Historique",
        "arch_empty": "Aucun audit archivé. Générez votre premier audit depuis l'Espace de Travail.",
        "arch_dl": "📥 PDF", "arch_filter": "Filtrer", "arch_filter_all": "Tous",
        "arch_resume": "📋 Résumé IA", "arch_lines": "lignes analysées",
        "step_read": "Lecture du fichier...", "step_detect": "Détection des colonnes...",
        "step_calc": "Calcul des indicateurs...", "step_ia_progress": "Analyse IA en cours...",
        "step_report": "Génération du rapport...", "step_geo": "Géocodage des villes...",
        "step_dist": "Calcul des distances ORS...", "step_prep": "Préparation des données...",
        "step_col_ia": "Détection des colonnes (IA)...", "step_mode": "Détection du mode de transport...",
        "pdf_title_stock": "AUDIT STRATEGIQUE DES STOCKS",
        "pdf_title_trans": "AUDIT FINANCIER TRANSPORT",
        "pdf_confidential": "CONFIDENTIEL", "pdf_strategic": "AUDIT STRATEGIQUE",
        "pdf_report": "RAPPORT D ANALYSE", "pdf_date": "Date",
        "pdf_footer": "Document genere par Logiflo.io. Recommandations a titre indicatif.",
        "pdf_cta_title": "PASSER A L'ETAPE SUIVANTE",
        "pdf_cta_body": "Ce rapport a ete genere par LOGIFLO.IO — concu par un logisticien terrain.\nPour un audit complet ou un accompagnement : contact@logiflo.io\nlogiflo-io.streamlit.app",
        "params_title": "⚙️ Configuration des Seuils",
        "params_alert": "Seuil d'Alerte", "params_rupture": "Seuil de Rupture Critique",
        "params_km": "Seuil Rentabilité EUR/KM",
        "poids_info": "⚖️ Coût moyen",
        "connectedas": "Connecté",
        "save_early": "💾 Sauvegarder",
    },
    "en": {
        "nav_workspace": "Workspace", "nav_archives": "Archives",
        "nav_params": "Settings", "nav_legal": "Legal Information",
        "nav_logout": "Log out",
        "home_sub": "Logistics Intelligence & Financial Optimization Platform",
        "home_stock": "STOCK AUDIT", "home_transport": "TRANSPORT AUDIT",
        "home_access": "REQUEST PRIVATE ACCESS",
        "login_id": "Username", "login_pw": "Password",
        "login_btn": "Sign in", "login_err": "Incorrect credentials.",
        "login_back": "← Back",
        "profile_title": "Select your Workspace",
        "profile_sub": "The interface will adapt to your permissions.",
        "profile_mgr": "MANAGER PROFILE (Strategy & Finance)",
        "profile_ops": "OPERATIONS PROFILE (Field Action)",
        "stock_title": "📦 Stock Financial Audit",
        "stock_import": "📥 Secure Import",
        "stock_import_sub": "Drop your inventory file (CSV or Excel).<br>The <b>Smart Ingester™ V5</b> automatically detects your columns, even with unusual names or typos.<br><span style='color:#00A87A;font-weight:600;'>✓ Price optional</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ History optional</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ All formats</span>",
        "stock_change_profile": "Change profile",
        "stock_kpi_capital": "Tied-up Capital", "stock_kpi_articles": "Items in Stock",
        "stock_kpi_service": "Service Level", "stock_kpi_rupture": "Stock-outs",
        "stock_btn_ia": "GENERATE FINANCIAL AUDIT (AI)",
        "stock_btn_ia_terrain": "GENERATE FIELD ANALYSIS (AI)",
        "stock_btn_save": "💾 Save", "stock_btn_dl": "📥 Download Report (PDF)",
        "stock_badge_no_price": "📊 Operational mode — analysis without prices",
        "stock_badge_conso": "📈 Consumption history detected",
        "stock_badge_no_conso": "⚠️ No consumption history — coverage not calculable",
        "stock_saved": "✅ Saved!", "stock_save_err": "⚠️ Google Sheets connection unavailable.",
        "stock_no_rupture": "✅ No stock-outs detected.",
        "stock_prio": "### 🚨 Immediate priorities", "stock_full": "### 📋 Full inventory",
        "stock_detail": "**Route detail — flagged items**",
        "trans_title": "🚚 Transport Profitability Audit",
        "trans_import": "🌍 Import Transport Flows",
        "trans_import_sub": "Drop your TMS or Excel file.<br>The engine automatically detects the transport mode from your data.<br><span style='color:#00A87A;font-weight:600;'>✓ Maritime</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ Air</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ Road</span> &nbsp;<span style='color:#00A87A;font-weight:600;'>✓ Rail</span>",
        "trans_kpi_marge": "Total Net Margin", "trans_kpi_taux": "Profitability Rate",
        "trans_kpi_fuite": "🚨 Margin Leak", "trans_kpi_sain": "✅ Network",
        "trans_btn_ia": "GENERATE PROFITABILITY AUDIT (AI)",
        "trans_btn_dl": "📥 Download Report (PDF)",
        "trans_tab_top": "🎯 Top 15 — Worst routes", "trans_tab_all": "🗺️ Overview",
        "trans_ca_miss": "💡 Revenue missing — estimated at 15% margin.",
        "trans_no_cost": "🚨 'Cost' column not found.",
        "trans_chart_title_top": "Top 15 most unprofitable routes",
        "trans_chart_title_scatter": "Overview — Profitability vs Revenue per route",
        "trans_chart_ca": "Revenue (€)", "trans_chart_marge": "Net margin (%)",
        "trans_chart_zero": "Zero threshold", "trans_chart_alert": "Alert threshold 10%",
        "trans_scatter_caption": "routes analysed",
        "trans_loss": "losing money", "trans_alert": "at risk", "trans_healthy": "healthy",
        "trans_stat_loss": "🔴 Loss", "trans_stat_alert": "🟠 Alert (< 10%)", "trans_stat_ok": "🟢 Healthy (> 10%)",
        "arch_title": "🗄️ Archives & History",
        "arch_empty": "No saved audits yet. Generate your first audit from the Workspace.",
        "arch_dl": "📥 PDF", "arch_filter": "Filter", "arch_filter_all": "All",
        "arch_resume": "📋 AI Summary", "arch_lines": "lines analysed",
        "step_read": "Reading file...", "step_detect": "Detecting columns...",
        "step_calc": "Computing indicators...", "step_ia_progress": "AI analysis in progress...",
        "step_report": "Generating report...", "step_geo": "Geocoding cities...",
        "step_dist": "Computing ORS distances...", "step_prep": "Preparing data...",
        "step_col_ia": "Detecting columns (AI)...", "step_mode": "Detecting transport mode...",
        "pdf_title_stock": "STRATEGIC STOCK AUDIT",
        "pdf_title_trans": "TRANSPORT FINANCIAL AUDIT",
        "pdf_confidential": "CONFIDENTIAL", "pdf_strategic": "STRATEGIC AUDIT",
        "pdf_report": "ANALYSIS REPORT", "pdf_date": "Date",
        "pdf_footer": "Generated by Logiflo.io. Recommendations are indicative only.",
        "pdf_cta_title": "TAKE THE NEXT STEP",
        "pdf_cta_body": "This report was generated by LOGIFLO.IO — built by a logistics professional.\nFor a full audit or implementation support: contact@logiflo.io\nlogiflo-io.streamlit.app",
        "params_title": "⚙️ Settings",
        "params_alert": "Alert Threshold", "params_rupture": "Critical Stock-out Threshold",
        "params_km": "Profitability Threshold EUR/KM",
        "poids_info": "⚖️ Average cost",
        "connectedas": "Logged in",
        "save_early": "💾 Save",
    }
}

def _(key: str) -> str:
    lang = st.session_state.get("language", "fr")
    return T.get(lang, T["fr"]).get(key, T["fr"].get(key, key))

# ══════════════════════════════════════════════════════════════════
# PROMPTS IA BILINGUES
# ══════════════════════════════════════════════════════════════════
def get_prompt_stock():
    lang = st.session_state.get("language", "fr")
    L = "RESPOND ENTIRELY IN ENGLISH." if lang == "en" else "RÉPONDS IMPÉRATIVEMENT EN FRANÇAIS."
    ID = "You are a Senior Financial Auditor and Supply Chain Director for Logiflo.io." if lang == "en" else "Tu es l'Auditeur Financier et Directeur Supply Chain Senior pour Logiflo.io."
    s1 = "OPERATIONAL DIAGNOSIS" if lang == "en" else "DIAGNOSTIC OPERATIONNEL"
    s2 = "FINANCIAL DIAGNOSIS & DORMANT STOCK" if lang == "en" else "DIAGNOSTIC FINANCIER & STOCKS DORMANTS"
    s3 = "IMMEDIATE ACTION PLAN (TOP 3)" if lang == "en" else "PLAN D'ACTION IMMÉDIAT (TOP 3)"
    s4 = "LOGIFLO SCORE" if lang == "en" else "SCORING LOGIFLO"
    return f"""{ID}
{L}

RULE on data:
- If prices available → full financial analysis (tied-up capital, dormant stock, cash trap)
- If NO prices → pure operational analysis (rotation, velocity, stock-outs in quantities)
- If consumption history available → calculate coverage in months and trend
- If NO consumption → flag BLIND SPOT, give sector benchmark (healthy coverage = 2-4 months)

Mandatory structure:

### {s1}
Service level and rotation. Name the 3 most critical references with exact figures.

### {s2}
If prices: capital, dormant stock, cash trap.
If no prices: velocity, zero-rotation items, hidden risks.

### {s3}
3 concrete recommendations.
Potential impact: High/Medium/Low | Execution difficulty: 1 to 5

### {s4}
- Stock Performance & Rotation: /100
- Stock-out Risk: /100
- Supply Chain Resilience: /100

RULES: Never invent amounts. Leave a blank line between each idea.
"""

def get_prompt_terrain():
    lang = st.session_state.get("language", "fr")
    L = "RESPOND IN ENGLISH, direct tone, short sentences. No financial jargon." if lang == "en" else "RÉPONDS EN FRANÇAIS, ton direct, phrases courtes. Pas de jargon financier."
    ID = "You are an experienced warehouse supervisor helping your team day-to-day." if lang == "en" else "Tu es un chef magasinier expérimenté qui aide son équipe au quotidien."
    s1 = "What's urgent" if lang == "en" else "Ce qui est urgent"
    s2 = "What's sleeping" if lang == "en" else "Ce qui dort"
    s3 = "Your 3 actions for this week" if lang == "en" else "Tes 3 actions pour cette semaine"
    s4 = "Summary" if lang == "en" else "En résumé"
    d = "Easy / Medium / Hard" if lang == "en" else "Facile / Moyen / Compliqué"
    return f"""{ID}
{L}

RULE on data:
- If no prices → quantities only
- If no consumption → say so clearly and observe what you can
- If consumption available → calculate coverage in weeks/months
- Always cite real references from the file

Structure:

### {s1}
Items to reorder now. Exact references and quantities.

### {s2}
Items with no movement. Concrete action for each.

### {s3}
Difficulty: {d}

### {s4}
2-3 sentences to brief your manager in 30 seconds.

RULES: Concrete only. No invented figures.
"""

def get_prompt_transport():
    lang = st.session_state.get("language", "fr")
    L = "RESPOND ENTIRELY IN ENGLISH." if lang == "en" else "RÉPONDS IMPÉRATIVEMENT EN FRANÇAIS."
    ID = "You are a Senior Transport & Supply Chain Strategy Auditor." if lang == "en" else "Tu es un Auditeur Senior en Stratégie Transport & Supply Chain."
    s1 = "PROFITABILITY AUDIT" if lang == "en" else "AUDIT DE RENTABILITE"
    s2 = "NETWORK DIAGNOSIS" if lang == "en" else "DIAGNOSTIC RÉSEAU"
    s3 = "RATIONALIZATION PLAN (TOP 3)" if lang == "en" else "PLAN DE RATIONALISATION (TOP 3)"
    s4 = "LOGIFLO SCORE" if lang == "en" else "SCORING LOGIFLO"
    return f"""{ID}
{L}
DON'T BE A PARROT: deduce hidden problems.
If weight is missing: flag STRATEGIC BLIND SPOT.
Adapt vocabulary to detected mode (maritime: TEU, demurrage / air: AWB, chargeable weight / road: FTL, cost/km).

CNR 2025-2026 sector benchmarks:
- Long-haul road (articulated diesel): 1.85–2.10 EUR/km
- Regional road (rigid): 1.40–1.65 EUR/km
- Fuel share: ~26.5% of total cost | Non-fuel cost inflation 2025: +2.4%
- Alert threshold: margin < 8% | Toxic: < 5% | Loss: < 0%

Mandatory structure:

### {s1}
Global margin and Yield. Name the 3 routes/clients destroying profitability. Expert hypothesis on cause.

### {s2}
Spatial coherence. If weight available: cost/kg and estimated load factor. Compare cost/km to CNR benchmarks.

### {s3}
3 aggressive mode-specific recommendations.
Cash Impact: High/Medium/Low | Execution difficulty: 1 to 5

### {s4}
- Profitability & Transport Yield: /100
- Operational Efficiency: /100
- OPEX Control: /100

RULES: Never invent amounts. Leave a blank line between each idea.
"""

# ══════════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════════
@st.cache_resource
def get_gsheet_client():
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds)
    except: return None

def get_user_sheet(username):
    gc = get_gsheet_client()
    if not gc or not SHEET_ID: return None
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: return sh.worksheet(username)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=username, rows=1000, cols=12)
            ws.append_row(["date","heure","module","nb_lignes","kpi_1","kpi_2","kpi_3",
                           "kpi_label_1","kpi_label_2","kpi_label_3","resume_ia","pdf_base64"])
            return ws
    except: return None

def save_audit_to_sheets(username, module, nb_lignes, kpis, labels, resume_ia, pdf_bytes):
    ws = get_user_sheet(username)
    if not ws: return False
    try:
        now = datetime.datetime.now()
        ws.append_row([now.strftime("%d/%m/%Y"), now.strftime("%H:%M"), module, nb_lignes,
            round(kpis[0],2) if len(kpis)>0 else "",
            round(kpis[1],2) if len(kpis)>1 else "",
            round(kpis[2],2) if len(kpis)>2 else "",
            labels[0] if len(labels)>0 else "",
            labels[1] if len(labels)>1 else "",
            labels[2] if len(labels)>2 else "",
            resume_ia[:800] if resume_ia else "",
            base64.b64encode(pdf_bytes).decode("utf-8") if pdf_bytes else ""])
        return True
    except: return False

def load_archives_from_sheets(username):
    ws = get_user_sheet(username)
    if not ws: return None
    try:
        records = ws.get_all_records()
        return pd.DataFrame(records) if records else pd.DataFrame()
    except: return None

# ══════════════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════════════
for k, v in {
    "page": "accueil", "module": "", "auth": False, "current_user": None,
    "language": "fr",
    "df_stock": None, "df_trans": None, "history_stock": [], "stock_view": "MANAGER",
    "seuil_bas": 15, "seuil_rupture": 0, "seuil_km": 0,
    "geo_cache": {}, "route_cache": {}, "trans_mapping": None, "trans_filename": None,
    "analysis_stock": None, "analysis_trans": None,
    "last_pdf": None, "last_kpis": [], "last_labels": [],
    "trans_mode_detected": None,
}.items():
    if k not in st.session_state: st.session_state[k] = v

# ══════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');
:root{--navy:#0B2545;--navy2:#162D52;--green:#00C896;--green2:#00A87A;--slate:#4A6080;--light:#F0F4F8;--red:#E8304A;--white:#FFFFFF;}
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;color:var(--navy);}
.block-container{padding-top:2rem!important;padding-bottom:2rem!important;max-width:95%!important;}
.kpi-card{background:var(--white);padding:24px;border-radius:12px;border:1px solid #e2e8f0;border-top:3px solid var(--green);box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);transition:transform 0.2s;}
.kpi-card:hover{transform:translateY(-2px);}
.kpi-card h4{color:var(--slate)!important;font-family:'DM Sans',sans-serif!important;font-size:0.75rem!important;text-transform:uppercase;font-weight:600;letter-spacing:1.5px;margin-bottom:10px;}
.kpi-card h2{font-family:'Syne',sans-serif!important;font-size:2.2rem!important;font-weight:800!important;margin-top:0;line-height:1;letter-spacing:-1px;}
.kpi-card p{font-size:12px;color:var(--slate);margin-top:6px;}
div.stButton>button{border-radius:8px;font-family:'Syne',sans-serif;font-weight:700;background-color:var(--navy);color:#f8fafc;border:none;transition:0.3s;}
div.stButton>button:hover{background-color:var(--navy2);transform:translateY(-2px);}
[data-testid="stSidebar"]{background-color:var(--navy)!important;}
[data-testid="stSidebar"] *{color:#ffffff!important;font-size:1rem!important;}
[data-testid="stSidebar"] hr{border-color:#1e3a5f!important;}
div[role="radiogroup"]>label{padding-bottom:12px;cursor:pointer;}
.sidebar-logo{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;color:white;letter-spacing:-0.5px;}
.sidebar-logo span{color:#00C896;}
.import-card{background:var(--white);padding:25px;border-radius:12px;border-left:6px solid var(--green);margin-bottom:20px;box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);}
.import-card h3{margin-top:0;color:var(--navy);font-family:'Syne',sans-serif;font-size:1rem;}
.import-card p{color:var(--slate);font-size:14px;margin-bottom:0;line-height:1.5;}
.report-text{background:var(--light);padding:32px;border-radius:12px;border-left:6px solid var(--navy);line-height:1.8;}
.report-text h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:800;color:var(--navy);text-transform:uppercase;letter-spacing:1.5px;margin-top:28px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.report-text h3:first-child{margin-top:0;}
.report-text p{color:#2d3748;font-size:14px;margin-bottom:8px;}
.report-terrain{background:#f8fff8;padding:28px;border-radius:12px;border-left:6px solid var(--green);line-height:1.9;}
.report-terrain h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:700;color:var(--green2);margin-top:24px;margin-bottom:8px;}
.report-terrain h3:first-child{margin-top:0;}
.report-terrain p{color:#1a2e1a;font-size:15px;margin-bottom:6px;}
.mode-badge{display:inline-flex;align-items:center;gap:8px;background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:var(--green2);font-size:13px;font-weight:600;padding:8px 16px;border-radius:8px;margin-bottom:16px;}
.sans-prix-badge{background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:var(--green2);font-size:12px;font-weight:600;padding:4px 12px;border-radius:20px;display:inline-block;margin-bottom:12px;}
.archive-card{background:var(--white);border:1px solid #E2EAF4;border-radius:12px;padding:20px;margin-bottom:16px;border-left:4px solid var(--green);}
.archive-card h4{font-family:'Syne',sans-serif;font-size:14px;font-weight:700;color:var(--navy);margin-bottom:8px;}
.archive-kpi{display:inline-block;background:var(--light);border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;color:var(--navy);margin-right:8px;}
.big-emoji{font-size:70px;margin-bottom:10px;display:block;text-align:center;}
.legal-text{background:var(--white);padding:32px;border-radius:12px;border:1px solid #E2EAF4;line-height:1.9;}
.legal-text h2{font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:800;color:var(--navy);margin-top:28px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.legal-text h2:first-child{margin-top:0;}
.legal-text p,.legal-text li{color:#2d3748;font-size:14px;margin-bottom:6px;}
.legal-box{background:var(--light);border-left:4px solid var(--green);padding:16px 20px;border-radius:8px;margin:12px 0;}
.legal-box p{color:var(--navy)!important;font-weight:500;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
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

def nettoyer(t):
    t = str(t).lower()
    t = unicodedata.normalize('NFD', t).encode('ascii', 'ignore').decode("utf-8")
    return re.sub(r'[^a-z0-9]', '', t)

# ══════════════════════════════════════════════════════════════════
# SMART INGESTER V5
# ══════════════════════════════════════════════════════════════════
SYNONYMES = {
    "reference": ["reference","ref","article","code","codearticle","codeproduit","cdarticle",
                  "cdart","cdproduit","codemat","codematiere","nomarticle","nomproduit",
                  "sku","ean","ean13","upc","gtin","produit","designation","libelle",
                  "description","descproduit","descarticle","nom","item","itemcode",
                  "itemno","itemref","partnumber","partno","partref","refarticle",
                  "refproduit","refcommande","numero","numeroproduit","matricule",
                  "identifiant","id","cable","cablage","matiere","materiel","composant",
                  "piece","repere","nomenclature","famille","sousfamille","categorie",
                  "productcode","productref","dsg","desig","design","designat",
                  "articlecode","articleref","artcode","artref","artno","artnum"],
    "quantite":  ["quantite","qte","qty","qtstk","qte_stk","qtestk","stock","stk","stockactuel",
                  "stockdispo","stockdisponible","stockreel","stockphysique","niveaustock",
                  "qtestock","qtedispo","qtedisponible","qtereel","qtephysique",
                  "volume","pieces","pcs","units","unit","unites","restant",
                  "solde","soldedisponible","encours","inventaire","disponible",
                  "existant","existants","present","metre","metres","meter","meters",
                  "bobine","bobines","longueur","longueurstock","quantitedisponible",
                  "quantitestock","quantiterestante","quantitepresente",
                  "nbarticle","nbarticles","nbpieces","nbunites","nb","nbre","nombre",
                  "qte_disponible","qt_stk","qtstck","qtstock"],
    "prix_unitaire": ["prix","prixunitaire","prixachat","prixderevient","prixmoyen",
                      "prixmoyenpondere","pmp","pa","pu","pxu","px_u","price","unitprice",
                      "avgprice","cout","coutunitaire","coutachat","coutderevient","coutmoyen",
                      "cost","unitcost","avgcost","valeur","valeurunitaire","valeurachat",
                      "tarif","tarifunitaire","montantunitaire","achat","prixfournisseur",
                      "euro","eur","devise","prixbase","baseachat","priceeuro","priceeur"],
    "conso_an1": ["conso2022","conso22","consommation2022","sorties2022","ventes2022",
                  "c2022","n3","nminus3","annee2022","a2022","quantite2022","qte2022","cso22"],
    "conso_an2": ["conso2023","conso23","consommation2023","sorties2023","ventes2023",
                  "c2023","n2","nminus2","annee2023","a2023","quantite2023","qte2023","cso23"],
    "conso_an3": ["conso2024","conso24","consommation2024","sorties2024","ventes2024",
                  "c2024","n1","nminus1","annee2024","a2024","quantite2024","qte2024","cso24"],
    "conso_an4": ["conso2025","conso25","consommation2025","sorties2025","ventes2025",
                  "c2025","n0","nactuel","annee2025","a2025","quantite2025","qte2025","cso25",
                  "consoactuelle","consoencoursannee"],
}

def _levenshtein(s1, s2):
    if len(s1) < len(s2): return _levenshtein(s2, s1)
    if len(s2) == 0: return len(s1)
    prev = list(range(len(s2)+1))
    for i, c1 in enumerate(s1):
        curr = [i+1]
        for j, c2 in enumerate(s2):
            curr.append(min(prev[j+1]+1, curr[j]+1, prev[j]+(c1 != c2)))
        prev = curr
    return prev[-1]

def _score_nom(propre, std):
    syns = SYNONYMES.get(std, [])
    best = 0
    for syn in syns:
        if propre == syn: return 100
        if len(syn) >= 4 and propre.startswith(syn): best = max(best, 95)
        if len(syn) >= 3 and syn in propre: best = max(best, 88)
        if len(propre) >= 3 and propre in syn: best = max(best, 82)
        r = difflib.SequenceMatcher(None, propre, syn).ratio()
        best = max(best, int(r * 85))
        if len(propre) >= 3 and len(syn) >= 3:
            dist = _levenshtein(propre, syn)
            ml = max(len(propre), len(syn))
            if ml > 0: best = max(best, int((1 - dist/ml) * 78))
    year_bonus = {"conso_an1":["2022","22"],"conso_an2":["2023","23"],
                  "conso_an3":["2024","24"],"conso_an4":["2025","25"]}
    if std in year_bonus and any(y in propre for y in year_bonus[std]):
        best = max(best, 85)
    return best

def _score_contenu(series, std):
    sample = series.dropna().head(50)
    if len(sample) == 0: return 0
    cleaned = (sample.astype(str)
               .str.replace(r'[€$£\s\xa0%]','',regex=True)
               .str.replace(',','.',regex=False)
               .str.replace(r'[^\d.\-]','',regex=True))
    numeric   = pd.to_numeric(cleaned, errors='coerce')
    pct_num   = numeric.notna().mean()
    vals      = numeric.dropna()
    raw_text  = sample.astype(str)
    avg_len   = raw_text.str.len().mean()
    pct_alpha = raw_text.str.contains(r'[a-zA-Z]', na=False).mean()
    unique_r  = sample.nunique() / len(sample)
    has_dec   = (vals % 1 != 0).mean() if len(vals) > 0 else 0
    pct_int   = (vals % 1 == 0).mean() if len(vals) > 0 else 0
    pct_pos   = (vals >= 0).mean()     if len(vals) > 0 else 0
    pct_zero  = (vals == 0).mean()     if len(vals) > 0 else 0
    if std == "reference":
        score = 0
        if pct_alpha > 0.5: score += 40
        if unique_r > 0.7:  score += 25
        if 3 <= avg_len <= 50: score += 20
        if pct_num < 0.5:   score += 15
        if pct_num > 0.9 and pct_alpha < 0.1: score -= 30
        return max(0, min(score, 100))
    elif std == "quantite":
        if pct_num < 0.6: return 10
        score = 40
        if pct_int > 0.85:   score += 30
        elif pct_int > 0.65: score += 15
        if pct_zero > 0.05:  score += 8
        if pct_pos > 0.85:   score += 8
        if has_dec > 0.55:   score -= 20
        if pct_alpha > 0.3:  score -= 25
        return max(0, min(score, 100))
    elif std == "prix_unitaire":
        if pct_num < 0.6: return 5
        score = 35
        if has_dec > 0.45:   score += 30
        elif has_dec > 0.25: score += 15
        if pct_zero < 0.05:  score += 12
        if pct_pos > 0.85:   score += 8
        if pct_int > 0.95:   score -= 15
        if pct_alpha > 0.3:  score -= 25
        return max(0, min(score, 100))
    elif std in ("conso_an1","conso_an2","conso_an3","conso_an4"):
        if pct_num < 0.5: return 5
        score = 30
        if pct_int > 0.80:   score += 25
        elif pct_int > 0.60: score += 12
        if pct_zero > 0.15:  score += 15
        if pct_pos > 0.5:    score += 10
        if has_dec > 0.5:    score -= 15
        if pct_alpha > 0.3:  score -= 25
        return max(0, min(score, 100))
    return 0

def smart_ingester_stock_ultime(df, client_ai=None):
    df = df.dropna(how='all').copy()
    df = df[df.apply(lambda r: r.astype(str).str.strip().ne('').any(), axis=1)]
    CIBLES = list(SYNONYMES.keys())
    propres = {col: nettoyer(col) for col in df.columns}
    scores = {std: {} for std in CIBLES}
    for col in df.columns:
        propre = propres[col]
        for std in CIBLES:
            sn = _score_nom(propre, std)
            sc = _score_contenu(df[col], std)
            if sn >= 70:   sf = sn + min(int(sc * 0.05), 5)
            elif sn >= 45: sf = int(sn * 0.75 + sc * 0.25)
            else:          sf = int(sn * 0.20 + sc * 0.80)
            scores[std][col] = min(sf, 100)
    for col in df.columns:
        vals = pd.to_numeric(
            df[col].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),
            errors='coerce').dropna()
        if len(vals) > 5:
            if (vals % 1 == 0).mean() > 0.9 and vals.median() > 10:
                scores["quantite"][col]      = min(scores["quantite"][col] + 10, 100)
                scores["prix_unitaire"][col] = max(scores["prix_unitaire"][col] - 8, 0)
            if (vals % 1 != 0).mean() > 0.5 and vals.median() < 1000:
                scores["prix_unitaire"][col] = min(scores["prix_unitaire"][col] + 10, 100)
                scores["quantite"][col]      = max(scores["quantite"][col] - 8, 0)
    trouvees = {}; utilisees = set()
    ORDRE  = ["reference","quantite","prix_unitaire","conso_an4","conso_an3","conso_an2","conso_an1"]
    SEUILS = {"reference":35,"quantite":55,"prix_unitaire":55,
              "conso_an4":55,"conso_an3":55,"conso_an2":55,"conso_an1":55}
    for std in ORDRE:
        seuil = SEUILS.get(std, 55)
        candidats = [(col, scores[std][col]) for col in scores[std]
                     if col not in trouvees and scores[std][col] >= seuil]
        if not candidats: continue
        nom_forts = [(col, sc) for col, sc in candidats if _score_nom(propres[col], std) >= 70]
        gagnant = max(nom_forts, key=lambda x: _score_nom(propres[x[0]], std))[0] if nom_forts \
                  else max(candidats, key=lambda x: x[1])[0]
        trouvees[gagnant] = std; utilisees.add(std)
    cols = list(df.columns)
    if "reference" not in utilisees:
        for c in cols:
            if c not in trouvees:
                s = df[c].dropna().head(20)
                if s.astype(str).str.contains(r'[a-zA-Z]', na=False).mean() > 0.3 or \
                   s.nunique()/max(len(s),1) > 0.6:
                    trouvees[c] = "reference"; utilisees.add("reference"); break
    if "quantite" not in utilisees:
        for c in cols:
            if c not in trouvees:
                num = pd.to_numeric(df[c].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'), errors='coerce')
                if num.notna().mean() > 0.6 and (num.dropna() % 1 == 0).mean() > 0.6:
                    trouvees[c] = "quantite"; utilisees.add("quantite"); break
    critiques = [s for s in ["reference","quantite"] if s not in utilisees]
    if critiques and client_ai:
        titres = list(df.columns)
        sample_data = df.head(5).astype(str).to_dict(orient='list')
        prompt = f"Columns: {titres}\nData (5 rows): {json.dumps(sample_data, ensure_ascii=False)[:3000]}\nMissing concepts: {critiques}\nRespond ONLY JSON: {{\"concept\": \"exact_title\"}} or null. Choose from: {titres}"
        try:
            r = client_ai.chat.completions.create(model="gpt-4o-mini",
                messages=[{"role":"system","content":prompt}], temperature=0.0)
            raw = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
            gpt_map = json.loads(raw)
            for std, col in gpt_map.items():
                if std in critiques and col in df.columns and col not in trouvees:
                    trouvees[col] = std; utilisees.add(std)
        except: pass
    df = df.rename(columns=trouvees)
    manq = [c for c in ["reference","quantite"] if c not in df.columns]
    if manq:
        return None, f"Columns not found: {', '.join(manq)}. Columns in your file: {list(df.columns[:10])}"
    df["quantite"] = pd.to_numeric(
        df["quantite"].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),
        errors='coerce')
    df = df.dropna(subset=["quantite"]).copy()
    df = df[df["reference"].astype(str).str.strip().ne('')]
    df = df[~df["reference"].astype(str).str.lower().isin(['nan','none',''])]
    if "prix_unitaire" not in df.columns:
        df["prix_unitaire"] = 0.0; df["_sans_prix"] = True
    else:
        df["prix_unitaire"] = pd.to_numeric(
            df["prix_unitaire"].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),
            errors='coerce').fillna(0)
        df["_sans_prix"] = (df["prix_unitaire"] == 0).all()
    has_conso = False; conso_cols = []
    for c in ["conso_an1","conso_an2","conso_an3","conso_an4"]:
        if c in df.columns:
            df[c] = pd.to_numeric(
                df[c].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),
                errors='coerce').fillna(0)
            conso_cols.append(c); has_conso = True
    df["_has_conso"] = has_conso
    df["_conso_moy"] = df[conso_cols].mean(axis=1) if has_conso else 0.0
    return df.copy(), "OK"

# ══════════════════════════════════════════════════════════════════
# DÉTECTION MODE TRANSPORT
# ══════════════════════════════════════════════════════════════════
def detect_transport_mode(df, dep_col=None, arr_col=None, mode_col=None):
    PORTS = ["havre","marseille","dunkerque","bordeaux","nantes","rouen","hamburg",
             "rotterdam","antwerp","anvers","amsterdam","felixstowe","southampton",
             "barcelona","barcelone","valencia","genova","piraeus","istanbul",
             "dakar","casablanca","lagos","abidjan","mombasa","durban","santos",
             "buenos aires","callao","shanghai","ningbo","guangzhou","shenzhen",
             "hongkong","singapore","singapour","busan","tokyo","yokohama",
             "los angeles","new york","savannah","miami","montreal","vancouver",
             "sydney","melbourne","dubai","jeddah","mumbai","colombo","tanger"]
    AIRPORTS = ["cdg","ory","lyo","mrs","nce","bor","tls","jfk","lax","ord","mia",
                "atl","sfo","fra","muc","ber","ham","vie","zrh","gva","ams","bru",
                "lhr","lgw","dxb","auh","doh","ist","bom","del","hkg","nrt","icn",
                "sin","kul","bkk","syd","mel","gru","mex","yyz","jnb","nbo","cai"]
    KW_AIR  = ["aérien","aerien","air","avion","airfreight","awb","airline"]
    KW_SEA  = ["maritime","mer","sea","ocean","bateau","navire","conteneur","container","teu","fcl","lcl","armateur","roro","reefer"]
    KW_RAIL = ["ferroviaire","rail","train","sncf","wagon"]
    KW_ROAD = ["routier","route","camion","truck","ftl","ltl","vl","tir","messagerie"]
    scores = {"aerien":0,"maritime":0,"ferroviaire":0,"routier":0}
    if mode_col and mode_col in df.columns:
        for v in df[mode_col].dropna().astype(str).str.lower():
            for kw in KW_AIR:  scores["aerien"]    += 2*(kw in v)
            for kw in KW_SEA:  scores["maritime"]  += 2*(kw in v)
            for kw in KW_RAIL: scores["ferroviaire"]+= 2*(kw in v)
            for kw in KW_ROAD: scores["routier"]   += 2*(kw in v)
    for col in [dep_col, arr_col]:
        if col and col in df.columns:
            for v in df[col].dropna().astype(str).str.lower():
                vc = nettoyer(v)
                if any(p in vc for p in AIRPORTS): scores["aerien"]  += 1
                if any(p in vc for p in PORTS):    scores["maritime"] += 1
    hdrs = [nettoyer(c) for c in df.columns]
    if any("awb" in h for h in hdrs):                         scores["aerien"]  += 5
    if any("bl" in h or "conteneur" in h or "teu" in h for h in hdrs): scores["maritime"] += 5
    dominant = max(scores, key=scores.get)
    total = sum(scores.values())
    if total == 0 or scores[dominant] < 2:
        return "routier", "🚛 Mode Routier / Road Mode", "🚛"
    labels = {"aerien":("✈️ Mode Aérien / Air Freight","✈️"),
              "maritime":("⚓ Mode Maritime / Sea Freight","⚓"),
              "ferroviaire":("🚂 Mode Ferroviaire / Rail","🚂"),
              "routier":("🚛 Mode Routier / Road","🚛")}
    label, emoji = labels[dominant]
    return dominant, label, emoji

# ══════════════════════════════════════════════════════════════════
# PROGRESSION NOMMÉE
# ══════════════════════════════════════════════════════════════════
class StepProgress:
    def __init__(self, steps):
        self._ph = st.empty(); self._steps = steps
        self._n = len(steps); self._i = 0
    def step(self, label=None):
        self._i += 1
        lbl = label or (self._steps[self._i-1] if self._i <= self._n else "")
        self._ph.progress(self._i/self._n, text=f"⏳ {lbl}")
    def done(self): self._ph.empty()

# ══════════════════════════════════════════════════════════════════
# AUTO MAP TRANSPORT + IA
# ══════════════════════════════════════════════════════════════════
def auto_map_columns_with_ai(df):
    titres = list(df.columns)
    profil = {col: {"exemples": list(df[col].dropna().astype(str).unique()[:5])} for col in titres}
    prompt = f"""Titres: {titres}\nDonnées: {json.dumps(profil, ensure_ascii=False)[:3000]}
Associe à un titre EXACT. Si absent: null.
Concepts: "client","ca","co","dep","arr","dist","poids","mode".
JSON uniquement."""
    try:
        r = client.chat.completions.create(model="gpt-4o-mini",
            messages=[{"role":"system","content":prompt}], temperature=0.0)
        raw = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        return {k: v for k, v in json.loads(raw).items() if v in titres}
    except:
        return {"client": titres[0], "ca": titres[1] if len(titres)>1 else None, "co": None}

def generate_ai_analysis(data_summary):
    if st.session_state.module == "transport":
        prompt = get_prompt_transport()
    elif st.session_state.get("stock_view") == "TERRAIN":
        prompt = get_prompt_terrain()
    else:
        prompt = get_prompt_stock()
    try:
        r = client.chat.completions.create(model="gpt-4o-mini",
            messages=[{"role":"system","content":prompt},
                      {"role":"user","content":f"Data: {data_summary}. Write the audit."}],
            temperature=0.3)
        texte = r.choices[0].message.content
        try: return texte.encode('latin-1').decode('utf-8')
        except: return texte
    except Exception as e: return f"AI Error: {str(e)}"

# ══════════════════════════════════════════════════════════════════
# PDF
# ══════════════════════════════════════════════════════════════════
class PDFReport(FPDF):
    def footer(self):
        self.set_y(-15); self.set_font("Arial","I",8); self.set_text_color(150,150,150)
        self.multi_cell(0, 4, _("pdf_footer"), align="C")

def generate_expert_pdf(title, content, figs=None):
    pdf = PDFReport()
    pdf.add_page(); pdf.set_fill_color(11,37,69); pdf.rect(0,0,210,297,'F')
    pdf.set_y(100); pdf.set_text_color(255,255,255)
    pdf.set_font("Arial","B",32); pdf.cell(0,15,"LOGIFLO.IO",ln=True,align='C')
    pdf.set_font("Arial","",14); pdf.set_text_color(200,200,200)
    pdf.cell(0,10,_("pdf_strategic"),ln=True,align='C')
    pdf.ln(30); pdf.set_text_color(255,255,255); pdf.set_font("Arial","B",20)
    pdf.cell(0,10,unicodedata.normalize('NFKD',title).encode('ASCII','ignore').decode('utf-8'),ln=True,align='C')
    pdf.ln(10); pdf.set_font("Arial","",12)
    pdf.cell(0,10,f"{_('pdf_date')} : {datetime.date.today().strftime('%d/%m/%Y')}",ln=True,align='C')
    pdf.cell(0,10,_("pdf_confidential"),ln=True,align='C')
    pdf.add_page()
    pdf.set_fill_color(240,244,248); pdf.rect(0,0,210,30,'F')
    pdf.set_y(10); pdf.set_text_color(11,37,69); pdf.set_font("Arial","B",18)
    pdf.cell(0,10,_("pdf_report"),ln=True,align='L')
    pdf.line(10,25,200,25); pdf.ln(15)
    if figs:
        for fig in figs:
            try:
                img = fig.to_image(format="png", width=800, height=350)
                with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                    tmp.write(img); tp = tmp.name
                pdf.image(tp, x=15, y=pdf.get_y()+2, w=180); pdf.ln(95)
            except: pass
    if pdf.get_y() > 200: pdf.add_page()
    content = (content.replace("\u2019","'").replace("\u2018","'")
               .replace("\u201c",'"').replace("\u201d",'"')
               .replace("\u20ac","EUR").replace("\u2022","-"))
    for line in content.split('\n'):
        line = line.strip()
        if not line: pdf.ln(4); continue
        if line.startswith('### '):
            t = unicodedata.normalize('NFKD',line[4:]).encode('ASCII','ignore').decode('utf-8')
            pdf.ln(6); pdf.set_font("Arial","BU",12); pdf.set_text_color(11,37,69)
            pdf.cell(0,8,t.upper(),ln=True)
            pdf.set_font("Arial","",11); pdf.set_text_color(40,40,40)
        else:
            pdf.multi_cell(0,6,unicodedata.normalize('NFKD',line.replace("**","")).encode('ASCII','ignore').decode('utf-8'))
    # CTA final
    if pdf.get_y() > 230: pdf.add_page()
    pdf.ln(10); pdf.set_fill_color(11,37,69); pdf.set_text_color(255,255,255)
    pdf.set_font("Arial","B",13)
    cta_t = unicodedata.normalize('NFKD',_("pdf_cta_title")).encode('ASCII','ignore').decode('utf-8')
    pdf.cell(0,10,cta_t,ln=True,align='C')
    pdf.set_font("Arial","",10)
    for line in _("pdf_cta_body").split('\n'):
        line_clean = unicodedata.normalize('NFKD',line).encode('ASCII','ignore').decode('utf-8')
        pdf.cell(0,8,line_clean,ln=True,align='C')
    return pdf.output(dest='S').encode('latin-1')

# ══════════════════════════════════════════════════════════════════
# ROUTING ORS
# ══════════════════════════════════════════════════════════════════
def calculate_haversine(lon1,lat1,lon2,lat2):
    R=6371.0; dlat=math.radians(lat2-lat1); dlon=math.radians(lon2-lon1)
    a=math.sin(dlat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

def fetch_geo(city, _t=None):
    if not city or str(city).strip() in ("","nan","None"): return city, None
    try:
        r = requests.get("https://nominatim.openstreetmap.org/search",
            params={"q":str(city).strip(),"format":"json","limit":1},
            headers={"User-Agent":"Logiflo.io/2.0"}, timeout=5)
        if r.status_code == 200:
            d = r.json()
            if d: return city, [float(d[0]["lon"]), float(d[0]["lat"])]
    except: pass
    return city, None

def geocode_cities_mapbox(cities):
    villes = [c for c in set(str(v) for v in cities)
              if c not in st.session_state.geo_cache and c not in ("","nan","None")]
    if villes:
        bar = st.progress(0, text=f"📍 {_('step_geo')}")
        for i, city in enumerate(villes):
            _, coord = fetch_geo(city)
            if coord: st.session_state.geo_cache[city] = coord
            time.sleep(1.1)
            bar.progress((i+1)/len(villes), text=f"📍 {_('step_geo')} ({i+1}/{len(villes)})")
        bar.empty()
    return {c: st.session_state.geo_cache[c] for c in set(str(v) for v in cities) if c in st.session_state.geo_cache}

@st.cache_data(show_spinner=False)
def _ors_distance(lon1,lat1,lon2,lat2):
    for profile in ["driving-hgv","driving-car"]:
        try:
            r = requests.post(f"https://api.openrouteservice.org/v2/directions/{profile}",
                json={"coordinates":[[lon1,lat1],[lon2,lat2]],"instructions":False},
                headers={"Accept":"application/json","Content-Type":"application/json","Authorization":ORS_API_KEY},
                timeout=6)
            if r.status_code == 200:
                return r.json()["routes"][0]["summary"]["distance"]/1000.0
        except: continue
    return None

def fetch_route(dep, arr, mode, coords, _t=None):
    c1,c2 = coords.get(str(dep)), coords.get(str(arr))
    if not c1 or not c2: return (dep,arr,mode), 0.0
    lon1,lat1=c1; lon2,lat2=c2
    dv = calculate_haversine(lon1,lat1,lon2,lat2); m = str(mode).lower()
    if any(k in m for k in ["mer","sea","maritime","bateau","port","ferry","conteneur"]): return (dep,arr,mode), dv*1.25
    elif any(k in m for k in ["air","avion","aerien","flight"]): return (dep,arr,mode), dv*1.05
    elif any(k in m for k in ["fer","rail","train","sncf"]): return (dep,arr,mode), dv*1.15
    else:
        d = _ors_distance(lon1,lat1,lon2,lat2)
        return (dep,arr,mode), (d if d and d>0 else dv*1.30)

def smart_multimodal_router(df, dep_col, arr_col, mode_col=None):
    coords = geocode_cities_mapbox(pd.concat([df[dep_col],df[arr_col]]).dropna().unique())
    uniq = []
    for _, row in df.iterrows():
        dep=row[dep_col]; arr=row[arr_col]
        mode = str(row[mode_col]).lower() if mode_col and pd.notna(row.get(mode_col)) else "route"
        k = (dep,arr,mode)
        if k not in st.session_state.route_cache and k not in uniq: uniq.append(k)
    if uniq:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            for key, dist in [f.result() for f in concurrent.futures.as_completed(
                [ex.submit(fetch_route,r[0],r[1],r[2],coords) for r in uniq])]:
                st.session_state.route_cache[key] = dist
    df["_DIST_CALCULEE"] = [
        st.session_state.route_cache.get(
            (row[dep_col],row[arr_col],
             str(row[mode_col]).lower() if mode_col and pd.notna(row.get(mode_col)) else "route"),0.0)
        for _,row in df.iterrows()]
    return df

def super_clean(val):
    if pd.isna(val): return 0.0
    try: return float(str(val).replace('€','').replace('$','').replace('EUR','').replace(' ','').replace('\xa0','').replace(',','.'))
    except: return 0.0

# ══════════════════════════════════════════════════════════════════
# PAGES
# ══════════════════════════════════════════════════════════════════
if st.session_state.page == "accueil":
    st.markdown(f"<h1 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;font-weight:800;letter-spacing:-1px;'>LOGIFLO.IO</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center;font-size:1.1em;color:#4A6080;'>{_('home_sub')}</p><br>", unsafe_allow_html=True)
    # Sélecteur de langue sur la page accueil
    _,lc,_ = st.columns([2,1,2])
    with lc:
        lang_choice = st.selectbox("", ["🇫🇷 Français","🇬🇧 English"], label_visibility="collapsed", key="lang_home")
        st.session_state.language = "en" if "English" in lang_choice else "fr"
    st.markdown("<br>", unsafe_allow_html=True)
    c1,c2 = st.columns(2)
    with c1:
        st.markdown("<span class='big-emoji'>📦</span>", unsafe_allow_html=True)
        if st.button(_("home_stock"), use_container_width=True):
            st.session_state.module="stock"; st.session_state.page="choix_profil_stock"; st.rerun()
    with c2:
        st.markdown("<span class='big-emoji'>🌍</span>", unsafe_allow_html=True)
        if st.button(_("home_transport"), use_container_width=True):
            st.session_state.module="transport"; st.session_state.page="login"; st.rerun()
    st.markdown("<br><br>", unsafe_allow_html=True)
    _,cm,_ = st.columns([1,1,1])
    if cm.button(_("home_access"), use_container_width=True):
        st.session_state.page="contact"; st.rerun()

elif st.session_state.page == "contact":
    _,cc,_ = st.columns([1,1.5,1])
    with cc:
        with st.form("vip"):
            st.text_input("Nom / Name"); st.text_input("Email"); st.text_input("Entreprise / Company")
            st.selectbox("Volume", ["< 10M EUR","10-50M EUR","> 50M EUR"])
            if st.form_submit_button("Submit", use_container_width=True):
                st.success("✅ Request sent. We'll contact you within 24h.")
        if st.button(_("login_back"), use_container_width=True): st.session_state.page="accueil"; st.rerun()

elif st.session_state.page == "choix_profil_stock":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>{_('profile_title')}</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center;color:#4A6080;'>{_('profile_sub')}</p><br><br>", unsafe_allow_html=True)
    c1,c2 = st.columns(2)
    with c1:
        st.markdown("<span class='big-emoji'>📊</span>", unsafe_allow_html=True)
        if st.button(_("profile_mgr"), use_container_width=True):
            st.session_state.stock_view="MANAGER"; st.session_state.page="login"; st.rerun()
    with c2:
        st.markdown("<span class='big-emoji'>👷</span>", unsafe_allow_html=True)
        if st.button(_("profile_ops"), use_container_width=True):
            st.session_state.stock_view="TERRAIN"; st.session_state.page="login"; st.rerun()

elif st.session_state.page == "login":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>{'Accès Sécurisé' if st.session_state.language=='fr' else 'Secure Access'} — {st.session_state.module.upper()}</h2><br>", unsafe_allow_html=True)
    _,cl,_ = st.columns([1,1.2,1])
    with cl:
        with st.form("login_form"):
            u = st.text_input(_("login_id"))
            p = st.text_input(_("login_pw"), type="password")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.form_submit_button(_("login_btn"), use_container_width=True):
                if u in USERS_DB and USERS_DB[u] == p:
                    st.session_state.auth=True; st.session_state.current_user=u
                    st.session_state.page="app"; st.rerun()
                else: st.error(_("login_err"))
        if st.button(_("login_back"), use_container_width=True): st.session_state.page="accueil"; st.rerun()

elif st.session_state.auth and st.session_state.page == "app":

    with st.sidebar:
        st.markdown(f"""
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;">
                <div class="sidebar-logo">LOGI<span>FLO</span>.IO</div>
                <div style="font-size:20px;line-height:1.2;">📦<br>📦📦</div>
            </div>
            <div style="font-size:12px;color:#4A6080;margin-bottom:8px;">
                👤 {_('connectedas')} : <b style="color:white;">{st.session_state.current_user}</b>
            </div>
        """, unsafe_allow_html=True)
        lang_sb = st.selectbox("", ["🇫🇷 Français","🇬🇧 English"],
            index=0 if st.session_state.language=="fr" else 1,
            label_visibility="collapsed", key="lang_sidebar")
        st.session_state.language = "en" if "English" in lang_sb else "fr"
        st.markdown("---")
        nav = st.radio("", [_("nav_workspace"),_("nav_archives"),_("nav_params"),_("nav_legal")],
                       label_visibility="collapsed")
        st.markdown("---")
        if st.button(_("nav_logout"), use_container_width=True): st.session_state.clear(); st.rerun()
        st.markdown("<div style='margin-top:40px;border-top:1px solid #1e3a5f;padding-top:14px;font-size:11px;color:#4A6080;'>© 2026 Logiflo B2B Enterprise</div>", unsafe_allow_html=True)

    # ── INFORMATIONS LÉGALES ──────────────────────────────────────
    if nav == _("nav_legal"):
        st.title(f"⚖️ {_('nav_legal')}")
        tab1,tab2,tab3 = st.tabs(["📋 Mentions Légales / Legal Notice","🔒 Politique de Confidentialité / Privacy","📄 CGUV / T&C"])
        with tab1:
            st.markdown("""<div class="legal-text">
            <h2>1. ÉDITEUR / PUBLISHER</h2>
            <div class="legal-box"><p><strong>Logiflo B2B Enterprise</strong> — SASU (en cours d'immatriculation / being registered)<br>
            Siège social / Registered office : Marseille, France<br>
            Responsable / Representative : Eric [NOM]<br>
            Email : contact@logiflo.io<br>
            App : https://logiflo-io.streamlit.app</p></div>
            <h2>2. HÉBERGEMENT / HOSTING</h2>
            <p>App : Streamlit Cloud — Snowflake Inc., Bozeman MT USA</p>
            <p>Site vitrine / Website : GitHub Pages — GitHub Inc., San Francisco CA USA</p>
            <h2>3. PROPRIÉTÉ INTELLECTUELLE / INTELLECTUAL PROPERTY</h2>
            <p>All elements of LOGIFLO.IO (code, algorithms, Smart Ingester™, AI engines, interface) are the exclusive property of Logiflo B2B Enterprise, protected under French and international intellectual property law.</p>
            <h2>4. CRÉDITS / CREDITS</h2>
            <ul><li>AI: OpenAI GPT-4o-mini</li><li>Routing: OpenRouteService (HeiGIT)</li>
            <li>Geocoding: Nominatim / OpenStreetMap</li><li>Framework: Streamlit</li></ul>
            <p style="color:#4A6080;font-size:13px;"><em>Last updated: April 2026</em></p>
            </div>""", unsafe_allow_html=True)
        with tab2:
            st.markdown("""<div class="legal-text">
            <div class="legal-box"><p>Compliant with GDPR (EU) 2016/679 — Controller: Logiflo B2B Enterprise — contact@logiflo.io</p></div>
            <h2>ZERO DATA RETENTION POLICY</h2>
            <div class="legal-box"><p>
            ✅ Raw files processed in RAM only — never stored permanently<br>
            ✅ Data never sold or transferred to third parties<br>
            ✅ Data never used to train public AI models<br>
            ✅ Automatic purge on disconnect</p></div>
            <h2>DATA COLLECTED</h2>
            <p><strong>Connection data:</strong> username, login date/time, module used.</p>
            <p><strong>Archive data (voluntary):</strong> aggregated metrics, AI summary, PDF — stored in your personal space only.</p>
            <h2>SUB-PROCESSORS</h2>
            <ul><li>Streamlit Cloud (Snowflake) — hosting — USA (EU SCC)</li>
            <li>OpenAI — AI analysis — USA (GDPR DPA)</li>
            <li>Google Sheets — archiving — EU/USA</li>
            <li>OpenRouteService — distances — Germany (EU)</li></ul>
            <h2>YOUR RIGHTS (GDPR ART. 15-22)</h2>
            <p>Access, rectification, erasure, portability: <strong>contact@logiflo.io</strong> — 30 days response.<br>
            CNIL complaint: <strong>www.cnil.fr</strong></p>
            <p style="color:#4A6080;font-size:13px;"><em>Last updated: April 2026</em></p>
            </div>""", unsafe_allow_html=True)
        with tab3:
            st.markdown("""<div class="legal-text">
            <p>Full T&C (15 articles) available on request: <strong>contact@logiflo.io</strong></p>
            <h2>KEY POINTS</h2>
            <div class="legal-box"><p>⚠️ Audits and AI recommendations are provided as <strong>decision support only</strong>. They do not constitute financial, legal or accounting advice. The Client remains the sole decision-maker.</p></div>
            <h2>DATA OWNERSHIP</h2>
            <p>The Client retains full ownership of their data. Generated reports belong to the Client.</p>
            <h2>LIABILITY</h2>
            <p>Logiflo's liability is limited to amounts paid in the last 12 months.</p>
            <h2>GOVERNING LAW</h2>
            <p>French law — Commercial Courts of Marseille.</p>
            <p style="color:#4A6080;font-size:13px;"><em>Version 1.0 — April 2026</em></p>
            </div>""", unsafe_allow_html=True)

    # ── ARCHIVES ─────────────────────────────────────────────────
    elif nav == _("nav_archives"):
        st.title(_("arch_title"))
        st.markdown("---")
        with st.spinner("Loading..."):
            df_arch = load_archives_from_sheets(st.session_state.current_user)
        if df_arch is None:
            st.warning("⚠️ Google Sheets connection unavailable.")
        elif df_arch.empty:
            st.info(_("arch_empty"))
        else:
            cf1,cf2 = st.columns(2)
            mf = cf1.selectbox(_("arch_filter"), [_("arch_filter_all"),"stock","transport"])
            nb = cf2.slider("", 5, 50, 10, label_visibility="collapsed")
            ds = df_arch.copy()
            if mf != _("arch_filter_all"): ds = ds[ds["module"]==mf]
            ds = ds.iloc[::-1].head(nb)
            for _,row in ds.iterrows():
                icon = "📦" if row.get("module")=="stock" else "🚚"
                st.markdown(f"""<div class="archive-card">
                    <h4>{icon} Audit {str(row.get('module','')).upper()} — {row.get('date','')} à {row.get('heure','')}</h4>
                    <div style="font-size:12px;color:#4A6080;margin-bottom:8px;">{row.get('nb_lignes','')} {_('arch_lines')}</div>
                    <span class="archive-kpi">{row.get('kpi_label_1','')}: {row.get('kpi_1','')}</span>
                    <span class="archive-kpi">{row.get('kpi_label_2','')}: {row.get('kpi_2','')}</span>
                    <span class="archive-kpi">{row.get('kpi_label_3','')}: {row.get('kpi_3','')}</span>
                </div>""", unsafe_allow_html=True)
                with st.expander(_("arch_resume")):
                    resume = row.get("resume_ia","")
                    if resume: st.markdown(render_report(str(resume),"manager"), unsafe_allow_html=True)
                pdf_b64 = row.get("pdf_base64","")
                if pdf_b64:
                    try:
                        st.download_button(_("arch_dl"), base64.b64decode(str(pdf_b64)),
                            f"Logiflo_{row.get('date','').replace('/','_')}_{row.get('module','')}.pdf",
                            key=f"dl_{row.get('date','')}_{row.get('heure','')}",
                            use_container_width=True)
                    except: pass

    # ── PARAMÈTRES ───────────────────────────────────────────────
    elif nav == _("nav_params"):
        st.title(_("params_title"))
        if st.session_state.module == "stock":
            st.session_state.seuil_bas      = st.slider(_("params_alert"), 0, 100, st.session_state.seuil_bas)
            st.session_state.seuil_rupture  = st.slider(_("params_rupture"), 0, 10, st.session_state.seuil_rupture)
        else:
            st.session_state.seuil_km       = st.slider(_("params_km"), 0, 1000, st.session_state.seuil_km)

    # ── ESPACE DE TRAVAIL ─────────────────────────────────────────
    elif nav == _("nav_workspace"):

        # ══════════════ MODULE STOCK ══════════════════════════════
        if st.session_state.module == "stock":
            st.title(_("stock_title"))
            ci,cb = st.columns([4,1])
            ci.markdown(f"**{'Profil Actif' if st.session_state.language=='fr' else 'Active Profile'} : {st.session_state.stock_view}**")
            if cb.button(_("stock_change_profile")): st.session_state.page="choix_profil_stock"; st.rerun()
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(f"""<div class='import-card'><h3>{_('stock_import')}</h3><p>{_('stock_import_sub')}</p></div>""", unsafe_allow_html=True)
            up = st.file_uploader("", type=["csv","xlsx"], key="stock_upload")
            st.markdown("---")

            if up:
                pg = StepProgress([_("step_read"),_("step_detect"),_("step_calc")])
                pg.step()
                try:
                    df_brut = pd.read_excel(up) if up.name.endswith("xlsx") else pd.read_csv(up, encoding='utf-8')
                except UnicodeDecodeError:
                    up.seek(0); df_brut = pd.read_csv(up, encoding='latin-1')
                except Exception:
                    up.seek(0); df_brut = pd.read_csv(up, sep=';', encoding='latin-1')
                pg.step()
                df_propre, statut = smart_ingester_stock_ultime(df_brut, client_ai=client)
                pg.step(); pg.done()
                if df_propre is None: st.error(statut)
                else: st.session_state.df_stock = df_propre

            if st.session_state.df_stock is not None:
                df = st.session_state.df_stock.copy()
                sans_prix = bool(df.get("_sans_prix", pd.Series([True])).iloc[0]) if "_sans_prix" in df.columns else True
                has_conso = bool(df.get("_has_conso", pd.Series([False])).iloc[0]) if "_has_conso" in df.columns else False

                if sans_prix: st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_no_price')}</span>", unsafe_allow_html=True)
                if has_conso: st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_conso')}</span>", unsafe_allow_html=True)
                else:         st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_no_conso')}</span>", unsafe_allow_html=True)

                if has_conso:
                    df["_conso_moy"] = df["_conso_moy"].fillna(0)
                    df["Couverture_mois"] = np.where(df["_conso_moy"]>0, df["quantite"]/df["_conso_moy"], 9999)
                    df["Statut"] = np.select(
                        [(df["quantite"]<=st.session_state.seuil_rupture),
                         (df["quantite"]>0)&(df["_conso_moy"]==0),
                         (df["quantite"]>0)&(df["Couverture_mois"]>6)],
                        ["🚨 RUPTURE","🔴 Dormant","🟠 Surstock (>6 mois)"], default="✅ Sain")
                else:
                    df["Statut"] = np.where(df["quantite"]<=st.session_state.seuil_rupture,"🚨 RUPTURE","✅ EN STOCK")

                df["valeur_totale"] = df["quantite"] * df["prix_unitaire"]
                val_totale = df["valeur_totale"].sum()
                ruptures   = df[df["Statut"]=="🚨 RUPTURE"]
                tx_serv    = (1-len(ruptures)/len(df))*100 if len(df)>0 else 100

                if not st.session_state.history_stock or st.session_state.history_stock[-1].get("valeur")!=val_totale:
                    st.session_state.history_stock.append({"date":datetime.datetime.now().strftime("%H:%M:%S"),"valeur":val_totale})

                cmap = {"🚨 RUPTURE":"#E8304A","✅ EN STOCK":"#00C896","✅ Sain":"#00C896",
                        "🔴 Dormant":"#c0392b","🟠 Surstock (>6 mois)":"#f39c12"}

                if st.session_state.stock_view == "MANAGER":
                    c1,c2,c3 = st.columns(3)
                    kpi1_val = f"{val_totale:,.0f} €" if not sans_prix else str(len(df))
                    kpi1_lbl = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                    c1.markdown(f"<div class='kpi-card'><h4>{kpi1_lbl}</h4><h2 style='color:#0B2545;'>{kpi1_val}</h2></div>", unsafe_allow_html=True)
                    c2.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_service')}</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>", unsafe_allow_html=True)
                    c3.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_rupture')}</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>", unsafe_allow_html=True)

                    st.markdown("<br>", unsafe_allow_html=True)
                    cp,cl2 = st.columns(2)
                    with cp:
                        fig_pie = px.pie(df, names="Statut", hole=0.4, color="Statut", color_discrete_map=cmap)
                        fig_pie.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                        st.plotly_chart(fig_pie, use_container_width=True)
                    with cl2:
                        if has_conso:
                            top15 = df.nlargest(15,"_conso_moy")[["reference","_conso_moy","quantite"]].copy()
                            fig_conso = px.bar(top15, x="reference", y=["quantite","_conso_moy"],
                                barmode="group",
                                color_discrete_map={"quantite":"#0B2545","_conso_moy":"#00C896"},
                                title="Stock vs Conso moy. (Top 15)")
                            fig_conso.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_conso, use_container_width=True)
                        else:
                            fig_line = px.line(pd.DataFrame(st.session_state.history_stock), x="date", y="valeur")
                            fig_line.update_traces(line_color="#00C896")
                            fig_line.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_line, use_container_width=True)

                    col_ia, col_sv = st.columns([3,1])
                    with col_ia:
                        run_ia = st.button(_("stock_btn_ia"), use_container_width=True)
                    with col_sv:
                        if st.button(_("stock_btn_save"), use_container_width=True, key="save_stock_early"):
                            kpi1 = val_totale if not sans_prix else float(len(df))
                            lbl1 = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                            ok = save_audit_to_sheets(st.session_state.current_user,"stock",len(df),
                                [kpi1,tx_serv,len(ruptures)],[lbl1,_("stock_kpi_service"),_("stock_kpi_rupture")],
                                st.session_state.analysis_stock or "", st.session_state.last_pdf or b"")
                            if ok: st.success(_("stock_saved"))
                            else: st.warning(_("stock_save_err"))

                    if run_ia:
                        pg2 = StepProgress([_("step_prep"),_("step_ia_progress"),_("step_report")])
                        pg2.step()
                        df_tox = df[df["Statut"].isin(["🔴 Dormant","🟠 Surstock (>6 mois)"])]
                        pires  = df_tox.nlargest(3,"quantite") if not df_tox.empty else df.nlargest(3,"quantite")
                        top_s  = ", ".join([f"{r['reference']} (qty:{r['quantite']:.0f})" for _,r in pires.iterrows()])
                        rupt_l = ruptures.nlargest(3,"quantite")["reference"].astype(str).tolist() if not ruptures.empty else "None"
                        med_i  = " BLIND SPOT: no consumption history. Sector benchmark: 2-4 months healthy coverage." if not has_conso \
                                 else f" Avg consumption: {df['_conso_moy'].mean():.1f}/period. Avg coverage: {df['Couverture_mois'].replace(9999,np.nan).mean():.1f} months."
                        prix_i = "" if sans_prix else f" Tied-up capital: {val_totale:.0f} EUR."
                        pg2.step()
                        st.session_state.analysis_stock = generate_ai_analysis(
                            f"Items: {len(df)}. Service level: {tx_serv:.1f}%. Stock-outs: {len(ruptures)}. "
                            f"Top dormant: {top_s}. Top stock-outs: {rupt_l}.{prix_i}{med_i} "
                            f"Prices: {'No' if sans_prix else 'Yes'}. History: {'Yes' if has_conso else 'No'}.")
                        pg2.step()
                        figs_pdf = [fig_pie]
                        if has_conso: figs_pdf.append(fig_conso)
                        st.session_state.last_pdf = generate_expert_pdf(_("pdf_title_stock"), st.session_state.analysis_stock, figs_pdf)
                        kpi1 = val_totale if not sans_prix else float(len(df))
                        lbl1 = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                        st.session_state.last_kpis   = [kpi1, tx_serv, len(ruptures)]
                        st.session_state.last_labels = [lbl1, _("stock_kpi_service"), _("stock_kpi_rupture")]
                        pg2.done()

                    if st.session_state.analysis_stock:
                        st.markdown(render_report(st.session_state.analysis_stock,"manager"), unsafe_allow_html=True)
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.session_state.last_pdf:
                            st.download_button(_("stock_btn_dl"), st.session_state.last_pdf,
                                "Audit_Stock_Logiflo.pdf", use_container_width=True)

                elif st.session_state.stock_view == "TERRAIN":
                    c1,c2 = st.columns(2)
                    c1.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_rupture')}</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>", unsafe_allow_html=True)
                    c2.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_service')}</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>", unsafe_allow_html=True)
                    st.markdown(_("stock_prio"))
                    if len(ruptures)>0:
                        cols_s = ["reference","quantite","Statut"]
                        if has_conso: cols_s.append("_conso_moy")
                        st.dataframe(ruptures[cols_s], use_container_width=True)
                    else: st.success(_("stock_no_rupture"))
                    if st.button(_("stock_btn_ia_terrain"), use_container_width=True):
                        pg3 = StepProgress([_("step_read"),_("step_ia_progress"),_("step_report")])
                        pg3.step()
                        top_c = df.nsmallest(5,"quantite")
                        top_s = ", ".join([f"{r['reference']} ({r['quantite']:.0f})" for _,r in top_c.iterrows()])
                        dorm_s = f"{len(df[df['_conso_moy']==0])} items no movement" if has_conso else "No history available"
                        med_t  = f" Avg consumption: {df['_conso_moy'].mean():.1f}/period." if has_conso else " No consumption history."
                        pg3.step()
                        st.session_state.analysis_stock = generate_ai_analysis(
                            f"Field stock: {len(df)} items. Stock-outs: {len(ruptures)}. "
                            f"Lowest stocks: {top_s}. Dormant: {dorm_s}.{med_t}")
                        pg3.done()
                    if st.session_state.analysis_stock:
                        st.markdown(render_report(st.session_state.analysis_stock,"terrain"), unsafe_allow_html=True)
                        st.markdown(_("stock_full"))
                        cols_s = ["reference","quantite","Statut"]
                        if has_conso: cols_s.append("_conso_moy")
                        st.dataframe(df[cols_s], use_container_width=True, height=400)

        # ══════════════ MODULE TRANSPORT ══════════════════════════
        elif st.session_state.module == "transport":
            st.title(_("trans_title"))
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(f"""<div class='import-card'><h3>{_('trans_import')}</h3><p>{_('trans_import_sub')}</p></div>""", unsafe_allow_html=True)
            up_t = st.file_uploader("", type=["csv","xlsx"], key="trans_upload")
            st.markdown("---")

            if up_t and st.session_state.trans_filename != up_t.name:
                pg4 = StepProgress([_("step_read"),_("step_col_ia"),_("step_mode")])
                pg4.step()
                try: df_t = pd.read_excel(up_t) if up_t.name.endswith("xlsx") else pd.read_csv(up_t, encoding='utf-8')
                except UnicodeDecodeError:
                    up_t.seek(0); df_t = pd.read_csv(up_t, encoding='latin-1')
                pg4.step()
                mapping = auto_map_columns_with_ai(df_t)
                pg4.step()
                dep_c_tmp  = mapping.get("dep")  if mapping.get("dep")  in df_t.columns else None
                arr_c_tmp  = mapping.get("arr")  if mapping.get("arr")  in df_t.columns else None
                mode_c_tmp = mapping.get("mode") if mapping.get("mode") in df_t.columns else None
                mode_det, mode_label, mode_emoji = detect_transport_mode(df_t, dep_c_tmp, arr_c_tmp, mode_c_tmp)
                st.session_state.trans_mapping       = mapping
                st.session_state.df_trans            = df_t
                st.session_state.trans_filename      = up_t.name
                st.session_state.trans_mode_detected = (mode_det, mode_label, mode_emoji)
                pg4.done()

            if st.session_state.df_trans is not None:
                df_t    = st.session_state.df_trans
                mapping = st.session_state.trans_mapping

                if st.session_state.trans_mode_detected:
                    _, mode_label, _ = st.session_state.trans_mode_detected
                    st.markdown(f"<div class='mode-badge'>{mode_label} — {'analyse adaptée activée' if st.session_state.language=='fr' else 'adapted analysis activated'}</div>", unsafe_allow_html=True)

                def col(k): return mapping.get(k) if mapping.get(k) in df_t.columns else None
                tour_c  = col("client") or df_t.columns[0]
                dep_c   = col("dep"); arr_c = col("arr"); dist_c = col("dist")
                mode_c  = col("mode"); ca_c = col("ca"); co_c = col("co"); poids_c = col("poids")

                if not co_c:
                    for c in df_t.columns:
                        if any(k in str(c).lower() for k in ["coût","cout","cost","achat"]): co_c=c; break
                if not ca_c:
                    for c in df_t.columns:
                        if any(k in str(c).lower() for k in ["ca","revenue","revenu","facture"]): ca_c=c; break
                if not co_c: st.error(_("trans_no_cost")); st.stop()

                df_t["_CO"] = df_t[co_c].apply(super_clean)
                if ca_c: df_t["_CA"] = df_t[ca_c].apply(super_clean)
                else:    df_t["_CA"] = df_t["_CO"]/0.85; st.warning(_("trans_ca_miss"))
                df_t["Marge_Nette"] = df_t["_CA"] - df_t["_CO"]

                if dep_c and arr_c and "_DIST_CALCULEE" not in df_t.columns:
                    pg5 = StepProgress([_("step_geo"),_("step_dist"),_("step_calc")])
                    pg5.step()
                    df_t = smart_multimodal_router(df_t, dep_c, arr_c, mode_c)
                    pg5.step(); pg5.done()
                    st.session_state.df_trans = df_t

                df_t["_DIST_FINALE"] = (df_t["_DIST_CALCULEE"] if "_DIST_CALCULEE" in df_t.columns and df_t["_DIST_CALCULEE"].sum()>0
                                        else (df_t[dist_c].apply(super_clean) if dist_c else 0))
                df_t["Rentabilité_%"] = np.where(df_t["_CA"]>0, df_t["Marge_Nette"]/df_t["_CA"]*100, 0)
                df_t["_DS"]  = df_t["_DIST_FINALE"].replace(0,1)
                df_t["Cout_KM"] = np.where(df_t["_DIST_FINALE"]>0, df_t["_CO"]/df_t["_DS"], 0)

                poids_info = ""
                if poids_c:
                    df_t["_POIDS"] = df_t[poids_c].apply(super_clean)
                    df_t["Cout_kg"] = np.where(df_t["_POIDS"]>0, df_t["_CO"]/df_t["_POIDS"].replace(0,1), 0)
                    poids_info = f" Total weight: {df_t['_POIDS'].sum():,.0f} kg. Avg cost/kg: {df_t['Cout_kg'].mean():.3f} EUR."

                marge_tot = df_t["Marge_Nette"].sum()
                ca_tot    = df_t["_CA"].sum()
                taux      = (marge_tot/ca_tot*100) if ca_tot>0 else 0
                traj_def  = len(df_t[df_t["Marge_Nette"]<0])
                cout_km   = df_t["Cout_KM"].mean()
                toxiques  = df_t[df_t["Marge_Nette"]<(df_t["_CA"]*0.05)]
                fuite     = toxiques["_CO"].sum()-toxiques["_CA"].sum()
                nb_tox    = len(toxiques)

                c1,c2,c3 = st.columns(3)
                c1.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_marge')}</h4><h2 style='color:#0B2545;'>{marge_tot:,.0f} €</h2></div>", unsafe_allow_html=True)
                c2.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_taux')}</h4><h2 style='color:#00C896;'>{taux:.1f} %</h2></div>", unsafe_allow_html=True)
                if fuite > 0:
                    c3.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_fuite')}</h4><h2 style='color:#E8304A;'>-{fuite:,.0f} €</h2><p>{nb_tox} {'trajets' if st.session_state.language=='fr' else 'routes'}</p></div>", unsafe_allow_html=True)
                else:
                    c3.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_sain')}</h4><h2 style='color:#00C896;'>{'Sain' if st.session_state.language=='fr' else 'Healthy'}</h2></div>", unsafe_allow_html=True)

                if poids_c: st.info(f"⚖️ {_('poids_info')} : **{df_t['Cout_kg'].mean():.3f} €/kg** | Total : **{df_t['_POIDS'].sum():,.0f} kg**")

                # Bouton save avant l'IA
                col_ia2, col_sv2 = st.columns([3,1])
                with col_ia2:
                    run_ia_t = st.button(_("trans_btn_ia"), use_container_width=True)
                with col_sv2:
                    if st.button(_("save_early"), use_container_width=True, key="save_trans_early"):
                        ok = save_audit_to_sheets(st.session_state.current_user,"transport",len(df_t),
                            [marge_tot,taux,nb_tox],[_("trans_kpi_marge"),_("trans_kpi_taux"),"Trajets toxiques"],
                            st.session_state.analysis_trans or "", st.session_state.last_pdf or b"")
                        if ok: st.success(_("stock_saved"))
                        else:  st.warning(_("stock_save_err"))

                # ── GRAPHIQUES REFAITS ────────────────────────────
                df_plot = df_t.copy()
                STAT_LOSS  = _("trans_stat_loss")
                STAT_ALERT = _("trans_stat_alert")
                STAT_OK    = _("trans_stat_ok")
                df_plot["Statut"] = np.where(
                    df_plot["Rentabilité_%"] < 0,   STAT_LOSS,
                    np.where(df_plot["Rentabilité_%"] < 10, STAT_ALERT, STAT_OK))
                CMAP = {STAT_LOSS:"#E8304A", STAT_ALERT:"#f39c12", STAT_OK:"#00C896"}

                tab_top, tab_global = st.tabs([_("trans_tab_top"), _("trans_tab_all")])

                with tab_top:
                    top_n = df_plot.nsmallest(15,"Marge_Nette").sort_values("Marge_Nette")
                    top_n["label"]     = top_n[tour_c].astype(str).str[:35]
                    top_n["pct_label"] = top_n["Rentabilité_%"].apply(lambda x: f"{x:.1f}%")
                    fig_top = px.bar(
                        top_n, x="Marge_Nette", y="label", orientation="h",
                        color="Statut", color_discrete_map=CMAP,
                        text="pct_label", custom_data=["_CA","_CO","Rentabilité_%"],
                        title=_("trans_chart_title_top"),
                        labels={"Marge_Nette": f"{'Marge nette' if st.session_state.language=='fr' else 'Net margin'} (€)", "label":""})
                    fig_top.update_traces(
                        textposition="outside",
                        hovertemplate=(
                            "<b>%{y}</b><br>"
                            f"{'Marge' if st.session_state.language=='fr' else 'Margin'} : <b>%{{x:,.0f}} €</b><br>"
                            f"CA : %{{customdata[0]:,.0f}} €<br>"
                            f"{'Coût' if st.session_state.language=='fr' else 'Cost'} : %{{customdata[1]:,.0f}} €<br>"
                            f"{'Rentabilité' if st.session_state.language=='fr' else 'Profitability'} : %{{customdata[2]:.1f}}%"
                            "<extra></extra>"))
                    fig_top.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        height=520, showlegend=False,
                        margin=dict(l=20,r=90,t=50,b=20),
                        xaxis=dict(title=f"{'Marge nette' if st.session_state.language=='fr' else 'Net margin'} (€)",
                                   tickformat=",.0f", gridcolor="#f0f4f8",
                                   zerolinecolor="#0B2545", zerolinewidth=2),
                        yaxis=dict(title="", tickfont=dict(size=12)),
                        font=dict(family="DM Sans",size=12,color="#0B2545"),
                        title=dict(font=dict(family="Syne",size=14,color="#0B2545")))
                    st.plotly_chart(fig_top, use_container_width=True)
                    st.markdown(_("stock_detail"))
                    cols_show = [c for c in [tour_c,"_CA","_CO","Marge_Nette","Rentabilité_%","Statut"] if c in df_t.columns]
                    rename_map = {tour_c:"Client / Route","_CA":"CA (€)","_CO":"Cost (€)",
                                  "Marge_Nette":"Margin (€)","Rentabilité_%":"Margin (%)"}
                    styled = top_n[cols_show].rename(columns=rename_map)
                    fmt = {}
                    for col_name in ["CA (€)","Cost (€)","Margin (€)"]:
                        if col_name in styled.columns: fmt[col_name] = "{:,.0f}"
                    if "Margin (%)" in styled.columns: fmt["Margin (%)"] = "{:.1f}%"
                    st.dataframe(
                        styled.style.format(fmt).applymap(
                            lambda v: "color:#E8304A;font-weight:600" if isinstance(v,(int,float)) and v<0 else "",
                            subset=[c for c in ["Margin (€)","Margin (%)"] if c in styled.columns]),
                        use_container_width=True, height=380)

                with tab_global:
                    fig_scatter = px.scatter(
                        df_plot, x="_CA", y="Rentabilité_%",
                        color="Statut", color_discrete_map=CMAP,
                        size=df_plot["_CO"].clip(lower=1), size_max=40,
                        hover_name=tour_c, custom_data=["Marge_Nette","_CO"],
                        title=_("trans_chart_title_scatter"),
                        labels={"_CA": _("trans_chart_ca"), "Rentabilité_%": _("trans_chart_marge")})
                    fig_scatter.update_traces(
                        hovertemplate=(
                            "<b>%{hovertext}</b><br>"
                            f"CA : %{{x:,.0f}} €<br>"
                            f"{'Marge' if st.session_state.language=='fr' else 'Margin'} : %{{customdata[0]:,.0f}} €<br>"
                            f"{'Coût' if st.session_state.language=='fr' else 'Cost'} : %{{customdata[1]:,.0f}} €<br>"
                            f"{'Rentabilité' if st.session_state.language=='fr' else 'Profitability'} : %{{y:.1f}}%"
                            "<extra></extra>"))
                    fig_scatter.add_hline(y=0, line_dash="solid", line_color="#E8304A", line_width=1.5,
                        annotation_text=_("trans_chart_zero"), annotation_position="right")
                    fig_scatter.add_hline(y=10, line_dash="dot", line_color="#f39c12", line_width=1,
                        annotation_text=_("trans_chart_alert"), annotation_position="right")
                    fig_scatter.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#fafbfc",
                        height=480, margin=dict(l=20,r=130,t=50,b=20),
                        xaxis=dict(title=_("trans_chart_ca"), tickformat=",.0f", gridcolor="#f0f4f8"),
                        yaxis=dict(title=_("trans_chart_marge"), ticksuffix="%", gridcolor="#f0f4f8",
                                   zerolinecolor="#E8304A", zerolinewidth=1.5),
                        font=dict(family="DM Sans",size=12,color="#0B2545"),
                        title=dict(font=dict(family="Syne",size=14,color="#0B2545")),
                        legend=dict(title="",orientation="h",yanchor="bottom",y=1.02))
                    st.plotly_chart(fig_scatter, use_container_width=True)
                    n_loss  = len(df_plot[df_plot["Statut"]==STAT_LOSS])
                    n_alert = len(df_plot[df_plot["Statut"]==STAT_ALERT])
                    n_ok    = len(df_plot[df_plot["Statut"]==STAT_OK])
                    st.caption(f"📊 {len(df_plot)} {_('trans_scatter_caption')} — {n_loss} {_('trans_loss')} | {n_alert} {_('trans_alert')} | {n_ok} {_('trans_healthy')}")

                # Variable PDF = Top 15
                fig_trans = fig_top

                if run_ia_t:
                    pg6 = StepProgress([_("step_prep"),_("step_ia_progress"),_("step_report")])
                    pg6.step()
                    top3 = df_t.nsmallest(3,"Marge_Nette")
                    pires_s = ", ".join([f"{r[tour_c]} ({r['Marge_Nette']:.0f} EUR)" for _,r in top3.iterrows()]) if not top3.empty else "None"
                    mode_info = f" Detected transport mode: {st.session_state.trans_mode_detected[0] if st.session_state.trans_mode_detected else 'road'}."
                    pg6.step()
                    st.session_state.analysis_trans = generate_ai_analysis(
                        f"Routes: {len(df_t)}. Margin: {marge_tot:.0f} EUR. Rate: {taux:.1f}%. "
                        f"Loss-making: {traj_def}. Top 3 worst: {pires_s}. Avg cost/km: {cout_km:.2f} EUR.{poids_info}{mode_info}")
                    pg6.step()
                    st.session_state.last_pdf = generate_expert_pdf(_("pdf_title_trans"),
                        st.session_state.analysis_trans, [fig_trans])
                    st.session_state.last_kpis   = [marge_tot, taux, nb_tox]
                    st.session_state.last_labels = [_("trans_kpi_marge"),_("trans_kpi_taux"),"Toxic routes"]
                    pg6.done()

                if st.session_state.analysis_trans:
                    st.markdown(render_report(st.session_state.analysis_trans,"manager"), unsafe_allow_html=True)
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.session_state.last_pdf:
                        st.download_button(_("trans_btn_dl"), st.session_state.last_pdf,
                            "Transport_Logiflo.pdf", use_container_width=True)
