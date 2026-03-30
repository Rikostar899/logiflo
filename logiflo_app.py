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

# =========================================
# 0. INIT
# =========================================
client   = OpenAI(api_key=st.secrets.get("OPENAI_API_KEY", ""))
ORS_API_KEY = st.secrets.get("ORS_API_KEY", "")
SHEET_ID    = st.secrets.get("GOOGLE_SHEET_ID", "")

USERS_DB = {
    "eric":"logiflo2026","admin":"admin123","demo_client1":"audit2026",
    "demo_client2":"test2026","jury":"pitch2026","partenaire":"partner2026","test":"test123",
}

# =========================================
# 0.1 TRADUCTIONS
# =========================================
T = {
    "fr": {
        "nav_workspace":"Espace de Travail","nav_archives":"Archives",
        "nav_params":"Paramètres","nav_legal":"Informations Légales","nav_logout":"Déconnexion",
        "home_title":"LOGIFLO.IO",
        "home_sub":"Plateforme d'Intelligence Logistique et d'Optimisation Financière",
        "home_stock":"AUDIT STOCKS","home_transport":"AUDIT TRANSPORT",
        "home_access":"DEMANDER UN ACCÈS PRIVÉ",
        "login_id":"Identifiant","login_pw":"Mot de passe","login_btn":"Connexion",
        "login_err":"Identifiants incorrects.","login_back":"← Retour",
        "profile_title":"Sélectionnez votre Espace de Travail",
        "profile_sub":"L'interface s'adaptera à vos habilitations.",
        "profile_mgr":"PROFIL MANAGER (Stratégie & Finance)",
        "profile_ops":"PROFIL TERRAIN (Action Opérationnelle)",
        "stock_title":"📦 Audit Financier des Stocks",
        "stock_import":"📥 Importation Sécurisée",
        "stock_import_sub":"Déposez votre fichier d'inventaire (CSV ou Excel).<br>Le <b>Smart Ingester™ V4</b> détecte automatiquement vos colonnes, même avec des noms atypiques.<br><span style='color:#00A87A;font-weight:600;'>✓ Prix optionnel &nbsp; ✓ Historique optionnel &nbsp; ✓ Tous formats</span>",
        "stock_kpi_capital":"Capital Immobilisé","stock_kpi_articles":"Articles en Stock",
        "stock_kpi_service":"Taux de Service","stock_kpi_rupture":"Articles en Rupture",
        "stock_btn_ia":"GÉNÉRER L'AUDIT FINANCIER (IA)","stock_btn_ia_terrain":"GÉNÉRER L'AUDIT IA",
        "stock_btn_save":"💾 Sauvegarder","stock_btn_dl":"📥 Télécharger le Rapport (PDF)",
        "stock_badge_no_price":"📊 Mode opérationnel — analyse sans prix",
        "stock_badge_conso":"📈 Historique de consommation détecté",
        "stock_badge_no_conso":"⚠️ Pas d'historique — couverture non calculable",
        "stock_saved":"✅ Sauvegardé !","stock_save_err":"⚠️ Connexion Google Sheets absente.",
        "stock_urgent":"🚨 Priorités immédiates","stock_full":"📋 Stock complet",
        "stock_no_rupture":"✅ Aucun article en rupture.",
        "trans_title":"🚚 Audit de Rentabilité Transport",
        "trans_import":"🌍 Importation des Flux de Transport",
        "trans_import_sub":"Déposez votre fichier TMS ou Excel. Le moteur <b>ORS</b> calcule les distances routières réelles.<br><span style='color:#00A87A;font-weight:600;'>✓ Maritime &nbsp; ✓ Aérien &nbsp; ✓ Routier &nbsp; ✓ Ferroviaire</span>",
        "trans_kpi_marge":"Marge Nette Globale","trans_kpi_taux":"Taux de Rentabilité",
        "trans_kpi_fuite":"🚨 Fuite de Marge","trans_kpi_sain":"✅ Réseau",
        "trans_btn_ia":"GÉNÉRER L'AUDIT DE RENTABILITÉ (IA)",
        "trans_btn_save":"💾 Sauvegarder","trans_btn_dl":"📥 Télécharger le Rapport (PDF)",
        "trans_tab_top":"🎯 Top 15 — Pires trajets","trans_tab_all":"🗺️ Vue d'ensemble",
        "trans_ca_miss":"💡 CA manquant — estimé à marge 15%.",
        "trans_no_cost":"🚨 Colonne 'Coût' introuvable.",
        "trans_top15_title":"Top 15 trajets les plus déficitaires",
        "trans_scatter_title":"Vue d'ensemble — Rentabilité vs CA par trajet",
        "trans_seuil_zero":"Seuil zéro","trans_seuil_alert":"Seuil alerte 10%",
        "trans_detail":"Détail des trajets en alerte",
        "trans_col_client":"Client / Trajet","trans_col_ca":"CA (€)",
        "trans_col_co":"Coût (€)","trans_col_marge":"Marge (€)","trans_col_pct":"Marge (%)",
        "arch_title":"🗄️ Archives & Historique",
        "arch_empty":"Aucun audit archivé. Générez votre premier audit depuis l'Espace de Travail.",
        "arch_dl":"📥 PDF","arch_filter":"Filtrer","arch_filter_all":"Tous",
        "arch_show":"audit(s) affiché(s)","arch_resume":"📋 Résumé IA",
        "step_read":"Lecture du fichier...","step_detect":"Détection des colonnes...",
        "step_calc":"Calcul des indicateurs...","step_ia":"Analyse IA en cours...",
        "step_report":"Génération du rapport...","step_geo":"Géocodage des villes...",
        "step_dist":"Calcul des distances ORS...","step_mode":"Détection du mode de transport...",
        "pdf_title_stock":"AUDIT STRATEGIQUE DES STOCKS",
        "pdf_title_trans":"AUDIT FINANCIER TRANSPORT",
        "pdf_confidential":"CONFIDENTIEL","pdf_strategic":"AUDIT STRATEGIQUE",
        "pdf_report":"RAPPORT D ANALYSE","pdf_date":"Date",
        "pdf_footer":"Document genere par Logiflo.io. Recommandations a titre indicatif.",
        "pdf_cta":"Ce rapport a ete genere par LOGIFLO.IO\nConcu par un logisticien terrain — pas par un consultant.\nPour aller plus loin : contact@logiflo.io | logiflo-io.streamlit.app",
        "mode_detected":"— analyse adaptée activée",
        "change_profile":"Changer de profil","active_profile":"Profil Actif",
        "params_title":"⚙️ Configuration des Seuils",
        "params_alert":"Seuil d'Alerte","params_rupture":"Seuil de Rupture Critique",
        "params_km":"Seuil Rentabilité EUR/KM",
        "contact_title":"Demande d'Accès Réservé",
        "contact_name":"Nom & Prénom","contact_email":"Email Professionnel",
        "contact_company":"Entreprise","contact_volume":"Volume géré :",
        "contact_issue":"Enjeu prioritaire :","contact_btn":"Transmettre",
        "contact_ok":"Demande transmise. Notre équipe vous contactera sous 24h.",
        "vol1":"Moins de 10M EUR","vol2":"De 10M à 50M EUR","vol3":"Plus de 50M EUR",
        "iss1":"Optimisation BFR (Stocks)","iss2":"Réduction coûts Transport","iss3":"Global Supply Chain",
    },
    "en": {
        "nav_workspace":"Workspace","nav_archives":"Archives",
        "nav_params":"Settings","nav_legal":"Legal Information","nav_logout":"Log out",
        "home_title":"LOGIFLO.IO",
        "home_sub":"Logistics Intelligence & Financial Optimization Platform",
        "home_stock":"STOCK AUDIT","home_transport":"TRANSPORT AUDIT",
        "home_access":"REQUEST PRIVATE ACCESS",
        "login_id":"Username","login_pw":"Password","login_btn":"Sign in",
        "login_err":"Incorrect credentials.","login_back":"← Back",
        "profile_title":"Select your Workspace",
        "profile_sub":"The interface will adapt to your permissions.",
        "profile_mgr":"MANAGER PROFILE (Strategy & Finance)",
        "profile_ops":"OPERATIONS PROFILE (Field Action)",
        "stock_title":"📦 Stock Financial Audit",
        "stock_import":"📥 Secure Import",
        "stock_import_sub":"Drop your inventory file (CSV or Excel).<br>The <b>Smart Ingester™ V4</b> automatically detects your columns, even with unusual names.<br><span style='color:#00A87A;font-weight:600;'>✓ Price optional &nbsp; ✓ History optional &nbsp; ✓ All formats</span>",
        "stock_kpi_capital":"Tied-up Capital","stock_kpi_articles":"Items in Stock",
        "stock_kpi_service":"Service Level","stock_kpi_rupture":"Stock-outs",
        "stock_btn_ia":"GENERATE FINANCIAL AUDIT (AI)","stock_btn_ia_terrain":"GENERATE AI AUDIT",
        "stock_btn_save":"💾 Save","stock_btn_dl":"📥 Download Report (PDF)",
        "stock_badge_no_price":"📊 Operational mode — analysis without prices",
        "stock_badge_conso":"📈 Consumption history detected",
        "stock_badge_no_conso":"⚠️ No history — coverage not calculable",
        "stock_saved":"✅ Saved!","stock_save_err":"⚠️ Google Sheets connection unavailable.",
        "stock_urgent":"🚨 Immediate Priorities","stock_full":"📋 Full Inventory",
        "stock_no_rupture":"✅ No stock-outs detected.",
        "trans_title":"🚚 Transport Profitability Audit",
        "trans_import":"🌍 Import Transport Flows",
        "trans_import_sub":"Drop your TMS or Excel file. The <b>ORS</b> engine computes real road distances.<br><span style='color:#00A87A;font-weight:600;'>✓ Maritime &nbsp; ✓ Air &nbsp; ✓ Road &nbsp; ✓ Rail</span>",
        "trans_kpi_marge":"Total Net Margin","trans_kpi_taux":"Profitability Rate",
        "trans_kpi_fuite":"🚨 Margin Leak","trans_kpi_sain":"✅ Network",
        "trans_btn_ia":"GENERATE PROFITABILITY AUDIT (AI)",
        "trans_btn_save":"💾 Save","trans_btn_dl":"📥 Download Report (PDF)",
        "trans_tab_top":"🎯 Top 15 — Worst routes","trans_tab_all":"🗺️ Overview",
        "trans_ca_miss":"💡 Revenue missing — estimated at 15% margin.",
        "trans_no_cost":"🚨 'Cost' column not found.",
        "trans_top15_title":"Top 15 most unprofitable routes",
        "trans_scatter_title":"Overview — Profitability vs Revenue per route",
        "trans_seuil_zero":"Break-even","trans_seuil_alert":"Alert threshold 10%",
        "trans_detail":"Underperforming routes — detail",
        "trans_col_client":"Client / Route","trans_col_ca":"Revenue (€)",
        "trans_col_co":"Cost (€)","trans_col_marge":"Margin (€)","trans_col_pct":"Margin (%)",
        "arch_title":"🗄️ Archives & History",
        "arch_empty":"No saved audits yet. Generate your first audit from the Workspace.",
        "arch_dl":"📥 PDF","arch_filter":"Filter","arch_filter_all":"All",
        "arch_show":"audit(s) shown","arch_resume":"📋 AI Summary",
        "step_read":"Reading file...","step_detect":"Detecting columns...",
        "step_calc":"Computing indicators...","step_ia":"AI analysis in progress...",
        "step_report":"Generating report...","step_geo":"Geocoding cities...",
        "step_dist":"Computing ORS distances...","step_mode":"Detecting transport mode...",
        "pdf_title_stock":"STRATEGIC STOCK AUDIT",
        "pdf_title_trans":"TRANSPORT FINANCIAL AUDIT",
        "pdf_confidential":"CONFIDENTIAL","pdf_strategic":"STRATEGIC AUDIT",
        "pdf_report":"ANALYSIS REPORT","pdf_date":"Date",
        "pdf_footer":"Generated by Logiflo.io. Recommendations are indicative only.",
        "pdf_cta":"This report was generated by LOGIFLO.IO\nDesigned by a field logistics professional — not a consultant.\nTo go further: contact@logiflo.io | logiflo-io.streamlit.app",
        "mode_detected":"— adapted analysis activated",
        "change_profile":"Change profile","active_profile":"Active Profile",
        "params_title":"⚙️ Threshold Configuration",
        "params_alert":"Alert Threshold","params_rupture":"Critical Stock-out Threshold",
        "params_km":"Profitability Threshold EUR/KM",
        "contact_title":"Request Private Access",
        "contact_name":"Full Name","contact_email":"Professional Email",
        "contact_company":"Company","contact_volume":"Managed volume:",
        "contact_issue":"Main challenge:","contact_btn":"Submit",
        "contact_ok":"Request submitted. Our team will contact you within 24h.",
        "vol1":"Less than 10M EUR","vol2":"10M to 50M EUR","vol3":"More than 50M EUR",
        "iss1":"BFR Optimization (Stock)","iss2":"Transport Cost Reduction","iss3":"Global Supply Chain",
    }
}

def _(key):
    lang = st.session_state.get("language","fr")
    return T.get(lang,T["fr"]).get(key, T["fr"].get(key, key))

# =========================================
# 0.2 GOOGLE SHEETS
# =========================================
@st.cache_resource
def get_gsheet_client():
    try:
        creds=Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds)
    except: return None

def get_user_sheet(username):
    gc=get_gsheet_client()
    if not gc or not SHEET_ID: return None
    try:
        sh=gc.open_by_key(SHEET_ID)
        try: return sh.worksheet(username)
        except gspread.WorksheetNotFound:
            ws=sh.add_worksheet(title=username,rows=1000,cols=12)
            ws.append_row(["date","heure","module","nb_lignes","kpi_1","kpi_2","kpi_3",
                           "kpi_label_1","kpi_label_2","kpi_label_3","resume_ia","pdf_base64"])
            return ws
    except: return None

def save_audit_to_sheets(username,module,nb_lignes,kpis,labels,resume_ia,pdf_bytes):
    ws=get_user_sheet(username)
    if not ws: return False
    try:
        now=datetime.datetime.now()
        ws.append_row([now.strftime("%d/%m/%Y"),now.strftime("%H:%M"),module,nb_lignes,
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
    ws=get_user_sheet(username)
    if not ws: return None
    try:
        records=ws.get_all_records()
        return pd.DataFrame(records) if records else pd.DataFrame()
    except: return None

def get_historique_audits(username, module, n=4, current_kpis=None, current_labels=None):
    """
    Charge les n derniers audits du même module depuis Google Sheets.
    Retourne un dict avec les tendances calculées ou None si pas d'historique.
    """
    try:
        df = load_archives_from_sheets(username)
        if df is None or df.empty:
            return None
        # Filtrer par module
        df = df[df["module"] == module].copy()
        if len(df) < 2:
            return None  # pas assez d'historique pour une tendance

        # Convertir les colonnes numériques
        for col in ["kpi_1","kpi_2","kpi_3"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Trier par date/heure descendant, prendre les n derniers
        try:
            df["_dt"] = pd.to_datetime(df["date"] + " " + df["heure"],
                                        format="%d/%m/%Y %H:%M", errors="coerce")
            df = df.sort_values("_dt", ascending=False)
        except:
            pass  # si parse date échoue, on garde l'ordre sheets

        recent = df.head(n).iloc[::-1]  # remettre chronologique

        # Construire l'historique depuis les archives
        history = []
        for _hr, row in recent.iterrows():
            entry = {
                "date":   row.get("date","?"),
                "kpi_1":  row.get("kpi_1", 0),
                "kpi_2":  row.get("kpi_2", 0),
                "kpi_3":  row.get("kpi_3", 0),
                "label_1":row.get("kpi_label_1","KPI1"),
                "label_2":row.get("kpi_label_2","KPI2"),
                "label_3":row.get("kpi_label_3","KPI3"),
                "resume": str(row.get("resume_ia",""))[:400],
            }
            history.append(entry)

        # Ajouter l'audit ACTUEL (non encore sauvegardé) comme dernier point
        if current_kpis and len(current_kpis) >= 2:
            import datetime as _dt
            current_labels_safe = current_labels or ["KPI1","KPI2","KPI3"]
            history.append({
                "date":    _dt.date.today().strftime("%d/%m/%Y"),
                "kpi_1":  float(current_kpis[0]) if len(current_kpis) > 0 else 0,
                "kpi_2":  float(current_kpis[1]) if len(current_kpis) > 1 else 0,
                "kpi_3":  float(current_kpis[2]) if len(current_kpis) > 2 else 0,
                "label_1": current_labels_safe[0] if len(current_labels_safe) > 0 else "KPI1",
                "label_2": current_labels_safe[1] if len(current_labels_safe) > 1 else "KPI2",
                "label_3": current_labels_safe[2] if len(current_labels_safe) > 2 else "KPI3",
                "resume":  "",
            })

        if len(history) < 2:
            return None

        # Calculer les tendances entre le plus ancien et le plus récent
        first = history[0]
        last  = history[-1]

        def delta_pct(new, old):
            try:
                new, old = float(new), float(old)
                if old == 0: return None
                return round((new - old) / abs(old) * 100, 1)
            except:
                return None

        def tendance_label(d, lang="fr", invert=False):
            """invert=True : une baisse est bonne (ex: nb ruptures)"""
            if d is None: return ""
            if lang == "en":
                if invert:
                    return f"{'▼ improving' if d < 0 else '▲ worsening'} ({abs(d):.1f}%)"
                return f"{'▲ up' if d > 0 else '▼ down'} ({abs(d):.1f}%)"
            else:
                if invert:
                    return f"{'▼ en amelioration' if d < 0 else '▲ en degradation'} ({abs(d):.1f}%)"
                return f"{'▲ hausse' if d > 0 else '▼ baisse'} ({abs(d):.1f}%)"

        d1 = delta_pct(last["kpi_1"], first["kpi_1"])
        d2 = delta_pct(last["kpi_2"], first["kpi_2"])
        d3 = delta_pct(last["kpi_3"], first["kpi_3"])

        return {
            "history":   history,
            "n_audits":  len(history),
            "first_date":first["date"],
            "last_date": last["date"],
            "delta_1":   d1,
            "delta_2":   d2,
            "delta_3":   d3,
        }
    except Exception:
        return None


def format_historique_pour_prompt(hist, module, lang="fr"):
    """
    Formate l'historique en texte structuré pour injection dans le prompt IA.
    Adapte le vocabulaire selon le module et la langue.
    """
    if not hist:
        return ""

    h = hist["history"]
    n = hist["n_audits"]

    if lang == "en":
        lines = [f"\n=== HISTORICAL TREND — last {n} audits ==="]
        lines.append(f"Period: {hist['first_date']} → {hist['last_date']}")
        lines.append("")

        for i, entry in enumerate(h):
            tag = "CURRENT" if i == len(h)-1 else f"Audit {i+1}"
            l1 = entry["label_1"][:20]
            l2 = entry["label_2"][:20]
            l3 = entry["label_3"][:20]
            lines.append(f"[{tag} — {entry['date']}]")
            lines.append(f"  {l1}: {entry['kpi_1']:.1f} | {l2}: {entry['kpi_2']:.1f} | {l3}: {entry['kpi_3']:.1f}")
            if entry["resume"] and i == len(h)-1:
                pass  # ne pas répéter le résumé actuel

        lines.append("")
        lines.append("COMPUTED TRENDS (first → last):")

        d1,d2,d3 = hist["delta_1"], hist["delta_2"], hist["delta_3"]

        if module == "transport":
            if d1 is not None:
                direction = "improving" if d1 > 0 else "declining"
                lines.append(f"  Net margin: {direction} ({d1:+.1f}%)")
            if d2 is not None:
                direction = "improving" if d2 > 0 else "declining"
                lines.append(f"  Profitability rate: {direction} ({d2:+.1f}%)")
            if d3 is not None:
                direction = "improving" if d3 < 0 else "worsening"
                lines.append(f"  Toxic routes: {direction} ({d3:+.1f}%)")
            lines.append("")
            lines.append("INSTRUCTIONS FOR THIS ANALYSIS:")
            lines.append("- Compare current figures to this historical trend")
            lines.append("- If margin is declining: identify root cause (new routes? lost client? fuel?)")
            lines.append("- If toxic routes increasing: flag as structural risk, not anomaly")
            lines.append("- Mention explicit trend in your PROFITABILITY AUDIT section")
            lines.append("- If trend reversal detected: highlight it as a positive signal")

        elif module in ("stock","terrain"):
            if d1 is not None:
                lines.append(f"  Capital/Items: {d1:+.1f}% vs first audit")
            if d2 is not None:
                direction = "improving" if d2 > 0 else "declining"
                lines.append(f"  Service level: {direction} ({d2:+.1f}%)")
            if d3 is not None:
                direction = "worsening" if d3 > 0 else "improving"
                lines.append(f"  Stock-outs: {direction} ({d3:+.1f}%)")
            lines.append("")
            lines.append("INSTRUCTIONS FOR THIS ANALYSIS:")
            lines.append("- Compare current figures to this historical trend")
            lines.append("- If service level declining: flag as urgent priority")
            lines.append("- If stock-outs increasing: identify if structural or seasonal")
            lines.append("- If dormant stock growing: estimate cash impact over trend period")
            lines.append("- Mention trend explicitly in your OPERATIONAL DIAGNOSIS section")

        lines.append("=== END HISTORICAL DATA ===\n")

    else:
        lines = [f"\n=== TENDANCE HISTORIQUE — {n} derniers audits ==="]
        lines.append(f"Periode : {hist['first_date']} -> {hist['last_date']}")
        lines.append("")

        for i, entry in enumerate(h):
            tag = "ACTUEL" if i == len(h)-1 else f"Audit {i+1}"
            l1 = entry["label_1"][:25]
            l2 = entry["label_2"][:25]
            l3 = entry["label_3"][:25]
            lines.append(f"[{tag} — {entry['date']}]")
            lines.append(f"  {l1}: {entry['kpi_1']:.1f} | {l2}: {entry['kpi_2']:.1f} | {l3}: {entry['kpi_3']:.1f}")

        lines.append("")
        lines.append("TENDANCES CALCULEES (premier -> dernier audit) :")

        d1,d2,d3 = hist["delta_1"], hist["delta_2"], hist["delta_3"]

        if module == "transport":
            if d1 is not None:
                sens = "en hausse" if d1 > 0 else "en baisse"
                lines.append(f"  Marge nette : {sens} ({d1:+.1f}%)")
            if d2 is not None:
                sens = "en hausse" if d2 > 0 else "en baisse"
                lines.append(f"  Taux de rentabilite : {sens} ({d2:+.1f}%)")
            if d3 is not None:
                sens = "en hausse" if d3 > 0 else "en baisse"
                lines.append(f"  Trajets toxiques : {sens} ({d3:+.1f}%)")
            lines.append("")
            lines.append("INSTRUCTIONS POUR CETTE ANALYSE :")
            lines.append("- Compare les chiffres actuels a cette tendance historique")
            lines.append("- Si la marge baisse : identifie la cause racine (nouveaux trajets? perte client? carburant?)")
            lines.append("- Si les trajets toxiques augmentent : signal de risque structurel, pas une anomalie")
            lines.append("- Mentionne explicitement la tendance dans ta section AUDIT DE RENTABILITE")
            lines.append("- Si retournement de tendance : le signaler comme signal positif")
            lines.append("- Si un client disparait entre deux audits : le nommer et analyser l impact")

        elif module == "stock":
            if d1 is not None:
                sens = "en hausse" if d1 > 0 else "en baisse"
                lines.append(f"  Capital/Articles : {sens} ({d1:+.1f}%)")
            if d2 is not None:
                sens = "en amelioration" if d2 > 0 else "en degradation"
                lines.append(f"  Taux de service : {sens} ({d2:+.1f}%)")
            if d3 is not None:
                sens = "en hausse" if d3 > 0 else "en baisse"
                lines.append(f"  Ruptures : {sens} ({d3:+.1f}%)")
            lines.append("")
            lines.append("INSTRUCTIONS POUR CETTE ANALYSE :")
            lines.append("- Compare les chiffres actuels a cette tendance historique")
            lines.append("- Si taux de service en baisse : priorite urgente dans ton plan d action")
            lines.append("- Si ruptures croissantes : identifier si structurel ou saisonnier")
            lines.append("- Si stock dormant augmente : estimer l impact cash sur la periode")
            lines.append("- Mentionne la tendance dans ton DIAGNOSTIC OPERATIONNEL")

        elif module == "terrain":
            if d2 is not None:
                sens = "meilleure" if d2 > 0 else "moins bonne"
                lines.append(f"  Disponibilite : {sens} ({d2:+.1f}%)")
            if d3 is not None:
                sens = "plus" if d3 > 0 else "moins"
                lines.append(f"  Articles a reapprovisionner : {sens} ({d3:+.1f}%)")
            lines.append("")
            lines.append("INSTRUCTIONS POUR CETTE ANALYSE :")
            lines.append("- Dis si la situation s ameliore ou se degrade par rapport aux semaines precedentes")
            lines.append("- Nomme les articles qui etaient deja en rupture la derniere fois")
            lines.append("- Signale si un article ne bouge pas depuis plusieurs audits consecutifs")

        lines.append("=== FIN DONNEES HISTORIQUES ===\n")

    return "\n".join(lines)


# =========================================
# 0.3 PROMPTS IA BILINGUES
# =========================================
def get_prompt_stock():
    lang=st.session_state.get("language","fr")
    if lang=="en":
        return """You are a Senior Financial Auditor and Supply Chain Director for Logiflo.io.
RESPOND ENTIRELY IN ENGLISH.

RULES on data:
- If prices available: full financial analysis (tied-up capital, dormant stock, cash trap)
- If NO prices: pure operational analysis (rotation, velocity, stock-outs in quantities)
- If consumption history available: calculate coverage in months and 3-year trend
- If NO consumption: flag BLIND SPOT, give sector benchmark (2-4 months healthy coverage)
- If historical audit data present: MANDATORY trend integration into diagnosis

Mandatory structure:

### OPERATIONAL DIAGNOSIS
Service level and rotation. Name the 3 most critical references with exact figures.
If historical data: compare to previous audit, state clearly improving or worsening.

### TREND AND EVOLUTION
INCLUDE ONLY IF HISTORICAL DATA IS AVAILABLE.
Which indicators improve, which deteriorate. Name recurring stock-outs across audits.
If critical threshold approaching on trend: issue explicit alert with projection.

### FINANCIAL DIAGNOSIS AND DORMANT STOCK
If prices: tied-up capital, dormant stock, cash trap estimate.
If no prices: velocity per reference, zero-rotation items, hidden risks.

### IMMEDIATE ACTION PLAN (TOP 3)
3 concrete actionable recommendations.
If historical data: prioritize recurring issues across multiple audits.
Potential impact: High/Medium/Low | Execution difficulty: 1 to 5

### LOGIFLO SCORE
- Stock Performance and Rotation: /100
- Stock-out Risk: /100
- Supply Chain Resilience: /100

RULES: Never invent amounts. Leave blank line between ideas.
If no historical data: analyze normally without mentioning its absence."""
    return """Tu es l'Auditeur Financier et Directeur Supply Chain Senior pour Logiflo.io.
REPONDS IMPERATIVEMENT EN FRANCAIS.

REGLE sur les donnees :
- Si prix disponibles : analyse financiere complete (capital immobilise, dormants, cash trap)
- Si PAS de prix : analyse operationnelle pure (rotation, velocite, ruptures en quantites)
- Si consommations disponibles : calcule couverture en mois et tendance sur 3 ans
- Si PAS de consommations : signale ANGLE MORT et donne mediane sectorielle (2-4 mois couverture saine)
- Si donnees historiques presentes : integre OBLIGATOIREMENT la tendance dans le diagnostic

Structure obligatoire :

### DIAGNOSTIC OPERATIONNEL
Taux de service et rotation. Nomme les 3 references critiques avec chiffres exacts.
Si historique : compare a l'audit precedent et indique si la situation s'ameliore ou se degrade.

### TENDANCE ET EVOLUTION
PRESENTE UNIQUEMENT SI HISTORIQUE DISPONIBLE.
Quels indicateurs progressent, lesquels se degradent. Nomme les ruptures recurrentes d'un audit a l'autre.
Si un seuil critique approche sur la tendance : emet une alerte explicite avec projection.

### DIAGNOSTIC FINANCIER ET STOCKS DORMANTS
Si prix : capital immobilise, dormants, cash trap.
Si pas de prix : velocite par reference, articles a rotation nulle, risques caches.

### PLAN D'ACTION IMMEDIAT (TOP 3)
3 recommandations concretes et actionnables.
Si historique : priorise les problemes recurrents sur plusieurs audits consecutifs.
Impact potentiel : Fort/Moyen/Faible | Difficulte : 1 a 5

### SCORING LOGIFLO
- Performance et Rotation stock : /100
- Risque de rupture : /100
- Resilience supply chain : /100

REGLES : N'invente aucun montant. Saute une ligne entre chaque idee.
Si pas d'historique : analyse normalement sans mentionner son absence."""

def get_prompt_terrain():
    lang=st.session_state.get("language","fr")
    if lang=="en":
        return """You are an experienced warehouse supervisor helping your team day-to-day.
RESPOND IN ENGLISH. Direct tone, short sentences. No jargon.

RULES on data:
- If no prices: quantities only
- If no consumption: say so clearly, observe what you can
- If consumption available: calculate coverage in weeks or months
- If historical data: clearly state better or worse than last time
- Always use real references from the file

Structure:

### What is urgent
Items to reorder today. Exact references, exact quantities.
If historical data: flag items already out of stock last time - recurring is a serious signal.

### What changed since last audit
INCLUDE ONLY IF HISTORICAL DATA IS AVAILABLE.
What improved: concrete list.
What got worse: list with one action each.
What is new: items appeared or disappeared from stock.

### What is sleeping
Items with no movement. For each: one concrete action.

### Your 3 actions this week
One line per action. Difficulty: Easy / Medium / Hard

### Summary
2 sentences max to brief your manager.
If historical data: end with "overall: improving / stable / worsening".

RULES: Concrete only. No invented figures. Talk like a colleague."""
    return """Tu es un chef magasinier experimente qui aide son equipe au quotidien.
REPONDS EN FRANCAIS. Ton direct, phrases courtes. Pas de jargon financier.

REGLE sur les donnees :
- Si pas de prix : parle en quantites uniquement
- Si pas de consommations : dis-le clairement et observe ce que tu peux quand meme
- Si consommations disponibles : calcule la couverture en semaines ou en mois
- Si historique disponible : dis clairement si c'est mieux ou moins bien qu'avant
- Cite toujours les vraies references du fichier (REF-001, ART-234, etc.)

Structure :

### Ce qui est urgent
Les articles a commander aujourd'hui. References exactes, quantites exactes.
Si historique : indique les articles qui etaient deja en rupture la derniere fois - si ca se repete c'est grave.

### Ce qui a change depuis le dernier audit
PRESENTE UNIQUEMENT SI HISTORIQUE DISPONIBLE.
Ce qui s'est ameliore : liste courte, concret.
Ce qui s'est degrade : liste courte, avec une action pour chaque point.
Ce qui est nouveau : articles apparus ou disparus du stock.

### Ce qui dort
Articles sans mouvement depuis longtemps. Pour chacun : que faire maintenant ?

### Tes 3 actions pour cette semaine
Une phrase par action. Difficulte : Facile / Moyen / Complique

### En resume
2 phrases max pour briefer ton chef en 30 secondes.
Si historique : termine par "situation globale : en amelioration / stable / en degradation".

REGLES : Concret uniquement. Pas de chiffres inventes. Parle comme a un collegue."""

def get_prompt_transport():
    lang=st.session_state.get("language","fr")
    if lang=="en":
        return """You are a Senior Transport and Supply Chain Strategy Auditor for Logiflo.io.
RESPOND ENTIRELY IN ENGLISH.
DO NOT JUST REPEAT THE DATA: deduce hidden problems and root causes.
If weight is missing: flag STRATEGIC BLIND SPOT.
Adapt vocabulary to detected mode:
- Maritime: TEU, container, demurrage, carrier, port, FCL/LCL
- Air: AWB, chargeable weight, vol/actual ratio, airline, air freight
- Road: FTL/LTL, cost/km, driver, lane, groupage, express
- Rail: wagon, slot, corridor, tonne-km

CNR benchmarks 2025-2026 (cite them in your analysis):
- Long-haul road articulated diesel: 1.85-2.10 EUR/km
- Regional road rigid truck: 1.40-1.65 EUR/km
- Fuel share: ~26.5% of total cost
- Thresholds: alert < 8% margin | toxic < 5% | loss < 0%

Mandatory structure:

### PROFITABILITY AUDIT
Global margin and Yield. Name the 3 routes/clients destroying profitability.
Expert hypothesis on root cause - not just description.
If historical data: state whether overall margin improving or worsening, cite trend in numbers.

### NETWORK TREND AND EVOLUTION
INCLUDE ONLY IF HISTORICAL DATA IS AVAILABLE.
Evolution of margin, route count, toxic clients over the period.
If a client disappears between audits: flag explicitly as potential revenue loss.
If toxic routes increasing: structural risk signal requiring urgent priority.

### NETWORK DIAGNOSIS
Spatial coherence and operational efficiency.
Compare cost/km to CNR benchmarks - cite percentage gaps.
If weight available: load efficiency and cost per tonne.

### RATIONALIZATION PLAN (TOP 3)
3 mode-specific immediately actionable recommendations.
If historical data: start with previously recommended actions not yet implemented.
Cash Impact: High/Medium/Low | Execution difficulty: 1 to 5

### LOGIFLO SCORE
- Profitability and Transport Yield: /100
- Operational Efficiency: /100
- OPEX Control: /100

RULES: Never invent amounts. Leave blank line between ideas.
If no historical data: analyze normally without mentioning its absence."""
    return """Tu es un Auditeur Senior en Strategie Transport et Supply Chain pour Logiflo.io.
REPONDS IMPERATIVEMENT EN FRANCAIS.
NE REPETE PAS LES DONNEES : deduis les problemes caches et les causes racines.
Si le poids est absent : signale ANGLE MORT STRATEGIQUE.
Adapte ton vocabulaire au mode detecte :
- Maritime : TEU, conteneur, demurrage, armateur, port, FCL/LCL
- Aerien : AWB, poids taxable, ratio vol/reel, compagnie, fret aerien
- Routier : FTL/LTL, cout/km, chauffeur, axe, messagerie, groupage
- Ferroviaire : wagon, sillon, corridor, tonne-km

Referentiels CNR 2025-2026 (cite-les dans ton analyse) :
- Longue distance articulé gazole : 1,85-2,10 EUR/km de reference
- Regional porteur : 1,40-1,65 EUR/km
- Part carburant : ~26,5% du cout total
- Seuils : alerte < 8% marge | toxique < 5% | perte < 0%

Structure obligatoire :

### AUDIT DE RENTABILITE
Marge globale et Yield. Nomme les 3 trajets/clients qui detruisent la rentabilite.
Hypothese experte sur la cause racine - pas juste une description des chiffres.
Si historique : indique si la marge globale s'ameliore ou se degrade, cite la tendance en chiffres.

### TENDANCE ET EVOLUTION DU RESEAU
PRESENTE UNIQUEMENT SI HISTORIQUE DISPONIBLE.
Evolution de la marge, du nombre de trajets, des clients toxiques sur la periode.
Si un client disparait entre deux audits : le signaler explicitement comme perte potentielle de CA.
Si les trajets toxiques augmentent : signal de risque structurel a traiter en priorite absolue.

### DIAGNOSTIC RESEAU
Coherence spatiale et efficacite operationnelle.
Compare le cout/km aux referentiels CNR - cite les ecarts en pourcentage.
Si poids disponible : analyse du taux de remplissage et du cout a la tonne.

### PLAN DE RATIONALISATION (TOP 3)
3 recommandations specifiques au mode detecte, actionnables immediatement.
Si historique : commence par les actions recommandees precedemment non encore mises en oeuvre.
Impact Cash : Fort/Moyen/Faible | Difficulte : 1 a 5

### SCORING LOGIFLO
- Rentabilite et Yield Transport : /100
- Efficacite Operationnelle : /100
- Maitrise des OPEX : /100

REGLES : N'invente aucun montant. Saute une ligne entre chaque idee.
Si pas d'historique : analyse normalement sans mentionner son absence."""

# =========================================
# 1. SESSION STATE
# =========================================
for k,v in {
    "page":"accueil","module":"","auth":False,"current_user":None,
    "language":"fr",
    "df_stock":None,"df_trans":None,"history_stock":[],"stock_view":"MANAGER",
    "seuil_bas":15,"seuil_rupture":0,"seuil_km":0,
    "geo_cache":{},"route_cache":{},"trans_mapping":None,"trans_filename":None,
    "analysis_stock":None,"analysis_trans":None,
    "last_pdf":None,"last_kpis":[],"last_labels":[],"trans_mode_detected":None,
}.items():
    if k not in st.session_state: st.session_state[k]=v

# =========================================
# 2. CSS
# =========================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');
:root{--navy:#0B2545;--navy2:#162D52;--green:#00C896;--green2:#00A87A;--slate:#4A6080;--light:#F0F4F8;--red:#E8304A;--orange:#f39c12;--white:#FFFFFF;}
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
.sidebar-logo{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;color:white;letter-spacing:-0.5px;}
.sidebar-logo span{color:#00C896;}
.import-card{background:var(--white);padding:25px;border-radius:12px;border-left:6px solid var(--green);margin-bottom:20px;box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);}
.import-card h3{margin-top:0;color:var(--navy);font-family:'Syne',sans-serif;font-size:1rem;}
.import-card p{color:var(--slate);font-size:14px;margin-bottom:0;line-height:1.5;}
.report-text{background:var(--light);padding:32px;border-radius:12px;border-left:6px solid var(--navy);line-height:1.8;}
.report-text h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:800;color:var(--navy);text-transform:uppercase;letter-spacing:1.5px;margin-top:28px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.report-text h3:first-child{margin-top:0;}
.report-text p{color:#2d3748;font-size:14px;margin-bottom:8px;}
.report-text strong{color:var(--navy);}
.report-terrain{background:#f8fff8;padding:28px;border-radius:12px;border-left:6px solid var(--green);line-height:1.9;}
.report-terrain h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:700;color:var(--green2);margin-top:24px;margin-bottom:8px;}
.report-terrain h3:first-child{margin-top:0;}
.report-terrain p{color:#1a2e1a;font-size:15px;margin-bottom:6px;}
.mode-badge{display:inline-flex;align-items:center;gap:8px;background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:var(--green2);font-size:13px;font-weight:600;padding:8px 16px;border-radius:8px;margin-bottom:16px;}
.sans-prix-badge{background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:var(--green2);font-size:12px;font-weight:600;padding:4px 12px;border-radius:20px;display:inline-block;margin-bottom:12px;margin-right:8px;}
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
""",unsafe_allow_html=True)

# =========================================
# 3. HELPERS
# =========================================
def render_report(texte,mode="manager"):
    css="report-terrain" if mode=="terrain" else "report-text"
    lines=[]
    for line in texte.split('\n'):
        line=line.strip()
        if not line: continue
        if line.startswith('### '):
            lines.append(f"<h3>{line[4:].strip()}</h3>")
        else:
            line=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',line)
            prefix="• " if (line.startswith('- ') or line.startswith('* ')) else ""
            body=line[2:] if prefix else line
            lines.append(f"<p>{prefix}{body}</p>")
    return f'<div class="{css}">{"".join(lines)}</div>'

def nettoyer(t):
    t=str(t).lower()
    t=unicodedata.normalize('NFD',t).encode('ascii','ignore').decode("utf-8")
    return re.sub(r'[^a-z0-9]','',t)

class StepProgress:
    """Barre de progression simple — texte configurable, sans detail technique."""
    def __init__(self, steps, text=None):
        self._ph = st.empty()
        self._n  = max(len(steps), 1)
        self._i  = 0
        lang = st.session_state.get("language", "fr")
        if text:
            self._txt = text
        else:
            self._txt = "Computing..." if lang == "en" else "Calcul en cours..."
        self._ph.progress(0, text=self._txt)
    def step(self, label=None):
        self._i += 1
        pct = min(self._i / self._n, 1.0)
        self._ph.progress(pct, text=self._txt)
    def done(self):
        self._ph.empty()

# =========================================
# 4. SMART INGESTER V5
# =========================================
SYNONYMES={
    "reference":["reference","ref","article","code","codearticle","codeproduit","cdarticle",
                 "cdart","cdproduit","codemat","codematiere","nomarticle","nomproduit",
                 "sku","ean","ean13","upc","gtin","produit","designation","libelle",
                 "description","descproduit","descarticle","nom","item","itemcode",
                 "itemno","itemref","partnumber","partno","partref","refarticle",
                 "refproduit","refcommande","numero","numeroproduit","matricule",
                 "identifiant","id","cable","cablage","matiere","materiel","composant",
                 "piece","repere","nomenclature","famille","sousfamille","categorie",
                 "productcode","productref","dsg","desig","design","designat",
                 "articlecode","articleref","artcode","artref","artno","artnum"],
    "quantite":["quantite","qte","qty","qtstk","qte_stk","qtestk","stock","stk","stockactuel",
                "stockdispo","stockdisponible","stockreel","stockphysique","niveaustock",
                "qtestock","qtedispo","qtedisponible","qtereel","qtephysique",
                "volume","pieces","pcs","units","unit","unites","restant",
                "solde","soldedisponible","encours","inventaire","disponible",
                "existant","existants","present","metre","metres","meter","meters",
                "bobine","bobines","longueur","longueurstock","quantitedisponible",
                "quantitestock","quantiterestante","quantitepresente",
                "nbarticle","nbarticles","nbpieces","nbunites","nb","nbre","nombre",
                "qte_disponible","qt_stk","qtstck","qtstock"],
    "prix_unitaire":["prix","prixunitaire","prixachat","prixderevient","prixmoyen",
                     "prixmoyenpondere","pmp","pa","pu","pxu","px_u","price","unitprice",
                     "avgprice","cout","coutunitaire","coutachat","coutderevient","coutmoyen",
                     "cost","unitcost","avgcost","valeur","valeurunitaire","valeurachat",
                     "tarif","tarifunitaire","montantunitaire","achat","prixfournisseur",
                     "euro","eur","devise","prixbase","baseachat","priceeuro","priceeur"],
    "conso_an1":["conso2022","conso22","consommation2022","sorties2022","ventes2022",
                 "c2022","n3","nminus3","annee2022","a2022","quantite2022","qte2022","cso22","cso2022"],
    "conso_an2":["conso2023","conso23","consommation2023","sorties2023","ventes2023",
                 "c2023","n2","nminus2","annee2023","a2023","quantite2023","qte2023","cso23","cso2023"],
    "conso_an3":["conso2024","conso24","consommation2024","sorties2024","ventes2024",
                 "c2024","n1","nminus1","annee2024","a2024","quantite2024","qte2024","cso24","cso2024"],
    "conso_an4":["conso2025","conso25","consommation2025","sorties2025","ventes2025",
                 "c2025","n0","nactuel","annee2025","a2025","quantite2025","qte2025",
                 "cso25","cso2025","sortie2025","consoactuelle","consoencoursannee"],
}

def _levenshtein(s1,s2):
    if len(s1)<len(s2): return _levenshtein(s2,s1)
    if len(s2)==0: return len(s1)
    prev=list(range(len(s2)+1))
    for i,c1 in enumerate(s1):
        curr=[i+1]
        for j,c2 in enumerate(s2):
            curr.append(min(prev[j+1]+1,curr[j]+1,prev[j]+(c1!=c2)))
        prev=curr
    return prev[-1]

def _score_nom(propre,std):
    syns=SYNONYMES.get(std,[]);best=0
    for syn in syns:
        if propre==syn: return 100
        if len(syn)>=4 and propre.startswith(syn): best=max(best,95)
        if len(syn)>=3 and syn in propre: best=max(best,88)
        if len(propre)>=3 and propre in syn: best=max(best,82)
        r=difflib.SequenceMatcher(None,propre,syn).ratio()
        best=max(best,int(r*85))
        if len(propre)>=3 and len(syn)>=3:
            dist=_levenshtein(propre,syn); ml=max(len(propre),len(syn))
            if ml>0: best=max(best,int((1-dist/ml)*78))
    year_bonus={"conso_an1":["2022","22"],"conso_an2":["2023","23"],
                "conso_an3":["2024","24"],"conso_an4":["2025","25"]}
    if std in year_bonus and any(y in propre for y in year_bonus[std]): best=max(best,85)
    return best

def _score_contenu(series,std):
    sample=series.dropna().head(50)
    if len(sample)==0: return 0
    cleaned=(sample.astype(str).str.replace(r'[€$£\s\xa0%]','',regex=True)
             .str.replace(',','.',regex=False).str.replace(r'[^\d.\-]','',regex=True))
    numeric=pd.to_numeric(cleaned,errors='coerce')
    pct_num=numeric.notna().mean(); vals=numeric.dropna()
    raw_text=sample.astype(str)
    avg_len=raw_text.str.len().mean()
    pct_alpha=raw_text.str.contains(r'[a-zA-Z]',na=False).mean()
    unique_r=sample.nunique()/len(sample)
    has_dec=(vals%1!=0).mean() if len(vals)>0 else 0
    pct_int=(vals%1==0).mean() if len(vals)>0 else 0
    pct_pos=(vals>=0).mean() if len(vals)>0 else 0
    pct_zero=(vals==0).mean() if len(vals)>0 else 0
    if std=="reference":
        score=0
        if pct_alpha>0.5: score+=40
        if unique_r>0.7: score+=25
        if 3<=avg_len<=50: score+=20
        if pct_num<0.5: score+=15
        if pct_num>0.9 and pct_alpha<0.1: score-=30
        return max(0,min(score,100))
    elif std=="quantite":
        if pct_num<0.6: return 10
        score=40
        if pct_int>0.85: score+=30
        elif pct_int>0.65: score+=15
        if pct_zero>0.05: score+=8
        if pct_pos>0.85: score+=8
        if has_dec>0.55: score-=20
        if pct_alpha>0.3: score-=25
        return max(0,min(score,100))
    elif std=="prix_unitaire":
        if pct_num<0.6: return 5
        score=35
        if has_dec>0.45: score+=30
        elif has_dec>0.25: score+=15
        if pct_zero<0.05: score+=12
        if pct_pos>0.85: score+=8
        if pct_int>0.95: score-=15
        if pct_alpha>0.3: score-=25
        return max(0,min(score,100))
    elif std in("conso_an1","conso_an2","conso_an3","conso_an4"):
        if pct_num<0.5: return 5
        score=30
        if pct_int>0.80: score+=25
        elif pct_int>0.60: score+=12
        if pct_zero>0.15: score+=15
        if pct_pos>0.5: score+=10
        if has_dec>0.5: score-=15
        if pct_alpha>0.3: score-=25
        return max(0,min(score,100))
    return 0

def smart_ingester_stock_ultime(df,client_ai=None):
    df=df.dropna(how='all').copy()
    df=df[df.apply(lambda r:r.astype(str).str.strip().ne('').any(),axis=1)]
    CIBLES=list(SYNONYMES.keys())
    propres={col:nettoyer(col) for col in df.columns}
    scores={std:{} for std in CIBLES}
    for col in df.columns:
        propre=propres[col]
        for std in CIBLES:
            sn=_score_nom(propre,std); sc=_score_contenu(df[col],std)
            if sn>=70: sf=int(sn*0.65+sc*0.35)
            elif sn>=45: sf=int(sn*0.55+sc*0.45)
            else: sf=int(sn*0.25+sc*0.75)
            scores[std][col]=min(sf,100)
    # Ajustement contextuel
    for col in df.columns:
        vals=pd.to_numeric(df[col].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),errors='coerce').dropna()
        if len(vals)>5:
            if (vals%1==0).mean()>0.9 and vals.median()>10:
                scores["quantite"][col]=min(scores["quantite"][col]+10,100)
                scores["prix_unitaire"][col]=max(scores["prix_unitaire"][col]-8,0)
            if (vals%1!=0).mean()>0.5 and vals.median()<1000:
                scores["prix_unitaire"][col]=min(scores["prix_unitaire"][col]+10,100)
                scores["quantite"][col]=max(scores["quantite"][col]-8,0)
    trouvees={}; utilisees=set()
    ORDRE=["reference","quantite","prix_unitaire","conso_an4","conso_an3","conso_an2","conso_an1"]
    SEUILS={"reference":35,"quantite":55,"prix_unitaire":55,
            "conso_an4":55,"conso_an3":55,"conso_an2":55,"conso_an1":55}
    for std in ORDRE:
        seuil=SEUILS.get(std,55)
        candidats=[(col,scores[std][col]) for col in scores[std]
                   if col not in trouvees and scores[std][col]>=seuil]
        if not candidats: continue
        nom_forts=[(col,sc) for col,sc in candidats if _score_nom(propres[col],std)>=70]
        gagnant=max(nom_forts,key=lambda x:_score_nom(propres[x[0]],std))[0] if nom_forts else max(candidats,key=lambda x:x[1])[0]
        trouvees[gagnant]=std; utilisees.add(std)
    cols=list(df.columns)
    if "reference" not in utilisees:
        for c in cols:
            if c not in trouvees:
                s=df[c].dropna().head(20)
                if s.astype(str).str.contains(r'[a-zA-Z]',na=False).mean()>0.3 or s.nunique()/max(len(s),1)>0.6:
                    trouvees[c]="reference"; utilisees.add("reference"); break
    if "quantite" not in utilisees:
        for c in cols:
            if c not in trouvees:
                num=pd.to_numeric(df[c].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),errors='coerce')
                if num.notna().mean()>0.6 and (num.dropna()%1==0).mean()>0.6:
                    trouvees[c]="quantite"; utilisees.add("quantite"); break
    critiques=[s for s in ["reference","quantite"] if s not in utilisees]
    if critiques and client_ai:
        titres=list(df.columns); sample_data=df.head(5).astype(str).to_dict(orient='list')
        prompt=f"""Logistics file. Columns: {titres}
Data (5 rows): {json.dumps(sample_data,ensure_ascii=False)[:3000]}
Missing concepts: {critiques}
Reply ONLY JSON: {{"concept": "exact_title"}} or null. Choose from: {titres}"""
        try:
            r=client_ai.chat.completions.create(model="gpt-4o-mini",
                messages=[{"role":"system","content":prompt}],temperature=0.0)
            raw=r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
            gpt_map=json.loads(raw)
            for std,col in gpt_map.items():
                if std in critiques and col in df.columns and col not in trouvees:
                    trouvees[col]=std; utilisees.add(std)
        except: pass
    df=df.rename(columns=trouvees)
    manq=[c for c in ["reference","quantite"] if c not in df.columns]
    if manq:
        return None,(f"Colonnes introuvables : {', '.join(manq)}.\nColonnes dans votre fichier : {list(df.columns[:10])}")
    df["quantite"]=pd.to_numeric(df["quantite"].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),errors='coerce')
    df=df.dropna(subset=["quantite"]).copy()
    df=df[df["reference"].astype(str).str.strip().ne('')]
    df=df[~df["reference"].astype(str).str.lower().isin(['nan','none',''])]
    if "prix_unitaire" not in df.columns:
        df["prix_unitaire"]=0.0; df["_sans_prix"]=True
    else:
        df["prix_unitaire"]=pd.to_numeric(df["prix_unitaire"].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),errors='coerce').fillna(0)
        df["_sans_prix"]=(df["prix_unitaire"]==0).all()
    has_conso=False; conso_cols=[]
    for c in ["conso_an1","conso_an2","conso_an3","conso_an4"]:
        if c in df.columns:
            df[c]=pd.to_numeric(df[c].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),errors='coerce').fillna(0)
            conso_cols.append(c); has_conso=True
    df["_has_conso"]=has_conso
    df["_conso_moy"]=df[conso_cols].mean(axis=1) if has_conso else 0.0
    return df.copy(),"Succès"

# =========================================
# 5. AUTO MAP TRANSPORT
# =========================================
def auto_map_columns_with_ai(df):
    titres=list(df.columns)
    profil={col:{"exemples":list(df[col].dropna().astype(str).unique()[:5])} for col in titres}
    prompt=f"""Titres: {titres}\nDonnées: {json.dumps(profil,ensure_ascii=False)}
Associe à un titre EXACT. Si absent: null.
Concepts: "client","ca","co","dep","arr","dist","poids","mode".
JSON uniquement."""
    try:
        r=client.chat.completions.create(model="gpt-4o-mini",
            messages=[{"role":"system","content":prompt}],temperature=0.0)
        raw=r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        return {k:v for k,v in json.loads(raw).items() if v in titres}
    except:
        return {"client":titres[0],"ca":titres[1] if len(titres)>1 else None,"co":None}

# =========================================
# 6. GÉNÉRATION IA
# =========================================
def generate_ai_analysis(data_summary, historique_txt=""):
    """
    Génère l'analyse IA.
    historique_txt : contexte historique formaté à injecter dans le prompt user.
    """
    if st.session_state.module=="transport":
        prompt=get_prompt_transport()
        module_key="transport"
    elif st.session_state.get("stock_view")=="TERRAIN":
        prompt=get_prompt_terrain()
        module_key="terrain"
    else:
        prompt=get_prompt_stock()
        module_key="stock"

    lang = st.session_state.get("language","fr")

    # Construction du message utilisateur avec historique si disponible
    if historique_txt:
        if lang == "en":
            user_msg = (
                f"Current audit data: {data_summary}\n\n"
                f"{historique_txt}\n"
                f"Generate the complete audit, integrating the historical trend into your analysis."
            )
        else:
            user_msg = (
                f"Donnees audit actuel : {data_summary}\n\n"
                f"{historique_txt}\n"
                f"Redige l audit complet en integrant la tendance historique dans ton analyse."
            )
    else:
        if lang == "en":
            user_msg = f"Data: {data_summary}. Generate the audit. No historical data available yet."
        else:
            user_msg = f"Donnees : {data_summary}. Redige l audit. Pas encore d historique disponible."

    try:
        r=client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"system","content":prompt},
                {"role":"user","content":user_msg}
            ],
            temperature=0.3,
            max_tokens=1200
        )
        texte=r.choices[0].message.content
        try: return texte.encode('latin-1').decode('utf-8')
        except: return texte
    except Exception as e: return f"AI Error: {str(e)}"

# =========================================
# 7. PDF
# =========================================
class PDFReport(FPDF):
    def footer(self):
        self.set_y(-15);self.set_font("Arial","I",8);self.set_text_color(150,150,150)
        footer_text=_("pdf_footer")
        self.multi_cell(0,4,_s(footer_text),align="C")

def _s(text):
    """
    Safe string pour fpdf — garantit 100% compatibilité latin-1.
    Remplace explicitement les caractères courants avant normalisation.
    """
    if text is None: return ""
    text = str(text)
    replacements = {
        "’":"'","‘":"'","“":'"',"”":'"',
        "–":"-","—":"-","…":"...","°":"deg",
        "€":"EUR","£":"GBP","©":"(c)","®":"(R)",
        "™":"TM","•":"-","‣":"-","●":"-",
        "→":"->","←":"<-","⇒":"=>","✓":"OK",
        "✔":"OK","✗":"X","✘":"X",
        "±":"+/-","×":"x","÷":"/",
        "≈":"~","≠":"!=","≤":"<=","≥":">=",
        "🔴":"[!]","🟠":"[!]","🟢":"[ok]",
        "📊":"","📈":"","📉":"",
        "⚠":"[!]","ℹ":"[i]","★":"*","☆":"*",
    }
    for char,repl in replacements.items():
        text = text.replace(char,repl)
    text = unicodedata.normalize('NFKD',text).encode('ASCII','ignore').decode('utf-8')
    try:
        text.encode('latin-1')
        return text
    except UnicodeEncodeError:
        return text.encode('latin-1',errors='ignore').decode('latin-1')

# Alias pour compatibilité
def _asc(text): return _s(text)

def _clean_pdf(text):
    """Nettoie le texte pour fpdf — utilise _s() pour garantie latin-1."""
    return _s(str(text).replace("**",""))

def generate_expert_pdf(title, content, figs=None, kpis=None, labels=None, module="stock"):
    """
    PDF structuré 5 pages :
    P1 Couverture | P2 Synthese executive | P3 Graphiques | P4 Analyse IA | P5 CTA
    """
    if kpis is None: kpis = []
    if labels is None: labels = []
    pdf = PDFReport()
    lang = st.session_state.get("language","fr")

    # ── PAGE 1 : COUVERTURE ──────────────────────────────────────
    pdf.add_page()
    pdf.set_fill_color(11,37,69); pdf.rect(0,0,210,297,'F')
    pdf.set_fill_color(0,200,150); pdf.rect(0,0,210,6,'F')
    pdf.set_y(80)
    pdf.set_text_color(255,255,255)
    pdf.set_font("Arial","B",38); pdf.cell(0,18,"LOGIFLO.IO",ln=True,align='C')
    pdf.set_font("Arial","",14); pdf.set_text_color(0,200,150)
    pdf.cell(0,10,"[ Logistics Intelligence ]",ln=True,align='C')
    pdf.ln(8)
    pdf.set_draw_color(0,200,150); pdf.set_line_width(0.8)
    pdf.line(40,pdf.get_y(),170,pdf.get_y()); pdf.ln(10)
    pdf.set_text_color(255,255,255); pdf.set_font("Arial","B",22)
    pdf.multi_cell(0,12,_s(title),align='C'); pdf.ln(8)
    pdf.set_font("Arial","",12); pdf.set_text_color(180,200,220)
    conf = "CONFIDENTIAL" if lang=="en" else "CONFIDENTIEL"
    pdf.cell(0,8,_s(f"Date : {datetime.date.today().strftime('%d/%m/%Y')}"),ln=True,align='C')
    pdf.cell(0,8,_s(conf),ln=True,align='C')
    pdf.set_fill_color(0,200,150); pdf.rect(0,291,210,6,'F')

    # ── PAGE 2 : SYNTHESE EXECUTIVE ──────────────────────────────
    pdf.add_page()
    pdf.set_fill_color(11,37,69); pdf.rect(0,0,210,18,'F')
    pdf.set_y(4); pdf.set_text_color(255,255,255); pdf.set_font("Arial","B",11)
    h2 = "EXECUTIVE SUMMARY" if lang=="en" else "SYNTHESE EXECUTIVE"
    pdf.cell(0,10,_s(h2),ln=True,align='C'); pdf.ln(8)

    # Titre
    pdf.set_text_color(11,37,69); pdf.set_font("Arial","B",16)
    kpi_title = "Key Indicators" if lang=="en" else "Indicateurs Cles"
    pdf.cell(0,10,_s(kpi_title),ln=True,align='L')
    pdf.set_draw_color(0,200,150); pdf.set_line_width(0.6)
    pdf.line(10,pdf.get_y(),200,pdf.get_y()); pdf.ln(8)

    # KPI cards
    if kpis and labels:
        n = min(len(kpis),len(labels),3)
        card_w = 56
        total_w = n*card_w+(n-1)*8
        start_x = (210-total_w)/2
        card_colors = [(0,168,122),(0,168,122),(232,48,74)]
        card_y = pdf.get_y()
        for i in range(n):
            cx = start_x + i*(card_w+8)
            pdf.set_fill_color(240,244,248); pdf.rect(cx,card_y,card_w,38,'F')
            r,g,b = card_colors[i] if i < len(card_colors) else (0,168,122)
            pdf.set_fill_color(r,g,b); pdf.rect(cx,card_y,card_w,3,'F')
            # Label
            pdf.set_xy(cx+2, card_y+5)
            pdf.set_font("Arial","",7); pdf.set_text_color(74,96,128)
            pdf.cell(card_w-4,6,_asc(labels[i]).upper()[:22],align='C')
            # Valeur
            pdf.set_xy(cx+2, card_y+14)
            pdf.set_font("Arial","B",18)
            pdf.set_text_color(r,g,b)
            val = kpis[i]
            if isinstance(val,float) and abs(val)>=1000:
                val_str = _s(f"{val:,.0f}")
            elif isinstance(val,float) and abs(val)<=100:
                val_str = _s(f"{val:.1f}%")
            else:
                val_str = _s(str(int(val)) if isinstance(val,float) else str(val))
            pdf.cell(card_w-4,12,val_str,align='C')
        pdf.ln(46)

    # Scoring extrait du contenu IA
    pdf.set_font("Arial","B",13); pdf.set_text_color(11,37,69)
    sc_title = "Logiflo Scoring" if lang=="en" else "Scoring Logiflo"
    pdf.cell(0,8,_s(sc_title),ln=True); pdf.ln(2)
    scoring_lines=[]
    in_sc=False
    for line in content.split('\n'):
        ls=line.strip()
        if 'SCORING' in ls.upper():
            in_sc=True; continue
        if in_sc:
            if ls.startswith('###') or ls.startswith('---'): break
            if ls and ('/' in ls or ':' in ls): scoring_lines.append(ls)
    if scoring_lines:
        for sl in scoring_lines[:3]:
            sv=0
            import re as _re
            nums=_re.findall(r'(\d+)\s*/\s*100',sl)
            if nums: sv=int(nums[0])
            bar_x=10; bar_y=pdf.get_y()+1; bar_total=140
            bar_fill=int((sv/100)*bar_total) if sv>0 else 0
            pdf.set_fill_color(225,232,240); pdf.rect(bar_x,bar_y,bar_total,5,'F')
            rc,gc,bc=(0,168,122) if sv>=70 else (243,156,18) if sv>=40 else (232,48,74)
            pdf.set_fill_color(rc,gc,bc)
            if bar_fill>0: pdf.rect(bar_x,bar_y,bar_fill,5,'F')
            pdf.set_xy(bar_x+bar_total+4,bar_y-1)
            pdf.set_font("Arial","B",8); pdf.set_text_color(11,37,69)
            label_sc = sl.split(':')[0] if ':' in sl else sl
            pdf.cell(50,6,_s(label_sc)[:38])
            pdf.set_font("Arial","",8); pdf.set_text_color(rc,gc,bc)
            score_txt = f"{sv}/100" if sv>0 else ""
            pdf.cell(0,6,score_txt,ln=True)
            pdf.ln(3)
    else:
        pdf.set_font("Arial","I",10); pdf.set_text_color(74,96,128)
        no_sc = "Generate AI analysis to see scoring." if lang=="en" else "Generez l'analyse IA pour voir le scoring."
        pdf.cell(0,8,_s(no_sc),ln=True)

    # ── PAGE 3 : GRAPHIQUES ───────────────────────────────────────
    if figs:
        pdf.add_page()
        pdf.set_fill_color(11,37,69); pdf.rect(0,0,210,18,'F')
        pdf.set_y(4); pdf.set_text_color(255,255,255); pdf.set_font("Arial","B",11)
        ch_label = "CHARTS & VISUALIZATIONS" if lang=="en" else "GRAPHIQUES & VISUALISATIONS"
        pdf.cell(0,10,_s(ch_label),ln=True,align='C'); pdf.ln(6)
        for fig in figs:
            _tp = None
            try:
                import uuid, plotly.io as _pio
                _tp = os.path.join(tempfile.gettempdir(), f"lgf_{uuid.uuid4().hex}.png")
                # Méthode 1 : to_image bytes puis write
                try:
                    _img_bytes = _pio.to_image(fig, format="png", width=860, height=360, scale=2)
                    with open(_tp,"wb") as _f: _f.write(_img_bytes)
                except Exception:
                    # Méthode 2 : write_image direct
                    try:
                        fig.write_image(_tp, format="png", width=860, height=360)
                    except Exception:
                        _tp = None
                if _tp and os.path.exists(_tp) and os.path.getsize(_tp) > 500:
                    if pdf.get_y() > 200: pdf.add_page(); pdf.ln(5)
                    pdf.image(_tp, x=12, y=pdf.get_y(), w=186)
                    pdf.ln(96)
            except Exception:
                pass
            finally:
                if _tp:
                    try: os.unlink(_tp)
                    except: pass

    # ── PAGE 4 : ANALYSE IA ───────────────────────────────────────
    pdf.add_page()
    pdf.set_fill_color(11,37,69); pdf.rect(0,0,210,18,'F')
    pdf.set_y(4); pdf.set_text_color(255,255,255); pdf.set_font("Arial","B",11)
    ai_label = "AI ANALYSIS & RECOMMENDATIONS" if lang=="en" else "ANALYSE IA & RECOMMANDATIONS"
    pdf.cell(0,10,_s(ai_label),ln=True,align='C'); pdf.ln(8)

    content_r=(content.replace("\u2019","'").replace("\u2018","'")
                      .replace("\u201c",'"').replace("\u201d",'"')
                      .replace("\u20ac","EUR").replace("\u2022","-")
                      .replace("\u2013","-").replace("\u2014","-"))
    skip_scoring=False
    for line in content_r.split('\n'):
        line=line.strip()
        if 'SCORING' in line.upper() and line.startswith('###'):
            skip_scoring=True
        if skip_scoring and not line.startswith('###'):
            continue
        if skip_scoring and line.startswith('###') and 'SCORING' not in line.upper():
            skip_scoring=False
        if not line:
            pdf.ln(2); continue
        if line.startswith('### '):
            if pdf.get_y()>255: pdf.add_page(); pdf.ln(5)
            t=_asc(line[4:])
            pdf.ln(4)
            pdf.set_fill_color(240,244,248); pdf.rect(10,pdf.get_y(),190,10,'F')
            pdf.set_fill_color(0,200,150); pdf.rect(10,pdf.get_y(),3,10,'F')
            pdf.set_font("Arial","B",10); pdf.set_text_color(11,37,69)
            pdf.set_x(16); pdf.cell(184,10,_s(t).upper(),ln=True); pdf.ln(3)
        elif line.startswith(('- ','* ')):
            if pdf.get_y()>272: pdf.add_page(); pdf.ln(5)
            pdf.set_font("Arial","",10); pdf.set_text_color(40,40,40)
            bt=_s(line[2:].replace("**",""))
            pdf.set_x(14); pdf.cell(5,6,"-"); pdf.set_x(19)
            pdf.multi_cell(181,6,bt)
        else:
            if pdf.get_y()>272: pdf.add_page(); pdf.ln(5)
            pdf.set_font("Arial","",10); pdf.set_text_color(40,40,40)
            cleaned=_s(line.replace("**",""))
            pdf.set_x(10); pdf.multi_cell(190,6,cleaned)

    # ── PAGE 5 : CALL TO ACTION ───────────────────────────────────
    pdf.add_page()
    pdf.set_fill_color(11,37,69); pdf.rect(0,0,210,297,'F')
    pdf.set_fill_color(0,200,150); pdf.rect(0,0,210,6,'F'); pdf.rect(0,291,210,6,'F')
    pdf.set_y(85)
    pdf.set_text_color(0,200,150); pdf.set_font("Arial","B",32)
    pdf.cell(0,16,"LOGIFLO.IO",ln=True,align='C')
    pdf.ln(6)
    pdf.set_draw_color(0,200,150); pdf.set_line_width(0.6)
    pdf.line(50,pdf.get_y(),160,pdf.get_y()); pdf.ln(12)
    if lang=="en":
        cta_lines=[
            ("This report was generated by LOGIFLO.IO",True,255),
            ("","",200),
            ("Designed by a field logistics professional.",False,200),
            ("Not by a consultant.",False,200),
            ("","",200),
            ("Because real margin leaks don't show",False,170),
            ("up in dashboards.",False,170),
            ("","",200),
            ("To go further :",True,255),
            ("contact@logiflo.io",False,150),
            ("logiflo-io.streamlit.app",False,150),
        ]
    else:
        cta_lines=[
            ("Ce rapport a ete genere par LOGIFLO.IO",True,255),
            ("","",200),
            ("Concu par un logisticien terrain.",False,200),
            ("Pas par un consultant.",False,200),
            ("","",200),
            ("Parce que les vraies fuites de marge",False,170),
            ("ne se voient pas dans les tableaux de bord.",False,170),
            ("","",200),
            ("Pour aller plus loin :",True,255),
            ("contact@logiflo.io",False,150),
            ("logiflo-io.streamlit.app",False,150),
        ]
    for (txt,bold,br) in cta_lines:
        if not txt: pdf.ln(5); continue
        pdf.set_font("Arial","B" if bold else "",12 if bold else 11)
        pdf.set_text_color(br,br,br)
        pdf.cell(0,9,_s(txt),ln=True,align='C')

    # Encodage sécurisé : remplace tout caractère non latin-1 avant output
    raw = pdf.output(dest='S')
    if isinstance(raw, str):
        return raw.encode('latin-1', errors='replace')
    return raw


# =========================================
# 8. ROUTING ORS
# =========================================
def calculate_haversine(lon1,lat1,lon2,lat2):
    R=6371.0;dlat=math.radians(lat2-lat1);dlon=math.radians(lon2-lon1)
    a=math.sin(dlat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

def fetch_geo(city,_t=None):
    if not city or str(city).strip() in ("","nan","None"): return city,None
    try:
        r=requests.get("https://nominatim.openstreetmap.org/search",
            params={"q":str(city).strip(),"format":"json","limit":1},
            headers={"User-Agent":"Logiflo.io/2.0"},timeout=5)
        if r.status_code==200:
            d=r.json()
            if d: return city,[float(d[0]["lon"]),float(d[0]["lat"])]
    except: pass
    return city,None

def geocode_cities_mapbox(cities):
    villes=[c for c in set(str(v) for v in cities)
            if c not in st.session_state.geo_cache and c not in ("","nan","None")]
    if villes:
        calc_txt = "Computing..." if st.session_state.get("language","fr")=="en" else "Calcul en cours..."
        bar=st.progress(0,text=calc_txt)
        for i,city in enumerate(villes):
            _discard,coord=fetch_geo(city)
            if coord: st.session_state.geo_cache[city]=coord
            time.sleep(1.1)
            bar.progress((i+1)/len(villes),text=calc_txt)
        bar.empty()
    return {c:st.session_state.geo_cache[c] for c in set(str(v) for v in cities) if c in st.session_state.geo_cache}

@st.cache_data(show_spinner=False)
def _ors_distance(lon1,lat1,lon2,lat2):
    for profile in ["driving-hgv","driving-car"]:
        try:
            r=requests.post(f"https://api.openrouteservice.org/v2/directions/{profile}",
                json={"coordinates":[[lon1,lat1],[lon2,lat2]],"instructions":False},
                headers={"Accept":"application/json","Content-Type":"application/json","Authorization":ORS_API_KEY},
                timeout=6)
            if r.status_code==200: return r.json()["routes"][0]["summary"]["distance"]/1000.0
        except: continue
    return None

def fetch_route(dep,arr,mode,coords,_t=None):
    c1,c2=coords.get(str(dep)),coords.get(str(arr))
    if not c1 or not c2: return (dep,arr,mode),0.0
    lon1,lat1=c1;lon2,lat2=c2
    dv=calculate_haversine(lon1,lat1,lon2,lat2);m=str(mode).lower()
    if any(k in m for k in ["mer","sea","maritime","bateau","port","ferry","conteneur"]): return (dep,arr,mode),dv*1.25
    elif any(k in m for k in ["air","avion","aerien","flight"]): return (dep,arr,mode),dv*1.05
    elif any(k in m for k in ["fer","rail","train","sncf"]): return (dep,arr,mode),dv*1.15
    else:
        d=_ors_distance(lon1,lat1,lon2,lat2)
        return (dep,arr,mode),(d if d and d>0 else dv*1.30)

def smart_multimodal_router(df,dep_col,arr_col,mode_col=None):
    coords=geocode_cities_mapbox(pd.concat([df[dep_col],df[arr_col]]).dropna().unique())
    uniq=[]
    for _r,row in df.iterrows():
        dep=row[dep_col];arr=row[arr_col]
        mode=str(row[mode_col]).lower() if mode_col and pd.notna(row.get(mode_col)) else "route"
        k=(dep,arr,mode)
        if k not in st.session_state.route_cache and k not in uniq: uniq.append(k)
    if uniq:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            for key,dist in [f.result() for f in concurrent.futures.as_completed(
                [ex.submit(fetch_route,r[0],r[1],r[2],coords) for r in uniq])]:
                st.session_state.route_cache[key]=dist
    df["_DIST_CALCULEE"]=[
        st.session_state.route_cache.get(
            (row[dep_col],row[arr_col],
             str(row[mode_col]).lower() if mode_col and pd.notna(row.get(mode_col)) else "route"),0.0)
        for _r,row in df.iterrows()]
    return df

def detect_transport_mode(df,dep_col=None,arr_col=None,mode_col=None):
    # Ports maritimes — noms complets (matching par inclusion sur tokens)
    PORTS=["havre","marseille","dunkerque","bordeaux","hamburg","rotterdam","antwerp",
           "anvers","amsterdam","barcelona","barcelone","genova","genes","piraeus","piree",
           "istanbul","dakar","casablanca","lagos","mombasa","durban","santos","shanghai",
           "ningbo","guangzhou","shenzhen","hongkong","singapore","singapour","busan",
           "tokyo","yokohama","losangeles","newyork","savannah","miami","sydney","dubai",
           "jeddah","mumbai","nhavasheva","colombo","tanger","tangermed","algier","alger",
           "tunis","tripoli","beyrouth","callao","buenosaires"]

    # Codes IATA aéroports — MATCHING EXACT PAR TOKEN uniquement
    # On split la valeur en tokens et on compare chaque token exactement
    AIRPORT_CODES={"cdg","ory","lyo","mrs","nce","bor","tls","sxb","bod","mlh",
                   "jfk","lax","ord","mia","atl","sfo","sea","bos","iah","dfw","ewr",
                   "lhr","lgw","man","fra","muc","txl","ber","ham","vie","zrh","gva",
                   "ams","bru","cph","arn","hel","mad","bcn","fco","mxp","lin","fcm",
                   "dxb","auh","doh","ist","tlv","bom","del","hkg","nrt","icn","kix",
                   "sin","kul","bkk","syd","mel","per","akl","gru","gig","bog","lim",
                   "mex","yyz","yvr","jnb","nbo","cai","cmn","dkr","los","acc","abv"}

    # Villes clairement routières — boost score road
    ROAD_CITIES={"paris","lyon","toulouse","bordeaux","lille","marseille","nantes",
                 "strasbourg","rennes","nice","grenoble","montpellier","tours","dijon",
                 "metz","nancy","reims","rouen","amiens","clermont","limoges","poitiers",
                 "bruxelles","brussels","amsterdam","berlin","munich","francfort","cologne",
                 "madrid","barcelona","rome","milan","geneve","zurich","vienne","varsovie",
                 "bucarest","budapest","prague","bratislava","ljubljana","zagreb",
                 "london","rotterdam","antwerp","hamburg","dusseldorf","Stuttgart",
                 "birmingham","manchester","edinburgh","glasgow","bristol"}

    KW_AIR  = ["aerien","aérien","air freight","airfreight","awb","air waybill",
               "fret aerien","airline cargo","avion","aerian"]
    KW_SEA  = ["maritime","seafreight","sea freight","ocean freight","bateau","navire",
               "conteneur","container","teu","fcl","lcl","armateur","roro","ro-ro",
               "reefer","vrac","bulk","mer","ocean"]
    KW_RAIL = ["ferroviaire","rail","train","sncf","wagon","fret ferroviaire","railway"]
    KW_ROAD = ["routier","road","camion","truck","ftl","ltl","vl","tir","messagerie",
               "groupage","express","fret routier","road freight","haulage","trucking"]

    scores = {"aerien":0,"maritime":0,"ferroviaire":0,"routier":0}

    # ── 1. Colonne mode explicite (poids fort) ────────────────────
    if mode_col and mode_col in df.columns:
        for v in df[mode_col].dropna().astype(str).str.lower():
            for kw in KW_AIR:
                if kw in v: scores["aerien"] += 3
            for kw in KW_SEA:
                if kw in v: scores["maritime"] += 3
            for kw in KW_RAIL:
                if kw in v: scores["ferroviaire"] += 3
            for kw in KW_ROAD:
                if kw in v: scores["routier"] += 3

    # ── 2. Colonnes dep/arr — MATCHING EXACT PAR TOKEN ───────────
    for col in [dep_col, arr_col]:
        if not col or col not in df.columns:
            continue
        for v in df[col].dropna().astype(str):
            # Split en tokens individuels (espace, tiret, slash)
            raw_tokens = re.split(r'[\s\-/,]+', v.strip())
            tokens_clean = [nettoyer(t) for t in raw_tokens if t.strip()]

            for tok in tokens_clean:
                # Aéroport : code IATA exact (3 lettres)
                if tok in AIRPORT_CODES:
                    scores["aerien"] += 2
                # Port : nom de port inclus dans le token ou vice versa
                if any(p in tok or tok in p for p in PORTS if len(p) >= 5):
                    scores["maritime"] += 1
                # Ville routière identifiée
                if tok in ROAD_CITIES or any(tok in rc for rc in ROAD_CITIES if len(rc) >= 5):
                    scores["routier"] += 1

    # ── 3. Analyse des headers du fichier ────────────────────────
    hdrs = [nettoyer(c) for c in df.columns]

    # Headers forte indication aérien
    if any("awb" in h for h in hdrs):              scores["aerien"] += 6
    if any("airwaybill" in h for h in hdrs):        scores["aerien"] += 6
    if any("chargeableweight" in h for h in hdrs):  scores["aerien"] += 5
    if any("flightdate" in h or "flightno" in h for h in hdrs): scores["aerien"] += 4

    # Headers forte indication maritime
    if any("bl" == h or "billoflading" in h for h in hdrs): scores["maritime"] += 6
    if any("teu" in h for h in hdrs):              scores["maritime"] += 5
    if any("conteneur" in h or "container" in h for h in hdrs): scores["maritime"] += 5
    if any("etd" in h or "eta" in h for h in hdrs): scores["maritime"] += 3
    if any("armateur" in h or "carrier" in h for h in hdrs): scores["maritime"] += 2

    # Headers forte indication routière
    if any("distancekm" in h or "km" in h for h in hdrs): scores["routier"] += 4
    if any("plaque" in h or "immatricul" in h for h in hdrs): scores["routier"] += 3
    if any("orderid" in h or "ordernum" in h for h in hdrs): scores["routier"] += 2

    # Headers forte indication ferroviaire
    if any("wagon" in h or "sncf" in h for h in hdrs): scores["ferroviaire"] += 6

    # ── 4. Analyse du contenu global pour confirmation ────────────
    # Si on a une colonne distance_km avec des valeurs typiques route
    for col in df.columns:
        if "km" in nettoyer(col) or "dist" in nettoyer(col):
            try:
                vals = pd.to_numeric(df[col], errors='coerce').dropna()
                if len(vals) > 3:
                    med = vals.median()
                    # Distance routière typique : 50-3000 km
                    if 50 <= med <= 3000:
                        scores["routier"] += 3
                    # Distance aérienne typique : > 500 km sans doute
            except: pass

    dominant = max(scores, key=scores.get)
    total = sum(scores.values())

    # Pas assez de signal → routier par défaut
    if total == 0 or scores[dominant] < 2:
        return "routier","🚛 Road (default)","🚛"

    # Cas d'égalité → routier gagne (mode le plus courant)
    top_val = scores[dominant]
    rivals = [k for k,v in scores.items() if v == top_val and k != dominant]
    if rivals:
        dominant = "routier"

    lang = st.session_state.get("language","fr")
    if lang == "fr":
        labels = {
            "aerien":     ("✈️ Mode Aérien détecté",      "✈️"),
            "maritime":   ("⚓ Mode Maritime détecté",    "⚓"),
            "ferroviaire":("🚂 Mode Ferroviaire détecté", "🚂"),
            "routier":    ("🚛 Mode Routier détecté",     "🚛"),
        }
    else:
        labels = {
            "aerien":     ("✈️ Air mode detected",        "✈️"),
            "maritime":   ("⚓ Maritime mode detected",   "⚓"),
            "ferroviaire":("🚂 Rail mode detected",       "🚂"),
            "routier":    ("🚛 Road mode detected",       "🚛"),
        }
    label, emoji = labels[dominant]
    return dominant, label, emoji

def super_clean(val):
    if pd.isna(val): return 0.0
    try: return float(str(val).replace('€','').replace('$','').replace('EUR','').replace(' ','').replace('\xa0','').replace(',','.'))
    except: return 0.0

# =========================================
# 9. PAGES
# =========================================
if st.session_state.page=="accueil":
    st.markdown(f"<h1 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;font-weight:800;letter-spacing:-1px;'>{_('home_title')}</h1>",unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center;font-size:1.1em;color:#4A6080;'>{_('home_sub')}</p><br>",unsafe_allow_html=True)
    # Sélecteur langue sur la page d'accueil
    _c1,lc,_c2=st.columns([3,1,3])
    with lc:
        lang_choice=st.selectbox("",["🇫🇷 Français","🇬🇧 English"],key="lang_accueil",label_visibility="collapsed")
        st.session_state.language="en" if "English" in lang_choice else "fr"
    st.markdown("<br>",unsafe_allow_html=True)
    c1,c2=st.columns(2)
    with c1:
        st.markdown("<span class='big-emoji'>📦</span>",unsafe_allow_html=True)
        if st.button(_("home_stock"),use_container_width=True):
            st.session_state.module="stock";st.session_state.page="choix_profil_stock";st.rerun()
    with c2:
        st.markdown("<span class='big-emoji'>🌍</span>",unsafe_allow_html=True)
        if st.button(_("home_transport"),use_container_width=True):
            st.session_state.module="transport";st.session_state.page="login";st.rerun()
    st.markdown("<br><br>",unsafe_allow_html=True)
    _c1,cm,_c2=st.columns([1,1,1])
    if cm.button(_("home_access"),use_container_width=True):
        st.session_state.page="contact";st.rerun()

elif st.session_state.page=="contact":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>{_('contact_title')}</h2>",unsafe_allow_html=True)
    _c1,cc,_c2=st.columns([1,1.5,1])
    with cc:
        with st.form("vip"):
            st.text_input(_("contact_name"));st.text_input(_("contact_email"));st.text_input(_("contact_company"))
            st.selectbox(_("contact_volume"),[_("vol1"),_("vol2"),_("vol3")])
            st.selectbox(_("contact_issue"),[_("iss1"),_("iss2"),_("iss3")])
            if st.form_submit_button(_("contact_btn"),use_container_width=True):
                st.success(_("contact_ok"))
        if st.button(_("login_back"),use_container_width=True): st.session_state.page="accueil";st.rerun()

elif st.session_state.page=="choix_profil_stock":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>{_('profile_title')}</h2>",unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center;color:#4A6080;'>{_('profile_sub')}</p><br><br>",unsafe_allow_html=True)
    c1,c2=st.columns(2)
    with c1:
        st.markdown("<span class='big-emoji'>📊</span>",unsafe_allow_html=True)
        if st.button(_("profile_mgr"),use_container_width=True):
            st.session_state.stock_view="MANAGER";st.session_state.page="login";st.rerun()
    with c2:
        st.markdown("<span class='big-emoji'>👷</span>",unsafe_allow_html=True)
        if st.button(_("profile_ops"),use_container_width=True):
            st.session_state.stock_view="TERRAIN";st.session_state.page="login";st.rerun()

elif st.session_state.page=="login":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>{'Secure Access' if st.session_state.get('language')=='en' else 'Accès Sécurisé'} — {st.session_state.module.upper()}</h2><br>",unsafe_allow_html=True)
    _c1,cl,_c2=st.columns([1,1.2,1])
    with cl:
        with st.form("login_form"):
            u=st.text_input(_("login_id"));p=st.text_input(_("login_pw"),type="password")
            st.markdown("<br>",unsafe_allow_html=True)
            if st.form_submit_button(_("login_btn"),use_container_width=True):
                if u in USERS_DB and USERS_DB[u]==p:
                    st.session_state.auth=True;st.session_state.current_user=u
                    st.session_state.page="app";st.rerun()
                else: st.error(_("login_err"))
        if st.button(_("login_back"),use_container_width=True): st.session_state.page="accueil";st.rerun()

elif st.session_state.auth and st.session_state.page=="app":
    with st.sidebar:
        st.markdown(f"""
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">
                <div class="sidebar-logo">LOGI<span>FLO</span>.IO</div>
                <div style="font-size:20px;line-height:1.2;">📦<br>📦📦</div>
            </div>
            <div style="font-size:12px;color:#4A6080;margin-bottom:12px;">
                👤 {st.session_state.current_user}
            </div>
        """,unsafe_allow_html=True)
        # Sélecteur langue dans sidebar
        lang_sb=st.selectbox("",["🇫🇷 Français","🇬🇧 English"],
            index=1 if st.session_state.get("language")=="en" else 0,
            key="lang_sidebar",label_visibility="collapsed")
        st.session_state.language="en" if "English" in lang_sb else "fr"
        st.markdown("---")
        nav=st.radio("",[_("nav_workspace"),_("nav_archives"),_("nav_params"),_("nav_legal")],
                     label_visibility="collapsed")
        st.markdown("---")
        if st.button(_("nav_logout"),use_container_width=True): st.session_state.clear();st.rerun()
        st.markdown("<div style='margin-top:40px;border-top:1px solid #1e3a5f;padding-top:14px;font-size:11px;color:#4A6080;'>© 2026 Logiflo B2B Enterprise</div>",unsafe_allow_html=True)

    # ── LEGAL ──
    if nav==_("nav_legal"):
        st.title(_("nav_legal"))
        tab1,tab2,tab3=st.tabs(["📋 Mentions Légales / Legal","🔒 Confidentialité / Privacy","📄 CGUV / Terms"])
        with tab1:
            st.markdown("""<div class="legal-text">
            <h2>Éditeur / Publisher</h2>
            <div class="legal-box"><p><strong>Logiflo B2B Enterprise</strong> — SASU (en cours d'immatriculation / being incorporated)<br>
            Marseille, France — contact@logiflo.io<br>
            App: https://logiflo-io.streamlit.app</p></div>
            <h2>Hébergement / Hosting</h2>
            <p>Streamlit Cloud — Snowflake Inc., USA | GitHub Pages — GitHub Inc., USA</p>
            <h2>Propriété Intellectuelle / Intellectual Property</h2>
            <p>All elements of LOGIFLO.IO (code, algorithms, Smart Ingester™, AI engines, UI) are the exclusive property of Logiflo B2B Enterprise, protected by intellectual property law.</p>
            <h2>Limitation de responsabilité / Liability</h2>
            <p>Analyses are provided for decision support only. Logiflo cannot be held responsible for decisions made on this basis.</p>
            <p style="color:#4A6080;font-size:13px;"><em>Dernière mise à jour / Last updated: April 2026</em></p>
            </div>""",unsafe_allow_html=True)
        with tab2:
            st.markdown("""<div class="legal-text">
            <div class="legal-box"><p>Conforme au RGPD / GDPR compliant (EU) 2016/679<br>
            Contact DPO: contact@logiflo.io</p></div>
            <h2>Zero Data Retention</h2>
            <div class="legal-box"><p>
            ✅ Raw files processed in RAM only — never permanently stored<br>
            ✅ Data never sold or shared with third parties<br>
            ✅ Data never used to train public AI models<br>
            ✅ Automatic purge on logout</p></div>
            <h2>Sous-traitants / Sub-processors</h2>
            <ul><li>Streamlit Cloud (Snowflake) — hosting — USA (EU SCCs)</li>
            <li>OpenAI — AI analysis — USA (GDPR DPA)</li>
            <li>Google Sheets — archiving — EU/USA</li>
            <li>OpenRouteService (HeiGIT) — distances — Germany EU</li></ul>
            <h2>Vos droits / Your rights (GDPR art. 15-22)</h2>
            <p>Access, rectification, erasure, portability: <strong>contact@logiflo.io</strong> — 30 days response.<br>
            CNIL complaint: <strong>www.cnil.fr</strong></p>
            <p style="color:#4A6080;font-size:13px;"><em>April 2026</em></p>
            </div>""",unsafe_allow_html=True)
        with tab3:
            st.markdown("""<div class="legal-text">
            <p>Full terms (15 articles) available on request: <strong>contact@logiflo.io</strong></p>
            <h2>Key Points</h2>
            <div class="legal-box"><p>⚠️ Audits are provided as <strong>decision support only</strong>.
            They do not constitute financial, legal or accounting advice. The Client remains the sole decision-maker.</p></div>
            <h2>Data ownership</h2>
            <p>The Client retains full ownership of their data. Generated reports belong to the Client.</p>
            <h2>Liability</h2><p>Limited to amounts paid over the last 12 months.</p>
            <h2>Governing law</h2><p>French law — Commercial Courts of Marseille.</p>
            <p style="color:#4A6080;font-size:13px;"><em>Version 1.0 — April 2026</em></p>
            </div>""",unsafe_allow_html=True)

    # ── ARCHIVES ──
    elif nav==_("nav_archives"):
        st.title(_("arch_title"))
        st.markdown(f"**{st.session_state.current_user}**")
        st.markdown("---")
        with st.spinner("Loading..."):
            df_arch=load_archives_from_sheets(st.session_state.current_user)
        if df_arch is None:
            st.warning("⚠️ Google Sheets connection unavailable.")
        elif df_arch.empty:
            st.info(_("arch_empty"))
        else:
            cf1,cf2=st.columns(2)
            mf=cf1.selectbox(_("arch_filter"),[_("arch_filter_all"),"stock","transport"])
            nb=cf2.slider("",5,50,10,label_visibility="collapsed")
            ds=df_arch.copy()
            if mf!=_("arch_filter_all"): ds=ds[ds["module"]==mf]
            ds=ds.iloc[::-1].head(nb)
            st.markdown(f"**{len(ds)} {_('arch_show')}**")
            st.markdown("<br>",unsafe_allow_html=True)
            for _idx,row in ds.iterrows():
                icon="📦" if row.get("module")=="stock" else "🚚"
                st.markdown(f"""<div class="archive-card">
                    <h4>{icon} {str(row.get('module','')).upper()} — {row.get('date','')} {row.get('heure','')}</h4>
                    <div style="font-size:12px;color:#4A6080;margin-bottom:8px;">{row.get('nb_lignes','')} rows</div>
                    <span class="archive-kpi">{row.get('kpi_label_1','')}: {row.get('kpi_1','')}</span>
                    <span class="archive-kpi">{row.get('kpi_label_2','')}: {row.get('kpi_2','')}</span>
                    <span class="archive-kpi">{row.get('kpi_label_3','')}: {row.get('kpi_3','')}</span>
                </div>""",unsafe_allow_html=True)
                with st.expander(_("arch_resume")):
                    resume=row.get("resume_ia","")
                    if resume: st.markdown(render_report(str(resume),"manager"),unsafe_allow_html=True)
                    else: st.info("N/A")
                pdf_b64=row.get("pdf_base64","")
                if pdf_b64:
                    try:
                        st.download_button(_("arch_dl"),base64.b64decode(str(pdf_b64)),
                            f"Logiflo_{row.get('date','').replace('/','_')}_{row.get('module','')}.pdf",
                            key=f"dl_{row.get('date','')}_{row.get('heure','')}",use_container_width=True)
                    except: pass

    elif nav==_("nav_params"):
        st.title(_("params_title"))
        if st.session_state.module=="stock":
            st.session_state.seuil_bas=st.slider(_("params_alert"),0,100,st.session_state.seuil_bas)
            st.session_state.seuil_rupture=st.slider(_("params_rupture"),0,10,st.session_state.seuil_rupture)
        else:
            st.session_state.seuil_km=st.slider(_("params_km"),0,1000,st.session_state.seuil_km)

    elif nav==_("nav_workspace"):

        # ══ MODULE STOCK ══
        if st.session_state.module=="stock":
            st.title(_("stock_title"))
            ci,cb=st.columns([4,1])
            ci.markdown(f"**{_('active_profile')} : {st.session_state.stock_view}**")
            if cb.button(_("change_profile")): st.session_state.page="choix_profil_stock";st.rerun()
            st.markdown("<br>",unsafe_allow_html=True)
            st.markdown(f"""<div class='import-card'><h3>{_('stock_import')}</h3>
                <p>{_('stock_import_sub')}</p></div>""",unsafe_allow_html=True)
            up=st.file_uploader("",type=["csv","xlsx"],key="stock_upload")
            st.markdown("---")
            if up:
                pg=StepProgress([_("step_read"),_("step_detect"),_("step_calc")])
                pg.step(_("step_read"))
                try:
                    df_brut=pd.read_excel(up) if up.name.endswith("xlsx") else pd.read_csv(up,encoding='utf-8')
                except UnicodeDecodeError:
                    up.seek(0);df_brut=pd.read_csv(up,encoding='latin-1')
                except:
                    up.seek(0);df_brut=pd.read_csv(up,sep=';',encoding='latin-1')
                pg.step(_("step_detect"))
                df_propre,statut=smart_ingester_stock_ultime(df_brut,client_ai=client)
                pg.step(_("step_calc"));pg.done()
                if df_propre is None: st.error(statut)
                else: st.session_state.df_stock=df_propre

            if st.session_state.df_stock is not None:
                df=st.session_state.df_stock.copy()
                sans_prix=bool(df.get("_sans_prix",pd.Series([True])).iloc[0]) if "_sans_prix" in df.columns else True
                has_conso=bool(df.get("_has_conso",pd.Series([False])).iloc[0]) if "_has_conso" in df.columns else False
                if sans_prix: st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_no_price')}</span>",unsafe_allow_html=True)
                if has_conso: st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_conso')}</span>",unsafe_allow_html=True)
                else: st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_no_conso')}</span>",unsafe_allow_html=True)

                # Statuts uniformes — toujours présents quel que soit has_conso
                if has_conso:
                    df["_conso_moy"]=df["_conso_moy"].fillna(0)
                    df["Couverture_mois"]=np.where(df["_conso_moy"]>0,df["quantite"]/df["_conso_moy"],9999)
                    df["Statut"]=np.select(
                        [(df["quantite"]<=st.session_state.seuil_rupture),
                         (df["quantite"]>0)&(df["_conso_moy"]==0),
                         (df["quantite"]>0)&(df["Couverture_mois"]>6)],
                        ["🔴 Rupture","🔴 Dormant","🟠 Surstock"],default="🟢 OK")
                else:
                    df["Statut"]=np.where(
                        df["quantite"]<=st.session_state.seuil_rupture,
                        "🔴 Rupture","🟢 OK"
                    )

                df["valeur_totale"]=df["quantite"]*df["prix_unitaire"]
                val_totale=df["valeur_totale"].sum()
                ruptures=df[df["Statut"]=="🔴 Rupture"]
                tx_serv=(1-len(ruptures)/len(df))*100 if len(df)>0 else 100

                if not st.session_state.history_stock or st.session_state.history_stock[-1].get("valeur")!=val_totale:
                    st.session_state.history_stock.append({"date":datetime.datetime.now().strftime("%H:%M:%S"),"valeur":val_totale})

                if st.session_state.stock_view=="MANAGER":
                    c1,c2,c3=st.columns(3)
                    kpi1_label=_("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                    kpi1_val=f"{val_totale:,.0f} €" if not sans_prix else str(len(df))
                    kpi1_color="#0B2545"
                    c1.markdown(f"<div class='kpi-card'><h4>{kpi1_label}</h4><h2 style='color:{kpi1_color};'>{kpi1_val}</h2></div>",unsafe_allow_html=True)
                    c2.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_service')}</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>",unsafe_allow_html=True)
                    c3.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_rupture')}</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>",unsafe_allow_html=True)
                    st.markdown("<br>",unsafe_allow_html=True)
                    cp,cl2=st.columns(2)
                    cmap={"🔴 Rupture":"#E8304A","🟢 OK":"#00C896","🟢 OK":"#00C896",
                          "🔴 Dormant":"#c0392b","🟠 Surstock":"#f39c12"}
                    with cp:
                        fig_pie=px.pie(df,names="Statut",hole=0.4,color="Statut",color_discrete_map=cmap)
                        fig_pie.update_layout(paper_bgcolor="rgba(0,0,0,0)",font=dict(family="DM Sans"))
                        st.plotly_chart(fig_pie,use_container_width=True)
                    with cl2:
                        if has_conso:
                            top15=df.nlargest(15,"_conso_moy")[["reference","_conso_moy","quantite"]].copy()
                            fig_conso=px.bar(top15,x="reference",y=["quantite","_conso_moy"],barmode="group",
                                color_discrete_map={"quantite":"#0B2545","_conso_moy":"#00C896"})
                            fig_conso.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_conso,use_container_width=True)
                        else:
                            fig_line=px.line(pd.DataFrame(st.session_state.history_stock),x="date",y="valeur")
                            fig_line.update_traces(line_color="#00C896")
                            fig_line.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_line,use_container_width=True)

                    col_audit,col_save=st.columns([3,1])
                    with col_audit: run_ia=st.button(_("stock_btn_ia"),use_container_width=True)
                    with col_save:
                        if st.button(_("stock_btn_save"),use_container_width=True,key="save_stock_early"):
                            kpi1=val_totale if not sans_prix else float(len(df))
                            label1=_("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                            ok=save_audit_to_sheets(st.session_state.current_user,"stock",len(df),
                                [kpi1,tx_serv,len(ruptures)],[label1,_("stock_kpi_service"),_("stock_kpi_rupture")],
                                st.session_state.analysis_stock or "",st.session_state.last_pdf or b"")
                            if ok: st.success(_("stock_saved"))
                            else: st.warning(_("stock_save_err"))

                    if run_ia:
                        _ia_txt = "Deep AI Analysis in progress..." if st.session_state.get("language","fr")=="en" else "Analyse approfondie IA en cours..."
                        pg2=StepProgress([_("step_read"),_("step_ia"),_("step_report")],text=_ia_txt)
                        pg2.step(_("step_read"))
                        df_tox=df[df["Statut"].isin(["🔴 Dormant","🟠 Surstock"])]
                        pires=df_tox.nlargest(3,"quantite") if not df_tox.empty else df.nlargest(3,"quantite")
                        top_str=", ".join([f"{r['reference']} (qty:{r['quantite']:.0f})" for _ii,r in pires.iterrows()])
                        rupt_l=ruptures.nlargest(3,"quantite")["reference"].astype(str).tolist() if not ruptures.empty else "None"
                        med_info=""
                        if not has_conso: med_info=" BLIND SPOT: no consumption history. Sector benchmark: 2-4 months healthy coverage."
                        else:
                            cm_glob=df["_conso_moy"].mean()
                            cv_moy=df["Couverture_mois"].replace(9999,np.nan).mean()
                            med_info=f" Avg consumption: {cm_glob:.1f}/period. Avg coverage: {cv_moy:.1f} months."
                        prix_info="" if sans_prix else f" Tied-up capital: {val_totale:.0f} EUR."
                        pg2.step(_("step_ia"))
                        # Chargement historique avec KPIs courants
                        _kpis_curr_s=[val_totale if not sans_prix else float(len(df)),tx_serv,float(len(ruptures))]
                        _labels_curr_s=[_("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles"),_("stock_kpi_service"),_("stock_kpi_rupture")]
                        _hist_s=get_historique_audits(st.session_state.current_user,"stock",
                                                      current_kpis=_kpis_curr_s,current_labels=_labels_curr_s)
                        _hist_txt_s=format_historique_pour_prompt(_hist_s,"stock",st.session_state.get("language","fr"))
                        st.session_state.analysis_stock=generate_ai_analysis(
                            f"Items: {len(df)}. Service level: {tx_serv:.1f}%. Stock-outs: {len(ruptures)}. "
                            f"Top dormant: {top_str}. Top stock-outs: {rupt_l}.{prix_info}{med_info} "
                            f"Prices: {'No' if sans_prix else 'Yes'}. Consumption history: {'Yes' if has_conso else 'No'}.",
                            historique_txt=_hist_txt_s)
                        # KPIs calculés AVANT generate_expert_pdf
                        kpi1=val_totale if not sans_prix else float(len(df))
                        label1=_("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
                        _kpis_final=[kpi1, tx_serv, float(len(ruptures))]
                        _labels_final=[label1, _("stock_kpi_service"), _("stock_kpi_rupture")]
                        st.session_state.last_kpis=_kpis_final
                        st.session_state.last_labels=_labels_final
                        figs_pdf=[fig_pie]
                        if has_conso: figs_pdf.append(fig_conso)
                        st.session_state.last_pdf=generate_expert_pdf(_("pdf_title_stock"),st.session_state.analysis_stock,figs_pdf,kpis=_kpis_final,labels=_labels_final,module="stock")
                        pg2.done()

                    if st.session_state.analysis_stock:
                        st.markdown(render_report(st.session_state.analysis_stock,"manager"),unsafe_allow_html=True)
                        st.markdown("<br>",unsafe_allow_html=True)
                        if st.session_state.last_pdf:
                            st.download_button(_("stock_btn_dl"),st.session_state.last_pdf,"Audit_Stock_Logiflo.pdf",use_container_width=True)

                elif st.session_state.stock_view=="TERRAIN":
                    c1,c2=st.columns(2)
                    c1.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_rupture')}</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>",unsafe_allow_html=True)
                    c2.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_service')}</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>",unsafe_allow_html=True)
                    st.markdown(f"### {_('stock_urgent')}")
                    if len(ruptures)>0:
                        cols_s=["reference","quantite","Statut"]
                        if has_conso: cols_s.append("_conso_moy")
                        st.dataframe(ruptures[cols_s],use_container_width=True)
                    else: st.success(_("stock_no_rupture"))
                    run_ops=st.button(_("stock_btn_ia_terrain"),use_container_width=True,key="terrain_ia")
                    if run_ops:
                        _ia_txt_t = "Deep AI Analysis in progress..." if st.session_state.get("language","fr")=="en" else "Analyse approfondie IA en cours..."
                        pg3=StepProgress([_("step_read"),_("step_ia"),_("step_report")],text=_ia_txt_t)
                        pg3.step(_("step_read"))
                        top_c=df.nsmallest(5,"quantite")
                        top_s=", ".join([f"{r['reference']} ({r['quantite']:.0f})" for _ii,r in top_c.iterrows()])
                        dorm_s="No history" if not has_conso else f"{len(df[df['_conso_moy']==0])} items no movement"
                        pg3.step(_("step_ia"))
                        # Chargement historique terrain
                        _hist_t = get_historique_audits(st.session_state.current_user,"stock")
                        _hist_txt_t = format_historique_pour_prompt(_hist_t,"terrain",st.session_state.get("language","fr"))
                        st.session_state.analysis_stock=generate_ai_analysis(
                            f"Field stock: {len(df)} refs. Stock-outs: {len(ruptures)}. "
                            f"Lowest stocks: {top_s}. Dormant: {dorm_s}. "
                            f"Prices: {'No' if sans_prix else 'Yes'}.",
                            historique_txt=_hist_txt_t)
                        pg3.done()
                    if st.session_state.analysis_stock:
                        st.markdown(render_report(st.session_state.analysis_stock,"terrain"),unsafe_allow_html=True)
                        st.markdown(f"### {_('stock_full')}")
                        cols_s=["reference","quantite","Statut"]
                        if has_conso: cols_s.append("_conso_moy")
                        st.dataframe(df[cols_s],use_container_width=True,height=400)

        # ══ MODULE TRANSPORT ══
        elif st.session_state.module=="transport":
            st.title(_("trans_title"))
            st.markdown("<br>",unsafe_allow_html=True)
            st.markdown(f"""<div class='import-card'><h3>{_('trans_import')}</h3>
                <p>{_('trans_import_sub')}</p></div>""",unsafe_allow_html=True)
            up_t=st.file_uploader("",type=["csv","xlsx"],key="trans_upload")
            st.markdown("---")

            if up_t and st.session_state.trans_filename!=up_t.name:
                _pg_trans=StepProgress([1,2,3,4])
                _pg_trans.step()
                try: df_t=pd.read_excel(up_t) if up_t.name.endswith("xlsx") else pd.read_csv(up_t,encoding='utf-8')
                except UnicodeDecodeError:
                    up_t.seek(0);df_t=pd.read_csv(up_t,encoding='latin-1')
                _pg_trans.step()
                mapping=auto_map_columns_with_ai(df_t)
                dep_c_tmp=mapping.get("dep") if mapping.get("dep") in df_t.columns else None
                arr_c_tmp=mapping.get("arr") if mapping.get("arr") in df_t.columns else None
                mode_c_tmp=mapping.get("mode") if mapping.get("mode") in df_t.columns else None
                mode_det,mode_label,mode_emoji=detect_transport_mode(df_t,dep_c_tmp,arr_c_tmp,mode_c_tmp)
                _pg_trans.step()
                st.session_state.trans_mapping=mapping
                st.session_state.df_trans=df_t
                st.session_state.trans_filename=up_t.name
                st.session_state.trans_mode_detected=(mode_det,mode_label,mode_emoji)
                _pg_trans.done()

            if st.session_state.df_trans is not None:
                df_t=st.session_state.df_trans
                mapping=st.session_state.trans_mapping
                if st.session_state.trans_mode_detected:
                    mode_det,mode_label,mode_emoji=st.session_state.trans_mode_detected
                    st.markdown(f"<div class='mode-badge'>{mode_label} {_('mode_detected')}</div>",unsafe_allow_html=True)

                def col(k): return mapping.get(k) if mapping.get(k) in df_t.columns else None
                tour_c=col("client") or df_t.columns[0]
                dep_c=col("dep");arr_c=col("arr");dist_c=col("dist")
                mode_c=col("mode");ca_c=col("ca");co_c=col("co");poids_c=col("poids")

                if not co_c:
                    for c in df_t.columns:
                        if any(k in str(c).lower() for k in ["cout","cost","achat","charge"]): co_c=c;break
                if not ca_c:
                    for c in df_t.columns:
                        if any(k in str(c).lower() for k in ["ca","revenue","revenu","facture"]): ca_c=c;break
                if not co_c: st.error(_("trans_no_cost"));st.stop()

                df_t["_CO"]=df_t[co_c].apply(super_clean)
                if ca_c: df_t["_CA"]=df_t[ca_c].apply(super_clean)
                else: df_t["_CA"]=df_t["_CO"]/0.85;st.warning(_("trans_ca_miss"))
                df_t["Marge_Nette"]=df_t["_CA"]-df_t["_CO"]

                if dep_c and arr_c and "_DIST_CALCULEE" not in df_t.columns:
                    _pg_dist=StepProgress([1,2,3])
                    _pg_dist.step()
                    df_t=smart_multimodal_router(df_t,dep_c,arr_c,mode_c)
                    _pg_dist.step()
                    st.session_state.df_trans=df_t;_pg_dist.done()

                df_t["_DIST_FINALE"]=(df_t["_DIST_CALCULEE"] if "_DIST_CALCULEE" in df_t.columns and df_t["_DIST_CALCULEE"].sum()>0
                                      else (df_t[dist_c].apply(super_clean) if dist_c else 0))
                df_t["Rentabilité_%"]=np.where(df_t["_CA"]>0,df_t["Marge_Nette"]/df_t["_CA"]*100,0)
                df_t["_DS"]=df_t["_DIST_FINALE"].replace(0,1)
                df_t["Cout_KM"]=np.where(df_t["_DIST_FINALE"]>0,df_t["_CO"]/df_t["_DS"],0)

                poids_info=""
                if poids_c:
                    df_t["_POIDS"]=df_t[poids_c].apply(super_clean)
                    df_t["Cout_kg"]=np.where(df_t["_POIDS"]>0,df_t["_CO"]/df_t["_POIDS"].replace(0,1),0)
                    poids_info=f" Total weight: {df_t['_POIDS'].sum():,.0f} kg. Avg cost/kg: {df_t['Cout_kg'].mean():.3f} EUR."

                marge_tot=df_t["Marge_Nette"].sum(); ca_tot=df_t["_CA"].sum()
                taux=(marge_tot/ca_tot*100) if ca_tot>0 else 0
                traj_def=len(df_t[df_t["Marge_Nette"]<0]); cout_km=df_t["Cout_KM"].mean()
                toxiques=df_t[df_t["Marge_Nette"]<(df_t["_CA"]*0.05)]
                fuite=toxiques["_CO"].sum()-toxiques["_CA"].sum(); nb_tox=len(toxiques)

                c1,c2,c3=st.columns(3)
                c1.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_marge')}</h4><h2 style='color:#0B2545;'>{marge_tot:,.0f} €</h2></div>",unsafe_allow_html=True)
                c2.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_taux')}</h4><h2 style='color:#00C896;'>{taux:.1f} %</h2></div>",unsafe_allow_html=True)
                if fuite>0:
                    c3.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_fuite')}</h4><h2 style='color:#E8304A;'>-{fuite:,.0f} €</h2><p>{nb_tox} toxic routes</p></div>",unsafe_allow_html=True)
                else:
                    c3.markdown(f"<div class='kpi-card'><h4>{_('trans_kpi_sain')}</h4><h2 style='color:#00C896;'>OK</h2></div>",unsafe_allow_html=True)

                if poids_c: st.info(f"⚖️ Avg cost: **{df_t['Cout_kg'].mean():.3f} €/kg** | Total: **{df_t['_POIDS'].sum():,.0f} kg**")

                # Bouton save avant IA
                col_audit2,col_save2=st.columns([3,1])
                with col_audit2: run_ia_t=st.button(_("trans_btn_ia"),use_container_width=True)
                with col_save2:
                    if st.button(_("trans_btn_save"),use_container_width=True,key="save_trans_early"):
                        ok=save_audit_to_sheets(st.session_state.current_user,"transport",len(df_t),
                            [marge_tot,taux,nb_tox],[_("trans_kpi_marge"),_("trans_kpi_taux"),"Toxic routes"],
                            st.session_state.analysis_trans or "",st.session_state.last_pdf or b"")
                        if ok: st.success(_("stock_saved"))
                        else: st.warning(_("stock_save_err"))

                # ── GRAPHIQUES REFAITS ──
                st.markdown("<br>",unsafe_allow_html=True)
                df_plot=df_t.copy()
                df_plot["Statut"]=np.where(
                    df_plot["Rentabilité_%"]<0,"🔴 Loss / Perte",
                    np.where(df_plot["Rentabilité_%"]<10,"🟠 Alert / Alerte","🟢 Healthy / Sain"))
                CMAP={"🔴 Loss / Perte":"#E8304A","🟠 Alert / Alerte":"#f39c12","🟢 Healthy / Sain":"#00C896"}

                tab_top,tab_global=st.tabs([_("trans_tab_top"),_("trans_tab_all")])

                with tab_top:
                    top_n=df_plot.nsmallest(15,"Marge_Nette").sort_values("Marge_Nette")
                    top_n["label"]=top_n[tour_c].astype(str).str[:35]
                    top_n["pct_label"]=top_n["Rentabilité_%"].apply(lambda x:f"{x:.1f}%")
                    fig_top=px.bar(top_n,x="Marge_Nette",y="label",orientation="h",
                        color="Statut",color_discrete_map=CMAP,text="pct_label",
                        custom_data=["_CA","_CO","Rentabilité_%"],
                        title=_("trans_top15_title"),
                        labels={"Marge_Nette":"Margin / Marge (€)","label":""})
                    fig_top.update_traces(
                        textposition="outside",
                        hovertemplate=(
                            "<b>%{y}</b><br>"
                            "Margin: <b>%{x:,.0f} €</b><br>"
                            "Revenue: %{customdata[0]:,.0f} €<br>"
                            "Cost: %{customdata[1]:,.0f} €<br>"
                            "Rate: %{customdata[2]:.1f}%<extra></extra>"))
                    fig_top.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                        height=520,showlegend=False,
                        margin=dict(l=20,r=90,t=50,b=20),
                        xaxis=dict(title="Margin / Marge (€)",tickformat=",.0f",
                                   gridcolor="#f0f4f8",zerolinecolor="#0B2545",zerolinewidth=2),
                        yaxis=dict(title="",tickfont=dict(size=11)),
                        font=dict(family="DM Sans",size=12,color="#0B2545"),
                        title=dict(font=dict(family="Syne",size=14,color="#0B2545")))
                    st.plotly_chart(fig_top,use_container_width=True)
                    # Tableau détail
                    st.markdown(f"**{_('trans_detail')}**")
                    cols_show=[tour_c,"_CA","_CO","Marge_Nette","Rentabilité_%","Statut"]
                    cols_show=[c for c in cols_show if c in df_t.columns]
                    rename_map={tour_c:_("trans_col_client"),"_CA":_("trans_col_ca"),
                                "_CO":_("trans_col_co"),"Marge_Nette":_("trans_col_marge"),
                                "Rentabilité_%":_("trans_col_pct")}
                    display_df=top_n[cols_show].rename(columns=rename_map)
                    num_cols=[_("trans_col_ca"),_("trans_col_co"),_("trans_col_marge")]
                    fmt={c:"{:,.0f}" for c in num_cols if c in display_df.columns}
                    if _("trans_col_pct") in display_df.columns: fmt[_("trans_col_pct")]="{:.1f}%"
                    st.dataframe(display_df.style.format(fmt).applymap(
                        lambda v:"color:#E8304A;font-weight:600" if isinstance(v,(int,float)) and v<0 else "",
                        subset=[c for c in [_("trans_col_marge"),_("trans_col_pct")] if c in display_df.columns]),
                        use_container_width=True,height=380)

                with tab_global:
                    fig_scatter=px.scatter(df_plot,x="_CA",y="Rentabilité_%",
                        color="Statut",color_discrete_map=CMAP,
                        size=df_plot["_CO"].clip(lower=1),size_max=40,
                        hover_name=tour_c,custom_data=["Marge_Nette","_CO"],
                        title=_("trans_scatter_title"),
                        labels={"_CA":"Revenue / CA (€)","Rentabilité_%":"Margin Rate / Taux Marge (%)"})
                    fig_scatter.update_traces(
                        hovertemplate=(
                            "<b>%{hovertext}</b><br>"
                            "Revenue: %{x:,.0f} €<br>"
                            "Margin: %{customdata[0]:,.0f} €<br>"
                            "Cost: %{customdata[1]:,.0f} €<br>"
                            "Rate: %{y:.1f}%<extra></extra>"))
                    fig_scatter.add_hline(y=0,line_dash="solid",line_color="#E8304A",line_width=2,
                        annotation_text=_("trans_seuil_zero"),annotation_position="right")
                    fig_scatter.add_hline(y=10,line_dash="dot",line_color="#f39c12",line_width=1.5,
                        annotation_text=_("trans_seuil_alert"),annotation_position="right")
                    fig_scatter.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="#fafbfc",height=500,
                        margin=dict(l=20,r=130,t=50,b=20),
                        xaxis=dict(title="Revenue / CA (€)",tickformat=",.0f",gridcolor="#f0f4f8"),
                        yaxis=dict(title="Margin Rate (%)",ticksuffix="%",gridcolor="#f0f4f8",
                                   zerolinecolor="#E8304A",zerolinewidth=1.5),
                        font=dict(family="DM Sans",size=12,color="#0B2545"),
                        title=dict(font=dict(family="Syne",size=14,color="#0B2545")),
                        legend=dict(title="",orientation="h",yanchor="bottom",y=1.02))
                    st.plotly_chart(fig_scatter,use_container_width=True)
                    n_loss=len(df_plot[df_plot["Statut"]=="🔴 Loss / Perte"])
                    n_alert=len(df_plot[df_plot["Statut"]=="🟠 Alert / Alerte"])
                    n_ok=len(df_plot[df_plot["Statut"]=="🟢 Healthy / Sain"])
                    st.caption(f"📊 {len(df_plot)} routes — {n_loss} loss | {n_alert} alert | {n_ok} healthy")

                fig_trans=fig_top  # pour le PDF

                if run_ia_t:
                    _ia_txt_tr2 = "Deep AI Analysis in progress..." if st.session_state.get("language","fr")=="en" else "Analyse approfondie IA en cours..."
                    pg6=StepProgress([1,2,3],text=_ia_txt_tr2)
                    pg6.step()
                    top3=df_t.nsmallest(3,"Marge_Nette")
                    pires_s=", ".join([f"{r[tour_c]} ({r['Marge_Nette']:.0f} EUR)" for _ii,r in top3.iterrows()]) if not top3.empty else "None"
                    mode_info=f" Dominant transport mode: {st.session_state.trans_mode_detected[0] if st.session_state.trans_mode_detected else 'road'}."
                    # KPIs calculés AVANT generate_pdf
                    _kpis_tr=[marge_tot,taux,nb_tox]
                    _labels_tr=[_("trans_kpi_marge"),_("trans_kpi_taux"),"Toxic"]
                    pg6.step()
                    # Historique avec KPIs courants
                    _hist_tr=get_historique_audits(st.session_state.current_user,"transport",
                                                   current_kpis=_kpis_tr,current_labels=_labels_tr)
                    _hist_txt_tr=format_historique_pour_prompt(_hist_tr,"transport",st.session_state.get("language","fr"))
                    st.session_state.analysis_trans=generate_ai_analysis(
                        f"Routes: {len(df_t)}. Total margin: {marge_tot:.0f} EUR. Rate: {taux:.1f}%. "
                        f"Loss routes: {traj_def}. Top 3 worst: {pires_s}. Avg cost/km: {cout_km:.2f} EUR.{poids_info}{mode_info}",
                        historique_txt=_hist_txt_tr)
                    st.session_state.last_kpis=_kpis_tr
                    st.session_state.last_labels=_labels_tr
                    st.session_state.last_pdf=generate_expert_pdf(_("pdf_title_trans"),st.session_state.analysis_trans,[fig_trans],kpis=_kpis_tr,labels=_labels_tr,module="transport")
                    pg6.done()

                if st.session_state.analysis_trans:
                    st.markdown(render_report(st.session_state.analysis_trans,"manager"),unsafe_allow_html=True)
                    st.markdown("<br>",unsafe_allow_html=True)
                    if st.session_state.last_pdf:
                        st.download_button(_("trans_btn_dl"),st.session_state.last_pdf,"Transport_Logiflo.pdf",use_container_width=True)
