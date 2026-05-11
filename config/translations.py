# -*- coding: utf-8 -*-
"""
Logiflo - config/translations.py
Version 6.1 (mai 2026) — pricing V1 + anglais pro verifie
"""

import streamlit as st

T = {
    "fr": {
        # Navigation
        "nav_dashboard": "Tableau de bord",
        "nav_compte": "Mon Compte",
        "nav_workspace": "Espace de Travail",
        "nav_archives": "Archives",
        "nav_params": "Parametres",
        "nav_legal": "Informations Legales",
        "nav_logout": "Deconnexion",

        # Accueil
        "home_title": "LOGIFLO.IO",
        "home_sub": "Plateforme d Intelligence Logistique et d Optimisation Financiere",
        "home_stock": "AUDIT STOCKS",
        "home_transport": "AUDIT TRANSPORT",
        "home_access": "AUDIT GRATUIT",

        # Login
        "login_id": "Identifiant",
        "login_pw": "Mot de passe",
        "login_btn": "Connexion",
        "login_err": "Identifiants incorrects.",
        "login_back": "← Retour",

        # Profil — V1 : Manager uniquement (Terrain masque)
        "profile_title": "Bienvenue sur Logiflo",
        "profile_sub": "Votre audit financier commence ici.",
        "profile_mgr": "PROFIL MANAGER (Strategie & Finance)",
        "profile_ops": "PROFIL TERRAIN (Action Operationnelle)",

        # Stock
        "stock_title": "Audit Financier des Stocks",
        "stock_import": "Importation Securisee",
        "stock_import_sub": "Deposez votre fichier d inventaire (CSV ou Excel). Le Smart Ingester detecte automatiquement vos colonnes.",
        "stock_kpi_capital": "Capital Immobilise",
        "stock_kpi_articles": "Articles en Stock",
        "stock_kpi_service": "Taux de Service",
        "stock_kpi_rupture": "Articles en Rupture",
        "stock_btn_ia": "GENERER L AUDIT FINANCIER (IA)",
        "stock_btn_ia_terrain": "GENERER L AUDIT IA",
        "stock_btn_save": "Sauvegarder",
        "stock_btn_dl": "Telecharger le Rapport (PDF)",
        "stock_badge_no_price": "Mode operationnel - analyse sans prix",
        "stock_badge_conso": "Historique de consommation detecte",
        "stock_badge_no_conso": "Pas d historique - couverture non calculable",
        "stock_saved": "Sauvegarde !",
        "stock_save_err": "Connexion Supabase absente.",
        "stock_urgent": "Priorites immediates",
        "stock_full": "Stock complet",
        "stock_no_rupture": "Aucun article en rupture.",

        # Transport
        "trans_title": "Audit de Rentabilite Transport",
        "trans_import": "Importation des Flux de Transport",
        "trans_import_sub": "Deposez votre fichier TMS ou Excel.",
        "trans_kpi_marge": "Marge Nette Globale",
        "trans_kpi_taux": "Taux de Rentabilite",
        "trans_kpi_fuite": "Fuite de Marge",
        "trans_kpi_sain": "Reseau",
        "trans_btn_ia": "GENERER L AUDIT DE RENTABILITE (IA)",
        "trans_btn_save": "Sauvegarder",
        "trans_btn_dl": "Telecharger le Rapport (PDF)",
        "trans_tab_top": "Top 15 - Pires trajets",
        "trans_tab_all": "Vue d ensemble",
        "trans_ca_miss": "CA manquant - estime a marge 15%.",
        "trans_no_cost": "Colonne Cout introuvable.",
        "trans_top15_title": "Top 15 trajets les plus deficitaires",
        "trans_scatter_title": "Vue d ensemble - Rentabilite vs CA par trajet",
        "trans_seuil_zero": "Seuil zero",
        "trans_seuil_alert": "Seuil alerte 10%",
        "trans_detail": "Detail des trajets en alerte",
        "trans_col_client": "Client / Trajet",
        "trans_col_ca": "CA (EUR)",
        "trans_col_co": "Cout (EUR)",
        "trans_col_marge": "Marge (EUR)",
        "trans_col_pct": "Marge (%)",

        # Archives
        "arch_title": "Archives & Historique",
        "arch_empty": "Aucun audit archive.",
        "arch_dl": "PDF",
        "arch_filter": "Filtrer",
        "arch_filter_all": "Tous",
        "arch_show": "audit(s) affiche(s)",
        "arch_resume": "Resume IA",

        # Etapes de chargement
        "step_read": "Lecture du fichier...",
        "step_detect": "Detection des colonnes...",
        "step_calc": "Calcul des indicateurs...",
        "step_ia": "Analyse IA en cours...",
        "step_report": "Generation du rapport...",
        "step_geo": "Geocodage des villes...",
        "step_dist": "Calcul des distances...",
        "step_mode": "Detection du mode de transport...",

        # PDF
        "pdf_title_stock": "AUDIT STRATEGIQUE DES STOCKS",
        "pdf_title_trans": "AUDIT FINANCIER TRANSPORT",
        "pdf_confidential": "CONFIDENTIEL",
        "pdf_footer": "Document genere par Logiflo.io. Recommandations a titre indicatif.",

        # Divers
        "mode_detected": "- analyse adaptee activee",
        "change_profile": "Changer de profil",
        "active_profile": "Profil Actif",

        # Parametres
        "params_title": "Configuration",

        # Contact / Pricing
        "contact_title": "Passez a l action",
        "contact_name": "Nom & Prenom",
        "contact_email": "Email Professionnel",
        "contact_company": "Entreprise",
        "contact_volume": "Volume gere :",
        "contact_issue": "Enjeu prioritaire :",
        "contact_btn": "Transmettre",
        "contact_ok": "Demande transmise.",
        "vol1": "Moins de 10M EUR",
        "vol2": "De 10M a 50M EUR",
        "vol3": "Plus de 50M EUR",
        "iss1": "Optimisation BFR (Stocks)",
        "iss2": "Reduction couts Transport",
        "iss3": "Global Supply Chain",

        # Plans
        "plan_gratuit": "Audit Gratuit",
        "plan_pro": "Pro",
        "plan_expert": "Expert",
        "plan_gratuit_desc": "1 audit par email, resultat limite",
        "plan_pro_desc": "590 EUR/mois (engagement 12 mois) ou 790 EUR audit ponctuel",
        "plan_expert_desc": "Bientot disponible - Multi-utilisateurs, API",
        "plan_upgrade": "Passer au plan Pro",
        "plan_contact": "Contactez-nous",
    },
    "en": {
        # Navigation
        "nav_dashboard": "Dashboard",
        "nav_compte": "My Account",
        "nav_workspace": "Workspace",
        "nav_archives": "Archives",
        "nav_params": "Settings",
        "nav_legal": "Legal Information",
        "nav_logout": "Log out",

        # Home
        "home_title": "LOGIFLO.IO",
        "home_sub": "Logistics Intelligence & Financial Optimization Platform",
        "home_stock": "STOCK AUDIT",
        "home_transport": "TRANSPORT AUDIT",
        "home_access": "FREE AUDIT",

        # Login
        "login_id": "Username",
        "login_pw": "Password",
        "login_btn": "Sign in",
        "login_err": "Incorrect credentials.",
        "login_back": "← Back",

        # Profile — V1: Manager only (Field hidden)
        "profile_title": "Welcome to Logiflo",
        "profile_sub": "Your financial audit starts here.",
        "profile_mgr": "MANAGER PROFILE (Strategy & Finance)",
        "profile_ops": "FIELD PROFILE (Operational Action)",

        # Stock
        "stock_title": "Stock Financial Audit",
        "stock_import": "Secure Import",
        "stock_import_sub": "Drop your inventory file (CSV or Excel). Smart Ingester detects columns automatically.",
        "stock_kpi_capital": "Tied-up Capital",
        "stock_kpi_articles": "Items in Stock",
        "stock_kpi_service": "Service Level",
        "stock_kpi_rupture": "Stock-outs",
        "stock_btn_ia": "GENERATE FINANCIAL AUDIT (AI)",
        "stock_btn_ia_terrain": "GENERATE AI AUDIT",
        "stock_btn_save": "Save",
        "stock_btn_dl": "Download Report (PDF)",
        "stock_badge_no_price": "Operational mode - analysis without prices",
        "stock_badge_conso": "Consumption history detected",
        "stock_badge_no_conso": "No history - coverage not calculable",
        "stock_saved": "Saved!",
        "stock_save_err": "Supabase connection unavailable.",
        "stock_urgent": "Immediate Priorities",
        "stock_full": "Full Inventory",
        "stock_no_rupture": "No stock-outs detected.",

        # Transport
        "trans_title": "Transport Profitability Audit",
        "trans_import": "Import Transport Flows",
        "trans_import_sub": "Drop your TMS or Excel file.",
        "trans_kpi_marge": "Total Net Margin",
        "trans_kpi_taux": "Profitability Rate",
        "trans_kpi_fuite": "Margin Leak",
        "trans_kpi_sain": "Network",
        "trans_btn_ia": "GENERATE PROFITABILITY AUDIT (AI)",
        "trans_btn_save": "Save",
        "trans_btn_dl": "Download Report (PDF)",
        "trans_tab_top": "Top 15 - Worst routes",
        "trans_tab_all": "Overview",
        "trans_ca_miss": "Revenue missing - estimated at 15% margin.",
        "trans_no_cost": "Cost column not found.",
        "trans_top15_title": "Top 15 most unprofitable routes",
        "trans_scatter_title": "Overview - Profitability vs Revenue per route",
        "trans_seuil_zero": "Break-even",
        "trans_seuil_alert": "Alert 10%",
        "trans_detail": "Underperforming routes",
        "trans_col_client": "Client / Route",
        "trans_col_ca": "Revenue (EUR)",
        "trans_col_co": "Cost (EUR)",
        "trans_col_marge": "Margin (EUR)",
        "trans_col_pct": "Margin (%)",

        # Archives
        "arch_title": "Archives & History",
        "arch_empty": "No saved audits yet.",
        "arch_dl": "PDF",
        "arch_filter": "Filter",
        "arch_filter_all": "All",
        "arch_show": "audit(s) shown",
        "arch_resume": "AI Summary",

        # Loading steps
        "step_read": "Reading file...",
        "step_detect": "Detecting columns...",
        "step_calc": "Computing indicators...",
        "step_ia": "AI analysis in progress...",
        "step_report": "Generating report...",
        "step_geo": "Geocoding cities...",
        "step_dist": "Computing distances...",
        "step_mode": "Detecting transport mode...",

        # PDF
        "pdf_title_stock": "STRATEGIC STOCK AUDIT",
        "pdf_title_trans": "TRANSPORT FINANCIAL AUDIT",
        "pdf_confidential": "CONFIDENTIAL",
        "pdf_footer": "Generated by Logiflo.io. Recommendations are indicative only.",

        # Misc
        "mode_detected": "- adapted analysis activated",
        "change_profile": "Change profile",
        "active_profile": "Active Profile",

        # Settings
        "params_title": "Settings",

        # Contact / Pricing
        "contact_title": "Take action",
        "contact_name": "Full Name",
        "contact_email": "Professional Email",
        "contact_company": "Company",
        "contact_volume": "Managed volume:",
        "contact_issue": "Main challenge:",
        "contact_btn": "Submit",
        "contact_ok": "Request submitted.",
        "vol1": "Less than 10M EUR",
        "vol2": "10M to 50M EUR",
        "vol3": "More than 50M EUR",
        "iss1": "Working Capital Optimization (Stock)",
        "iss2": "Transport Cost Reduction",
        "iss3": "Global Supply Chain",

        # Plans
        "plan_gratuit": "Free Audit",
        "plan_pro": "Pro",
        "plan_expert": "Expert",
        "plan_gratuit_desc": "1 audit per email, limited results",
        "plan_pro_desc": "590 EUR/month (12-month commitment) or 790 EUR one-time audit",
        "plan_expert_desc": "Coming soon - Multi-user, API, integrations",
        "plan_upgrade": "Upgrade to Pro",
        "plan_contact": "Contact us",
    },
}


def _(key):
    """Retourne la traduction pour la langue active."""
    lang = st.session_state.get("language", "fr")
    return T.get(lang, T["fr"]).get(key, T["fr"].get(key, key))
