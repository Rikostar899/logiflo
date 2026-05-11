# -*- coding: utf-8 -*-
"""
Logiflo - config/plans.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Gestion des plans, utilisateurs et acces
Version 6.1 (mai 2026)

Plans V1 :
  gratuit → 0 EUR, 1 audit par email, resultat limite
  pro     → 590 EUR/mois (engagement 12 mois) OU 790 EUR audit ponctuel
  expert  → Bientot disponible (locked, V2)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st
import os


# ════════════════════════════════════════════════════════════════════════════
# DEFINITION DES PLANS
# ════════════════════════════════════════════════════════════════════════════
PLAN_LIMITS = {
    "gratuit": {
        "label": "Audit Gratuit",
        "label_en": "Free Audit",
        "price_display_fr": "Gratuit",
        "price_display_en": "Free",
        "price_sub_fr": "1 audit unique par email",
        "price_sub_en": "1 audit per email address",
        "color": "#6B7280",
        "bg": "#F3F4F6",
        "icon": "○",
        "modules": 2,
        "audits_mois": 1,
        "historique_j": 0,
        "benchmarks": False,
        "prediction": False,
        "bfr": False,
        "terrain": False,
        "scoring_detail": False,
        "news": False,
        "pdf_pages": 2,
        "pdf_expert": False,
    },
    "pro": {
        "label": "Pro",
        "label_en": "Pro",
        "price_display_fr": "590 EUR / mois",
        "price_display_en": "590 EUR / month",
        "price_sub_fr": "Engagement 12 mois — ou 790 EUR l audit ponctuel sans engagement",
        "price_sub_en": "12-month commitment — or 790 EUR one-time audit, no commitment",
        "price_mensuel": "590 EUR/mois",
        "price_ponctuel": "790 EUR",
        "color": "#047857",
        "bg": "#D1FAE5",
        "icon": "◆",
        "modules": 2,
        "audits_mois": None,       # illimite (mensuel) ou 1 (ponctuel, gere par Stripe)
        "historique_j": 365,
        "benchmarks": True,
        "prediction": True,
        "bfr": True,
        "terrain": False,          # V1 : Manager uniquement
        "scoring_detail": True,
        "news": True,
        "pdf_pages": 5,
        "pdf_expert": True,
    },
    "expert": {
        "label": "Expert",
        "label_en": "Expert",
        "price_display_fr": "Bientot disponible",
        "price_display_en": "Coming soon",
        "price_sub_fr": "Multi-utilisateurs, API, integrations",
        "price_sub_en": "Multi-user, API, integrations",
        "color": "#B45309",
        "bg": "#FDE68A",
        "icon": "★",
        "modules": 99,
        "audits_mois": None,
        "historique_j": 730,
        "benchmarks": True,
        "prediction": True,
        "bfr": True,
        "terrain": True,           # V2
        "scoring_detail": True,
        "news": True,
        "pdf_pages": 5,
        "pdf_expert": True,
        "api": True,
        "logo_pdf": True,
    },
}


# ════════════════════════════════════════════════════════════════════════════
# MAPPING UTILISATEURS → PLANS
# ════════════════════════════════════════════════════════════════════════════
# Alias pour compatibilite avec logiflo_app.py (qui reference "starter")
PLAN_LIMITS["starter"] = PLAN_LIMITS["gratuit"]
PLAN_LIMITS["business"] = PLAN_LIMITS["pro"]


# ════════════════════════════════════════════════════════════════════════════
# MAPPING UTILISATEURS → PLANS
# ════════════════════════════════════════════════════════════════════════════
USERS_PLAN = {
    "eric": "expert",
    "admin": "expert",
    "jury": "expert",
    "demo_client1": "pro",
    "demo_client2": "pro",
    "partenaire": "pro",
    "test": "gratuit",
}


# ════════════════════════════════════════════════════════════════════════════
# CHARGEMENT UTILISATEURS (mots de passe — bcrypt en V2)
# ════════════════════════════════════════════════════════════════════════════
def _load_users():
    """Charge les utilisateurs depuis st.secrets ou defaults."""
    try:
        raw = st.secrets.get("USERS_DB", {})
        if isinstance(raw, dict) and raw:
            return dict(raw)
    except Exception:
        pass
    return {
        "eric": "logiflo2026",
        "admin": "admin123",
        "demo_client1": "audit2026",
        "demo_client2": "test2026",
        "jury": "pitch2026",
        "partenaire": "partner2026",
        "test": "test123",
    }


try:
    USERS_DB = _load_users()
except Exception:
    USERS_DB = {}


# ════════════════════════════════════════════════════════════════════════════
# FONCTIONS D'ACCES
# ════════════════════════════════════════════════════════════════════════════
def get_user_plan(username):
    """Retourne le plan de l'utilisateur (default: gratuit)."""
    return USERS_PLAN.get(str(username).lower(), "gratuit")


def can_access(feature, username=None):
    """Verifie si l'utilisateur peut acceder a une feature."""
    if username is None:
        username = st.session_state.get("current_user", "")
    plan = get_user_plan(username)
    return bool(PLAN_LIMITS.get(plan, PLAN_LIMITS["gratuit"]).get(feature, False))


def show_lock(feature, username=None):
    """Affiche un verrou pour les features non accessibles."""
    if username is None:
        username = st.session_state.get("current_user", "")
    lang = st.session_state.get("language", "fr")

    required = "pro"
    for p, l in PLAN_LIMITS.items():
        if l.get(feature):
            required = p
            break

    rp = PLAN_LIMITS.get(required, PLAN_LIMITS["pro"])

    labels = {
        "terrain": "Profil Terrain" if lang == "fr" else "Field Profile",
        "prediction": "Prediction rupture" if lang == "fr" else "Stockout Prediction",
        "bfr": "Alerte BFR" if lang == "fr" else "WCR Alert",
        "benchmarks": "Benchmarks sectoriels" if lang == "fr" else "Sector Benchmarks",
        "news": "Actualites" if lang == "fr" else "News Feed",
        "scoring_detail": "Scoring detaille" if lang == "fr" else "Detailed Scoring",
        "api": "Acces API" if lang == "fr" else "API Access",
        "pdf_expert": "Rapport PDF complet" if lang == "fr" else "Full PDF Report",
    }
    feat = labels.get(feature, feature)
    plan_label = rp.get("label_en", rp["label"]) if lang == "en" else rp["label"]

    st.markdown(
        f'<div style="border-left:3px solid {rp["color"]};border-radius:8px;'
        f'padding:12px 16px;background:{rp["bg"]};margin:8px 0;">'
        f'<div style="font-size:13px;color:#0B2545;">&#128274; {feat} '
        f'-- plan {plan_label}</div>'
        f'<div style="font-size:11px;color:#4A6080;">contact@logiflo.io</div></div>',
        unsafe_allow_html=True,
    )


def audit_counter_sidebar(username, plan):
    """Affiche le compteur d'audits dans la sidebar."""
    limit = PLAN_LIMITS.get(plan, {}).get("audits_mois")
    if limit is None:
        return True

    lang = st.session_state.get("language", "fr")
    used = st.session_state.get("audit_count_month", 0)
    color = (
        "#E8304A" if used >= limit
        else ("#F39C12" if used >= limit * 0.6 else "#00C896")
    )
    pct = min(int((used / limit) * 100), 100)
    lbl = "Audits ce mois" if lang == "fr" else "Audits this month"

    st.sidebar.markdown(
        f'<div style="padding:8px 12px;background:rgba(255,255,255,0.05);'
        f'border-radius:8px;margin-bottom:8px;">'
        f'<div style="font-size:10px;color:rgba(255,255,255,0.5);">{lbl}</div>'
        f'<div style="font-size:16px;font-weight:800;color:{color};">'
        f'{used}/{limit}</div>'
        f'<div style="height:3px;background:rgba(255,255,255,0.1);'
        f'border-radius:99px;overflow:hidden;margin-top:4px;">'
        f'<div style="height:100%;width:{pct}%;background:{color};'
        f'border-radius:99px;"></div></div></div>',
        unsafe_allow_html=True,
    )

    if used >= limit:
        if lang == "fr":
            st.sidebar.warning("Quota atteint. Passez au plan Pro pour des audits illimites.")
        else:
            st.sidebar.warning("Quota reached. Upgrade to Pro for unlimited audits.")
        return False
    return True
