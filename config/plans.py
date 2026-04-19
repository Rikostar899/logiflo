import streamlit as st
import pandas as pd
import datetime

USERS_PLAN = {
    "eric":"expert","admin":"expert","demo_client1":"starter",
    "demo_client2":"business","jury":"expert","partenaire":"business","test":"starter",
}

PLAN_LIMITS = {
    "starter":{"label":"Starter","price":"290EUR/mois","color":"#6D28D9","bg":"#F3E8FF","icon":"●","modules":1,"audits_mois":3,"historique_j":30,"benchmarks":False,"prediction":False,"bfr":False,"terrain":False,"scoring_detail":False,"news":False,"pdf_pages":3},
    "business":{"label":"Business","price":"490EUR/mois","color":"#047857","bg":"#D1FAE5","icon":"◆","modules":2,"audits_mois":None,"historique_j":180,"benchmarks":True,"prediction":True,"bfr":True,"terrain":True,"scoring_detail":True,"news":True,"pdf_pages":5},
    "expert":{"label":"Expert","price":"Sur devis","color":"#B45309","bg":"#FDE68A","icon":"★","modules":99,"audits_mois":None,"historique_j":730,"benchmarks":True,"prediction":True,"bfr":True,"terrain":True,"scoring_detail":True,"news":True,"pdf_pages":5,"api":True,"logo_pdf":True},
}

def _load_users():
    try:
        raw = st.secrets.get("USERS_DB", {})
        if isinstance(raw, dict) and raw:
            return dict(raw)
    except Exception:
        pass
    return {"eric":"logiflo2026","admin":"admin123","demo_client1":"audit2026","demo_client2":"test2026","jury":"pitch2026","partenaire":"partner2026","test":"test123"}

try:
    USERS_DB = _load_users()
except Exception:
    USERS_DB = {}

def get_user_plan(username):
    return USERS_PLAN.get(str(username).lower(), "starter")

def can_access(feature, username=None):
    if username is None:
        username = st.session_state.get("current_user", "")
    plan = get_user_plan(username)
    return bool(PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"]).get(feature, False))

def show_lock(feature, username=None):
    if username is None:
        username = st.session_state.get("current_user", "")
    lang = st.session_state.get("language", "fr")
    required = "business"
    for p, l in PLAN_LIMITS.items():
        if l.get(feature):
            required = p
            break
    rp = PLAN_LIMITS.get(required, PLAN_LIMITS["business"])
    labels = {"terrain":"Profil Terrain","prediction":"Prediction rupture","bfr":"Alerte BFR","benchmarks":"Benchmarks sectoriels","news":"Actualites","scoring_detail":"Scoring detaille","api":"Acces API"}
    feat = labels.get(feature, feature)
    st.markdown(f"""<div style="border-left:3px solid {rp['color']};border-radius:8px;padding:12px 16px;background:{rp['bg']};margin:8px 0;"><div style="font-size:13px;color:#0B2545;">&#128274; {feat} -- plan {rp['label']}</div><div style="font-size:11px;color:#4A6080;">contact@logiflo.io</div></div>""", unsafe_allow_html=True)

def audit_counter_sidebar(username, plan):
    if PLAN_LIMITS.get(plan, {}).get("audits_mois") is None:
        return True
    lang = st.session_state.get("language", "fr")
    max_a = PLAN_LIMITS[plan]["audits_mois"]
    used = st.session_state.get("audit_count_month", 0)
    color = "#E8304A" if used >= max_a else ("#F39C12" if used >= max_a*0.6 else "#00C896")
    pct = min(int((used/max_a)*100), 100)
    lbl = "Audits ce mois" if lang=="fr" else "Audits this month"
    st.sidebar.markdown(f"""<div style="padding:8px 12px;background:rgba(255,255,255,0.05);border-radius:8px;margin-bottom:8px;"><div style="font-size:10px;color:rgba(255,255,255,0.5);">{lbl}</div><div style="font-size:16px;font-weight:800;color:{color};">{used}/{max_a}</div><div style="height:3px;background:rgba(255,255,255,0.1);border-radius:99px;overflow:hidden;margin-top:4px;"><div style="height:100%;width:{pct}%;background:{color};border-radius:99px;"></div></div></div>""", unsafe_allow_html=True)
    if used >= max_a:
        st.sidebar.warning("Quota atteint" if lang=="fr" else "Quota reached")
        return False
    return True
