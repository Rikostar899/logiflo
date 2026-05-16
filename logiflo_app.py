import streamlit as st

st.set_page_config(page_title="LOGIFLO.IO", layout="wide", page_icon="📦")

# ══ SESSION STATE ══════════════════════════════════════════════════
for k, v in {
    "page": "login",
    "module": "stock",
    "auth": False,
    "current_user": None,
    "language": "fr",
    "df_stock_manager": None,
    "df_stock_terrain": None,
    "history_stock": [],
    "stock_view": "MANAGER",
    "seuil_bas": 15,
    "seuil_rupture": 0,
    "analysis_stock_manager": None,
    "analysis_stock_terrain": None,
    "last_pdf": None,
    "last_kpis": [],
    "last_labels": [],
    "audit_gratuit_done": False,
    "rgpd_ok": False,
    "_is_free": False,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ══ CSS ════════════════════════════════════════════════════════════
from components.css import inject_css
inject_css()

# ══ ROUTAGE ════════════════════════════════════════════════════════
page = st.session_state.get("page", "login")

# Pages publiques (pas besoin d'auth)
if page == "login":
    from pages.login import render_login
    render_login()

elif page == "profil":
    from pages.profil import render_profil
    render_profil()

elif page == "onboarding":
    from pages.onboarding import render_onboarding
    render_onboarding()

elif page == "plans":
    from pages.plans import render_plans
    render_plans()

elif page == "checkout":
    from pages.plans import render_checkout
    render_checkout()

elif page == "legal":
    from pages.legal import render_legal
    render_legal()

# Pages protégées (auth requise)
elif page == "app" and st.session_state.get("auth"):
    from components.sidebar import render_sidebar
    from config.translations import _

    nav = render_sidebar()

    if nav == _("nav_dashboard"):
        from pages.dashboard import render_dashboard
        render_dashboard()

    elif nav == _("nav_workspace"):
        from pages.workspace_stock import render_workspace
        render_workspace()

    elif nav == _("nav_archives"):
        from pages.archives import render_archives
        render_archives()

    elif nav in (_("nav_compte"), _("nav_params")):
        from pages.compte import render_compte
        render_compte()

    elif nav == _("nav_legal"):
        from pages.legal import render_legal
        render_legal()

# Fallback
else:
    st.session_state.page = "login"
    st.rerun()
