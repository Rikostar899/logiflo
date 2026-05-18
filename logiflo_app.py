import streamlit as st

st.set_page_config(page_title="LOGIFLO.IO", layout="wide", page_icon="📦")

for k, v in {
    "page": "login", "module": "stock", "auth": False,
    "current_user": None, "language": "fr",
    "df_stock_manager": None, "df_stock_terrain": None,
    "history_stock": [], "stock_view": "MANAGER",
    "seuil_bas": 15, "seuil_rupture": 0,
    "analysis_stock_manager": None, "analysis_stock_terrain": None,
    "last_pdf": None, "last_kpis": [], "last_labels": [],
    "audit_gratuit_done": False, "rgpd_ok": False, "_is_free": False, "_onboarding_done": False,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

from components.css import inject_css
inject_css()

page = st.session_state.get("page", "login")

if page == "login":
    from views.login import render_login
    render_login()
elif page == "profil":
    from views.profil import render_profil
    render_profil()
elif page == "onboarding":
    from views.onboarding import render_onboarding
    render_onboarding()
elif page == "audit_gratuit":
    from views.audit_gratuit import render_audit_gratuit
    render_audit_gratuit()
elif page == "plans":
    from views.plans import render_plans
    render_plans()
elif page == "checkout":
    from views.plans import render_checkout
    render_checkout()
elif page == "legal":
    from views.legal import render_legal
    render_legal()
elif page == "app" and st.session_state.get("auth"):
    from components.sidebar import render_sidebar
    from config.translations import _
    nav = render_sidebar()

    if nav == _("nav_dashboard"):
        from views.dashboard import render_dashboard
        render_dashboard()
    elif nav == _("nav_workspace"):
        from views.workspace_stock import render_workspace
        render_workspace()
    elif nav == _("nav_archives"):
        from views.archives import render_archives
        render_archives()
    elif nav in (_("nav_compte"), _("nav_params")):
        from views.compte import render_compte
        render_compte()
    elif nav == _("nav_legal"):
        from views.legal import render_legal
        render_legal()
else:
    st.session_state.page = "login"
    st.rerun()
