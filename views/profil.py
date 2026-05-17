import streamlit as st
from config.translations import _
from services.supabase_client import load_user_prefs


def render_profil():
    lang = st.session_state.get("language", "fr")
    st.markdown(
        '<div style="text-align:center;margin-bottom:8px;">'
        '<span style="font-family:Syne,sans-serif;font-weight:800;font-size:2rem;color:#0B2545;">'
        'LOGI<span style="color:#00C896;">FLO</span>.IO</span></div>',
        unsafe_allow_html=True)
    st.markdown(
        f'<h2 style="text-align:center;color:#0B2545;font-family:Syne,sans-serif;">'
        f'{"Welcome to Logiflo" if lang == "en" else "Bienvenue sur Logiflo"}</h2>'
        f'<p style="text-align:center;color:#4A6080;margin-bottom:30px;">'
        f'{"Choose your workspace" if lang == "en" else "Choisissez votre espace de travail"}</p>',
        unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div style="text-align:center;font-size:50px;margin-bottom:8px;">📊</div>', unsafe_allow_html=True)
        if st.button(_("profile_mgr"), use_container_width=True, key="btn_manager"):
            st.session_state.stock_view = "MANAGER"
            _go_next()
    with c2:
        st.markdown('<div style="text-align:center;font-size:50px;margin-bottom:8px;">📋</div>', unsafe_allow_html=True)
        if st.button(_("profile_ops"), use_container_width=True, key="btn_terrain"):
            st.session_state.stock_view = "TERRAIN"
            _go_next()


def _go_next():
    if st.session_state.get("_onboarding_done"):
        st.session_state.page = "app"
        st.session_state.rgpd_ok = True
        st.rerun()
        return
    user = st.session_state.get("current_user", "")
    try:
        prefs = load_user_prefs(user)
        if prefs and prefs.get("onboarding_done"):
            st.session_state._onboarding_done = True
            st.session_state.rgpd_ok = True
            st.session_state.page = "app"
            st.rerun()
            return
    except Exception:
        pass
    st.session_state.page = "onboarding"
    st.rerun()
