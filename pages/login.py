import streamlit as st
from config.plans import USERS_DB
from config.translations import _
from services.supabase_client import load_user_prefs


def render_login():
    lang = st.session_state.get("language", "fr")

    st.markdown(
        '<div style="text-align:center;margin:40px 0 8px;">'
        '<span style="font-family:Syne,sans-serif;font-weight:900;font-size:2.5rem;color:#0B2545;">'
        'LOGI<span style="color:#00C896;">FLO</span>.IO</span></div>'
        '<p style="text-align:center;color:#4A6080;font-size:0.9rem;margin-bottom:30px;">'
        f'{"Logistics Intelligence Platform" if lang == "en" else "Plateforme d Intelligence Logistique"}</p>',
        unsafe_allow_html=True,
    )

    # Sélecteur de langue
    _c1, lc, _c2 = st.columns([3, 1, 3])
    with lc:
        lang_choice = st.selectbox("", ["🇫🇷 Français", "🇬🇧 English"],
                                   key="lang_login", label_visibility="collapsed")
        st.session_state.language = "en" if "English" in lang_choice else "fr"

    _c1, cl, _c2 = st.columns([1, 1.5, 1])
    with cl:
        # Login avec identifiants
        with st.form("login_form"):
            u = st.text_input(_("login_id"))
            p = st.text_input(_("login_pw"), type="password")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.form_submit_button(_("login_btn"), use_container_width=True):
                if u in USERS_DB and USERS_DB[u] == p:
                    st.session_state.auth = True
                    st.session_state.current_user = u
                    st.session_state.module = "stock"
                    # Charger les prefs
                    try:
                        prefs = load_user_prefs(u)
                        if prefs:
                            if prefs.get("company_name"):
                                st.session_state["company_name"] = prefs["company_name"]
                            if prefs.get("language"):
                                st.session_state["language"] = prefs["language"]
                            if prefs.get("seuil_rupture") is not None:
                                st.session_state["seuil_rupture"] = int(prefs["seuil_rupture"])
                    except Exception:
                        pass
                    st.session_state.page = "profil"
                    st.rerun()
                else:
                    st.error(_("login_err"))

        # Séparateur
        st.markdown(
            '<div style="text-align:center;font-size:11px;color:rgba(0,0,0,0.3);margin:16px 0;">— '
            f'{"or" if lang == "en" else "ou"} —</div>',
            unsafe_allow_html=True,
        )

        # Audit gratuit (email seul)
        with st.form("free_form"):
            email = st.text_input(
                "Email professionnel" if lang == "fr" else "Professional email",
                placeholder="nom@entreprise.com",
            )
            if st.form_submit_button(
                "🆓 AUDIT GRATUIT" if lang == "fr" else "🆓 FREE AUDIT",
                use_container_width=True,
            ):
                if email and "@" in email:
                    st.session_state.auth = True
                    st.session_state.current_user = email
                    st.session_state.module = "stock"
                    st.session_state._is_free = True
                    st.session_state.page = "profil"
                    st.rerun()
                else:
                    st.error("Email invalide" if lang == "fr" else "Invalid email")
