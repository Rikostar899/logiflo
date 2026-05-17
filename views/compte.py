import streamlit as st
from config.plans import PLAN_LIMITS, get_user_plan
from config.translations import _
from services.supabase_client import save_user_prefs
from engine.pdf_gen import generate_exemple_excel


def render_compte():
    lang = st.session_state.get("language", "fr")
    username = st.session_state.current_user
    plan = get_user_plan(username)
    plan_info = PLAN_LIMITS.get(plan, PLAN_LIMITS.get("gratuit", {}))

    # En-tête profil
    st.markdown(
        f'<div style="background:linear-gradient(135deg,#0B2545,#0f2f5a);border-radius:14px;padding:22px 24px;margin-bottom:20px;display:flex;align-items:center;gap:16px;">'
        f'<div style="width:52px;height:52px;border-radius:50%;background:#00C896;display:flex;align-items:center;justify-content:center;font-family:Syne,sans-serif;font-size:18px;font-weight:800;color:#0B2545;">{username[:2].upper()}</div>'
        f'<div><div style="font-family:Syne,sans-serif;font-size:18px;font-weight:800;color:white;">{username}</div></div>'
        f'<div style="margin-left:auto;padding:4px 12px;border-radius:20px;background:{plan_info.get("bg","")};color:{plan_info.get("color","")};font-size:11px;font-weight:700;">{plan_info.get("icon","")} {plan_info.get("label","")}</div></div>',
        unsafe_allow_html=True)

    # Infos entreprise
    with st.form("form_company"):
        company = st.text_input("Nom d'entreprise" if lang == "fr" else "Company name",
                                value=st.session_state.get("company_name", ""))
        sectors = ["Textile", "Distribution", "Industrie", "Agroalimentaire", "Pharma", "Retail", "BTP", "Electronique", "Autre"]
        saved = st.session_state.get("company_sector", "")
        sector = st.selectbox("Secteur" if lang == "fr" else "Sector", sectors,
                              index=sectors.index(saved) if saved in sectors else 0)
        if st.form_submit_button("Sauvegarder" if lang == "fr" else "Save", use_container_width=True):
            st.session_state["company_name"] = company
            st.session_state["company_sector"] = sector
            save_user_prefs(username, {"company_name": company, "company_sector": sector})
            st.success("Sauvegarde" if lang == "fr" else "Saved")

    st.markdown("---")

    # Langue
    st.markdown(f"### {'Langue' if lang == 'fr' else 'Language'}")
    lang_opts = ["🇫🇷 Français", "🇬🇧 English"]
    lang_sel = st.radio("", lang_opts, index=1 if lang == "en" else 0, horizontal=True, label_visibility="collapsed")
    new_lang = "en" if "English" in lang_sel else "fr"
    if new_lang != lang:
        st.session_state["language"] = new_lang
        try: save_user_prefs(username, {"language": new_lang})
        except Exception: pass
        st.rerun()

    # Seuil rupture
    st.markdown(f"### {'Seuil alerte rupture' if lang == 'fr' else 'Stockout alert threshold'}")
    seuil = st.slider("", 0, 20, int(st.session_state.get("seuil_rupture", 5)), label_visibility="collapsed")
    if seuil != st.session_state.get("seuil_rupture", 5):
        st.session_state["seuil_rupture"] = seuil
        save_user_prefs(username, {"seuil_rupture": seuil})

    st.markdown("---")

    # Fichier exemple
    try:
        ex_bytes = generate_exemple_excel()
        if ex_bytes:
            st.download_button(
                "📥 Telecharger le fichier exemple" if lang == "fr" else "📥 Download sample file",
                ex_bytes, "logiflo_modele.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)
    except Exception:
        pass
