import streamlit as st
from services.supabase_client import save_user_prefs

SECTORS = [
    ("🏭", "Industrie / Manufacturing", "stock_industrie"),
    ("📦", "Distribution / Negoce", "stock_distribution"),
    ("👗", "Textile / Mode / Fashion", "stock_retail"),
    ("💊", "Pharma / Sante", "stock_pharma"),
    ("🍎", "Agroalimentaire", "stock_agroalim"),
    ("🏗️", "BTP / Construction", "stock_btp"),
    ("🛒", "Retail / E-commerce", "stock_retail"),
    ("💻", "Electronique / High-tech", "generique"),
    ("🔧", "Autre / Other", "generique"),
]


def render_onboarding():
    lang = st.session_state.get("language", "fr")

    st.markdown(
        '<div style="text-align:center;margin-bottom:8px;">'
        '<span style="font-family:Syne,sans-serif;font-weight:800;font-size:2rem;color:#0B2545;">'
        'LOGI<span style="color:#00C896;">FLO</span>.IO</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<h2 style="text-align:center;color:#0B2545;font-family:Syne,sans-serif;">'
        f'{"Last step before your first audit" if lang == "en" else "Derniere etape avant votre premier audit"}</h2>',
        unsafe_allow_html=True,
    )

    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        # ── Sélection secteur ──
        st.markdown(
            f'<div style="font-family:Syne,sans-serif;font-weight:700;font-size:1rem;color:#0B2545;margin-bottom:12px;">'
            f'{"Select your sector" if lang == "en" else "Selectionnez votre secteur"}</div>',
            unsafe_allow_html=True,
        )
        sector_labels = [f"{icon} {name}" for icon, name, _ in SECTORS]
        selected_idx = st.radio(
            "Secteur" if lang == "fr" else "Sector",
            range(len(SECTORS)),
            format_func=lambda i: sector_labels[i],
            label_visibility="collapsed",
        )
        selected_sector = SECTORS[selected_idx][2]

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Consentement RGPD ──
        if lang == "en":
            consent_text = """**What we do with your data:**
- Files are processed in memory only — never stored on disk
- A summary is sent to AI (OpenAI/Gemini, GDPR-compliant) for analysis
- Audit results (KPIs, summary, PDF) are stored in our EU database (Paris)
- Your data is never sold or shared with third parties
- Right to access, rectify, delete at any time: contact@logiflo.io"""
            check_label = "I accept the processing of my data as described above"
            btn_label = "START MY AUDIT"
        else:
            consent_text = """**Ce que nous faisons de vos donnees :**
- Les fichiers sont traites en memoire uniquement — jamais stockes sur disque
- Un resume est envoye a l'IA (OpenAI/Gemini, conforme RGPD) pour l'analyse
- Les resultats (KPIs, resume, PDF) sont stockes dans notre base UE (Paris)
- Vos donnees ne sont jamais vendues ni partagees avec des tiers
- Droit d'acces, rectification, suppression a tout moment : contact@logiflo.io"""
            check_label = "J'accepte le traitement de mes donnees tel que decrit ci-dessus"
            btn_label = "COMMENCER MON AUDIT"

        st.markdown(
            f'<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;'
            f'padding:20px;margin-bottom:16px;font-size:0.85rem;color:#4A6080;line-height:1.7;">'
            f'{consent_text.replace(chr(10), "<br>")}</div>',
            unsafe_allow_html=True,
        )

        accept = st.checkbox(check_label, key="onboarding_accept")

        st.markdown("<br>", unsafe_allow_html=True)
        if st.button(btn_label, use_container_width=True, type="primary",
                     disabled=not accept, key="onboarding_go"):
            # Sauvegarder dans Supabase
            user = st.session_state.get("current_user", "")
            try:
                save_user_prefs(user, {
                    "onboarding_done": True,
                    "sector": selected_sector,
                    "rgpd_accepted": True,
                })
            except Exception:
                pass
            st.session_state.rgpd_ok = True
            st.session_state._onboarding_done = True
            st.session_state["_user_sector"] = selected_sector
            st.session_state.page = "app"
            st.rerun()
