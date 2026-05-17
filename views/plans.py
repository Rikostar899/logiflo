import streamlit as st


def render_plans():
    lang = st.session_state.get("language", "fr")

    st.markdown(
        '<div style="text-align:center;margin-bottom:8px;">'
        '<span style="font-family:Syne,sans-serif;font-weight:800;font-size:2rem;color:#0B2545;">'
        'LOGI<span style="color:#00C896;">FLO</span>.IO</span></div>'
        f'<h2 style="text-align:center;color:#0B2545;font-family:Syne,sans-serif;font-weight:800;">'
        f'{"Choose your plan" if lang == "en" else "Choisissez votre offre"}</h2>',
        unsafe_allow_html=True)

    plans = [
        {
            "name": "Gratuit" if lang == "fr" else "Free",
            "price": "0 EUR", "period": "",
            "color": "#6B7280", "bg": "#F3F4F6",
            "features": [
                "1 audit par email" if lang == "fr" else "1 audit per email",
                "Resultat limite" if lang == "fr" else "Limited results",
                "Pas de PDF" if lang == "fr" else "No PDF export",
            ],
            "cta": "Actif" if lang == "fr" else "Active",
            "disabled": True,
        },
        {
            "name": "Pro",
            "price": "590", "period": " EUR/mois" if lang == "fr" else " EUR/month",
            "color": "#047857", "bg": "#D1FAE5",
            "features": [
                "Audits illimites" if lang == "fr" else "Unlimited audits",
                "Engagement 12 mois" if lang == "fr" else "12-month commitment",
                "PDF expert + Historique" if lang == "fr" else "Expert PDF + History",
                "Scoring + Benchmarks" if lang == "fr" else "Scoring + Benchmarks",
                "Predictions rupture" if lang == "fr" else "Stockout predictions",
                f"{'Ou' if lang == 'fr' else 'Or'} 790 EUR {'ponctuel' if lang == 'fr' else 'one-time'}",
            ],
            "cta": "Souscrire" if lang == "fr" else "Subscribe",
            "disabled": False,
        },
        {
            "name": "Expert",
            "price": "Bientot" if lang == "fr" else "Coming soon", "period": "",
            "color": "#B45309", "bg": "#FDE68A",
            "features": [
                "Multi-utilisateurs" if lang == "fr" else "Multi-user",
                "API dedicee" if lang == "fr" else "Dedicated API",
                "Integrations" if lang == "fr" else "Integrations",
            ],
            "cta": "Bientot disponible" if lang == "fr" else "Coming soon",
            "disabled": True,
        },
    ]

    cols = st.columns(3)
    for i, plan in enumerate(plans):
        with cols[i]:
            feats_html = "".join([
                f'<div style="display:flex;gap:6px;margin-bottom:6px;">'
                f'<span style="color:{plan["color"]};font-size:12px;">✓</span>'
                f'<span style="font-size:12px;color:#4A6080;">{f}</span></div>'
                for f in plan["features"]])
            price_display = (f'<span style="font-size:2rem;">{plan["price"]}</span>'
                             f'<span style="font-size:0.9rem;color:#4A6080;">{plan["period"]}</span>')

            st.markdown(
                f'<div style="background:white;border:1px solid #E2E8F0;border-radius:14px;padding:20px;'
                f'text-align:center;border-top:4px solid {plan["color"]};height:100%;">'
                f'<div style="display:inline-block;padding:3px 12px;border-radius:99px;background:{plan["bg"]};'
                f'color:{plan["color"]};font-size:0.72rem;font-weight:700;letter-spacing:1px;'
                f'text-transform:uppercase;margin-bottom:12px;">{plan["name"]}</div>'
                f'<div style="font-family:Syne,sans-serif;font-weight:800;color:#0B2545;margin-bottom:16px;">'
                f'{price_display}</div>'
                f'<div style="text-align:left;margin-bottom:16px;">{feats_html}</div></div>',
                unsafe_allow_html=True)

            if plan["disabled"]:
                st.button(plan["cta"], use_container_width=True, disabled=True, key=f"plan_{i}")
            else:
                if st.button(plan["cta"], use_container_width=True, type="primary", key=f"plan_{i}"):
                    st.session_state.page = "checkout"
                    st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    _c1, cc, _c2 = st.columns([1, 2, 1])
    if cc.button("Retour" if lang == "fr" else "Back", use_container_width=True, key="back_plans"):
        st.session_state.page = "login"
        st.rerun()


def render_checkout():
    lang = st.session_state.get("language", "fr")
    st.markdown(
        '<div style="text-align:center;margin-bottom:8px;">'
        '<span style="font-family:Syne,sans-serif;font-weight:800;font-size:2rem;color:#0B2545;">'
        'LOGI<span style="color:#00C896;">FLO</span>.IO</span></div>'
        f'<h2 style="text-align:center;color:#0B2545;font-family:Syne,sans-serif;">Souscription Pro</h2>',
        unsafe_allow_html=True)

    _c1, fc, _c2 = st.columns([1, 1.8, 1])
    with fc:
        with st.form("checkout_form"):
            st.text_input("Nom complet" if lang == "fr" else "Full name")
            st.text_input("Email professionnel" if lang == "fr" else "Professional email")
            st.text_input("Entreprise" if lang == "fr" else "Company")
            st.markdown(
                f'<div style="background:#F0F4F8;border-radius:10px;padding:14px;margin:12px 0;">'
                f'<div style="font-size:12px;color:#0B2545;">💳 '
                f'{"Paiement securise — Stripe a venir" if lang == "fr" else "Secure payment — Stripe coming soon"}'
                f'</div></div>', unsafe_allow_html=True)
            if st.form_submit_button("590 EUR/mois — Souscrire" if lang == "fr" else "590 EUR/month — Subscribe",
                                      use_container_width=True, type="primary"):
                st.success("Demande recue ! Nous vous contactons sous 24h." if lang == "fr"
                           else "Request received! We'll contact you within 24h.")
        if st.button("Retour" if lang == "fr" else "Back", use_container_width=True, key="back_checkout"):
            st.session_state.page = "plans"
            st.rerun()
