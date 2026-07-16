# -*- coding: utf-8 -*-
"""
Logiflo - views/onboarding.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Onboarding multi-pages (une question par ecran).
Collecte : secteur, tranche de CA, effectif, localisation.
Ecrit une seule fois dans la table 'organizations' au recap final.
Version 2.0 (juillet 2026) — refonte multi-etapes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st
from services.supabase_client import save_organization

# ── LISTES DE REPONSES ───────────────────────────────────────────────────
# (icone, libelle FR, libelle EN, valeur stockee)
SECTORS = [
    ("📦", "Distribution / Negoce", "Distribution / Wholesale", "stock_distribution"),
    ("🛒", "Retail / Commerce de detail", "Retail", "stock_retail"),
    ("💊", "Pharma / Sante", "Pharma / Health", "stock_pharma"),
    ("🍎", "Agroalimentaire", "Food & Beverage", "stock_agroalim"),
    ("🏭", "Industrie / Production", "Industry / Manufacturing", "stock_industrie"),
    ("💻", "E-commerce", "E-commerce", "stock_retail"),
    ("🔧", "Autre", "Other", "generique"),
]

REVENUE_BRACKETS = [
    ("Moins de 500K€", "Under 500K€", "<500K"),
    ("500K€ – 2M€", "500K€ – 2M€", "500K-2M"),
    ("2M€ – 10M€", "2M€ – 10M€", "2M-10M"),
    ("Plus de 10M€", "Over 10M€", ">10M"),
    ("Je prefere ne pas dire", "Prefer not to say", "undisclosed"),
]

# (libelle, valeur stockee = borne haute indicative pour tri)
EMPLOYEE_RANGES = [
    ("1 – 9 (TPE)", "1-9", 9),
    ("10 – 49 (PME)", "10-49", 49),
    ("50 – 249 (PME)", "50-249", 249),
    ("250 et plus (ETI+)", "250+", 250),
]

METROPOLES = [
    "Paris", "Marseille", "Lyon", "Toulouse", "Bordeaux",
    "Lille", "Nantes", "Nice", "Strasbourg", "Montpellier",
    "Rennes", "Grenoble",
]

TOTAL_STEPS = 4


# ── HELPERS UI ───────────────────────────────────────────────────────────
def _header():
    st.markdown(
        '<div style="text-align:center;margin-bottom:8px;">'
        '<span style="font-family:Syne,sans-serif;font-weight:800;font-size:2rem;color:#0B2545;">'
        'LOGI<span style="color:#00C896;">FLO</span>.IO</span></div>',
        unsafe_allow_html=True,
    )


def _progress(step, lang):
    """Barre de progression 'Etape X sur 4'."""
    pct = int(step / TOTAL_STEPS * 100)
    label = f"{'Step' if lang == 'en' else 'Etape'} {step} / {TOTAL_STEPS}"
    st.markdown(
        f'<div style="max-width:520px;margin:0 auto 4px auto;font-family:Syne,sans-serif;'
        f'font-size:0.8rem;color:#4A6080;font-weight:600;">{label}</div>'
        f'<div style="max-width:520px;margin:0 auto 24px auto;background:#E2E8F0;'
        f'border-radius:99px;height:8px;overflow:hidden;">'
        f'<div style="width:{pct}%;height:100%;background:#00C896;'
        f'border-radius:99px;transition:width .3s;"></div></div>',
        unsafe_allow_html=True,
    )


def _question_title(text):
    st.markdown(
        f'<h2 style="text-align:center;color:#0B2545;font-family:Syne,sans-serif;'
        f'font-size:1.6rem;margin-bottom:28px;">{text}</h2>',
        unsafe_allow_html=True,
    )


def _goto(step):
    st.session_state.onb_step = step
    st.rerun()


# ── ECRANS ───────────────────────────────────────────────────────────────
def _screen_sector(lang):
    _progress(1, lang)
    _question_title("Quel est votre secteur d'activite ?" if lang == "fr"
                    else "What is your sector?")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        for icon, fr, en, val in SECTORS:
            label = f"{icon}  {fr if lang == 'fr' else en}"
            if st.button(label, use_container_width=True, key=f"sec_{val}_{fr}"):
                st.session_state.onb_sector = val
                st.session_state.onb_sector_label = fr if lang == "fr" else en
                _goto(2)


def _screen_revenue(lang):
    _progress(2, lang)
    _question_title("Quel est votre chiffre d'affaires annuel ?" if lang == "fr"
                    else "What is your annual revenue?")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        for fr, en, val in REVENUE_BRACKETS:
            if st.button(fr if lang == "fr" else en,
                         use_container_width=True, key=f"rev_{val}"):
                st.session_state.onb_revenue = val
                st.session_state.onb_revenue_label = fr if lang == "fr" else en
                _goto(3)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("← " + ("Retour" if lang == "fr" else "Back"),
                     key="rev_back", use_container_width=True):
            _goto(1)


def _screen_employees(lang):
    _progress(3, lang)
    _question_title("Combien de collaborateurs ?" if lang == "fr"
                    else "How many employees?")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        for label, val, sort_val in EMPLOYEE_RANGES:
            if st.button(label, use_container_width=True, key=f"emp_{val}"):
                st.session_state.onb_employees = val
                st.session_state.onb_employees_sort = sort_val
                _goto(4)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("← " + ("Retour" if lang == "fr" else "Back"),
                     key="emp_back", use_container_width=True):
            _goto(2)


def _screen_location(lang):
    _progress(4, lang)
    _question_title("Ou etes-vous situe ?" if lang == "fr"
                    else "Where are you located?")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        st.markdown(
            f'<div style="font-family:Syne,sans-serif;font-weight:600;font-size:0.9rem;'
            f'color:#4A6080;margin-bottom:10px;">'
            f'{"Select a city or type another" if lang == "en" else "Choisissez une ville ou saisissez la votre"}</div>',
            unsafe_allow_html=True,
        )
        options = METROPOLES + [("Autre..." if lang == "fr" else "Other...")]
        choice = st.selectbox(
            "Ville" if lang == "fr" else "City",
            options, label_visibility="collapsed", key="loc_select",
        )
        custom = ""
        if choice == ("Autre..." if lang == "fr" else "Other..."):
            custom = st.text_input(
                "Votre ville / region" if lang == "fr" else "Your city / region",
                key="loc_custom", placeholder="ex : Avignon, Bruxelles...",
            )
        final_loc = custom.strip() if custom.strip() else (
            choice if choice not in ("Autre...", "Other...") else "")

        st.markdown("<br>", unsafe_allow_html=True)
        col_back, col_next = st.columns(2)
        with col_back:
            if st.button("← " + ("Retour" if lang == "fr" else "Back"),
                         key="loc_back", use_container_width=True):
                _goto(3)
        with col_next:
            if st.button(("Continuer →" if lang == "fr" else "Continue →"),
                         key="loc_next", use_container_width=True,
                         type="primary", disabled=not final_loc):
                st.session_state.onb_location = final_loc
                _goto(5)


def _screen_recap(lang):
    """Recapitulatif + consentement RGPD + ecriture Supabase."""
    _header()
    _question_title("Recapitulatif" if lang == "fr" else "Summary")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        rows = [
            ("Secteur" if lang == "fr" else "Sector",
             st.session_state.get("onb_sector_label", "—")),
            ("Chiffre d'affaires" if lang == "fr" else "Revenue",
             st.session_state.get("onb_revenue_label", "—")),
            ("Collaborateurs" if lang == "fr" else "Employees",
             st.session_state.get("onb_employees", "—")),
            ("Localisation" if lang == "fr" else "Location",
             st.session_state.get("onb_location", "—")),
        ]
        recap_html = '<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;padding:20px;margin-bottom:16px;">'
        for k, v in rows:
            recap_html += (
                f'<div style="display:flex;justify-content:space-between;'
                f'padding:8px 0;border-bottom:1px solid #F1F5F9;">'
                f'<span style="color:#4A6080;font-size:0.9rem;">{k}</span>'
                f'<span style="color:#0B2545;font-weight:700;font-size:0.9rem;">{v}</span></div>'
            )
        recap_html += "</div>"
        st.markdown(recap_html, unsafe_allow_html=True)

        # Consentement RGPD (repris de ton onboarding actuel)
        if lang == "en":
            consent_text = """**What we do with your data:**
- Files are processed in memory only — never stored on disk
- A summary is sent to AI (OpenAI/Gemini, GDPR-compliant) for analysis
- Audit results (KPIs, summary, PDF) are stored in our EU database (Paris)
- Your data is never sold or shared with third parties
- Right to access, rectify, delete at any time: contact@logiflo.io"""
            check_label = "I accept the processing of my data as described above"
            btn_label = "CONFIRM AND START"
        else:
            consent_text = """**Ce que nous faisons de vos donnees :**
- Les fichiers sont traites en memoire uniquement — jamais stockes sur disque
- Un resume est envoye a l'IA (OpenAI/Gemini, conforme RGPD) pour l'analyse
- Les resultats (KPIs, resume, PDF) sont stockes dans notre base UE (Paris)
- Vos donnees ne sont jamais vendues ni partagees avec des tiers
- Droit d'acces, rectification, suppression a tout moment : contact@logiflo.io"""
            check_label = "J'accepte le traitement de mes donnees tel que decrit ci-dessus"
            btn_label = "CONFIRMER ET DEMARRER"

        st.markdown(
            f'<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;'
            f'padding:20px;margin-bottom:16px;font-size:0.85rem;color:#4A6080;line-height:1.7;">'
            f'{consent_text.replace(chr(10), "<br>")}</div>',
            unsafe_allow_html=True,
        )
        accept = st.checkbox(check_label, key="onb_accept")

        st.markdown("<br>", unsafe_allow_html=True)
        col_back, col_go = st.columns(2)
        with col_back:
            if st.button("← " + ("Retour" if lang == "fr" else "Back"),
                         key="recap_back", use_container_width=True):
                _goto(4)
        with col_go:
            if st.button(btn_label, use_container_width=True, type="primary",
                         disabled=not accept, key="recap_go"):
                user = st.session_state.get("current_user", "")
                ok = save_organization(
                    user,
                    sector_key=st.session_state.get("onb_sector"),
                    revenue_bracket=st.session_state.get("onb_revenue"),
                    employee_count=st.session_state.get("onb_employees_sort"),
                    location=st.session_state.get("onb_location"),
                )
                # On bascule vers l'app meme si l'ecriture echoue (mode degrade),
                # mais on garde le secteur en session pour la suite immediate.
                st.session_state.rgpd_ok = True
                st.session_state._onboarding_done = True
                st.session_state["_user_sector"] = st.session_state.get("onb_sector")
                st.session_state.page = "app"
                st.rerun()


# ── POINT D'ENTREE ───────────────────────────────────────────────────────
def render_onboarding():
    """Routeur interne de l'onboarding multi-pages."""
    lang = st.session_state.get("language", "fr")
    step = st.session_state.get("onb_step", 1)

    # Header affiche sur toutes les etapes sauf recap (qui a le sien)
    if step <= TOTAL_STEPS:
        _header()

    if step == 1:
        _screen_sector(lang)
    elif step == 2:
        _screen_revenue(lang)
    elif step == 3:
        _screen_employees(lang)
    elif step == 4:
        _screen_location(lang)
    else:
        _screen_recap(lang)
