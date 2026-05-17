import streamlit as st
import pandas as pd
from engine.ingester import smart_ingester_stock_ultime
from engine.ai_analysis import generate_ai_analysis
from engine.pdf_gen import generate_free_pdf
from components.helpers import render_report

try:
    from openai import OpenAI
    import os
    _k = os.environ.get("OPENAI_API_KEY", "") or st.secrets.get("OPENAI_API_KEY", "")
    client = OpenAI(api_key=_k) if _k else None
except Exception:
    client = None

SECTORS_FREE = {
    "Industrie / Manufacturing": "stock_industrie",
    "Distribution / Negoce": "stock_distribution",
    "Textile / Mode": "stock_retail",
    "Pharma / Sante": "stock_pharma",
    "Agroalimentaire": "stock_agroalim",
    "BTP / Construction": "stock_btp",
    "Retail / E-commerce": "stock_retail",
    "Autre": "generique",
}


def render_audit_gratuit():
    lang = st.session_state.get("language", "fr")

    # Header + secteur en haut a droite
    h1, h2 = st.columns([3, 1])
    with h1:
        st.markdown(
            '<span style="font-family:Syne,sans-serif;font-weight:800;font-size:1.8rem;color:#0B2545;">'
            'LOGI<span style="color:#00C896;">FLO</span>.IO</span>'
            f'<span style="font-size:0.9rem;color:#4A6080;margin-left:12px;">'
            f'{"Free Audit" if lang == "en" else "Audit Gratuit"}</span>',
            unsafe_allow_html=True)
    with h2:
        sector_label = st.selectbox(
            "Secteur" if lang == "fr" else "Sector",
            list(SECTORS_FREE.keys()), key="free_sector",
            label_visibility="collapsed")
        sector_key = SECTORS_FREE[sector_label]

    st.markdown("---")

    # Deja fait ?
    if st.session_state.get("audit_gratuit_done"):
        st.markdown(
            f'<div style="background:white;border:1px solid #E2E8F0;border-radius:14px;padding:24px;text-align:center;">'
            f'<div style="font-size:2.5rem;margin-bottom:12px;">✅</div>'
            f'<div style="font-family:Syne,sans-serif;font-weight:700;font-size:1.1rem;color:#0B2545;">'
            f'{"Audit completed" if lang == "en" else "Audit termine"}</div>'
            f'<p style="color:#4A6080;font-size:0.85rem;margin-top:8px;">'
            f'{"Subscribe to unlock full analysis." if lang == "en" else "Souscrivez pour debloquer l analyse complete."}</p></div>',
            unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Voir les offres" if lang == "fr" else "See plans", use_container_width=True, key="free_plans"):
            st.session_state.page = "plans"
            st.rerun()
        return

    # Upload
    up = st.file_uploader(
        "Deposez votre fichier Excel ou CSV" if lang == "fr" else "Drop your Excel or CSV file",
        type=["csv", "xlsx"], key="free_upload")

    if not up:
        # CTA en bas
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#0B2545,#0f2f5a);border-radius:14px;padding:24px;text-align:center;">'
            f'<div style="font-family:Syne,sans-serif;font-weight:800;font-size:1.2rem;color:white;margin-bottom:8px;">'
            f'{"Unlock the full audit" if lang == "en" else "Debloquez l audit complet"}</div>'
            f'<p style="color:#A8C8E8;font-size:0.85rem;">Pro : 590 EUR/{"month" if lang == "en" else "mois"} '
            f'{"or" if lang == "en" else "ou"} 790 EUR {"one-time" if lang == "en" else "ponctuel"}</p></div>',
            unsafe_allow_html=True)
        return

    # Lire le fichier
    try:
        df = pd.read_excel(up) if up.name.endswith("xlsx") else pd.read_csv(up, encoding="utf-8")
    except Exception:
        up.seek(0)
        df = pd.read_csv(up, encoding="latin-1")

    # Ingester
    df_ok, statut = smart_ingester_stock_ultime(df, client_ai=client)
    if df_ok is None:
        st.error(statut)
        return

    sans_prix = bool(df_ok.get("_sans_prix", pd.Series([True])).iloc[0]) if "_sans_prix" in df_ok.columns else True
    df_ok["valeur_totale"] = df_ok["quantite"] * df_ok["prix_unitaire"]
    vt = df_ok["valeur_totale"].sum()
    rf = df_ok[df_ok["quantite"] <= 0]
    tx = (1 - len(rf) / max(len(df_ok), 1)) * 100

    # KPI Cards
    st.markdown("<br>", unsafe_allow_html=True)
    a1, a2, a3 = st.columns(3)
    kpi1 = f"{vt:,.0f} EUR" if not sans_prix else str(len(df_ok))
    a1.markdown(f"<div class='kpi-card'><h4>{'Capital' if not sans_prix else 'Articles'}</h4><h2 style='color:#0B2545;'>{kpi1}</h2></div>", unsafe_allow_html=True)
    a2.markdown(f"<div class='kpi-card'><h4>Service</h4><h2 style='color:#00C896;'>{tx:.1f}%</h2></div>", unsafe_allow_html=True)
    a3.markdown(f"<div class='kpi-card'><h4>Ruptures</h4><h2 style='color:#E8304A;'>{len(rf)}</h2></div>", unsafe_allow_html=True)

    # Analyse IA TRONQUÉE (pas de predictions, pas de top 5 actions)
    with st.spinner("Analyse IA..." if lang == "fr" else "AI Analysis..."):
        summary = generate_ai_analysis(
            f"Items:{len(df_ok)}. Service:{tx:.1f}%. Stockouts:{len(rf)}.",
            df_raw=df_ok, sector_key=sector_key)

    # Tronquer : garder seulement les 2 premières sections (diagnostic opérationnel + financier)
    lines = summary.strip().split('\n')
    sections_found = 0
    cut_idx = len(lines)
    for i, line in enumerate(lines):
        if line.strip().startswith('### '):
            sections_found += 1
            if sections_found > 2:
                cut_idx = i
                break
    truncated = '\n'.join(lines[:cut_idx])

    st.markdown(render_report(truncated, "manager"), unsafe_allow_html=True)

    # Masque flou sur le reste
    remaining = lines[cut_idx:cut_idx + 6]
    if remaining:
        st.markdown(
            f'<div style="position:relative;overflow:hidden;max-height:100px;">'
            f'<div style="filter:blur(5px);opacity:0.4;font-size:13px;color:#4A6080;line-height:1.8;padding:12px;">'
            f'{"... ".join(remaining)}</div>'
            f'<div style="position:absolute;bottom:0;left:0;right:0;height:100%;'
            f'background:linear-gradient(transparent,#F7F9FB 70%);"></div></div>',
            unsafe_allow_html=True)

    st.session_state.audit_gratuit_done = True

    # CTA
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        f'<div style="background:linear-gradient(135deg,#0B2545,#0f2f5a);border-radius:14px;padding:24px;text-align:center;">'
        f'<div style="font-family:Syne,sans-serif;font-weight:800;font-size:1.2rem;color:white;margin-bottom:8px;">'
        f'{"Unlock predictions, actions, PDF" if lang == "en" else "Debloquez predictions, actions, PDF"}</div>'
        f'<p style="color:#00C896;font-weight:700;">Pro : 590 EUR/{"month" if lang == "en" else "mois"}</p></div>',
        unsafe_allow_html=True)

    if st.button("Choisir mon plan" if lang == "fr" else "Choose my plan",
                 use_container_width=True, type="primary", key="free_to_plans"):
        st.session_state.page = "plans"
        st.rerun()
