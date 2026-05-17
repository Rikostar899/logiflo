import streamlit as st
import pandas as pd
import base64
from config.translations import _
from services.supabase_client import load_archives_from_sheets
from components.helpers import render_report


def render_archives():
    lang = st.session_state.get("language", "fr")
    view = st.session_state.get("stock_view", "MANAGER")
    st.title(_("arch_title"))

    with st.spinner("Chargement..." if lang == "fr" else "Loading..."):
        dfa = load_archives_from_sheets(st.session_state.current_user)

    if dfa is None or dfa.empty:
        st.info(_("arch_empty")); return

    if "created_at" in dfa.columns and "date" not in dfa.columns:
        dfa["date"] = pd.to_datetime(dfa["created_at"], errors="coerce").dt.strftime("%d/%m/%Y")
        dfa["heure"] = pd.to_datetime(dfa["created_at"], errors="coerce").dt.strftime("%H:%M")
    for c in ["module", "date", "heure", "kpi_1", "kpi_2", "kpi_3",
              "kpi_label_1", "kpi_label_2", "kpi_label_3", "resume_ia", "profil"]:
        if c not in dfa.columns: dfa[c] = ""
    for c in ["kpi_1", "kpi_2", "kpi_3"]:
        dfa[c] = pd.to_numeric(dfa[c], errors="coerce").fillna(0)

    # Filtrer par profil
    if view == "TERRAIN":
        dfa_view = dfa[dfa["profil"].astype(str).str.lower() == "terrain"].copy()
        if dfa_view.empty: dfa_view = dfa.copy()
    else:
        dfa_view = dfa[dfa["profil"].astype(str).str.lower() != "terrain"].copy()
        if dfa_view.empty: dfa_view = dfa.copy()

    mf = st.selectbox(_("arch_filter"), [_("arch_filter_all"), "stock"])
    ds = dfa_view.copy()
    if mf != _("arch_filter_all"):
        ds = ds[ds["module"].astype(str) == mf]
    ds = ds.iloc[::-1].head(50)
    st.markdown(f"**{len(ds)} {_('arch_show')}**")

    for i, row in ds.iterrows():
        date_a = str(row.get("date", "") or str(row.get("created_at", ""))[:10])
        h_a = str(row.get("heure", "") or str(row.get("created_at", ""))[11:16])
        k2 = float(row.get("kpi_2", 0))
        clr = "#00C896" if k2 >= 90 else ("#F39C12" if k2 >= 75 else "#E8304A")

        st.markdown(
            f'<div class="archive-card">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">'
            f'<h4 style="margin:0;">📦 STOCK</h4>'
            f'<span style="font-size:13px;color:#0B2545;font-weight:700;">{date_a} — {h_a}</span></div>'
            f'<span style="display:inline-block;background:#F0F4F8;border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;margin-right:8px;">{row.get("kpi_label_1","")}: {float(row.get("kpi_1",0)):.0f}</span>'
            f'<span style="display:inline-block;background:#F0F4F8;border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;color:{clr};margin-right:8px;">{row.get("kpi_label_2","")}: {k2:.1f}%</span>'
            f'<span style="display:inline-block;background:#F0F4F8;border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;">{row.get("kpi_label_3","")}: {float(row.get("kpi_3",0)):.0f}</span></div>',
            unsafe_allow_html=True)

        with st.expander(_("arch_resume")):
            res = row.get("resume_ia", "")
            if res:
                st.markdown(render_report(str(res), "terrain" if view == "TERRAIN" else "manager"), unsafe_allow_html=True)
            else:
                st.info("N/A")

        if view != "TERRAIN":
            pdf_b = row.get("pdf_base64", "")
            if pdf_b:
                try:
                    st.download_button(_("arch_dl"), base64.b64decode(str(pdf_b)),
                                       f"Logiflo_{date_a.replace('/', '_')}_stock.pdf",
                                       key=f"dl_arch_{i}", use_container_width=True)
                except Exception:
                    pass
