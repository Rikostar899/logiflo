import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import datetime

from config.translations import _
from config.plans import can_access, show_lock
from config.sectoral_db import detect_sector
from engine.ingester import smart_ingester_stock_ultime
from engine.ai_analysis import generate_ai_analysis, format_historique_pour_prompt
from engine.scoring import compute_logiflo_score
from engine.pdf_gen import (generate_expert_pdf, render_prediction_rupture,
                             predict_ruptures, compute_alerte_bfr)
from services.supabase_client import save_audit_to_sheets, get_historique_audits
from components.helpers import render_report, StepProgress

# Client OpenAI (peut etre None)
try:
    from openai import OpenAI
    import os
    _oai_key = os.environ.get("OPENAI_API_KEY", "") or st.secrets.get("OPENAI_API_KEY", "")
    client = OpenAI(api_key=_oai_key) if _oai_key else None
except Exception:
    client = None


def render_workspace():
    lang = st.session_state.get("language", "fr")
    view = st.session_state.get("stock_view", "MANAGER")

    st.title(_("stock_title"))

    # Badge profil + bouton changer
    ci, cb = st.columns([4, 1])
    ci.markdown(f"**{_('active_profile')} : {view}**")
    if cb.button(_("change_profile"), key="ws_change"):
        st.session_state.page = "profil"
        st.rerun()

    # Zone upload
    st.markdown(f"<div class='import-card'><h3>{_('stock_import')}</h3><p>{_('stock_import_sub')}</p></div>", unsafe_allow_html=True)
    up = st.file_uploader("", type=["csv", "xlsx"], key="stock_upload")
    st.markdown("---")

    # Ingestion du fichier
    if up:
        pg = StepProgress([_("step_read"), _("step_detect"), _("step_calc")])
        pg.step(_("step_read"))
        try:
            df_brut = pd.read_excel(up) if up.name.endswith("xlsx") else pd.read_csv(up, encoding="utf-8")
        except UnicodeDecodeError:
            up.seek(0)
            df_brut = pd.read_csv(up, encoding="latin-1")
        except Exception as e:
            st.error(f"Erreur lecture fichier : {e}" if lang == "fr" else f"File read error: {e}")
            pg.done()
            return

        # Garde-fou : fichier trop gros → échantillonnage
        MAX_ROWS = 50000
        if len(df_brut) > MAX_ROWS:
            st.warning(
                f"Fichier volumineux ({len(df_brut):,} lignes). Echantillonnage automatique a {MAX_ROWS:,} lignes pour eviter les problemes de memoire."
                if lang == "fr" else
                f"Large file ({len(df_brut):,} rows). Auto-sampling to {MAX_ROWS:,} rows to prevent memory issues.")
            df_brut = df_brut.sample(n=MAX_ROWS, random_state=42).reset_index(drop=True)

        # Garde-fou : détection fichier de transactions (pas un inventaire)
        _first_col = df_brut.columns[0] if len(df_brut.columns) > 0 else ""
        _first_low = str(_first_col).lower().replace(" ", "").replace("_", "")
        _transaction_kw = ("transaction", "order", "commande", "facture", "invoice", "ticket")
        _has_date_col = any("date" in str(c).lower() for c in df_brut.columns)
        if any(kw in _first_low for kw in _transaction_kw) and _has_date_col:
            st.warning(
                "Ce fichier ressemble a un journal de ventes/transactions, pas a un inventaire stock. "
                "Logiflo attend 1 ligne par article (reference, stock, prix). "
                "Un fichier de transactions necessite d'etre agrege par produit avant l'analyse."
                if lang == "fr" else
                "This file looks like a sales/transaction log, not a stock inventory. "
                "Logiflo expects 1 row per item (reference, stock, price). "
                "A transaction file needs to be aggregated by product before analysis.")

        pg.step(_("step_detect"))
        df_propre, statut = smart_ingester_stock_ultime(df_brut, client_ai=client)
        pg.step(_("step_calc"))
        pg.done()
        if df_propre is None:
            st.error(statut)
        else:
            _df_key = "df_stock_manager" if view == "MANAGER" else "df_stock_terrain"
            st.session_state[_df_key] = df_propre

    # Affichage si un df est chargé
    _df_key = "df_stock_manager" if view == "MANAGER" else "df_stock_terrain"
    if st.session_state.get(_df_key) is None:
        return

    df = st.session_state[_df_key].copy()
    sans_prix = bool(df.get("_sans_prix", pd.Series([True])).iloc[0]) if "_sans_prix" in df.columns else True
    has_conso = bool(df.get("_has_conso", pd.Series([False])).iloc[0]) if "_has_conso" in df.columns else False

    # Badges
    if sans_prix:
        st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_no_price')}</span>", unsafe_allow_html=True)
    if has_conso:
        st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_conso')}</span>", unsafe_allow_html=True)
    else:
        st.markdown(f"<span class='sans-prix-badge'>{_('stock_badge_no_conso')}</span>", unsafe_allow_html=True)

    # Calculs KPI
    has_peremption = bool(df.get("_has_peremption", pd.Series([False])).iloc[0]) if "_has_peremption" in df.columns else False

    # ── Risque de peremption (independant de has_conso, calcule en amont) ──
    if has_peremption and "date_peremption" in df.columns:
        _today = pd.Timestamp.now().normalize()
        df["_jours_avant_peremption"] = (df["date_peremption"] - _today).dt.days
        df["_perime_critique"] = df["_jours_avant_peremption"].notna() & (df["_jours_avant_peremption"] <= 7) & (df["quantite"] > 0)
        df["_perime_alerte"] = df["_jours_avant_peremption"].notna() & (df["_jours_avant_peremption"] > 7) & (df["_jours_avant_peremption"] <= 30) & (df["quantite"] > 0)
    else:
        df["_perime_critique"] = False
        df["_perime_alerte"] = False

    if has_conso:
        df["_conso_moy"] = df["_conso_moy"].fillna(0)
        # Couverture en mois (pour surstock)
        df["Couverture_mois"] = np.where(df["_conso_moy"] > 0, df["quantite"] / df["_conso_moy"], 9999)
        # Couverture en semaines (pour rupture imminente)
        df["_conso_hebdo"] = df["_conso_moy"] / 52
        df["_couv_semaines"] = np.where(df["_conso_hebdo"] > 0, df["quantite"] / df["_conso_hebdo"], 9999)

        df["Statut"] = np.select(
            [(df["quantite"] <= st.session_state.get("seuil_rupture", 0)),
             (df["_perime_critique"]),
             (df["quantite"] > 0) & (df["_conso_hebdo"] > 0) & (df["_couv_semaines"] <= 1),
             (df["_perime_alerte"]),
             (df["quantite"] > 0) & (df["_conso_moy"] == 0),
             (df["quantite"] > 0) & (df["Couverture_mois"] > 6)],
            ["🔴 Rupture", "🟡 Péremption Critique", "🔴 Rupture Imminente", "🟡 Péremption Proche",
             "🔴 Dormant", "🟠 Surstock"], default="🟢 OK")
    else:
        df["Statut"] = np.select(
            [(df["quantite"] <= st.session_state.get("seuil_rupture", 0)),
             (df["_perime_critique"]),
             (df["_perime_alerte"])],
            ["🔴 Rupture", "🟡 Péremption Critique", "🟡 Péremption Proche"], default="🟢 OK")

    df["valeur_totale"] = df["quantite"] * df["prix_unitaire"]
    val_totale = df["valeur_totale"].sum()
    # Ruptures = stock zero + imminentes (< 1 semaine de couverture)
    ruptures = df[df["Statut"].str.contains("Rupture", na=False)]
    tx_serv = (1 - len(ruptures) / max(len(df), 1)) * 100

    # ══ VUE MANAGER ══
    if view == "MANAGER":
        _render_manager(df, val_totale, tx_serv, ruptures, sans_prix, has_conso, lang)
    # ══ VUE TERRAIN ══
    else:
        _render_terrain(df, tx_serv, ruptures, has_conso, lang)


def _render_manager(df, val_totale, tx_serv, ruptures, sans_prix, has_conso, lang):
    # KPI Cards
    _perim_count = int((df["_perime_critique"] | df["_perime_alerte"]).sum()) if "_perime_critique" in df.columns else 0
    if _perim_count > 0:
        c1, c2, c3, c4 = st.columns(4)
    else:
        c1, c2, c3 = st.columns(3)
    kpi1_label = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
    kpi1_val = f"{val_totale:,.0f} EUR" if not sans_prix else str(len(df))
    c1.markdown(f"<div class='kpi-card'><h4>{kpi1_label}</h4><h2 style='color:#0B2545;'>{kpi1_val}</h2></div>", unsafe_allow_html=True)
    c2.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_service')}</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>", unsafe_allow_html=True)
    c3.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_rupture')}</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>", unsafe_allow_html=True)
    if _perim_count > 0:
        _lbl_perim = "Expiring soon" if lang == "en" else "Péremption proche"
        c4.markdown(f"<div class='kpi-card'><h4>{_lbl_perim}</h4><h2 style='color:#E8A800;'>{_perim_count}</h2></div>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # Graphiques (dans le dashboard, pas dans le PDF)
    cp, cl2 = st.columns(2)
    cmap = {"🔴 Rupture": "#E8304A", "🔴 Rupture Imminente": "#FF6B6B",
            "🟡 Péremption Critique": "#E8A800", "🟡 Péremption Proche": "#F4C542",
            "🟢 OK": "#00C896", "🔴 Dormant": "#c0392b", "🟠 Surstock": "#f39c12"}
    with cp:
        fig_pie = px.pie(df, names="Statut", hole=0.4, color="Statut", color_discrete_map=cmap)
        fig_pie.update_layout(paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_pie, use_container_width=True, key="pie_stock_ws")
    with cl2:
        if has_conso and "_conso_moy" in df.columns:
            top15 = df.nlargest(15, "_conso_moy")[["reference", "_conso_moy", "quantite"]].copy()
            fig_bar = px.bar(top15, x="reference", y=["quantite", "_conso_moy"], barmode="group",
                             color_discrete_map={"quantite": "#0B2545", "_conso_moy": "#00C896"})
            fig_bar.update_layout(paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_bar, use_container_width=True, key="bar_conso_ws")
        else:
            st.info("Graphique de consommation disponible avec historique." if lang == "fr"
                    else "Consumption chart available with history.")

    # Boutons audit + save
    col_a, col_s = st.columns([3, 1])
    with col_a:
        run_ia = st.button(_("stock_btn_ia"), use_container_width=True, key="ws_run_ia")
    with col_s:
        if st.button(_("stock_btn_save"), use_container_width=True, key="ws_save"):
            kpi1 = val_totale if not sans_prix else float(len(df))
            label1 = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
            ok = save_audit_to_sheets(
                st.session_state.current_user, "stock", len(df),
                [kpi1, tx_serv, len(ruptures)],
                [label1, _("stock_kpi_service"), _("stock_kpi_rupture")],
                st.session_state.get("analysis_stock_manager", ""),
                st.session_state.get("last_pdf", b""),
            )
            st.success(_("stock_saved") if ok else _("stock_save_err"))

    # Prédictions rupture
    if can_access("prediction"):
        render_prediction_rupture(df, lang=lang)
    else:
        show_lock("prediction")

    # Génération IA
    if run_ia:
        pg2 = StepProgress([_("step_read"), _("step_ia"), _("step_report")])
        pg2.step(_("step_read"))
        kpi1 = val_totale if not sans_prix else float(len(df))
        label1 = _("stock_kpi_capital") if not sans_prix else _("stock_kpi_articles")
        _kpis = [kpi1, tx_serv, float(len(ruptures))]
        _labels = [label1, _("stock_kpi_service"), _("stock_kpi_rupture")]
        st.session_state.last_kpis = _kpis
        st.session_state.last_labels = _labels

        # Data summary
        df_tox = df[df["Statut"].isin(["🔴 Dormant", "🟠 Surstock"])]
        pires = df_tox.nlargest(3, "quantite") if not df_tox.empty else df.nlargest(3, "quantite")
        top_str = ", ".join([f"{r['reference']} (qty:{r['quantite']:.0f})" for _, r in pires.iterrows()])
        med = "" if not has_conso else f" Avg conso: {df['_conso_moy'].mean():.1f}."
        prix = "" if sans_prix else f" Capital: {val_totale:.0f} EUR."

        pg2.step(_("step_ia"))
        _sector = detect_sector(df=df, module="stock")
        st.session_state["_last_sector_key"] = _sector
        _hist = get_historique_audits(st.session_state.current_user, "stock",
                                      current_kpis=_kpis, current_labels=_labels)
        _hist_txt = format_historique_pour_prompt(_hist, "stock", lang)

        st.session_state.analysis_stock_manager = generate_ai_analysis(
            f"Items:{len(df)}. Service:{tx_serv:.1f}%. Stockouts:{len(ruptures)}. Top:{top_str}.{prix}{med}",
            historique_txt=_hist_txt, df_raw=df, sector_key=_sector)

        st.session_state.last_pdf = generate_expert_pdf(
            _("pdf_title_stock"), st.session_state.analysis_stock_manager,
            figs=None, kpis=_kpis, labels=_labels, module="stock")
        pg2.done()

    # Affichage résultat
    if st.session_state.get("analysis_stock_manager"):
        st.markdown(render_report(st.session_state.analysis_stock_manager, "manager"), unsafe_allow_html=True)
        _render_scoring(df, lang)
        if st.session_state.get("last_pdf") and len(st.session_state.last_pdf) > 100:
            st.download_button(_("stock_btn_dl"), data=st.session_state.last_pdf,
                               file_name="Audit_Stock_Logiflo.pdf", mime="application/pdf",
                               use_container_width=True, key="dl_stock_ws")


def _render_terrain(df, tx_serv, ruptures, has_conso, lang):
    # KPI Cards Terrain (2 cards)
    c1, c2 = st.columns(2)
    c1.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_rupture')}</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>", unsafe_allow_html=True)
    c2.markdown(f"<div class='kpi-card'><h4>{_('stock_kpi_service')}</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>", unsafe_allow_html=True)

    # Liste urgences
    st.markdown(f"### {_('stock_urgent')}")
    if len(ruptures) > 0:
        cols_show = ["reference", "quantite", "Statut"]
        if has_conso and "_conso_moy" in df.columns:
            cols_show.append("_conso_moy")
        st.dataframe(ruptures[cols_show], use_container_width=True)
    else:
        st.success(_("stock_no_rupture"))

    # Bouton audit terrain
    run_ops = st.button(_("stock_btn_ia_terrain"), use_container_width=True, key="terrain_ia_ws")
    if run_ops:
        pg3 = StepProgress([_("step_read"), _("step_ia"), _("step_report")])
        pg3.step(_("step_read"))
        top_c = df.nsmallest(5, "quantite")
        top_s = ", ".join([f"{r['reference']} ({r['quantite']:.0f})" for _, r in top_c.iterrows()])
        pg3.step(_("step_ia"))
        _kpis_t = [float(len(df)), float(len(ruptures)), tx_serv]
        _labels_t = ["Articles", "Ruptures", "Service %"]
        _hist_t = get_historique_audits(st.session_state.current_user, "stock",
                                        current_kpis=_kpis_t, current_labels=_labels_t)
        _hist_txt_t = format_historique_pour_prompt(_hist_t, "terrain", lang)
        st.session_state.analysis_stock_terrain = generate_ai_analysis(
            f"Field stock: {len(df)} refs. Stockouts: {len(ruptures)}. Lowest: {top_s}.",
            historique_txt=_hist_txt_t, df_raw=df,
            sector_key=detect_sector(df=df, module="stock"))
        pg3.done()

    if st.session_state.get("analysis_stock_terrain"):
        st.markdown(render_report(st.session_state.analysis_stock_terrain, "terrain"), unsafe_allow_html=True)
        st.markdown(f"### {_('stock_full')}")
        cols_show = ["reference", "quantite", "Statut"]
        if has_conso and "_conso_moy" in df.columns:
            cols_show.append("_conso_moy")
        st.dataframe(df[cols_show], use_container_width=True, height=400)


def _render_scoring(df, lang):
    try:
        _score = compute_logiflo_score(
            module="stock", df=df,
            kpis=st.session_state.get("last_kpis", []),
            labels=st.session_state.get("last_labels", []),
            sector_key=detect_sector(df=df, module="stock"), lang=lang)
        _global = _score.get("global", 0)
        _details = _score.get("details", {})
        _clr = "#00C896" if _global >= 70 else ("#F39C12" if _global >= 40 else "#E8304A")
        st.markdown(
            f'<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;padding:18px 22px;margin:16px 0;">'
            f'<div style="font-size:13px;font-weight:700;color:#0B2545;margin-bottom:14px;text-transform:uppercase;letter-spacing:1px;">Scoring Logiflo</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:48px;font-weight:800;color:{_clr};line-height:1;">{_global}<span style="font-size:18px;color:#4A6080;">/100</span></div>'
            f'<div style="height:8px;background:#F0F4F8;border-radius:99px;overflow:hidden;margin:12px 0;">'
            f'<div style="height:100%;width:{_global}%;background:{_clr};border-radius:99px;"></div></div>',
            unsafe_allow_html=True)
        for lbl, val in _details.items():
            dc = "#00C896" if val >= 70 else ("#F39C12" if val >= 40 else "#E8304A")
            st.markdown(
                f'<div style="margin-bottom:8px;"><div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:4px;">'
                f'<span style="color:#4A6080;">{lbl}</span><span style="color:{dc};font-weight:700;">{val}/100</span></div>'
                f'<div style="height:5px;background:#F0F4F8;border-radius:99px;overflow:hidden;">'
                f'<div style="height:100%;width:{val}%;background:{dc};border-radius:99px;"></div></div></div>',
                unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    except Exception:
        pass
