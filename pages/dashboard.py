import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import datetime
import re

from config.plans import can_access, show_lock
from config.translations import _
from services.supabase_client import load_archives_from_sheets
from services.news import render_news_widget
from components.helpers import render_report


def render_dashboard():
    lang = st.session_state.get("language", "fr")
    username = st.session_state.current_user
    df_arch = load_archives_from_sheets(username)

    if df_arch is not None and not df_arch.empty:
        if "created_at" in df_arch.columns and "date" not in df_arch.columns:
            df_arch["date"] = pd.to_datetime(df_arch["created_at"], errors="coerce").dt.strftime("%d/%m/%Y")
            df_arch["heure"] = pd.to_datetime(df_arch["created_at"], errors="coerce").dt.strftime("%H:%M")
        for c in ["module", "date", "heure", "kpi_1", "kpi_2", "kpi_3",
                   "kpi_label_1", "kpi_label_2", "kpi_label_3", "resume_ia"]:
            if c not in df_arch.columns: df_arch[c] = ""
        for c in ["kpi_1", "kpi_2", "kpi_3"]:
            df_arch[c] = pd.to_numeric(df_arch[c], errors="coerce").fillna(0)

    # Message de bienvenue
    hour = datetime.datetime.now().hour
    greet = ("Good morning" if hour < 12 else "Good afternoon" if hour < 18 else "Good evening") if lang == "en" else ("Bonjour" if hour < 18 else "Bonsoir")
    tagline = "Your supply chain at a glance" if lang == "en" else "Votre supply chain en un coup d'oeil"
    st.markdown(
        f'<div style="background:linear-gradient(135deg,#0B2545,#0f2f5a);border-radius:14px;padding:22px 28px;margin-bottom:20px;border-left:4px solid #00C896;">'
        f'<div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;color:white;margin-bottom:3px;">{greet}, <span style="color:#00C896;">{username}</span></div>'
        f'<div style="font-size:12px;color:rgba(255,255,255,0.5);">{tagline}</div></div>',
        unsafe_allow_html=True)

    if df_arch is None or df_arch.empty:
        st.info("Aucun audit encore. Lancez votre premier audit." if lang == "fr" else "No audit yet. Launch your first audit.")
        return

    _mod = "stock"
    col_l, col_r = st.columns(2)

    # Camembert dernier audit
    with col_l:
        try:
            dfs = df_arch[df_arch["module"] == _mod].sort_values(
                "created_at" if "created_at" in df_arch.columns else "date", ascending=True)
            if not dfs.empty:
                lp = dfs.iloc[-1]
                k2 = float(lp.get("kpi_2", 0)); k3 = float(lp.get("kpi_3", 0))
                ok = max(0.1, 100 - k2 - min(k3 * 5, 30))
                fig_p = go.Figure(go.Pie(
                    labels=[str(lp.get("kpi_label_2", "")), str(lp.get("kpi_label_3", "")), "Sain"],
                    values=[k2, max(k3, 0.1), ok], hole=0.45,
                    marker=dict(colors=["#00C896", "#E8304A", "#E2E8F0"])))
                fig_p.update_layout(margin=dict(t=36, b=50, l=0, r=0), height=240, paper_bgcolor="white",
                                    title=dict(text=f"📦 Stock — {str(lp.get('date', ''))}", font=dict(size=12)))
                st.plotly_chart(fig_p, use_container_width=True, config={"displayModeBar": False})
        except Exception:
            pass

    # Courbe évolutive
    with col_r:
        try:
            dfc = df_arch[df_arch["module"] == _mod].sort_values(
                "created_at" if "created_at" in df_arch.columns else "date", ascending=True)
            if len(dfc) >= 2:
                dates = [str(d)[:10] for d in dfc["date"].tolist()]
                fig_c = go.Figure(go.Scatter(
                    x=list(range(len(dfc))), y=dfc["kpi_2"].tolist(),
                    mode="lines+markers", line=dict(color="#00C896", width=2.5),
                    marker=dict(size=9, color="#00C896"), fill="tozeroy",
                    fillcolor="rgba(0,200,150,0.08)"))
                fig_c.update_layout(
                    title=dict(text=f"📈 {dfc['kpi_label_2'].iloc[-1]} — {len(dfc)} audits", font=dict(size=12)),
                    xaxis=dict(tickmode="array", tickvals=list(range(len(dfc))), ticktext=[d[:5] for d in dates]),
                    margin=dict(t=36, b=20, l=30, r=10), height=240, paper_bgcolor="white", showlegend=False)
                st.plotly_chart(fig_c, use_container_width=True, config={"displayModeBar": False})
            else:
                st.info("2 audits minimum pour la tendance." if lang == "fr" else "2 audits minimum for trend.")
        except Exception:
            pass

    # Recap dernier audit
    try:
        last = df_arch.sort_values("created_at" if "created_at" in df_arch.columns else "date", ascending=False).iloc[0]
        k1 = float(last.get("kpi_1", 0)); k2 = float(last.get("kpi_2", 0)); k3 = float(last.get("kpi_3", 0))
        clr_k2 = "#00C896" if k2 >= 90 else ("#F39C12" if k2 >= 75 else "#E8304A")
        st.markdown(
            f'<div style="background:white;border:1px solid #E2E8F0;border-radius:14px;padding:18px 22px;margin:8px 0;">'
            f'<div style="font-size:11px;font-weight:700;color:#4A6080;letter-spacing:2px;text-transform:uppercase;margin-bottom:14px;">📦 Recap — {last.get("date", "")}</div>'
            f'<div style="display:flex;gap:12px;flex-wrap:wrap;">'
            f'<div style="flex:1;min-width:120px;background:#F0F4F8;border-radius:8px;padding:10px;text-align:center;"><div style="font-size:10px;color:#4A6080;">{last.get("kpi_label_1","")}</div><div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;color:#0B2545;">{k1:,.0f}</div></div>'
            f'<div style="flex:1;min-width:120px;background:#F0F4F8;border-radius:8px;padding:10px;text-align:center;"><div style="font-size:10px;color:#4A6080;">{last.get("kpi_label_2","")}</div><div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;color:{clr_k2};">{k2:.1f}%</div></div>'
            f'<div style="flex:1;min-width:120px;background:#F0F4F8;border-radius:8px;padding:10px;text-align:center;"><div style="font-size:10px;color:#4A6080;">{last.get("kpi_label_3","")}</div><div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;color:#0B2545;">{k3:,.0f}</div></div>'
            f'</div></div>',
            unsafe_allow_html=True)
    except Exception:
        pass

    # News
    if can_access("news"):
        try:
            render_news_widget("stock_distribution", lang=lang)
        except Exception:
            pass
    else:
        show_lock("news")
