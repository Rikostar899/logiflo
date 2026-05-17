import streamlit as st
from config.plans import PLAN_LIMITS, get_user_plan, audit_counter_sidebar
from config.translations import _


def render_sidebar():
    """Affiche la sidebar et retourne l'item de navigation selectionne."""
    with st.sidebar:
        username = st.session_state.get("current_user", "")
        plan = get_user_plan(username)
        pinfo = PLAN_LIMITS.get(plan, PLAN_LIMITS.get("gratuit", {}))
        audit_counter_sidebar(username, plan)

        st.markdown(
            f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">'
            f'<div class="sidebar-logo">LOGI<span>FLO</span>.IO</div></div>'
            f'<div style="font-size:12px;color:#4A6080;margin-bottom:6px;">\U0001f464 {username}</div>'
            f'<div style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;'
            f'border-radius:20px;margin-bottom:10px;background:{pinfo.get("bg","#F3F4F6")};'
            f'color:{pinfo.get("color","#6B7280")};border:1px solid {pinfo.get("color","#6B7280")}40;'
            f'font-size:10px;font-weight:700;">{pinfo.get("icon","")}{pinfo.get("label","")}</div>',
            unsafe_allow_html=True,
        )
        st.markdown("---")

        view = st.session_state.get("stock_view", "MANAGER")

        if view == "MANAGER":
            nav_items = [_("nav_dashboard"), _("nav_workspace"), _("nav_archives"),
                         _("nav_compte"), _("nav_legal")]
        else:
            nav_items = [_("nav_workspace"), _("nav_archives"),
                         _("nav_compte"), _("nav_legal")]

        nav = st.radio("", nav_items, label_visibility="collapsed")
        st.markdown("---")

        if st.button(_("change_profile"), use_container_width=True, key="sb_change_profile"):
            st.session_state.page = "profil"
            st.rerun()

        if st.button(_("nav_logout"), use_container_width=True, key="sb_logout"):
            st.session_state.clear()
            st.rerun()

        st.markdown(
            '<div style="margin-top:40px;border-top:1px solid #1e3a5f;padding-top:14px;'
            'font-size:11px;color:#4A6080;">\u00a9 2026 Logiflo</div>',
            unsafe_allow_html=True,
        )

    return nav
